"""Reusable, variable-centric experiment tracking."""
from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Mapping
from datetime import datetime, timezone
from hashlib import sha256
import functools
import json
import logging
import threading
import warnings
from pathlib import Path
from typing import Any
from uuid import uuid4
from dateutil import parser as dtparser
import polars as pl
from acquirium.internals.models import compute_ref_uri
from acquirium.ExperimentMetadata import (
    EXPERIMENT, ExperimentMetadataGraph, annotation_uri, experiment_query,
    matched_ids, metadata_values, node_uri, records_frame, run_uri, saved_metadata, study_uri, sync_study,
    variable_uri,
)

UTC = timezone.utc

# Server receipt time is deliberately distinct from a caller's occurred_at.
# The former answers when Acquirium learned something; the latter answers when
# the outside-world event happened.
def _now() -> datetime: return datetime.now(UTC)
def timestamp(value: datetime | str) -> datetime:
    """Parse an ISO timestamp, treating a timezone-less value as UTC."""
    parsed = dtparser.isoparse(value) if isinstance(value, str) else value
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
def _database_timestamp(value: datetime | str) -> datetime:
    """Store UTC wall time consistently in SQL TIMESTAMP columns."""
    return timestamp(value).replace(tzinfo=None)
def _json(value: Any) -> str:
    try: return json.dumps(value, sort_keys=True, default=str)
    except TypeError as error: raise ValueError("experiment values must be JSON serializable") from error
def _metadata_matches(actual: dict[str, Any], expected: dict[str, Any] | None) -> bool:
    return expected is None or all(actual.get(key) == value for key, value in expected.items())

def _publishes(method):
    """Hold the graph lock across a write's SQL commit and queuing its node."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._graph_lock:
            return method(self, *args, **kwargs)
    return wrapper

class ExperimentStore:
    """SQL persistence behind the manager's intentionally small HTTP API."""
    def __init__(self, store: Any, artifact_dir: Path, *, graph_store=None, resolve=None) -> None:
        self.store, self.artifact_dir = store, artifact_dir
        self._live_resolve = resolve
        self.metadata_graph = ExperimentMetadataGraph(graph_store, self._resolve) if graph_store is not None else None
        self._graph_dirty = self.metadata_graph is not None
        # Held across each SQL write and queuing its node, and by reads while
        # they publish the queue, so a reader never sees a committed record
        # whose graph node is not yet published. It is separate from the
        # store's writer lock so graph work does not stall ingestion.
        self._graph_lock = threading.RLock()
        # Nodes written since the last publication, by (class, id) -> study.
        # Every graph update rebuilds inference over the whole plant model, so
        # writes only queue their node and the next read publishes the queue
        # in one update: a sweep of many runs costs one rebuild, not one per
        # start and finish.
        self._pending: dict[tuple[Any, str], str] = {}
        # Records a rebuild could not convert to graph nodes, by (class, id).
        # Reads of these records raise their error; other reads proceed.
        self._unpublished: dict[tuple[Any, str], dict[str, Any]] = {}
        artifact_dir.mkdir(parents=True, exist_ok=True)
        # Keep the experiment ledger beside canonical time-series storage.  The
        # ledger stores small provenance records; time-series samples continue
        # to live in the normal Acquirium tables.
        with store._lock, store._write_conn() as conn:
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_templates (template_id VARCHAR PRIMARY KEY, name VARCHAR UNIQUE NOT NULL, created_at TIMESTAMP NOT NULL)")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_variables (variable_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, label VARCHAR NOT NULL, role VARCHAR NOT NULL, kind VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, UNIQUE(template_id, label))")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_runs (run_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, status VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, error_json VARCHAR)")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_observations (run_id VARCHAR NOT NULL, variable_id VARCHAR, sequence BIGINT NOT NULL, recorded_at TIMESTAMP NOT NULL, occurred_at TIMESTAMP, value_json VARCHAR, artifact_digest VARCHAR, ref_uri VARCHAR, range_start TIMESTAMP, range_end TIMESTAMP, PRIMARY KEY(run_id, sequence))")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_artifacts (digest VARCHAR PRIMARY KEY, filename VARCHAR NOT NULL, media_type VARCHAR, byte_length BIGINT NOT NULL, created_at TIMESTAMP NOT NULL)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_variables_template_idx ON experiment_variables(template_id)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_runs_template_idx ON experiment_runs(template_id, status, started_at)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_observations_variable_idx ON experiment_observations(variable_id, run_id, sequence)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_observations_ref_idx ON experiment_observations(ref_uri, range_start, range_end)")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_metadata_resolutions (text VARCHAR NOT NULL, kind VARCHAR NOT NULL, uri VARCHAR NOT NULL, PRIMARY KEY(text, kind))")
        with store._own_conn() as conn:
            rows = self._execute(conn, "SELECT text, kind, uri FROM experiment_metadata_resolutions").fetchall()
        self._resolutions = {(row[0], row[1]): row[2] for row in rows}

    def _resolve(self, text, kind):
        """Resolve metadata text once and reuse that answer from then on.

        Rebuilding the graph from SQL must not depend on the resolver giving
        the same answer later: its indexes may still be loading after a
        restart, or a vocabulary change may move a match. Pinning also keeps
        one spelling on one URI across every run of a sweep.
        """
        key = (text, kind)
        if key in self._resolutions:
            return self._resolutions[key]
        uri = self._live_resolve(text, kind) if self._live_resolve is not None else None
        if uri is None:
            return None
        with self.store._lock, self.store._write_conn() as conn:
            self._execute(conn, "INSERT INTO experiment_metadata_resolutions VALUES (?, ?, ?) ON CONFLICT DO NOTHING", [text, kind, str(uri)])
            # A concurrent first resolution may have won; keep its answer.
            uri = self._execute(conn, "SELECT uri FROM experiment_metadata_resolutions WHERE text=? AND kind=?", [text, kind]).fetchone()[0]
        self._resolutions[key] = uri
        return uri

    def _prepare_metadata(self, uri, metadata, *, variable=False):
        metadata = metadata_values(metadata)
        if self.metadata_graph is not None:
            self.metadata_graph.validate(uri, metadata, variable=variable)
        return metadata

    @staticmethod
    def _study_record(item):
        return (study_uri(item["template_id"]), EXPERIMENT.Study,
                {"study_id": item["template_id"], "label": item["name"], "created_at": item["created_at"]}, {})

    @staticmethod
    def _variable_record(template_id, item):
        fields = {key: item[key] for key in ("variable_id", "label", "role", "kind", "created_at")}
        fields["study_id"] = template_id
        return variable_uri(item["variable_id"]), EXPERIMENT.Variable, fields, item["metadata"]

    @staticmethod
    def _run_record(item):
        fields = {key: item.get(key) for key in ("run_id", "status", "started_at", "finished_at")}
        fields["study_id"] = item["template_id"]
        return run_uri(item["run_id"]), EXPERIMENT.Run, fields, item["metadata"]

    @staticmethod
    def _annotation_record(item):
        fields = {key: item.get(key) for key in ("annotation_id", "run_id", "ref_uri", "range_start", "range_end", "created_at")}
        fields["study_id"] = item["template_id"]
        return annotation_uri(item["annotation_id"]), EXPERIMENT.StreamAnnotation, fields, item["metadata"]

    _ID_FIELDS = {
        EXPERIMENT.Study: "study_id", EXPERIMENT.Run: "run_id",
        EXPERIMENT.Variable: "variable_id", EXPERIMENT.StreamAnnotation: "annotation_id",
    }

    @classmethod
    def _record_key(cls, record):
        _, kind, fields, _ = record
        return kind, fields[cls._ID_FIELDS[kind]]

    def _current_record(self, kind, identifier, study_id):
        """The node's record as SQL holds it now, or None if it is gone."""
        try:
            if kind == EXPERIMENT.Study:
                return self._study_record(self.study(identifier))
            if kind == EXPERIMENT.Run:
                return self._run_record(self.run(identifier))
            if kind == EXPERIMENT.Variable:
                item = next((item for item in self.variables(study_id) if item["variable_id"] == identifier), None)
                return self._variable_record(study_id, item) if item else None
        except KeyError:
            return None
        run_id = identifier.rpartition(":")[0]
        item = next((item for item in self.stream_annotations(run_id=run_id) if item["annotation_id"] == identifier), None)
        return self._annotation_record(item) if item else None

    def _publish_metadata(self, record):
        """Queue a written node; the next read publishes its current state."""
        if self.metadata_graph is None:
            return
        with self._graph_lock:
            self._pending[self._record_key(record)] = record[2]["study_id"]

    @staticmethod
    def _failure(key, study_id, error):
        kind, identifier = key
        return {"kind": str(kind).rpartition(":")[2], "id": identifier, "study_id": study_id, "error": str(error)}

    def _rebuild_metadata_graph(self):
        """Rebuild while the caller holds the graph lock.

        A record that cannot become a graph node (a key now reserved, a unit
        that no longer resolves) is left out and remembered with its error.
        Reads of that record raise; reads of every other record still work.
        """
        # The rebuild reads every record from SQL, queued ones included.
        self._pending = {}
        records = []
        for study in self.studies():
            template_id = study["template_id"]
            records.append(self._study_record(study))
            for variable in self.variables(template_id):
                records.append(self._variable_record(template_id, variable))
            for run in self.runs(template_id):
                records.append(self._run_record(run))
        for annotation in self.stream_annotations():
            records.append(self._annotation_record(annotation))
        failures = self.metadata_graph.replace(records)
        self._unpublished = {}
        for record in records:
            if record[0] in failures:
                key = self._record_key(record)
                self._unpublished[key] = self._failure(key, record[2]["study_id"], failures[record[0]])
        for failure in self._unpublished.values():
            logging.getLogger(__name__).warning(
                "Experiment %s %s is not in the metadata graph: %s", failure["kind"], failure["id"], failure["error"])
        self._graph_dirty = False

    def _publish_pending(self, *, retry=True):
        """Publish queued nodes, and retry left-out ones, in one graph update.

        Each node is published as SQL holds it now, so a run started and
        finished since the last read is written once, in its final state.
        Call with the graph lock held.
        """
        try:
            self._publish_queue(retry)
        except Exception:
            # SQL holds every queued record. Rebuild from it on the next read.
            self._graph_dirty = True
            raise

    def _publish_queue(self, retry):
        written, self._pending = self._pending, {}
        retried = {key: failure["study_id"] for key, failure in self._unpublished.items()
                   if retry and key not in written}
        removed, records = [], []
        for key, study_id in {**retried, **written}.items():
            record = self._current_record(*key, study_id)
            if record is None:
                removed.append(key)
            else:
                records.append((key, study_id, record))
        # Replace every written node, but touch a left-out node only once it
        # converts; otherwise each read would rewrite and re-infer the graph.
        deleted = [node_uri(*key) for key in written]
        triples = []
        for key, study_id, record in records:
            try:
                converted = self.metadata_graph.triples(record)
            except Exception as error:
                self._unpublished[key] = self._failure(key, study_id, error)
                continue
            if key in retried:
                deleted.append(record[0])
            triples.extend(converted)
            self._unpublished.pop(key, None)
        for key in removed:
            self._unpublished.pop(key, None)
        self.metadata_graph.update(deleted, triples)

    def sync_metadata_graph(self):
        """Publish queued writes, then backfill or repair from SQL as needed.

        Every experiment graph read calls this first. Holding the graph lock
        also waits out any write whose SQL row is committed but whose node is
        not yet queued.
        """
        if self.metadata_graph is None:
            raise RuntimeError("experiment metadata queries require a graph backend")
        with self._graph_lock:
            if self._graph_dirty:
                self._rebuild_metadata_graph()
            elif self._pending or self._unpublished:
                self._publish_pending()
            return {"ok": True, "unpublished": list(self._unpublished.values())}

    def publish_queued_metadata(self):
        """Publish queued writes before a general graph query reads the graph.

        Attribute discovery and plant queries can see experiment nodes, so
        they must not miss a write that no experiment read has published yet.
        A failure is logged, not raised: the experiment graph is repaired on
        the next experiment read and must never fail an unrelated query.
        """
        if self.metadata_graph is None or not self._pending:
            return
        with self._graph_lock:
            try:
                if self._graph_dirty:
                    self._rebuild_metadata_graph()
                elif self._pending:
                    self._publish_pending(retry=False)
            except Exception:
                logging.getLogger(__name__).exception("Experiment graph update deferred; SQL record retained")

    def _read_graph(self, kind, id_field, identifiers):
        metadata = self.metadata_graph.read(kind, id_field, identifiers)
        for identifier in identifiers:
            if identifier in metadata:
                continue
            failure = self._unpublished.get((kind, identifier))
            if failure is not None:
                raise ValueError(f"experiment {failure['kind'].lower()} {identifier!r} has metadata "
                                 f"the graph cannot hold: {failure['error']}")
            raise RuntimeError(f"experiment metadata graph has no node for {identifier!r}")
        return metadata

    def read_metadata(self, items, cls, id_field):
        """Hydrate HTTP records from shared graph objects in one batch per kind.

        Storage-only callers can still inspect SQL snapshots. A configured
        graph backend must be repaired before reads; failures are not hidden by
        returning an older SQL metadata snapshot.
        """
        if not items or self.metadata_graph is None:
            return items
        self.sync_metadata_graph()
        metadata = self._read_graph(cls, id_field, [item[id_field] for item in items])
        return [
            {**item, "metadata": saved_metadata(metadata[item[id_field]], item["metadata"])}
            for item in items
        ]

    def read_observation_metadata(self, items):
        if not items or self.metadata_graph is None:
            return items
        self.sync_metadata_graph()
        runs = self._read_graph(EXPERIMENT.Run, "run_id", [item["experiment_id"] for item in items])
        variables = self._read_graph(EXPERIMENT.Variable, "variable_id", [item["variable_id"] for item in items])
        return [
            {**item, "experiment_metadata": saved_metadata(runs[item["experiment_id"]], item["experiment_metadata"]),
             "variable_metadata": saved_metadata(variables[item["variable_id"]], item["variable_metadata"])}
            for item in items
        ]

    @staticmethod
    def _execute(conn: Any, statement: str, parameters: list[Any] | None = None):
        """Keep the ledger SQL portable across DuckDB and PostgreSQL."""
        if conn.__class__.__module__.startswith("psycopg"):
            statement = statement.replace("?", "%s")
        return conn.execute(statement, parameters or [])

    def _active(self, conn: Any, run_id: str) -> str:
        # All observations are append-only while running.  A terminal run is a
        # durable scientific record, not a mutable dashboard row.
        row = self._execute(conn, "SELECT status, template_id FROM experiment_runs WHERE run_id=?", [run_id]).fetchone()
        if row is None: raise KeyError(run_id)
        if row[0] != "running": raise ValueError("experiment run is terminal")
        return row[1]

    def _validate_observation(self, conn: Any, run_id: str, variable_id: str) -> None:
        template_id = self._active(conn, run_id)
        row = self._execute(conn, "SELECT template_id FROM experiment_variables WHERE variable_id=?", [variable_id]).fetchone()
        if row is None:
            raise KeyError(variable_id)
        if row[0] != template_id:
            raise ValueError("variable belongs to a different study")

    def _next_observation(self, conn: Any, run_id: str) -> tuple[int, datetime]:
        # Values and stream annotations share one run-local recording order.
        sequence = int(self._execute(conn, "SELECT coalesce(max(sequence),0)+1 FROM experiment_observations WHERE run_id=?", [run_id]).fetchone()[0])
        return sequence, _now()
    @_publishes
    def define(self, name: str) -> dict[str, Any]:
        if not name.strip(): raise ValueError("experiment name is required")
        with self.store._lock, self.store._write_conn() as conn:
            # Defining a study repeatedly is normal for scripts imported more
            # than once. The name therefore identifies a reusable declaration.
            row = self._execute(conn, "SELECT template_id, created_at FROM experiment_templates WHERE name=?", [name]).fetchone()
            if row: return {"template_id": row[0], "name": name, "created_at": row[1]}
            item = {"template_id": str(uuid4()), "name": name, "created_at": _now()}
            self._execute(conn, "INSERT INTO experiment_templates VALUES (?, ?, ?)", [item["template_id"], item["name"], _database_timestamp(item["created_at"])])
        self._publish_metadata(self._study_record(item))
        return item
    @_publishes
    def declare(self, template_id: str, label: str, role: str, kind: str, metadata: dict[str, Any]) -> dict[str, Any]:
        if not label.strip() or role not in {"input", "output", "annotation"}: raise ValueError("a non-empty label and valid role are required")
        metadata = metadata_values(metadata)
        # The graph lock serialises declarations, so the label check and the
        # insert need not share a transaction. Resolution happens between them,
        # outside the store's writer lock that ingestion also waits on.
        with self.store._own_conn() as conn:
            row = self._execute(conn, "SELECT variable_id, role, kind, metadata_json, created_at FROM experiment_variables WHERE template_id=? AND label=?", [template_id, label]).fetchone()
        if row:
            # A label cannot silently change shape: an old experiment must
            # remain interpretable after the script evolves.
            previous = _json(metadata_values(json.loads(row[3])))
            if (row[1], row[2], previous) != (role, kind, _json(metadata)): raise ValueError(f"variable {label!r} was already declared differently")
            return {"variable_id": row[0], "label": label, "role": role, "kind": kind, "metadata": metadata, "created_at": timestamp(row[4]), "created": False}
        # Study variables use the same source/name identity as streams.
        variable_id, created_at = str(compute_ref_uri(f"study/{template_id}", label)), _now()
        metadata = self._prepare_metadata(variable_id, metadata, variable=True)
        with self.store._lock, self.store._write_conn() as conn:
            self._execute(conn, "INSERT INTO experiment_variables VALUES (?, ?, ?, ?, ?, ?, ?)", [variable_id, template_id, label, role, kind, _json(metadata), _database_timestamp(created_at)])
        item = {"variable_id": variable_id, "label": label, "role": role, "kind": kind, "metadata": metadata, "created_at": created_at, "created": True}
        self._publish_metadata(self._variable_record(template_id, item))
        return item
    @_publishes
    def start(self, template_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        # Metadata belongs to this execution, while declarations belong to the
        # reusable study. This is what lets a parameter sweep share variables.
        run = {"run_id": str(uuid4()), "template_id": template_id, "status": "running", "metadata": metadata, "started_at": _now()}
        run["metadata"] = self._prepare_metadata(run_uri(run["run_id"]), metadata)
        with self.store._lock, self.store._write_conn() as conn:
            if self._execute(conn, "SELECT 1 FROM experiment_templates WHERE template_id=?", [template_id]).fetchone() is None:
                raise KeyError(template_id)
            self._execute(conn, "INSERT INTO experiment_runs (run_id,template_id,status,metadata_json,started_at) VALUES (?,?,?,?,?)", [run["run_id"], template_id, "running", _json(run["metadata"]), _database_timestamp(run["started_at"])])
        self._publish_metadata(self._run_record(run))
        return run
    def observe(self, run_id: str, variable_id: str, *, value: Any = None, occurred_at: datetime | None = None, artifact_digest: str | None = None, ref_uri: str | None = None, interval: tuple[datetime,datetime] | None = None) -> dict[str, Any]:
        with self.store._lock, self.store._write_conn() as conn:
            self._validate_observation(conn, run_id, variable_id)
            # Wall clocks can collide or arrive out of order. Sequence is the
            # authoritative order of observations within one experiment.
            seq, recorded = self._next_observation(conn, run_id)
            self._execute(conn, "INSERT INTO experiment_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, variable_id, seq, _database_timestamp(recorded), _database_timestamp(occurred_at) if occurred_at else None, _json(value) if value is not None else None, artifact_digest, ref_uri, _database_timestamp(interval[0]) if interval else None, _database_timestamp(interval[1]) if interval else None])
        return {"sequence": seq, "recorded_at": recorded}
    @_publishes
    def finish(self, run_id: str, status: str, error: Any = None) -> dict[str, Any]:
        if status not in {"succeeded", "failed"}: raise ValueError("status must be succeeded or failed")
        with self.store._lock, self.store._write_conn() as conn:
            self._active(conn, run_id)
            finished = _now(); self._execute(conn, "UPDATE experiment_runs SET status=?, finished_at=?, error_json=? WHERE run_id=?", [status, _database_timestamp(finished), _json(error) if error else None, run_id])
        self._publish_metadata(self._run_record(self.run(run_id)))
        return {"run_id": run_id, "status": status, "finished_at": finished}
    def attach_file(self, run_id: str, variable_id: str, filename: str, media_type: str | None, data: bytes) -> dict[str, Any]:
        digest = sha256(data).hexdigest(); target = self.artifact_dir / digest
        with self.store._lock, self.store._write_conn() as conn:
            self._validate_observation(conn, run_id, variable_id)
            # Validate ownership before storing any artifact bytes or metadata.
            # Identical bytes share one content-addressed artifact.
            if not target.exists(): target.write_bytes(data)
            self._execute(conn, "INSERT INTO experiment_artifacts VALUES (?, ?, ?, ?, ?) ON CONFLICT(digest) DO NOTHING", [digest, filename, media_type, len(data), _database_timestamp(_now())])
        self.observe(run_id, variable_id, artifact_digest=digest)
        return {"digest": digest, "filename": filename, "byte_length": len(data)}

    @_publishes
    def annotate_stream(self, run_id: str, ref_uri: str, start: datetime | str, end: datetime | str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        """Associate a live stream interval with a run without copying samples."""
        if not ref_uri or not ref_uri.strip():
            raise ValueError("a stream reference URI is required")
        start_at = timestamp(start)
        end_at = timestamp(end) if end is not None else None
        if end_at is not None and end_at < start_at:
            raise ValueError("annotation end must be at or after start")
        metadata = {} if metadata is None else metadata
        if not isinstance(metadata, dict):
            raise ValueError("annotation metadata must be a JSON object")
        metadata = self._prepare_metadata(annotation_uri(run_id), metadata)
        metadata_json = json.dumps(metadata, sort_keys=True, allow_nan=False)
        with self.store._lock, self.store._write_conn() as conn:
            # An annotation can document a trial after its run has ended. It
            # is dated separately and never changes previously recorded values.
            row = self._execute(conn, "SELECT template_id FROM experiment_runs WHERE run_id=?", [run_id]).fetchone()
            if row is None:
                raise KeyError(run_id)
            sequence, recorded = self._next_observation(conn, run_id)
            self._execute(conn, """
                INSERT INTO experiment_observations
                (run_id, variable_id, sequence, recorded_at, value_json, ref_uri, range_start, range_end)
                VALUES (?, NULL, ?, ?, ?, ?, ?, ?)
                """, [run_id, sequence, _database_timestamp(recorded), metadata_json, ref_uri,
                      _database_timestamp(start_at),
                      _database_timestamp(end_at) if end_at is not None else None])
        item = {"annotation_id": f"{run_id}:{sequence}", "run_id": run_id,
                "template_id": row[0], "ref_uri": ref_uri, "range_start": start_at,
                "range_end": end_at, "metadata": metadata, "created_at": recorded}
        self._publish_metadata(self._annotation_record(item))
        return item

    @_publishes
    def close_stream_annotation(self, annotation_id: str, end: datetime | str) -> dict[str, Any]:
        """Set the end of an open interval, including after a run finishes."""
        end_at = timestamp(end)
        run_id, separator, sequence_text = annotation_id.rpartition(":")
        if not separator or not sequence_text.isdecimal():
            raise KeyError(annotation_id)
        with self.store._lock, self.store._write_conn() as conn:
            row = self._execute(conn, """
                SELECT range_start, range_end FROM experiment_observations
                WHERE run_id=? AND sequence=? AND variable_id IS NULL AND ref_uri IS NOT NULL
                """, [run_id, int(sequence_text)]).fetchone()
            if row is None:
                raise KeyError(annotation_id)
            if row[1] is not None:
                raise ValueError("stream annotation is already closed")
            if end_at < timestamp(row[0]):
                raise ValueError("annotation end must be at or after start")
            self._execute(conn, "UPDATE experiment_observations SET range_end=? WHERE run_id=? AND sequence=?", [_database_timestamp(end_at), run_id, int(sequence_text)])
        for item in self.stream_annotations(run_id=run_id):
            if item["annotation_id"] == annotation_id:
                self._publish_metadata(self._annotation_record(item))
                break
        return {"annotation_id": annotation_id, "range_end": end_at}

    def stream_annotations(self, *, run_id: str | None = None, template_id: str | None = None,
                           ref_uri: str | None = None, start: datetime | str | None = None,
                           end: datetime | str | None = None) -> list[dict[str, Any]]:
        """Find annotations by experiment, Study, stream, or overlapping time."""
        if start is not None and end is not None and timestamp(end) < timestamp(start):
            raise ValueError("query end must be at or after start")
        clauses, parameters = ["o.variable_id IS NULL", "o.ref_uri IS NOT NULL"], []
        for clause, value in (("o.run_id=?", run_id), ("r.template_id=?", template_id), ("o.ref_uri=?", ref_uri)):
            if value is not None:
                clauses.append(clause); parameters.append(value)
        if start is not None:
            clauses.append("(o.range_end IS NULL OR o.range_end>=?)")
            parameters.append(_database_timestamp(start))
        if end is not None:
            clauses.append("o.range_start<=?")
            parameters.append(_database_timestamp(end))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.store._own_conn() as conn:
            rows = self._execute(conn, f"""
                SELECT o.run_id, o.sequence, r.template_id, o.ref_uri,
                       o.range_start, o.range_end, o.value_json, o.recorded_at
                FROM experiment_observations AS o
                JOIN experiment_runs AS r ON r.run_id=o.run_id
                {where}
                ORDER BY o.range_start, o.run_id, o.sequence
                """, parameters).fetchall()
        return [{"annotation_id": f"{row[0]}:{row[1]}", "run_id": row[0], "template_id": row[2], "ref_uri": row[3],
                 "range_start": timestamp(row[4]), "range_end": timestamp(row[5]) if row[5] is not None else None,
                 "metadata": json.loads(row[6]), "created_at": timestamp(row[7])} for row in rows]

    def studies(self, name: str | None = None) -> list[dict[str, Any]]:
        statement = "SELECT template_id, name, created_at FROM experiment_templates"
        parameters: list[Any] = []
        if name is not None:
            statement += " WHERE name=?"
            parameters.append(name)
        statement += " ORDER BY created_at, name"
        with self.store._own_conn() as conn:
            rows = self._execute(conn, statement, parameters).fetchall()
        return [
            {"template_id": row[0], "name": row[1], "created_at": timestamp(row[2])}
            for row in rows
        ]

    def study(self, identifier: str) -> dict[str, Any]:
        with self.store._own_conn() as conn:
            row = self._execute(
                conn,
                """
                SELECT template_id, name, created_at
                FROM experiment_templates
                WHERE template_id=? OR name=?
                """,
                [identifier, identifier],
            ).fetchone()
        if row is None:
            raise KeyError(identifier)
        return {"template_id": row[0], "name": row[1], "created_at": timestamp(row[2])}

    def variables(self, template_id: str) -> list[dict[str, Any]]:
        with self.store._own_conn() as conn:
            exists = self._execute(
                conn,
                "SELECT 1 FROM experiment_templates WHERE template_id=?",
                [template_id],
            ).fetchone()
            if exists is None:
                raise KeyError(template_id)
            rows = self._execute(
                conn,
                """
                SELECT variable_id, label, role, kind, metadata_json, created_at
                FROM experiment_variables
                WHERE template_id=?
                ORDER BY created_at, label
                """,
                [template_id],
            ).fetchall()
        return [
            {
                "variable_id": row[0],
                "label": row[1],
                "role": row[2],
                "kind": row[3],
                "metadata": json.loads(row[4]),
                "created_at": timestamp(row[5]),
            }
            for row in rows
        ]

    def runs(
        self,
        template_id: str,
        *,
        status: str | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        run_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if run_ids == []:
            return []
        clauses = ["template_id=?"]
        parameters: list[Any] = [template_id]
        if run_ids is not None:
            clauses.append(f"run_id IN ({','.join('?' for _ in run_ids)})")
            parameters.extend(run_ids)
        for condition, value in (
            ("status=?", status),
            ("started_at>=?", started_after),
            ("started_at<?", started_before),
        ):
            if value is not None:
                clauses.append(condition)
                parameters.append(_database_timestamp(value) if "started_at" in condition else value)
        with self.store._own_conn() as conn:
            rows = self._execute(
                conn,
                f"""
                SELECT run_id, status, metadata_json, started_at, finished_at, error_json
                FROM experiment_runs
                WHERE {' AND '.join(clauses)}
                ORDER BY started_at, run_id
                """,
                parameters,
            ).fetchall()
        result = [self._run_row(template_id, row) for row in rows]
        return [item for item in result if _metadata_matches(item["metadata"], metadata)]

    def run(self, run_id: str) -> dict[str, Any]:
        with self.store._own_conn() as conn:
            row = self._execute(
                conn,
                """
                SELECT template_id, status, metadata_json, started_at, finished_at, error_json
                FROM experiment_runs WHERE run_id=?
                """,
                [run_id],
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return self._run_row(row[0], (run_id, *row[1:]))

    @staticmethod
    def _run_row(template_id: str, row: Any) -> dict[str, Any]:
        return {
            "run_id": row[0],
            "template_id": template_id,
            "status": row[1],
            "metadata": json.loads(row[2]),
            "started_at": timestamp(row[3]),
            "finished_at": timestamp(row[4]) if row[4] else None,
            "error": json.loads(row[5]) if row[5] else None,
        }

    def observations(
        self,
        template_id: str,
        *,
        run_id: str | None = None,
        variable_id: str | None = None,
        status: str | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        run_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if run_ids == []:
            return []
        clauses = ["r.template_id=?"]
        parameters: list[Any] = [template_id]
        if run_ids is not None:
            clauses.append(f"r.run_id IN ({','.join('?' for _ in run_ids)})")
            parameters.extend(run_ids)
        for condition, value in (
            ("r.run_id=?", run_id),
            ("o.variable_id=?", variable_id),
            ("r.status=?", status),
            ("r.started_at>=?", started_after),
            ("r.started_at<?", started_before),
        ):
            if value is not None:
                clauses.append(condition)
                parameters.append(_database_timestamp(value) if "started_at" in condition else value)
        with self.store._own_conn() as conn:
            rows = self._execute(
                conn,
                f"""
                SELECT r.run_id, r.status, r.metadata_json, r.started_at,
                       r.finished_at, o.sequence, o.recorded_at, o.occurred_at,
                       o.value_json, o.artifact_digest, o.ref_uri,
                       o.range_start, o.range_end, v.variable_id, v.label,
                       v.role, v.kind, v.metadata_json, a.filename,
                       a.media_type, a.byte_length
                FROM experiment_observations AS o
                JOIN experiment_runs AS r ON r.run_id=o.run_id
                JOIN experiment_variables AS v ON v.variable_id=o.variable_id
                LEFT JOIN experiment_artifacts AS a ON a.digest=o.artifact_digest
                WHERE {' AND '.join(clauses)}
                ORDER BY r.started_at, r.run_id, o.sequence
                """,
                parameters,
            ).fetchall()
        result = [
            {
                "experiment_id": row[0],
                "status": row[1],
                "experiment_metadata": json.loads(row[2]),
                "started_at": timestamp(row[3]),
                "finished_at": timestamp(row[4]) if row[4] else None,
                "sequence": row[5],
                "recorded_at": timestamp(row[6]),
                "occurred_at": timestamp(row[7]) if row[7] else None,
                "value": json.loads(row[8]) if row[8] is not None else None,
                "artifact_digest": row[9],
                "ref_uri": row[10],
                "range_start": timestamp(row[11]) if row[11] else None,
                "range_end": timestamp(row[12]) if row[12] else None,
                "variable_id": row[13],
                "variable": row[14],
                "role": row[15],
                "kind": row[16],
                "variable_metadata": json.loads(row[17]),
                "filename": row[18],
                "media_type": row[19],
                "byte_length": row[20],
            }
            for row in rows
        ]
        return [item for item in result if _metadata_matches(item["experiment_metadata"], metadata)]

@dataclass(frozen=True)
class Point:
    """A deliberately small semantic handle; storage only needs its URI."""
    uri: str

@dataclass(frozen=True)
class RecordedSeries:
    """A time-series output recorded by one Experiment."""
    ref_uri: str
    _client: Any = field(repr=False, compare=False)

    def dataframe(self, **kwargs: Any):
        """Fetch the recorded samples as a Polars DataFrame."""
        return self._client.timeseries_df(self.ref_uri, **kwargs)

    def __str__(self) -> str:
        return self.ref_uri

@dataclass
class StreamAnnotation:
    """A run's claim on a time interval of an existing stream."""
    annotation_id: str
    run_id: str
    template_id: str
    ref_uri: str
    range_start: datetime
    range_end: datetime | None
    metadata: dict[str, Any]
    created_at: datetime
    _client: Any = field(repr=False, compare=False)

    @classmethod
    def from_item(cls, item: dict[str, Any], client: Any) -> "StreamAnnotation":
        return cls(item["annotation_id"], item["run_id"], item["template_id"], item["ref_uri"],
                   timestamp(item["range_start"]),
                   timestamp(item["range_end"]) if item.get("range_end") else None,
                   dict(item["metadata"]), timestamp(item["created_at"]), client)

    def close(self, end: datetime | str) -> None:
        result = self._client.close_experiment_stream_annotation(self.annotation_id, end=timestamp(end).isoformat())
        self.range_end = timestamp(result["range_end"])

    def dataframe(self, **kwargs: Any):
        """Read the current stream values within the claimed interval."""
        options = dict(kwargs)
        options.setdefault("start", self.range_start.isoformat())
        if self.range_end is not None:
            options.setdefault("end", self.range_end.isoformat())
        return self._client.timeseries_df(self.ref_uri, **options)

@dataclass(frozen=True)
class Observation:
    """One persisted value recorded for a variable in an Experiment."""
    experiment_id: str
    variable: "ExperimentVariable"
    sequence: int
    recorded_at: datetime
    occurred_at: datetime | None
    value: Any
    status: str
    experiment_metadata: dict[str, Any]
    started_at: datetime
    finished_at: datetime | None
    ref_uri: str | None = None
    range_start: datetime | None = None
    range_end: datetime | None = None
    artifact_digest: str | None = None
    filename: str | None = None
    media_type: str | None = None
    byte_length: int | None = None
    _client: Any = field(default=None, repr=False, compare=False)

    def dataframe(self, **kwargs: Any):
        """Fetch samples for a time-series observation."""
        if self.variable.kind != "timeseries" or self.ref_uri is None:
            raise TypeError("dataframe() is only available for time-series observations")
        options = dict(kwargs)
        if self.range_start is not None:
            options.setdefault("start", self.range_start.isoformat())
        if self.range_end is not None:
            options.setdefault("end", self.range_end.isoformat())
        return self._client.timeseries_df(self.ref_uri, **options)

    def as_dict(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "variable_id": self.variable.variable_id,
            "variable": self.variable.label,
            "role": self.variable.role,
            "kind": self.variable.kind,
            "variable_metadata": self.variable.metadata,
            "status": self.status,
            "experiment_metadata": self.experiment_metadata,
            "sequence": self.sequence,
            "recorded_at": self.recorded_at,
            "occurred_at": self.occurred_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "value": self.value,
            "ref_uri": self.ref_uri,
            "range_start": self.range_start,
            "range_end": self.range_end,
            "artifact_digest": self.artifact_digest,
            "filename": self.filename,
            "media_type": self.media_type,
            "byte_length": self.byte_length,
        }

class _Builder:
    """Delay declaration until the caller selects the variable's value kind."""
    def __init__(self, study: "Study", label: str, role: str): self.study, self.label, self.role = study, label, role
    def _make(self, kind: str, **metadata: Any) -> "ExperimentVariable":
        item = self.study.client.declare_experiment_variable(self.study.study_id, label=self.label, role=self.role, kind=kind, metadata=metadata)
        collection = self.study._collections[self.role]
        variable = collection._variables.get(self.label)
        if variable is None:
            variable = collection._add(item)
        if item.get("created", False) and self.study._experiment is not None:
            warnings.warn(
                f"New variable {self.label!r} declared during an active experiment; "
                "it will also be available to future runs of this study.",
                UserWarning,
                stacklevel=3,
            )
        return variable
    def json(self, **metadata: Any): return self._make("json", **metadata)
    def text(self, **metadata: Any): return self._make("text", **metadata)
    def scalar(self, *, unit: str | None = None, **metadata: Any): return self._make("scalar", unit=unit, **metadata)
    def file(self, *, media_type: str | None = None, **metadata: Any): return self._make("file", media_type=media_type, **metadata)
    def timeseries(self, *, observed: str | Point | None = None, unit: str | None = None, **metadata: Any):
        uri = observed.uri if isinstance(observed, Point) else observed
        return self._make("timeseries", observed=str(uri) if uri is not None else None, unit=unit, **metadata)

class ExperimentVariable:
    def __init__(
        self,
        study: "Study",
        variable_id: str,
        label: str,
        role: str,
        kind: str,
        metadata: dict[str, Any] | None = None,
        created_at: datetime | str | None = None,
    ):
        self.study = study
        self.variable_id = variable_id
        self.label = label
        self.role = role
        self.kind = kind
        self.metadata = dict(metadata or {})
        self.created_at = timestamp(created_at) if created_at is not None else None
    @property
    def uri(self) -> str:
        return variable_uri(self.variable_id)

    @property
    def run_id(self) -> str:
        if self.study._experiment is None: raise RuntimeError("start an experiment before mutating variables")
        return self.study._experiment.run_id
    def record(self, value: Any, *, occurred_at: datetime | None = None):
        """Record a value, event, file path, or time-series rows in the active run."""
        # Check before file/stream operations, including empty time series.
        self.run_id
        if self.kind in {"file", "timeseries"} and occurred_at is not None:
            raise TypeError("occurred_at is only supported for JSON, text, scalar, and log values")
        if self.kind == "file":
            return self.study.client.attach_experiment_file(
                self.run_id, self.variable_id, value,
                media_type=self.study._metadata[self.variable_id].get("media_type"),
            )
        if self.kind == "timeseries":
            return self.add(value)
        return self.set(value, occurred_at=occurred_at)
    def set(self, value: Any, *, occurred_at: datetime | None = None): return self.study.client.observe_experiment(self.run_id, self.variable_id, value=value, occurred_at=occurred_at.isoformat() if occurred_at else None)
    def append(self, value: Any, *, occurred_at: datetime | None = None): return self.set(value, occurred_at=occurred_at)
    def attach(self, path: str | Path): return self.study.client.attach_experiment_file(self.run_id, self.variable_id, path)
    def use(self, ref_uri: str | Point, *, interval: tuple[datetime, datetime] | None = None):
        # Inputs can point at pre-existing streams without copying their data.
        # The recorded reference/range is enough to explain what was consumed.
        body = {"ref_uri": str(ref_uri.uri if isinstance(ref_uri, Point) else ref_uri)}
        if interval: body.update(start=interval[0].isoformat(), end=interval[1].isoformat())
        return self.study.client.observe_experiment(self.run_id, self.variable_id, **body)
    def add(self, rows: Any):
        if self.kind != "timeseries": raise TypeError("add() is only valid for time-series variables")
        metadata = self.study._metadata[self.variable_id]; observed = metadata.get("observed")
        # A run-scoped source keeps scenarios from overwriting each other's
        # output while still producing ordinary Acquirium stream references.
        experiment = self.study._experiment
        if experiment is None:
            raise RuntimeError("start an experiment before mutating variables")
        source = f"experiment/{experiment.run_id}"; ref_name = self.label
        if hasattr(rows, "to_pylist"):
            rows = [(item.get("time", item.get("ts")), item["value"]) for item in rows.to_pylist()]
        elif hasattr(rows, "to_dicts"):
            rows = [(item.get("time", item.get("ts")), item["value"]) for item in rows.to_dicts()]
        rows = [(timestamp(when), value) for when, value in rows]
        if not rows: return None
        # Register graph metadata before writing data. This makes the output
        # discoverable by its observed plant property immediately.
        if self.variable_id not in experiment._registered_streams:
            self.study.ac.register_streams([{
                "source_id": source,
                "ref_name": ref_name,
                "point_uri": observed,
                "unit": metadata.get("unit"),
                "label": self.label,
                "value_kind": "numeric",
            }])
            # Registration succeeded even if the following sample write fails.
            # Cache on the run so later runs get their own stream metadata.
            experiment._registered_streams.add(self.variable_id)
        self.study.ac.insert_timeseries(source, ref_name, rows, point_uri=observed)
        ref_uri = self.study.ac.reference_uri(source, ref_name)
        self.use(ref_uri, interval=(min(x[0] for x in rows), max(x[0] for x in rows)))
        return RecordedSeries(str(ref_uri), self.study.client)

    def observations(self, experiments: "ExperimentCollection | None" = None) -> "ObservationCollection":
        """Select this variable's recorded values across Experiments."""
        selected = experiments or self.study.experiments
        if selected.study is not self.study:
            raise ValueError("experiments belong to a different study")
        return selected.observations(self)

class ExperimentOutputs(Mapping):
    """Run-scoped assignment convenience; reads return study variable handles."""
    def __init__(self, experiment: "Experiment"):
        self.experiment = experiment

    def __getitem__(self, label: str) -> ExperimentVariable:
        return self.experiment.study.output[label]

    def __iter__(self):
        return iter(self.experiment.study.output.keys())

    def __len__(self):
        return len(self.experiment.study.output)

    def __setitem__(self, label: str, value: Any) -> None:
        study = self.experiment.study
        if study._experiment is not self.experiment:
            raise RuntimeError("cannot record through an inactive experiment")
        if label in study.output:
            variable = study.output[label]
        else:
            # Validate before creating a persistent declaration. Files and streams
            # need explicit metadata; do not guess their meaning from the payload.
            try:
                json.dumps(value, allow_nan=False)
            except (TypeError, ValueError) as error:
                raise TypeError("new outputs require a JSON-compatible value; declare files and time series explicitly") from error
            builder = study.output(label)
            if isinstance(value, str):
                variable = builder.text()
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                variable = builder.scalar()
            else:
                variable = builder.json()
        variable.record(value)


class Experiment:
    """One current or historical execution of a Study."""
    def __init__(self, study: "Study", item: dict[str, Any] | str):
        if isinstance(item, str):
            item = {"run_id": item, "status": "running", "metadata": {}}
        self.study = study
        self.run_id = item["run_id"]
        self.status = item.get("status", "running")
        self.metadata = dict(item.get("metadata") or {})
        self.started_at = timestamp(item["started_at"]) if item.get("started_at") else None
        self.finished_at = timestamp(item["finished_at"]) if item.get("finished_at") else None
        self.error = item.get("error")
        self._registered_streams: set[str] = set()
        self.output = ExperimentOutputs(self)

    @property
    def uri(self) -> str:
        return run_uri(self.run_id)

    @property
    def experiment_id(self) -> str:
        return self.run_id

    def as_dict(self) -> dict[str, Any]:
        return {
            "experiment_id": self.run_id,
            "status": self.status,
            "metadata": self.metadata,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
        }

    def observations(self, variable: ExperimentVariable | str | None = None) -> "ObservationCollection":
        return ObservationCollection(self.study, run_id=self.run_id, variable=variable)

    def annotate_stream(self, ref_uri: str, *, start: datetime | str, end: datetime | str | None = None,
                        metadata: dict[str, Any] | None = None) -> StreamAnnotation:
        """Tag historical or live samples from an existing stream as part of this run."""
        item = self.study.client.annotate_experiment_stream(
            self.run_id, ref_uri, start=timestamp(start).isoformat(),
            end=timestamp(end).isoformat() if end is not None else None,
            metadata={} if metadata is None else metadata,
        )
        return StreamAnnotation.from_item(item, self.study.client)

    def stream_annotations(self, *, ref_uri: str | None = None,
                           start: datetime | str | None = None,
                           end: datetime | str | None = None) -> list[StreamAnnotation]:
        return self.study.stream_annotations(run_id=self.run_id, ref_uri=ref_uri, start=start, end=end)

    def finish(self):
        result = self.study.client.finish_experiment(self.run_id)
        self.status = result["status"]
        self.finished_at = timestamp(result["finished_at"])
        if self.study._experiment is not None and self.study._experiment.run_id == self.run_id:
            self.study._experiment = None
        return result

    def fail(self, error: BaseException | Any):
        result = self.study.client.finish_experiment(self.run_id, failed=True, error={"type": type(error).__name__, "message": str(error)})
        self.status = result["status"]
        self.finished_at = timestamp(result["finished_at"])
        if self.study._experiment is not None and self.study._experiment.run_id == self.run_id:
            self.study._experiment = None
        return result

class VariableCollection:
    """Declare and access handles registered through this Study object.

    Iteration yields handles; items() yields (label, handle) pairs.
    """
    def __init__(self, study: "Study", role: str):
        self.study, self.role = study, role
        self._variables: dict[str, ExperimentVariable] = {}

    def _add(self, item: dict[str, Any]) -> ExperimentVariable:
        variable = ExperimentVariable(
            self.study,
            item["variable_id"],
            item["label"],
            item["role"],
            item["kind"],
            item.get("metadata"),
            item.get("created_at"),
        )
        self._variables[variable.label] = variable
        self.study._metadata[variable.variable_id] = variable.metadata
        return variable

    def __call__(self, label: str, **metadata: Any):
        builder = _Builder(self.study, label, self.role)
        if self.role == "annotation":
            return builder._make("log", **metadata)
        if metadata:
            raise TypeError("provide metadata to the type constructor, e.g. scalar(unit='USD')")
        return builder

    def __getitem__(self, label: str) -> ExperimentVariable:
        return self._variables[label]

    def __iter__(self):
        return iter(self._variables.values())

    def __len__(self):
        return len(self._variables)

    def __contains__(self, label: str):
        return label in self._variables

    def keys(self):
        return self._variables.keys()

    def values(self):
        return self._variables.values()

    def items(self):
        return self._variables.items()


class VariableCatalog:
    """All persisted variable declarations for a Study, including metadata."""
    def __init__(self, study: "Study", variables: list[ExperimentVariable] | None = None, *, query=None, filters=None):
        self.study = study
        self._selection = variables
        self._query = query
        self._filters = filters or {}

    def _items(self) -> list[ExperimentVariable]:
        selected_ids = None
        if self._query is not None:
            selected_ids = matched_ids(self.query())
            for item in self.study.client.list_experiment_variables(self.study.study_id):
                if item["label"] not in self.study._collections[item["role"]]:
                    self.study._collections[item["role"]]._add(item)
        variables = self._selection if self._selection is not None else [
            variable
            for collection in self.study._collections.values()
            for variable in collection.values()
        ]
        if selected_ids is not None:
            variables = [variable for variable in variables if variable.uri in selected_ids]
        variables = [
            variable for variable in variables
            if all(getattr(variable, key) == value for key, value in self._filters.items())
        ]
        return sorted(
            variables,
            key=lambda variable: (
                variable.created_at is None,
                variable.created_at or datetime.max.replace(tzinfo=UTC),
                variable.label,
            ),
        )

    def __getitem__(self, label_or_id: str) -> ExperimentVariable:
        for variable in self._items():
            if variable.label == label_or_id or variable.variable_id == label_or_id:
                return variable
        raise KeyError(label_or_id)

    def __iter__(self):
        return iter(self._items())

    def __len__(self):
        return len(self._items())

    def __contains__(self, label_or_id: str):
        try:
            self[label_or_id]
        except KeyError:
            return False
        return True

    def get(self, label_or_id: str, default: Any = None):
        try:
            return self[label_or_id]
        except KeyError:
            return default

    def all(self) -> list[ExperimentVariable]:
        return self._items()

    def query(self):
        """Return PR #120's query builder for this Study's variable nodes."""
        if self._query is None:
            query = experiment_query(self.study, variable=True)
        else:
            sync_study(self.study)
            query = self._query._with_graph(self._query.query_graph)
        if self._filters:
            query = query.where(**self._filters)
        if self._selection:
            query = query.where(variable_id=[variable.variable_id for variable in self._selection])
        return query

    def where(self, *expressions, role: str | None = None, kind: str | None = None, **attrs) -> "VariableCatalog":
        filters = dict(self._filters)
        if role is not None:
            filters["role"] = role
        if kind is not None:
            filters["kind"] = kind
        query = self._query
        if expressions or attrs:
            if query is None:
                query = experiment_query(self.study, variable=True)
            else:
                sync_study(self.study)
                query = query._with_graph(query.query_graph)
            query = query.where(*expressions, **attrs)
        return VariableCatalog(self.study, self._selection, query=query, filters=filters)

    def frame(self) -> pl.DataFrame:
        variables = self._items()
        base_columns = {"variable_id", "label", "role", "kind", "metadata", "created_at"}
        metadata_columns = sorted({
            key for variable in variables for key in variable.metadata
            if key not in base_columns
        })
        items = []
        for variable in variables:
            item = {
                "variable_id": variable.variable_id, "label": variable.label,
                "role": variable.role, "kind": variable.kind,
                "metadata": variable.metadata, "created_at": variable.created_at,
            }
            for key in metadata_columns:
                item[key] = variable.metadata.get(key)
            items.append(item)
        return records_frame(items)


class ExperimentCollection:
    """A lazily filtered selection of a Study's Experiments."""
    def __init__(self, study: "Study", *, query=None, **filters: Any):
        self.study = study
        self._filters = filters
        self._query = query

    def query(self):
        """Return a native metadata query, scoped to this Study and selection."""
        if self._query is None:
            query = experiment_query(self.study)
        else:
            sync_study(self.study)
            query = self._query._with_graph(self._query.query_graph)
        attrs = {}
        if self._filters.get("status") is not None:
            attrs["status"] = self._filters["status"]
        if self._filters.get("metadata"):
            attrs.update(self._filters["metadata"])
        if attrs:
            query = query.where(**attrs)
        from acquirium.Client.explore.expr import AttrProxy
        attr = AttrProxy()
        if self._filters.get("started_after") is not None:
            query = query.where(attr.started_at >= timestamp(self._filters["started_after"]))
        if self._filters.get("started_before") is not None:
            query = query.where(attr.started_at < timestamp(self._filters["started_before"]))
        return query

    def where(
        self,
        *expressions,
        status: str | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        **attrs: Any,
    ) -> "ExperimentCollection":
        filters = dict(self._filters)
        for key, value in {
            "status": status,
            "started_after": started_after,
            "started_before": started_before,
        }.items():
            if value is not None:
                filters[key] = value
        if metadata is not None:
            filters["metadata"] = {**filters.get("metadata", {}), **metadata}
        query = self._query
        if expressions or attrs:
            if query is None:
                query = experiment_query(self.study)
            else:
                sync_study(self.study)
                query = query._with_graph(query.query_graph)
            query = query.where(*expressions, **attrs)
        return ExperimentCollection(self.study, query=query, **filters)

    def _selected_run_ids(self):
        if self._query is None:
            return None
        uris = matched_ids(self.query())
        prefix = str(EXPERIMENT["run/"])
        return {uri[len(prefix):] for uri in uris if uri.startswith(prefix)}

    def all(self) -> list[Experiment]:
        selected_ids = self._selected_run_ids()
        filters = dict(self._filters)
        if selected_ids is not None:
            filters["run_ids"] = sorted(selected_ids)
        return [
            Experiment(self.study, item)
            for item in self.study.client.list_experiments(self.study.study_id, **filters)
        ]

    def frame(self) -> pl.DataFrame:
        items = [experiment.as_dict() for experiment in self.all()]
        return records_frame(items)

    def get(self, experiment_id: str) -> Experiment:
        item = self.study.client.get_experiment(experiment_id)
        if item["template_id"] != self.study.study_id:
            raise KeyError(experiment_id)
        return Experiment(self.study, item)

    def observations(self, variable: ExperimentVariable | str | None = None) -> "ObservationCollection":
        return ObservationCollection(self.study, variable=variable, selection=self, **self._filters)


class ObservationCollection:
    """A selection of persisted observations materialized as objects or data."""
    def __init__(
        self,
        study: "Study",
        *,
        run_id: str | None = None,
        variable: ExperimentVariable | str | None = None,
        selection: ExperimentCollection | None = None,
        **experiment_filters: Any,
    ):
        self.study = study
        self.run_id = run_id
        if isinstance(variable, str):
            variable = study.variables[variable]
        if variable is not None and variable.study is not study:
            raise ValueError("variable belongs to a different study")
        self.variable = variable
        self.experiment_filters = experiment_filters
        self.selection = selection

    def _rows(self) -> list[dict[str, Any]]:
        selected_ids = self.selection._selected_run_ids() if self.selection is not None else None
        if selected_ids == set():
            return []
        filters = dict(self.experiment_filters)
        if selected_ids is not None:
            filters["run_ids"] = sorted(selected_ids)
        return self.study.client.list_experiment_observations(
            self.study.study_id,
            run_id=self.run_id,
            variable_id=self.variable.variable_id if self.variable else None,
            **filters,
        )

    def all(self) -> list[Observation]:
        observations = []
        rows = self._rows()
        # Each catalog lookup rebuilds and sorts the Study's variables; index
        # them once per call instead of once per row.
        variables = {variable.variable_id: variable for variable in self.study.variables}
        if any(item["variable_id"] not in variables for item in rows):
            # Another session may have extended the Study since it was opened.
            # Preserve existing handles while hydrating missing declarations.
            for declaration in self.study.client.list_experiment_variables(self.study.study_id):
                if declaration["variable_id"] not in variables:
                    self.study._collections[declaration["role"]]._add(declaration)
            variables = {variable.variable_id: variable for variable in self.study.variables}
        for item in rows:
            variable = variables[item["variable_id"]]
            observations.append(Observation(
                experiment_id=item["experiment_id"],
                variable=variable,
                sequence=item["sequence"],
                recorded_at=timestamp(item["recorded_at"]),
                occurred_at=timestamp(item["occurred_at"]) if item.get("occurred_at") else None,
                value=item.get("value"),
                status=item["status"],
                experiment_metadata=dict(item.get("experiment_metadata") or {}),
                started_at=timestamp(item["started_at"]),
                finished_at=timestamp(item["finished_at"]) if item.get("finished_at") else None,
                ref_uri=item.get("ref_uri"),
                range_start=timestamp(item["range_start"]) if item.get("range_start") else None,
                range_end=timestamp(item["range_end"]) if item.get("range_end") else None,
                artifact_digest=item.get("artifact_digest"),
                filename=item.get("filename"),
                media_type=item.get("media_type"),
                byte_length=item.get("byte_length"),
                _client=self.study.client,
            ))
        return observations

    def frame(self) -> pl.DataFrame:
        return records_frame([observation.as_dict() for observation in self.all()])

    def latest(self) -> Observation:
        observations = self.all()
        if not observations:
            raise LookupError("no observations matched this selection")
        return observations[-1]


class Study:
    """Reusable variable declarations plus a single active Experiment."""
    def __init__(self, ac: Any, item: dict, variables: list[dict[str, Any]] | None = None):
        self.ac, self.client, self.study_id, self.name, self._experiment, self._metadata = ac, ac.client, item["template_id"], item["name"], None, {}
        self.created_at = timestamp(item["created_at"]) if item.get("created_at") else None
        self.input = VariableCollection(self, "input")
        self.output = VariableCollection(self, "output")
        self.log = VariableCollection(self, "annotation")
        self._collections = {"input": self.input, "output": self.output, "annotation": self.log}
        for variable in variables or []:
            self._collections[variable["role"]]._add(variable)
        self.variables = VariableCatalog(self)
        self.experiments = ExperimentCollection(self)
    @property
    def uri(self) -> str:
        return study_uri(self.study_id)

    def start(self, metadata: dict | None = None) -> Experiment:
        # Variable objects deliberately route through one active experiment;
        # nested/concurrent experiments on the same Study are not ambiguous.
        if self._experiment is not None: raise RuntimeError("finish the active experiment before starting another")
        self._experiment = Experiment(self, self.client.start_experiment(self.study_id, metadata)); return self._experiment

    def resume(self, run_id: str) -> Experiment:
        """Bind a persisted running Experiment to this Study's variable handles."""
        if self._experiment is not None:
            raise RuntimeError("finish the active experiment before resuming another")
        item = self.client.get_experiment(run_id)
        if item["template_id"] != self.study_id:
            raise ValueError("experiment belongs to a different study")
        if item["status"] != "running":
            raise ValueError("only a running experiment can be resumed")
        self._experiment = Experiment(self, item)
        return self._experiment

    def stream_annotations(self, *, run_id: str | None = None, ref_uri: str | None = None,
                           start: datetime | str | None = None,
                           end: datetime | str | None = None) -> list[StreamAnnotation]:
        items = self.client.list_experiment_stream_annotations(
            template_id=self.study_id, run_id=run_id, ref_uri=ref_uri,
            start=timestamp(start).isoformat() if start is not None else None,
            end=timestamp(end).isoformat() if end is not None else None,
        )
        return [StreamAnnotation.from_item(item, self.client) for item in items]

class StudyService:
    """Top-level `ac.study` entry point; `define` is idempotent by study name."""
    def __init__(self, ac: Any): self.ac = ac
    def _hydrate(self, item: dict[str, Any]) -> Study:
        self.ac.client.sync_experiment_metadata()
        variables = self.ac.client.list_experiment_variables(item["template_id"])
        return Study(self.ac, item, variables)
    def define(self, name: str) -> Study:
        return self._hydrate(self.ac.client.define_experiment(name))
    def get(self, name_or_id: str) -> Study:
        return self._hydrate(self.ac.client.get_experiment_study(name_or_id))
    def all(self) -> list[Study]:
        return [self._hydrate(item) for item in self.ac.client.list_experiment_studies()]
    def stream_annotations(self, *, ref_uri: str | None = None,
                           start: datetime | str | None = None,
                           end: datetime | str | None = None) -> list[StreamAnnotation]:
        items = self.ac.client.list_experiment_stream_annotations(
            ref_uri=ref_uri,
            start=timestamp(start).isoformat() if start is not None else None,
            end=timestamp(end).isoformat() if end is not None else None,
        )
        return [StreamAnnotation.from_item(item, self.ac.client) for item in items]
