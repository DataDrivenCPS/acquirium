"""Reusable, variable-centric experiment tracking."""
from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Mapping
from datetime import datetime, timezone
from hashlib import sha256
import functools
import math
from copy import deepcopy
from contextlib import nullcontext
import base64
from fnmatch import fnmatchcase
import io
import json
import re
import reprlib
import warnings
from pathlib import Path
from typing import Any
from uuid import uuid4
import polars as pl
from acquirium.internals.models import compute_ref_uri
from acquirium.Storage.migrations import EXPERIMENT_MIGRATIONS, migrate
from acquirium.Storage.metadata_outbox import enqueue, execute
from acquirium.ExperimentSamples import sample_frame
from acquirium.Client.streams import Stream, StreamInterval
from acquirium.ExperimentValues import ARTIFACT_KINDS, TABLE_MEDIA_TYPE, check_kind, to_json, timestamp, parquet as _parquet, sample_rows as _sample_rows, value_entry, recording_entries, artifact_entry
from acquirium.ObjectCodecs import codec_path, encode as _encode, load_codec
from acquirium.ModelAdapters import (
    AdapterDefinition, Component, ModelAdapter, UNSET, read_component,
)
from acquirium.ExperimentMetadata import (
    EXPERIMENT, ExperimentMetadata, study_record, variable_record, run_record, annotation_record, annotation_uri, experiment_query,
    matched_ids, metadata_values, node_uri, records_frame, run_uri, study_uri, sync_study,
    variable_uri,
)

from acquirium.ExperimentDeclarations import (
    FORMAT, Declaration, prepare_declaration, validate_declaration,
    stream_keys as _stream_keys, stream_key as _stream_key,
)

UTC = timezone.utc

# Server receipt time is deliberately distinct from a caller's occurred_at.
# The former answers when Acquirium learned something; the latter answers when
# the outside-world event happened.
def _now() -> datetime: return datetime.now(UTC)
def _database_timestamp(value: datetime | str) -> datetime:
    """Store UTC wall time consistently in SQL TIMESTAMP columns."""
    return timestamp(value).replace(tzinfo=None)
def _json(value: Any) -> str:
    try: return json.dumps(value, sort_keys=True, default=str, allow_nan=False)
    except (TypeError, ValueError) as error: raise ValueError("experiment values must be JSON serializable, without NaN or infinity") from error


# Printed handles show only what they already hold; a repr never queries the server.
_brief = reprlib.Repr()
_brief.maxstring = _brief.maxother = 60
_brief.maxdict = _brief.maxlist = 4
_ROLE_NAMES = {"annotation": "log"}

def _count(number: int, noun: str) -> str:
    return f"{number} {noun}{'' if number == 1 else 's'}"

def _selection_repr(name: str, study: "Study", details: dict[str, Any], *, attributes: bool = False) -> str:
    """Describe a lazy selection; `.all()` or `.frame()` reads it."""
    parts = [f"study={study.name!r}"]
    parts += [f"{key}={_brief.repr(value)}" for key, value in details.items() if value not in (None, {}, [])]
    if attributes:
        parts.append("with attribute filters")
    return f"<{name} {' '.join(parts)}>"

def _publishes(method):
    """Serialize metadata transactions with ordered graph delivery."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self.metadata.lock:
            return method(self, *args, **kwargs)
    return wrapper

@dataclass
class _PreparedObservation:
    variable_id: str
    key: tuple | None = None
    occurred_at: datetime | None = None
    value: Any = None
    artifact_digest: str | None = None
    ref_uri: str | None = None
    interval: tuple[datetime, datetime] | None = None
    attachment: tuple[dict, bytes] | None = None
    sample_ref: str | None = None


@dataclass
class _PreparedBatch:
    observations: list[_PreparedObservation] = field(default_factory=list)
    registrations: list[tuple] = field(default_factory=list)
    updates: list[dict] = field(default_factory=list)
    frame: pl.DataFrame | None = None


# Artifact bytes are split into rows of at most this size, which keeps each
# value well below per-value limits and bounds the memory a single row needs.
ARTIFACT_CHUNK_BYTES = 4 << 20


class ExperimentStore:
    """Transactional recordings and durable handoff to graph-owned metadata."""
    def __init__(self, store: Any, *, graph_store, resolve=None) -> None:
        self.store = store
        self._live_resolve = resolve
        self.metadata = ExperimentMetadata(store, graph_store, self._resolve)
        self.metadata_graph = self.metadata.writer
        # Serialize graph delivery with metadata reads and lifecycle writes.
        # The store's separate writer lock remains available during graph I/O.
        # The ledger stores provenance records and artifact bytes; time-series
        # samples are Escapement streams (``store.samples``).
        with store._lock, store._write_conn() as conn:
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_templates (template_id VARCHAR PRIMARY KEY, name VARCHAR UNIQUE NOT NULL, created_at TIMESTAMP NOT NULL)")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_variables (variable_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, label VARCHAR NOT NULL, role VARCHAR NOT NULL, kind VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, UNIQUE(template_id, label))")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_runs (run_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, status VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, error_json VARCHAR)")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_observations (run_id VARCHAR NOT NULL, variable_id VARCHAR, sequence BIGINT NOT NULL, recorded_at TIMESTAMP NOT NULL, occurred_at TIMESTAMP, value_json VARCHAR, artifact_digest VARCHAR, ref_uri VARCHAR, range_start TIMESTAMP, range_end TIMESTAMP, PRIMARY KEY(run_id, sequence))")
            self.metadata.preflight_legacy(conn)
            migrate(conn, backend="postgres" if conn.__class__.__module__.startswith("psycopg") else "duckdb", component="experiments",
                    migrations=EXPERIMENT_MIGRATIONS)
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_artifacts (digest VARCHAR PRIMARY KEY, filename VARCHAR NOT NULL, media_type VARCHAR, byte_length BIGINT NOT NULL, created_at TIMESTAMP NOT NULL)")
            # Artifact bytes commit with the rows that reference them.
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_artifact_chunks (digest VARCHAR NOT NULL, seq INTEGER NOT NULL, data BYTEA NOT NULL, PRIMARY KEY(digest, seq))")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_variables_template_idx ON experiment_variables(template_id)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_runs_template_idx ON experiment_runs(template_id, status, started_at)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_observations_variable_idx ON experiment_observations(variable_id, run_id, sequence)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_observations_ref_idx ON experiment_observations(ref_uri, range_start, range_end)")
        self._resolutions = {}
        # Finish any deletion a crash interrupted.
        self._cleanup_streams()

    def _resolve(self, text, kind):
        key = (text, kind)
        if key in self._resolutions:
            return self._resolutions[key]
        uri = self._live_resolve(text, kind) if self._live_resolve is not None else None
        if uri is not None:
            self._resolutions[key] = str(uri)
        return uri

    def _prepare_metadata(self, uri, metadata, *, variable=False):
        metadata = metadata_values(metadata)
        return metadata, self.metadata_graph.validate(uri, metadata, variable=variable)

    def _queue_metadata(self, conn, update):
        # The resolved graph operation commits with its lifecycle row.
        self.metadata.enqueue(conn, update)

    def sync_metadata_graph(self):
        return self.metadata.sync()

    def publish_queued_metadata(self):
        self.metadata.publish()

    def _metadata(self, kind, identifiers):
        return self.metadata.read(kind, identifiers)

    def read_metadata(self, items, cls, id_field):
        return self.metadata.read_metadata(items, cls, id_field)

    def read_observation_metadata(self, items):
        return self.metadata.read_observation_metadata(items)

    def _filter_metadata(self, items, kind, id_field, metadata):
        if not items or not metadata:
            return items
        from acquirium.Metadata import dumps
        expected = {key: dumps(value) for key,value in metadata.items()}
        field = "experiment_metadata" if id_field == "experiment_id" else "metadata"
        return [item for item in items if all(dumps(item[field].get(key)) == value for key,value in expected.items())]

    _execute = staticmethod(execute)

    def _active(self, conn: Any, run_id: str) -> str:
        # All observations are append-only while running.  A terminal run is a
        # durable scientific record, not a mutable dashboard row.
        row = self._execute(conn, "SELECT status, template_id FROM experiment_runs WHERE run_id=?", [run_id]).fetchone()
        if row is None: raise KeyError(run_id)
        if row[0] != "running": raise ValueError("experiment run is terminal")
        return row[1]

    def _recording_variables(self, conn, run_id, identifiers):
        """Load each variable once and enforce ownership against the active run."""
        template_id = self._active(conn, run_id)
        identifiers = set(identifiers)
        rows = self._execute(conn,
            f"SELECT variable_id,template_id,label,kind FROM experiment_variables WHERE variable_id IN ({','.join('?' for _ in identifiers)})",
            list(identifiers)).fetchall() if identifiers else []
        found = {identifier: (label, kind) for identifier, _, label, kind in rows}
        if missing := identifiers - found.keys():
            raise KeyError(next(iter(missing)))
        if any(owner != template_id for _, owner, _, _ in rows):
            raise ValueError("variable belongs to a different study")
        return template_id, found

    def _next_observation(self, conn: Any, run_id: str) -> tuple[int, datetime]:
        # Values and stream annotations share one run-local recording order.
        sequence = int(self._execute(conn, "SELECT coalesce(max(sequence),0)+1 FROM experiment_observations WHERE run_id=?", [run_id]).fetchone()[0])
        return sequence, _now()
    @_publishes
    def define(self, name: str) -> dict[str, Any]:
        if not name.strip(): raise ValueError("experiment name is required")
        item = {"template_id": str(uuid4()), "name": name, "created_at": _now()}
        update = self.metadata_graph.record_update(study_record(item))
        with self.store._lock, self.store._write_conn() as conn:
            # Defining a study repeatedly is normal for scripts imported more
            # than once. The name therefore identifies a reusable declaration.
            row = self._execute(conn, "SELECT template_id, created_at FROM experiment_templates WHERE name=?", [name]).fetchone()
            if row: return {"template_id": row[0], "name": name, "created_at": row[1]}
            self._execute(conn, "INSERT INTO experiment_templates VALUES (?, ?, ?)", [item["template_id"], item["name"], _database_timestamp(item["created_at"])])
            self._queue_metadata(conn, update)
        return item
    def declare(self, template_id: str, label: str, role: str, kind: str, metadata: dict[str, Any]) -> dict[str, Any]:
        return self.declare_batch(template_id, [dict(label=label, role=role, kind=kind, metadata=metadata)])[0]

    @_publishes
    def declare_batch(self, template_id: str, declarations: list[dict]) -> list[dict]:
        """Validate and prepare outside the writer lock, then commit together."""
        while True:
            result, pending = self._prepare_declarations(template_id, declarations)
            with self.store._lock, self.store._write_conn() as conn:
                labels = [item["label"] for item, _ in pending]
                marks = ",".join("?" for _ in labels)
                if labels and self._execute(conn, f"SELECT 1 FROM experiment_variables WHERE template_id=? AND label IN ({marks}) LIMIT 1", [template_id, *labels]).fetchone():
                    # A concurrent declaration committed after preparation.
                    # Prepare again so it is compared rather than reinserted.
                    continue
                for item, update in pending:
                    self._execute(conn, "INSERT INTO experiment_variables VALUES (?, ?, ?, ?, ?, ?)", [item["variable_id"], template_id, item["label"], item["role"], item["kind"], _database_timestamp(item["created_at"])])
                    self._queue_metadata(conn, update)
            return result

    def _prepare_declarations(self, template_id, declarations):
        # Only labels in this request are compared, so a new declaration does
        # not load (or wait for graph delivery of) the rest of the Study.
        labels = list(dict.fromkeys(declaration["label"] for declaration in declarations))
        with self.store._own_conn() as conn:
            if self._execute(conn, "SELECT 1 FROM experiment_templates WHERE template_id=?", [template_id]).fetchone() is None:
                raise KeyError(template_id)
            rows = self._execute(conn, f"SELECT variable_id, label, role, kind, created_at FROM experiment_variables WHERE template_id=? AND label IN ({self._marks(labels)})", [template_id, *labels]).fetchall() if labels else []
        saved = self._metadata(EXPERIMENT.Variable, [row[0] for row in rows]) if rows else {}
        existing = {row[1]: dict(variable_id=row[0], label=row[1], role=row[2], kind=row[3], metadata=saved[row[0]], created_at=timestamp(row[4]), created=False) for row in rows}
        result, pending = [], []
        for declaration in declarations:
            label, role, kind = (declaration[name] for name in ("label", "role", "kind"))
            prepared = prepare_declaration(label, role, kind, {k:v for k,v in declaration.get("metadata", {}).items() if k != "_resolved_semantics"})
            previous = existing.get(label)
            if previous is not None:
                if not prepared.equivalent(previous["role"], previous["kind"], previous["metadata"]):
                    raise ValueError(f"variable {label!r} was already declared differently")
                result.append({**previous, "created": False})
                continue
            variable_id = str(compute_ref_uri(f"study/{template_id}", label))
            prepared = prepared.resolve(self._resolve)
            metadata = prepared.document()
            item = dict(variable_id=variable_id, label=label, role=role, kind=kind, metadata=metadata, created_at=_now(), created=True)
            existing[label] = item
            pending.append((item, self.metadata_graph.record_update(variable_record(template_id, item))))
            result.append(item)
        return result, pending

    @_publishes
    def start(self, template_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        # Metadata belongs to this execution, while declarations belong to the
        # reusable study. This is what lets a parameter sweep share variables.
        run = {"run_id": str(uuid4()), "template_id": template_id, "status": "running", "metadata": metadata, "started_at": _now()}
        run["metadata"], prepared = self._prepare_metadata(run_uri(run["run_id"]), metadata)
        update = self.metadata_graph.record_update(run_record(run), prepared=prepared)
        with self.store._lock, self.store._write_conn() as conn:
            if self._execute(conn, "SELECT 1 FROM experiment_templates WHERE template_id=?", [template_id]).fetchone() is None:
                raise KeyError(template_id)
            self._execute(conn, "INSERT INTO experiment_runs (run_id,template_id,status,started_at) VALUES (?,?,?,?)", [run["run_id"], template_id, "running", _database_timestamp(run["started_at"])])
            self._queue_metadata(conn, update)
        return run
    def observe(self, run_id: str, variable_id: str, *, value: Any = None, occurred_at: datetime | None = None, artifact_digest: str | None = None, ref_uri: str | None = None, interval: tuple[datetime,datetime] | None = None, key: tuple | list | None = None) -> dict[str, Any]:
        return self.observe_batch(run_id, [dict(variable_id=variable_id, value=value, occurred_at=occurred_at, artifact_digest=artifact_digest, ref_uri=ref_uri, interval=interval, key=key)])[0]

    def observe_batch(self, run_id: str, observations: list[dict]) -> list[dict]:
        """Compatibility adapter for the original observation transport."""
        forms = {"artifact": "artifact", "artifact_digest": "artifact_reference", "ref_uri": "reference", "value": "value"}
        entries = []
        for observation in observations:
            payloads = [(field, observation[field]) for field in forms if observation.get(field) is not None]
            if len(payloads) > 1:
                raise ValueError("each recording entry must have one recording form")
            field, value = payloads[0] if payloads else ("value", None)
            item = {k: v for k, v in observation.items() if k not in forms}
            if item.get("interval") is None:
                item.pop("interval", None)
            entries.append({**item, "form": forms[field], field: value})
        return self.record_batch(run_id, entries)

    def record_batch(self, run_id: str, entries: list[dict]) -> list[dict]:
        # Sample metadata must serialize with source clears. Attachments and
        # value-only batches queue no graph intent and need only the SQL lock.
        with self.metadata.lock if any(e.get("form") == "samples" for e in entries) else nullcontext():
            return self._write_recording(run_id, self._prepare_recording(run_id, entries))

    def _prepare_recording(self, run_id, entries):
        """Validate inputs and prepare samples/graph intent before taking the writer lock."""
        from acquirium.ExperimentMetadata import stream_update
        batch, frames = _PreparedBatch(), []
        with self.store._own_conn() as conn:
            template_id, declarations = self._recording_variables(conn, run_id, (e["variable_id"] for e in entries))
        series = [identifier for identifier, (_, kind) in declarations.items() if kind == "timeseries"]
        definitions = self._metadata(EXPERIMENT.Variable, series) if series else {}
        source = f"experiment/{run_id}"
        stream_ids = set()
        forms = {"value": "value", "artifact": "artifact", "artifact_reference": "artifact_digest",
                 "samples": "samples", "reference": "ref_uri"}
        for entry in entries:
            form = entry.get("form")
            if form not in forms:
                raise ValueError("unknown recording form")
            payload = forms[form]
            if (set(forms.values()) - {payload}) & entry.keys():
                raise ValueError("each recording entry must have one recording form")
            if payload not in entry:
                raise ValueError(f"{form} recording requires {payload}")
            allowed = {"form", "variable_id", "key", "occurred_at", payload} | ({"interval"} if form == "reference" else set())
            if unexpected := entry.keys() - allowed:
                raise ValueError(f"unexpected recording fields: {sorted(unexpected)}")
            label, kind = declarations[entry["variable_id"]]
            metadata = definitions.get(entry["variable_id"], {})
            keys = _stream_keys(kind, metadata)
            key = entry.get("key")
            if keys:
                key = _stream_key(key, keys)
            elif key is not None:
                raise ValueError("only keyed time-series observations accept a key")
            occurred_at = entry.get("occurred_at")
            if occurred_at is not None:
                if kind not in {"scalar", "text", "json", "log"}:
                    raise ValueError("occurred_at is only supported for JSON, text, scalar, and log values")
                occurred_at = timestamp(occurred_at)
            item = _PreparedObservation(entry["variable_id"], key=key, occurred_at=occurred_at)
            if form == "samples":
                if kind != "timeseries":
                    raise ValueError(f"{label!r} is a {kind} variable; samples require a time series")
                rows = _sample_rows(entry["samples"])
                if not rows:
                    # Receipts pair with entries by position; skipping one
                    # would shift every later receipt onto the wrong entry.
                    raise ValueError(f"samples recording for {label!r} requires at least one sample")
                key = tuple(key or ())
                name = _stream_name(entry["variable_id"], key)
                ref = str(compute_ref_uri(source, name))
                if ref in stream_ids:
                    raise ValueError(f"duplicate stream reference {ref!r}")
                stream_ids.add(ref)
                point = f"{ref}__point"
                batch.registrations.append((point, source, name, ref, "numeric"))
                frames.append(sample_frame(ref, rows))
                batch.updates.append(stream_update(self.metadata_graph, source, name, template_id, run_id,
                    entry["variable_id"], label, metadata, keys, key))
                item.ref_uri = item.sample_ref = ref
                item.interval = (min(t for t,_ in rows), max(t for t,_ in rows))
            elif form == "artifact":
                artifact = entry["artifact"]
                data = base64.b64decode(artifact["data"], validate=True)
                if kind not in ARTIFACT_KINDS:
                    raise ValueError(f"{label!r} is a {kind} variable; only file, table, and object variables take attachments")
                if kind == "table" and (len(data) < 8 or data[:4] != b"PAR1" or data[-4:] != b"PAR1"):
                    raise ValueError(f"{label!r} is a table; attach a Parquet file")
                item.artifact_digest = sha256(data).hexdigest()
                item.attachment = (artifact, data)
            elif form == "artifact_reference":
                item.artifact_digest = entry["artifact_digest"]
                if kind not in ARTIFACT_KINDS or item.artifact_digest is None:
                    raise ValueError(f"{label!r} is a {kind} variable; attach its bytes instead")
            elif form == "reference":
                if kind != "timeseries":
                    raise ValueError(f"{label!r} is a {kind} variable and cannot reference a stream or file")
                item.ref_uri = entry["ref_uri"]
                if not isinstance(item.ref_uri, str) or not item.ref_uri.strip():
                    raise ValueError("a stream reference URI is required")
                interval = entry.get("interval")
                if interval is not None:
                    if len(interval) != 2:
                        raise ValueError("a stream interval requires start and end")
                    item.interval = tuple(timestamp(t) for t in interval)
                    if item.interval[1] < item.interval[0]:
                        raise ValueError("a stream interval must have start <= end")
            elif form == "value":
                item.value = to_json(entry["value"])
                try:
                    check_kind(kind, item.value, label=repr(label))
                except TypeError as error:
                    raise ValueError(str(error)) from error
            batch.observations.append(item)
        batch.frame = pl.concat(frames) if frames else None
        return batch

    def _write_recording(self, run_id, batch):
        """Write samples, then commit the observations that point at them.

        1. Reserve the recording's streams in `experiment_streams`, so a run's
           deletion finds every stream it may have written.
        2. Write the samples to Escapement and wait until they are published.
        3. Commit observations, artifacts, and graph intent in one transaction.

        An observation therefore never points at missing samples. A failure
        after step 2 leaves samples no observation names; a retry rewrites
        the same keys, and deleting the run clears them.
        """
        if batch.frame is not None:
            with self.store._lock, self.store._write_conn() as conn:
                self._check_recording(conn, run_id, batch)
                self._own_streams(conn, run_id, batch.registrations)
            self._write_samples(f"experiment/{run_id}", batch.frame)
        results = []
        with self.store._lock, self.store._write_conn() as conn:
            self._check_recording(conn, run_id, batch)
            sequence, recorded = self._next_observation(conn, run_id)
            for offset, item in enumerate(batch.observations):
                if item.attachment is not None:
                    self._store_artifact(conn, item.artifact_digest, *item.attachment, recorded)
                self._execute(conn, "INSERT INTO experiment_observations (run_id, variable_id, sequence, recorded_at, occurred_at, value_json, artifact_digest, ref_uri, range_start, range_end, key_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, item.variable_id, sequence + offset, _database_timestamp(recorded), _database_timestamp(item.occurred_at) if item.occurred_at else None, _json(item.value) if item.value is not None else None, item.artifact_digest, item.ref_uri, _database_timestamp(item.interval[0]) if item.interval else None, _database_timestamp(item.interval[1]) if item.interval else None, _json(item.key) if item.key is not None else None])
                receipt = {"sequence": sequence + offset, "recorded_at": recorded}
                if item.sample_ref is not None:
                    receipt["ref_uri"] = item.sample_ref
                results.append(receipt)
            for update in batch.updates:
                self.metadata.enqueue(conn, update)
        return results

    def _check_recording(self, conn, run_id, batch):
        """Authoritative SQL state checks, repeated in each transaction."""
        self._recording_variables(conn, run_id, (item.variable_id for item in batch.observations))
        for item in batch.observations:
            if item.artifact_digest and item.attachment is None and self._execute(conn,
                    "SELECT 1 FROM experiment_artifacts WHERE digest=?", [item.artifact_digest]).fetchone() is None:
                raise ValueError("unknown artifact digest")

    def _own_streams(self, conn, run_id, registrations):
        """Reserve the streams a recording writes, before any sample is."""
        for _point, source, name, ref, _kind in registrations:
            self._execute(conn, "INSERT INTO experiment_streams (ref_uri, run_id, source_id, ref_name) VALUES (?, ?, ?, ?) "
                          "ON CONFLICT (ref_uri) DO NOTHING", [ref, run_id, source, name])

    def _write_samples(self, source, frame):
        if self.store.samples is None:
            raise RuntimeError("this experiment ledger has no sample store")
        self.store.samples.write(source, frame)

    def _cleanup_streams(self) -> int:
        """Clear the samples of streams whose run is gone, then forget them.

        A stream's row outlives its run until its samples are cleared, so this
        finishes every deletion, including one a crash interrupted. Running it
        again does nothing new. Returns how many samples were cleared.
        """
        with self.store._own_conn() as conn:
            refs = [row[0] for row in self._execute(conn, "SELECT ref_uri FROM experiment_streams "
                    "WHERE run_id NOT IN (SELECT run_id FROM experiment_runs)").fetchall()]
        if not refs:
            return 0
        cleared = self.store.samples.clear(refs) if self.store.samples is not None else 0
        with self.store._lock, self.store._write_conn() as conn:
            self._execute(conn, f"DELETE FROM experiment_streams WHERE ref_uri IN ({self._marks(refs)})", refs)
        return cleared

    def _store_artifact(self, conn, digest, artifact, data, recorded):
        # Identical bytes are stored once, under the first name they arrived with.
        if self._execute(conn, "SELECT 1 FROM experiment_artifacts WHERE digest=?", [digest]).fetchone():
            return
        self._execute(conn, "INSERT INTO experiment_artifacts VALUES (?, ?, ?, ?, ?)", [digest, artifact["filename"], artifact.get("media_type"), len(data), _database_timestamp(recorded)])
        for seq, start in enumerate(range(0, len(data), ARTIFACT_CHUNK_BYTES)):
            self._execute(conn, "INSERT INTO experiment_artifact_chunks VALUES (?, ?, ?)", [digest, seq, data[start:start + ARTIFACT_CHUNK_BYTES]])

    @_publishes
    def finish(self, run_id: str, status: str, error: Any = None) -> dict[str, Any]:
        if status not in {"succeeded", "failed"}: raise ValueError("status must be succeeded or failed")
        finished = _now()
        update = self.metadata_graph.fields_update(run_uri(run_id), {"status": status, "finished_at": finished})
        with self.store._lock, self.store._write_conn() as conn:
            self._active(conn, run_id)
            self._execute(conn, "UPDATE experiment_runs SET status=?, finished_at=?, error_json=? WHERE run_id=?", [status, _database_timestamp(finished), _json(error) if error else None, run_id])
            self._queue_metadata(conn, update)
        return {"run_id": run_id, "status": status, "finished_at": finished}
    def attach_file(self, run_id: str, variable_id: str, filename: str, media_type: str | None, data: bytes) -> dict[str, Any]:
        digest = sha256(data).hexdigest()
        self.record_batch(run_id, [artifact_entry(variable_id, data, filename=filename, media_type=media_type)])
        return {"digest": digest, "filename": filename, "byte_length": len(data)}

    def artifact(self, digest: str) -> dict[str, Any]:
        """Return a stored file's bytes with the name and type it was saved under."""
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise KeyError(digest)
        with self.store._own_conn() as conn:
            row = self._execute(conn, "SELECT filename, media_type, byte_length FROM experiment_artifacts WHERE digest=?", [digest]).fetchone()
            if row is None:
                raise KeyError(digest)
            chunks = self._execute(conn, "SELECT data FROM experiment_artifact_chunks WHERE digest=? ORDER BY seq", [digest]).fetchall()
        data = b"".join(bytes(chunk[0]) for chunk in chunks)
        if len(data) != row[2]:
            raise RuntimeError(f"artifact {digest} is missing its stored bytes")
        return {"filename": row[0], "media_type": row[1], "data": data}

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
        metadata, prepared = self._prepare_metadata(annotation_uri(run_id), metadata)
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
                """, [run_id, sequence, _database_timestamp(recorded), None, ref_uri,
                      _database_timestamp(start_at),
                      _database_timestamp(end_at) if end_at is not None else None])
            item = {"annotation_id": f"{run_id}:{sequence}", "run_id": run_id,
                    "template_id": row[0], "ref_uri": ref_uri, "range_start": start_at,
                    "range_end": end_at, "metadata": metadata, "created_at": recorded}
            self._queue_metadata(conn, self.metadata_graph.record_update(annotation_record(item), prepared=prepared))
        return item

    @_publishes
    def close_stream_annotation(self, annotation_id: str, end: datetime | str) -> dict[str, Any]:
        """Set the end of an open interval, including after a run finishes."""
        end_at = timestamp(end)
        run_id, separator, sequence_text = annotation_id.rpartition(":")
        if not separator or not sequence_text.isdecimal():
            raise KeyError(annotation_id)
        update = self.metadata_graph.fields_update(annotation_uri(annotation_id), {"range_end": end_at})
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
            self._queue_metadata(conn, update)
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
        items = [{"annotation_id": f"{row[0]}:{row[1]}", "run_id": row[0], "template_id": row[2], "ref_uri": row[3],
                 "range_start": timestamp(row[4]), "range_end": timestamp(row[5]) if row[5] is not None else None,
                 "metadata": {}, "created_at": timestamp(row[7])} for row in rows]
        return self.read_metadata(items, EXPERIMENT.StreamAnnotation, "annotation_id")

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
                SELECT variable_id, label, role, kind, created_at
                FROM experiment_variables
                WHERE template_id=?
                ORDER BY created_at, label
                """,
                [template_id],
            ).fetchall()
        items = [
            {
                "variable_id": row[0],
                "label": row[1],
                "role": row[2],
                "kind": row[3],
                "metadata": {},
                "created_at": timestamp(row[4]),
            }
            for row in rows
        ]
        return self.read_metadata(items, EXPERIMENT.Variable, "variable_id")

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
                SELECT run_id, status, started_at, finished_at, error_json
                FROM experiment_runs
                WHERE {' AND '.join(clauses)}
                ORDER BY started_at, run_id
                """,
                parameters,
            ).fetchall()
        result = [self._run_row(template_id, row) for row in rows]
        result = self.read_metadata(result, EXPERIMENT.Run, "run_id")
        return self._filter_metadata(result, EXPERIMENT.Run, "run_id", metadata)

    def run(self, run_id: str) -> dict[str, Any]:
        with self.store._own_conn() as conn:
            row = self._execute(
                conn,
                """
                SELECT template_id, status, started_at, finished_at, error_json
                FROM experiment_runs WHERE run_id=?
                """,
                [run_id],
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return self.read_metadata([self._run_row(row[0], (run_id, *row[1:]))], EXPERIMENT.Run, "run_id")[0]

    @staticmethod
    def _run_row(template_id: str, row: Any) -> dict[str, Any]:
        return {
            "run_id": row[0],
            "template_id": template_id,
            "status": row[1],
            "metadata": {},
            "started_at": timestamp(row[2]),
            "finished_at": timestamp(row[3]) if row[3] else None,
            "error": json.loads(row[4]) if row[4] else None,
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
        kind: str | None = None,
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
            ("v.kind=?", kind),
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
                SELECT r.run_id, r.status, r.started_at,
                       r.finished_at, o.sequence, o.recorded_at, o.occurred_at,
                       o.value_json, o.artifact_digest, o.ref_uri,
                       o.range_start, o.range_end, v.variable_id, v.label,
                       v.role, v.kind, a.filename,
                       a.media_type, a.byte_length, o.key_json
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
                "experiment_metadata": {},
                "started_at": timestamp(row[2]),
                "finished_at": timestamp(row[3]) if row[3] else None,
                "sequence": row[4],
                "recorded_at": timestamp(row[5]),
                "occurred_at": timestamp(row[6]) if row[6] else None,
                "value": json.loads(row[7]) if row[7] is not None else None,
                "artifact_digest": row[8],
                "ref_uri": row[9],
                "range_start": timestamp(row[10]) if row[10] else None,
                "range_end": timestamp(row[11]) if row[11] else None,
                "variable_id": row[12],
                "variable": row[13],
                "role": row[14],
                "kind": row[15],
                "variable_metadata": {},
                "filename": row[16],
                "media_type": row[17],
                "byte_length": row[18],
                "key": json.loads(row[19]) if row[19] is not None else None,
            }
            for row in rows
        ]
        result = self.read_observation_metadata(result)
        return self._filter_metadata(result, EXPERIMENT.Run, "experiment_id", metadata)

    # ---- deletion and storage use ----

    @staticmethod
    def _marks(values) -> str:
        return ",".join("?" for _ in values)

    def _run_sources(self, run_ids):
        return [f"experiment/{run_id}" for run_id in run_ids]

    def _owned_refs(self, conn, run_ids) -> list[str]:
        """Streams a run's recording created, as opposed to streams it used."""
        if not run_ids:
            return []
        rows = self._execute(conn, f"SELECT ref_uri FROM experiment_streams WHERE run_id IN ({self._marks(run_ids)})", run_ids).fetchall()
        return [row[0] for row in rows]

    def _delete_runs(self, conn, run_ids, *, sweep=False) -> dict[str, Any]:
        """Remove runs, their observations, and unshared files in `conn`.

        Returns the counts removed; graph node deletions are queued in the same
        transaction. With `sweep`, every unreferenced artifact goes too. The
        runs' stream rows stay: the caller runs `_cleanup_streams` after the
        commit, which clears their samples in Escapement and then the rows.
        """
        run_ids = list(dict.fromkeys(run_ids))
        removed = {"runs": len(run_ids), "observations": 0, "annotations": 0, "samples": 0, "files": 0}
        keys, digests = [(EXPERIMENT.Run, run_id) for run_id in run_ids], []
        if run_ids:
            marks = self._marks(run_ids)
            refs = self._owned_refs(conn, run_ids)
            if refs:
                # Another run's record of a stream must not lose its samples.
                users = self._execute(conn, f"""
                    SELECT DISTINCT run_id FROM experiment_observations
                    WHERE ref_uri IN ({self._marks(refs)}) AND run_id NOT IN ({marks})
                    ORDER BY run_id
                    """, [*refs, *run_ids]).fetchall()
                if users:
                    raise ValueError("other experiments reference streams these runs recorded: "
                                     + ", ".join(row[0] for row in users))
            for run_id, sequence, variable_id in self._execute(conn, f"""
                    SELECT run_id, sequence, variable_id FROM experiment_observations
                    WHERE run_id IN ({marks})""", run_ids).fetchall():
                if variable_id is None:
                    removed["annotations"] += 1
                    keys.append((EXPERIMENT.StreamAnnotation, f"{run_id}:{sequence}"))
                else:
                    removed["observations"] += 1
            digests = [row[0] for row in self._execute(conn, f"""
                SELECT DISTINCT artifact_digest FROM experiment_observations
                WHERE run_id IN ({marks}) AND artifact_digest IS NOT NULL""", run_ids).fetchall()]
            self._execute(conn, f"DELETE FROM experiment_observations WHERE run_id IN ({marks})", run_ids)
            self._execute(conn, f"DELETE FROM experiment_runs WHERE run_id IN ({marks})", run_ids)
        sweep_sources = []
        if sweep:
            sweep_sources = [row[0] for row in self._execute(conn, "SELECT DISTINCT source_id FROM experiment_streams").fetchall()]
            digests = [row[0] for row in self._execute(conn, "SELECT digest FROM experiment_artifacts").fetchall()]
        if digests:
            # Identical bytes are stored once, so keep a file another run still uses.
            unused = [row[0] for row in self._execute(conn, f"""
                SELECT digest FROM experiment_artifacts
                WHERE digest IN ({self._marks(digests)})
                  AND NOT EXISTS (SELECT 1 FROM experiment_observations AS o WHERE o.artifact_digest=experiment_artifacts.digest)
                """, digests).fetchall()]
            if unused:
                self._execute(conn, f"DELETE FROM experiment_artifact_chunks WHERE digest IN ({self._marks(unused)})", unused)
                self._execute(conn, f"DELETE FROM experiment_artifacts WHERE digest IN ({self._marks(unused)})", unused)
            removed["files"] = len(unused)
        for key in keys:
            enqueue(conn, {"operation": "delete", "subject": node_uri(*key)})
        graph = self.metadata_graph.graph_store
        for source in self._run_sources(run_ids):
            enqueue(conn, {"clear": [str(graph.source_graph_uri(source))]})
        if sweep_sources:
            enqueue(conn, {"clear": [str(graph.source_graph_uri(source)) for source in sweep_sources]})
        return removed

    def _after_delete(self, removed) -> dict[str, Any]:
        """After the SQL commit: deliver the graph changes, then clear the
        deleted runs' samples. An error clearing them leaves the deletion
        complete; the next cleanup finishes it."""
        self.sync_metadata_graph()
        removed["samples"] = self._cleanup_streams()
        return removed

    def _refuse_running(self, conn, clause, parameters) -> None:
        running = [row[0] for row in self._execute(
            conn, f"SELECT run_id FROM experiment_runs WHERE status='running' AND {clause} ORDER BY started_at, run_id",
            parameters).fetchall()]
        if running:
            raise ValueError("finish or fail running experiments before deleting them: " + ", ".join(running))

    @_publishes
    def delete_runs(self, run_ids: list[str]) -> dict[str, Any]:
        """Delete finished runs with everything they recorded."""
        run_ids = list(dict.fromkeys(run_ids))
        if not run_ids:
            return {"runs": 0, "observations": 0, "annotations": 0, "samples": 0, "files": 0}
        with self.store._lock, self.store._write_conn() as conn:
            marks = self._marks(run_ids)
            found = {row[0] for row in self._execute(conn, f"SELECT run_id FROM experiment_runs WHERE run_id IN ({marks})", run_ids).fetchall()}
            missing = [run_id for run_id in run_ids if run_id not in found]
            if missing:
                raise KeyError(missing[0])
            self._refuse_running(conn, f"run_id IN ({marks})", run_ids)
            result = self._delete_runs(conn, run_ids)
        return self._after_delete(result)

    @_publishes
    def delete_study(self, template_id: str) -> dict[str, Any]:
        """Delete a Study, its declarations, and every run."""
        with self.store._lock, self.store._write_conn() as conn:
            if self._execute(conn, "SELECT 1 FROM experiment_templates WHERE template_id=?", [template_id]).fetchone() is None:
                raise KeyError(template_id)
            self._refuse_running(conn, "template_id=?", [template_id])
            run_ids = [row[0] for row in self._execute(conn, "SELECT run_id FROM experiment_runs WHERE template_id=?", [template_id]).fetchall()]
            result = self._delete_runs(conn, run_ids)
            variables = [row[0] for row in self._execute(conn, "SELECT variable_id FROM experiment_variables WHERE template_id=?", [template_id]).fetchall()]
            self._execute(conn, "DELETE FROM experiment_variables WHERE template_id=?", [template_id])
            self._execute(conn, "DELETE FROM experiment_templates WHERE template_id=?", [template_id])
            for uri in [study_uri(template_id), *map(variable_uri, variables)]:
                enqueue(conn, {"operation": "delete", "subject": uri})
        return {"studies": 1, "variables": len(variables), **self._after_delete(result)}

    @_publishes
    def clear(self) -> dict[str, Any]:
        """Delete every Study and its records; other Acquirium data is untouched."""
        with self.store._lock, self.store._write_conn() as conn:
            self._refuse_running(conn, "1=1", [])
            run_ids = [row[0] for row in self._execute(conn, "SELECT run_id FROM experiment_runs").fetchall()]
            studies = int(self._execute(conn, "SELECT count(*) FROM experiment_templates").fetchone()[0])
            variables = int(self._execute(conn, "SELECT count(*) FROM experiment_variables").fetchone()[0])
            result = self._delete_runs(conn, run_ids, sweep=True)
            identifiers = self._execute(conn, "SELECT variable_id FROM experiment_variables").fetchall()
            study_ids = self._execute(conn, "SELECT template_id FROM experiment_templates").fetchall()
            for uri in [*(variable_uri(row[0]) for row in identifiers), *(study_uri(row[0]) for row in study_ids)]:
                enqueue(conn, {"operation": "delete", "subject": uri})
            for table in ("experiment_variables", "experiment_templates"):
                self._execute(conn, f"DELETE FROM {table}")
        self._resolutions = {}
        return {"studies": studies, "variables": variables, **self._after_delete(result)}

    def usage(self, *, template_id: str | None = None, run_ids: list[str] | None = None) -> dict[str, Any]:
        """Count what the selected experiments store and estimate its size.

        Files are exact and counted once however many runs share them. Sample
        and ledger sizes estimate the stored values (8 bytes per number or
        timestamp, text by length), not the database's compressed size.
        """
        clauses, parameters = [], []
        if template_id is not None:
            clauses.append("template_id=?"); parameters.append(template_id)
        if run_ids is not None:
            if not run_ids:
                clauses.append("1=0")
            else:
                clauses.append(f"run_id IN ({self._marks(run_ids)})"); parameters.extend(run_ids)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        whole_studies = run_ids is None
        with self.store._own_conn() as conn:
            if template_id is not None and self._execute(conn, "SELECT 1 FROM experiment_templates WHERE template_id=?", [template_id]).fetchone() is None:
                raise KeyError(template_id)
            selected = [row[0] for row in self._execute(conn, f"SELECT run_id FROM experiment_runs {where}", parameters).fetchall()]
            runs_ledger = int(self._execute(conn, f"""
                SELECT coalesce(sum(length(run_id) + length(template_id) + length(status)
                    + coalesce(length(error_json), 0) + 16), 0)
                FROM experiment_runs {where}""", parameters).fetchone()[0])
            usage = {"studies": 0, "variables": 0, "runs": len(selected), "observations": 0,
                     "annotations": 0, "samples": 0, "files": 0,
                     "sample_bytes": 0, "file_bytes": 0, "ledger_bytes": runs_ledger}
            if whole_studies:
                study_where, study_parameters = ("WHERE template_id=?", [template_id]) if template_id is not None else ("", [])
                usage["studies"] = int(self._execute(conn, f"SELECT count(*) FROM experiment_templates {study_where}", study_parameters).fetchone()[0])
                usage["ledger_bytes"] += int(self._execute(conn, f"""
                    SELECT coalesce(sum(length(template_id) + length(name) + 8), 0)
                    FROM experiment_templates {study_where}""", study_parameters).fetchone()[0])
                variables = self._execute(conn, f"""
                    SELECT count(*), coalesce(sum(length(variable_id) + length(template_id) + length(label)
                        + length(role) + length(kind) + 8), 0)
                    FROM experiment_variables {study_where}""", study_parameters).fetchone()
                usage["variables"] = int(variables[0]); usage["ledger_bytes"] += int(variables[1])
            if selected:
                marks = self._marks(selected)
                row = self._execute(conn, f"""
                    SELECT count(variable_id), count(*) - count(variable_id),
                        coalesce(sum(length(run_id) + coalesce(length(variable_id), 0)
                            + coalesce(length(value_json), 0) + coalesce(length(artifact_digest), 0)
                            + coalesce(length(ref_uri), 0) + coalesce(length(key_json), 0) + 40), 0)
                    FROM experiment_observations WHERE run_id IN ({marks})""", selected).fetchone()
                usage["observations"], usage["annotations"] = int(row[0]), int(row[1])
                usage["ledger_bytes"] += int(row[2])
                files = self._execute(conn, f"""
                    SELECT count(*), coalesce(sum(byte_length), 0) FROM experiment_artifacts
                    WHERE digest IN (SELECT artifact_digest FROM experiment_observations WHERE run_id IN ({marks}))
                    """, selected).fetchone()
                usage["files"], usage["file_bytes"] = int(files[0]), int(files[1])
                refs = self._owned_refs(conn, selected)
                if refs and self.store.samples is not None:
                    usage["samples"], usage["sample_bytes"] = self.store.samples.usage(refs)
        usage["total_bytes"] = usage["sample_bytes"] + usage["file_bytes"] + usage["ledger_bytes"]
        return usage

@dataclass(frozen=True)
class Point:
    """A deliberately small semantic handle; storage only needs its URI."""
    uri: str

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

    @property
    def stream(self) -> Stream:
        return Stream(self.ref_uri, self._client)

    def dataframe(self, **kwargs: Any):
        """Read the current stream values within the claimed interval."""
        options = dict(kwargs)
        options.setdefault("start", self.range_start)
        if self.range_end is not None:
            options.setdefault("end", self.range_end)
        return self.stream.dataframe(**options)

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
    key: tuple | None = None
    _client: Any = field(default=None, repr=False, compare=False)

    def dataframe(self, **kwargs: Any):
        """Read a table, or fetch a time-series observation's samples."""
        if self.variable.kind == "table":
            if kwargs:
                raise TypeError("a table observation's dataframe() takes no options")
            return pl.read_parquet(io.BytesIO(self.read_bytes()))
        if self.variable.kind != "timeseries" or self.ref_uri is None:
            raise TypeError("dataframe() is only available for table and time-series observations")
        options = dict(kwargs)
        if self.range_start is not None:
            options.setdefault("start", self.range_start)
        if self.range_end is not None:
            options.setdefault("end", self.range_end)
        return self.stream.dataframe(**options)

    @property
    def stream(self) -> Stream | None:
        """The stream a time-series observation recorded or referenced."""
        return Stream(self.ref_uri, self._client) if self.ref_uri is not None else None

    @property
    def interval(self) -> StreamInterval | None:
        """The stream and time range this observation covers, to record in another run."""
        if self.ref_uri is None or self.range_start is None or self.range_end is None:
            return None
        return self.stream.between(self.range_start, self.range_end)

    def read_bytes(self) -> bytes:
        """Download the saved bytes of a file, table, or object observation."""
        if self.artifact_digest is None:
            raise TypeError("read_bytes() is only available for file, table, and object observations")
        return self._client.experiment_artifact(self.artifact_digest)

    def load(self, *, codec: type | None = None) -> Any:
        """Rebuild the object an object variable recorded, with its codec.

        `codec` overrides the declared one, for instance where the declared
        codec cannot be imported, or to decode a file observation's bytes.
        Decoding runs the codec here, in this process; the server never does.
        """
        if codec is None:
            if self.variable.kind != "object":
                raise TypeError(f"{self.variable.label!r} is a {self.variable.kind} variable; pass load(codec=...) to decode its bytes")
            decoder = load_codec(self.variable.codec)
        else:
            decoder = load_codec(codec_path(codec))
        return decoder.loads(self.read_bytes())

    def __repr__(self) -> str:
        if self.ref_uri is not None:
            value = f"stream {self.ref_uri}"
            if self.range_start is not None:
                end = self.range_end.isoformat() if self.range_end is not None else "open"
                value += f" [{self.range_start.isoformat()}, {end}]"
        elif self.artifact_digest is not None:
            value = f"{self.variable.kind} {self.filename!r} ({_count(self.byte_length or 0, 'byte')})"
        else:
            value = _brief.repr(self.value)
        return f"<Observation {self.variable.label!r} = {value} (run {self.experiment_id}, #{self.sequence})>"

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
            "key": self.key,
        }

class _Builder:
    """Delay declaration until the caller selects the variable's value kind."""
    def __init__(self, study: "Study", label: str, role: str): self.study, self.label, self.role = study, label, role
    def _make(self, kind: str, **metadata: Any) -> "ExperimentVariable":
        metadata = prepare_declaration(self.label, self.role, kind, metadata).document()
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
    def object(self, *, codec: type, **metadata: Any):
        """An object saved through `codec` and rebuilt by `observation.load()`."""
        return self._make("object", codec=codec_path(codec), media_type=codec.media_type, **metadata)
    def table(self, **metadata: Any): return self._make("table", **metadata)
    def timeseries(self, *, observed: Mapping[tuple, str] | str | Point | None = None, unit: str | None = None, keys: tuple[str, ...] = (), **metadata: Any):
        return self._make("timeseries", observed=observed, unit=unit, keys=keys, **metadata)

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
        self.declaration = Declaration.from_document(label, role, kind, metadata or {}, variable_id=variable_id)
        self.created_at = timestamp(created_at) if created_at is not None else None
    @property
    def variable_id(self):
        return self.declaration.variable_id

    @property
    def label(self):
        return self.declaration.label

    @property
    def role(self):
        return self.declaration.role

    @property
    def kind(self):
        return self.declaration.kind

    @property
    def metadata(self):
        return self.declaration.metadata

    @property
    def unit(self):
        return self.declaration.unit

    @property
    def quantity_kind(self):
        return self.declaration.quantity_kind

    @property
    def keys(self):
        return self.declaration.keys

    @property
    def observed(self):
        return self.declaration.observed

    @property
    def source_path(self):
        return self.declaration.source_path

    @property
    def description(self):
        return self.declaration.description

    @property
    def source_unit(self):
        return self.declaration.source_unit

    @property
    def media_type(self):
        return self.declaration.media_type

    @property
    def codec(self):
        """The import path of an object variable's codec."""
        return self.declaration.codec

    def _serialize(self, value: Any) -> tuple[bytes, str, str]:
        codec = load_codec(self.codec)
        return (_encode(codec, value, label=self.label),
                f"{self.label}{getattr(codec, 'suffix', '')}", codec.media_type)

    def object_entry(self, value: Any) -> dict:
        """Serialize `value` with this object variable's codec for recording."""
        data, filename, media_type = self._serialize(value)
        return artifact_entry(self.variable_id, data, filename=filename, media_type=media_type)

    @property
    def uri(self) -> str:
        return variable_uri(self.variable_id)

    def __repr__(self) -> str:
        role = _ROLE_NAMES.get(self.role, self.role)
        kind = "" if self.kind == role else f" {self.kind}"
        metadata = f" {_brief.repr(self.metadata)}" if self.metadata else ""
        return f"<ExperimentVariable {role} {self.label!r}{kind}{metadata}>"

    @property
    def run_id(self) -> str:
        if self.study._experiment is None: raise RuntimeError("start an experiment before mutating variables")
        return self.study._experiment.run_id
    def record(self, value: Any, *, occurred_at: datetime | None = None):
        """Record a value, event, file, table, object, or time series in the active run.

        A time-series variable takes rows, which are stored with the run, or
        `stream.between(start, end)`, which records a reference: later reads
        see the stream's current samples in that range.
        """
        # Check before file/stream operations, including empty time series.
        self.run_id
        if self.kind in {"file", "table", "object", "timeseries"} and occurred_at is not None:
            raise TypeError("occurred_at is only supported for JSON, text, scalar, and log values")
        if self.kind == "table":
            return self.study.client.attach_experiment_bytes(
                self.run_id, self.variable_id, _parquet(value),
                filename=f"{self.label}.parquet", media_type=TABLE_MEDIA_TYPE,
            )
        if self.kind == "file":
            return self.study.client.attach_experiment_file(
                self.run_id, self.variable_id, value,
                media_type=self.media_type,
            )
        if self.kind == "object":
            data, filename, media_type = self._serialize(value)
            return self.study.client.attach_experiment_bytes(
                self.run_id, self.variable_id, data, filename=filename, media_type=media_type,
            )
        if self.kind == "timeseries":
            if isinstance(value, StreamInterval):
                return self.study.client.record_experiment_batch(self.run_id, recording_entries(self, value))[0]
            if isinstance(value, Stream):
                raise TypeError("record a stream as stream.between(start, end), or its rows as stream.dataframe()")
            return self.add(value)
        if isinstance(value, (Stream, StreamInterval)):
            raise TypeError(f"{self.label!r} is a {self.kind} variable; only time-series variables record streams")
        return self.set(value, occurred_at=occurred_at)
    def set(self, value: Any, *, occurred_at: datetime | None = None):
        return self.study.client.record_experiment_batch(self.run_id,
            [value_entry(self.variable_id, self.kind, self.label, value, occurred_at=occurred_at)])[0]
    def append(self, value: Any, *, occurred_at: datetime | None = None): return self.set(value, occurred_at=occurred_at)
    def attach(self, path: str | Path): return self.study.client.attach_experiment_file(self.run_id, self.variable_id, path)
    def add(self, rows: Any):
        if self.kind != "timeseries": raise TypeError("add() is only valid for time-series variables")
        self.run_id
        if isinstance(rows, StreamInterval):
            raise TypeError("add() records rows; use record() for a stream interval")
        entries = recording_entries(self, rows)
        streams = {tuple(entry["key"] or ()): Stream(receipt["ref_uri"], self.study.client)
                   for entry, receipt in zip(entries, _record_snapshot(self.study, entries))}
        return streams if self.keys else streams.get(())

    def observations(self, experiments: "ExperimentCollection | None" = None) -> "ObservationCollection":
        """Select this variable's recorded values across Experiments."""
        selected = experiments or self.study.experiments
        if selected.study is not self.study:
            raise ValueError("experiments belong to a different study")
        return selected.observations(self)

def _stream_name(variable_id: str, key: tuple) -> str:
    # Reuse canonical reference identities for the source-local storage name.
    # JSON preserves key types and boundaries; labels remain display metadata.
    return str(compute_ref_uri(f"experiment-variable/{variable_id}", _json(list(key))))


def _record_snapshot(study: "Study", entries: list[dict]):
    """Send prepared values together; callers have already checked the active run."""
    return study.client.record_experiment_batch(study._experiment.run_id, entries) if entries else []


class DeclaredVariables(Mapping):
    """A fixed selection of declarations/readers, with role-specific views."""

    def __init__(self, study: "Study", adapter: ModelAdapter, components: list[Component],
                 variables: list[ExperimentVariable]):
        self.study, self.adapter = study, adapter
        self._components = tuple(deepcopy(component) for component in components)
        self._variables = {variable.label: variable for variable in variables}

    def _view(self, role):
        components = [component for component in self._components if component.role == role]
        return DeclaredVariables(self.study, self.adapter, components,
            [self[c.label] for c in components])

    @property
    def input(self):
        return self._view("input")

    @property
    def output(self):
        return self._view("output")

    def __getitem__(self, label: str) -> ExperimentVariable:
        return self._variables[label]

    def __iter__(self):
        return iter(self._variables)

    def __len__(self):
        return len(self._variables)

    def frame(self) -> pl.DataFrame:
        return VariableCatalog(self.study, list(self._variables.values())).frame()

    def preview(self, *, model_time=UNSET) -> dict[str, Any]:
        """Read this selection locally, without a run or recording requests."""
        return {c.label: read_component(self.adapter, c, sampling=c.kind == "timeseries", model_time=model_time)
                for c in self._components}

    def _active(self):
        if self.study._experiment is None:
            raise RuntimeError("start an experiment before recording a model")

    def record_values(self) -> list[dict]:
        """Read and batch scalar, text, JSON, and table values in the active run."""
        self._active()
        entries = [entry for c in self._components if c.kind != "timeseries"
                   for entry in read_component(self.adapter, c,
                       prepare=lambda value: recording_entries(self[c.label], value))]
        return _record_snapshot(self.study, entries)

    def sample(self, *, at: datetime | str, model_time=UNSET) -> list[dict]:
        """Read time-varying values, selecting model_time and storing them at at."""
        self._active()
        when = timestamp(at)
        entries = [entry for c in self._components if c.kind == "timeseries"
                   for entry in read_component(self.adapter, c, sampling=True, model_time=model_time,
                       prepare=lambda value: recording_entries(self[c.label], value, at=when))]
        return _record_snapshot(self.study, entries)



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

    def __repr__(self) -> str:
        return f"<ExperimentOutputs run={self.experiment.run_id} {_brief.repr(list(self))}>"

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
                converted = to_json(value)
            except (TypeError, ValueError) as error:
                raise TypeError(f"new outputs require a JSON-compatible value ({error}); "
                                "declare files, tables, and time series explicitly") from error
            builder = study.output(label)
            if isinstance(converted, str):
                variable = builder.text()
            elif isinstance(converted, (int, float)):
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
        self.output = ExperimentOutputs(self)

    @property
    def uri(self) -> str:
        return run_uri(self.run_id)

    @property
    def experiment_id(self) -> str:
        return self.run_id

    def __repr__(self) -> str:
        metadata = f" {_brief.repr(self.metadata)}" if self.metadata else ""
        return f"<Experiment {self.run_id} {self.status}{metadata}>"

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
            metadata=to_json({} if metadata is None else metadata, path="metadata"),
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

    def delete(self) -> dict[str, Any]:
        """Delete this finished run with its observations, samples, and unshared files."""
        return self.study.client.delete_experiments([self.run_id])

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

    def __repr__(self) -> str:
        return f"<VariableCollection {_ROLE_NAMES.get(self.role, self.role)} {_brief.repr(list(self._variables))}>"


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

    def __repr__(self) -> str:
        selected = [variable.label for variable in self._selection] if self._selection else None
        return _selection_repr("VariableCatalog", self.study, {**self._filters, "variables": selected},
                               attributes=self._query is not None)

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
        documents = {variable.variable_id: variable.declaration.document() for variable in variables}
        metadata_columns = sorted({
            key for document in documents.values() for key in document
            if key not in base_columns and key not in {FORMAT, "_resolved_semantics"}
        })
        items = []
        for variable in variables:
            item = {
                "variable_id": variable.variable_id, "label": variable.label,
                "role": variable.role, "kind": variable.kind,
                "metadata": variable.metadata, "created_at": variable.created_at,
            }
            for key in metadata_columns:
                item[key] = documents[variable.variable_id].get(key)
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

    def scalars(self, *variables: ExperimentVariable | str, metadata: tuple[str, ...] | list[str] = ()) -> pl.DataFrame:
        """One row per selected run and one column per scalar variable.

        Each cell holds the variable's last recorded value in that run, or
        null if the run did not record it. Columns are `experiment_id`, the
        requested run `metadata` keys (dotted paths reach nested fields), then
        the variables: those given, or every scalar in the Study.
        """
        runs = self.all()
        observations = self.study.client.list_experiment_observations(
            self.study.study_id, run_ids=[run.run_id for run in runs], kind="scalar") if runs else []
        # Hydrate declarations another session added before choosing columns.
        self.study._variables_by_id(item["variable_id"] for item in observations)
        if variables:
            selected = [self.study.variables[variable] if isinstance(variable, str) else variable for variable in variables]
            for variable in selected:
                if variable.study is not self.study:
                    raise ValueError("variable belongs to a different study")
                if variable.kind != "scalar":
                    raise TypeError(f"{variable.label!r} is a {variable.kind} variable; scalars() reads scalar variables")
        else:
            selected = [variable for variable in self.study.variables if variable.kind == "scalar"]
        columns = ["experiment_id", *metadata, *(variable.label for variable in selected)]
        repeated = sorted({name for name in columns if columns.count(name) > 1})
        if repeated:
            raise ValueError(f"scalars() columns would repeat: {', '.join(repeated)}")
        # Observations arrive in recording order, so the last one wins.
        latest = {(item["experiment_id"], item["variable_id"]): item["value"] for item in observations}
        if not runs:
            return pl.DataFrame(schema={"experiment_id": pl.String, **{key: pl.Null for key in metadata},
                                        **{variable.label: pl.Float64 for variable in selected}})

        def field(values, path):
            for key in path.split("."):
                values = values.get(key) if isinstance(values, dict) else None
            return values

        rows = []
        for run in runs:
            row = {"experiment_id": run.run_id}
            row.update({key: field(run.metadata, key) for key in metadata})
            row.update({variable.label: latest.get((run.run_id, variable.variable_id)) for variable in selected})
            rows.append(row)
        frame = records_frame(rows)
        # A variable no selected run recorded is still a numeric column.
        return frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(variable.label) for variable in selected
                                  if frame[variable.label].null_count() == frame.height)

    def delete(self) -> dict[str, Any]:
        """Delete the selected runs; nothing is removed if any is still running."""
        return self.study.client.delete_experiments([experiment.run_id for experiment in self.all()])

    def usage(self) -> dict[str, Any]:
        """Count and size what the selected runs store."""
        return self.study.client.experiment_usage(run_ids=[experiment.run_id for experiment in self.all()])

    def __repr__(self) -> str:
        return _selection_repr("ExperimentCollection", self.study, self._filters, attributes=self._query is not None)


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
        variables = self.study._variables_by_id(item["variable_id"] for item in rows)
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
                key=tuple(item["key"]) if item.get("key") is not None else None,
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

    def __repr__(self) -> str:
        details = {"run": self.run_id, "variable": self.variable.label if self.variable else None,
                   **self.experiment_filters}
        attributes = self.selection is not None and self.selection._query is not None
        return _selection_repr("ObservationCollection", self.study, details, attributes=attributes)


class Study:
    """Reusable variable declarations plus a single active Experiment."""
    def __init__(self, ac: Any, item: dict, variables: list[dict[str, Any]] | None = None):
        self.ac, self.client, self.study_id, self.name, self._experiment = ac, ac.client, item["template_id"], item["name"], None
        self.created_at = timestamp(item["created_at"]) if item.get("created_at") else None
        self.input = VariableCollection(self, "input")
        self.output = VariableCollection(self, "output")
        self.log = VariableCollection(self, "annotation")
        self._collections = {"input": self.input, "output": self.output, "annotation": self.log}
        for variable in variables or []:
            self._collections[variable["role"]]._add(variable)
        self.variables = VariableCatalog(self)
        self.experiments = ExperimentCollection(self)

    def declare_from(self, adapter: ModelAdapter, *, include=None, exclude=None) -> DeclaredVariables:
        """Declare a fixed selection with roles chosen on adapter.input/output."""
        if isinstance(adapter, AdapterDefinition):
            adapter = adapter.freeze()
        def patterns(value):
            return [value] if isinstance(value, str) else list(value or [])
        include, exclude = patterns(include), patterns(exclude)
        available = list(adapter.components())
        if not available:
            raise ValueError("the adapter is empty; declare components with adapter.scalar(), adapter.timeseries(), or adapter.input before declare_from()")
        components, declarations, labels = [], [], set()
        for component in available:
            component = validate_declaration(deepcopy(component), model=True)
            if include and not any(fnmatchcase(component.label, pattern) for pattern in include):
                continue
            if any(fnmatchcase(component.label, pattern) for pattern in exclude):
                continue
            if component.label in labels:
                raise ValueError(f"duplicate component label {component.label!r}")
            labels.add(component.label)
            metadata = component.document()
            # A spelling override may assert the same unit, but never relabel
            # values requiring conversion. Check before declaring anything.
            if component.source_unit and component.source_unit != component.unit:
                if component.unit is None:
                    raise ValueError(f"{component.label!r}: cannot discard known source units")
                factors = self.client.get_conversion_factors(component.source_unit, component.unit)
                if (not factors["compatible"]
                        or not math.isclose(factors["from_multiplier"], factors["to_multiplier"], rel_tol=1e-12, abs_tol=0)
                        or not math.isclose(factors["from_offset"], factors["to_offset"], rel_tol=1e-12, abs_tol=0)):
                    raise ValueError(f"{component.label!r}: unit override requires conversion; use adapters.define() with an explicit conversion reader and its resulting unit")
            declarations.append(dict(label=component.label, role=component.role, kind=component.kind,
                                     metadata=metadata))
            components.append(component)
        if not components:
            raise ValueError("no adapter components match include/exclude")
        items = self.client.declare_experiment_variables(self.study_id, declarations)
        variables = []
        for item in items:
            collection = self._collections[item["role"]]
            variable = collection._variables.get(item["label"])
            if variable is None:
                variable = collection._add(item)
            variables.append(variable)
        if self._experiment is not None and any(item.get("created") for item in items):
            warnings.warn("New model variables declared during an active experiment; they will also be available to future runs.", UserWarning, stacklevel=2)
        return DeclaredVariables(self, adapter, components, variables)
    @property
    def uri(self) -> str:
        return study_uri(self.study_id)

    def __repr__(self) -> str:
        counts = ", ".join(_count(len(self._collections[role]), _ROLE_NAMES.get(role, role))
                           for role in ("input", "output", "annotation"))
        active = f"; running {self._experiment.run_id}" if self._experiment is not None else ""
        return f"<Study {self.name!r}: {counts}{active}>"

    def start(self, metadata: dict | None = None) -> Experiment:
        # Variable objects deliberately route through one active experiment;
        # nested/concurrent experiments on the same Study are not ambiguous.
        if self._experiment is not None: raise RuntimeError("finish the active experiment before starting another")
        metadata = to_json(metadata or {}, path="metadata")
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

    def _variables_by_id(self, wanted=()) -> dict[str, ExperimentVariable]:
        """Index handles by ID, loading declarations another session added."""
        # Each catalog lookup rebuilds and sorts the Study's variables; index
        # them once per call instead of once per row.
        variables = {variable.variable_id: variable for variable in self.variables}
        if any(variable_id not in variables for variable_id in wanted):
            # Preserve existing handles while hydrating missing declarations.
            for declaration in self.client.list_experiment_variables(self.study_id):
                if declaration["variable_id"] not in variables:
                    self._collections[declaration["role"]]._add(declaration)
            variables = {variable.variable_id: variable for variable in self.variables}
        return variables

    def delete(self) -> dict[str, Any]:
        """Delete this Study, its declarations, and every run."""
        return self.client.delete_experiment_study(self.study_id)

    def usage(self) -> dict[str, Any]:
        """Count and size what this Study stores."""
        return self.client.experiment_usage(template_id=self.study_id)

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
    def usage(self) -> dict[str, Any]:
        """Count and size what every Study stores; shared files count once."""
        return self.ac.client.experiment_usage()
    def clear(self, *, confirm: bool = False) -> dict[str, Any]:
        """Delete every Study and run. Streams, metadata, and the plant model stay."""
        if not confirm:
            raise ValueError("clear() deletes every Study and run; pass confirm=True")
        return self.ac.client.clear_experiments(confirm=True)
    def stream_annotations(self, *, ref_uri: str | None = None,
                           start: datetime | str | None = None,
                           end: datetime | str | None = None) -> list[StreamAnnotation]:
        items = self.ac.client.list_experiment_stream_annotations(
            ref_uri=ref_uri,
            start=timestamp(start).isoformat() if start is not None else None,
            end=timestamp(end).isoformat() if end is not None else None,
        )
        return [StreamAnnotation.from_item(item, self.ac.client) for item in items]
