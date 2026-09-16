"""Reusable, variable-centric experiment tracking."""
from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Mapping
from datetime import datetime, timezone
from hashlib import sha256
import json
import warnings
from pathlib import Path
from typing import Any
from uuid import uuid4
from dateutil import parser as dtparser
import polars as pl

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

class ExperimentStore:
    """SQL persistence behind the manager's intentionally small HTTP API."""
    def __init__(self, store: Any, artifact_dir: Path) -> None:
        self.store, self.artifact_dir = store, artifact_dir
        artifact_dir.mkdir(parents=True, exist_ok=True)
        # Keep the experiment ledger beside canonical time-series storage.  The
        # ledger stores small provenance records; time-series samples continue
        # to live in the normal Acquirium tables.
        with store._lock, store._write_conn() as conn:
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_templates (template_id VARCHAR PRIMARY KEY, name VARCHAR UNIQUE NOT NULL, created_at TIMESTAMP NOT NULL)")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_variables (variable_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, label VARCHAR NOT NULL, role VARCHAR NOT NULL, kind VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, UNIQUE(template_id, label))")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_runs (run_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, status VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, error_json VARCHAR)")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_observations (run_id VARCHAR NOT NULL, variable_id VARCHAR NOT NULL, sequence BIGINT NOT NULL, recorded_at TIMESTAMP NOT NULL, occurred_at TIMESTAMP, value_json VARCHAR, artifact_digest VARCHAR, ref_uri VARCHAR, range_start TIMESTAMP, range_end TIMESTAMP, PRIMARY KEY(run_id, sequence))")
            self._execute(conn, "CREATE TABLE IF NOT EXISTS experiment_artifacts (digest VARCHAR PRIMARY KEY, filename VARCHAR NOT NULL, media_type VARCHAR, byte_length BIGINT NOT NULL, created_at TIMESTAMP NOT NULL)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_variables_template_idx ON experiment_variables(template_id)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_runs_template_idx ON experiment_runs(template_id, status, started_at)")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS experiment_observations_variable_idx ON experiment_observations(variable_id, run_id, sequence)")

    @staticmethod
    def _execute(conn: Any, statement: str, parameters: list[Any] | None = None):
        """Keep the ledger SQL portable across DuckDB and PostgreSQL."""
        if conn.__class__.__module__.startswith("psycopg"):
            statement = statement.replace("?", "%s")
        return conn.execute(statement, parameters or [])

    def _active(self, conn: Any, run_id: str) -> None:
        # All observations are append-only while running.  A terminal run is a
        # durable scientific record, not a mutable dashboard row.
        row = self._execute(conn, "SELECT status FROM experiment_runs WHERE run_id=?", [run_id]).fetchone()
        if row is None: raise KeyError(run_id)
        if row[0] != "running": raise ValueError("experiment run is terminal")
    def define(self, name: str) -> dict[str, Any]:
        if not name.strip(): raise ValueError("experiment name is required")
        with self.store._lock, self.store._write_conn() as conn:
            # Defining a study repeatedly is normal for scripts imported more
            # than once. The name therefore identifies a reusable declaration.
            row = self._execute(conn, "SELECT template_id, created_at FROM experiment_templates WHERE name=?", [name]).fetchone()
            if row: return {"template_id": row[0], "name": name, "created_at": row[1]}
            item = {"template_id": str(uuid4()), "name": name, "created_at": _now()}
            self._execute(conn, "INSERT INTO experiment_templates VALUES (?, ?, ?)", [item["template_id"], item["name"], _database_timestamp(item["created_at"])])
            return item
    def declare(self, template_id: str, label: str, role: str, kind: str, metadata: dict[str, Any]) -> dict[str, Any]:
        if not label.strip() or role not in {"input", "output", "annotation"}: raise ValueError("a non-empty label and valid role are required")
        with self.store._lock, self.store._write_conn() as conn:
            row = self._execute(conn, "SELECT variable_id, role, kind, metadata_json, created_at FROM experiment_variables WHERE template_id=? AND label=?", [template_id, label]).fetchone()
            if row:
                # A label cannot silently change shape: an old experiment must
                # remain interpretable after the script evolves.
                if (row[1], row[2], row[3]) != (role, kind, _json(metadata)): raise ValueError(f"variable {label!r} was already declared differently")
                return {"variable_id": row[0], "label": label, "role": role, "kind": kind, "metadata": metadata, "created_at": timestamp(row[4]), "created": False}
            variable_id, created_at = str(uuid4()), _now()
            self._execute(conn, "INSERT INTO experiment_variables VALUES (?, ?, ?, ?, ?, ?, ?)", [variable_id, template_id, label, role, kind, _json(metadata), _database_timestamp(created_at)])
            return {"variable_id": variable_id, "label": label, "role": role, "kind": kind, "metadata": metadata, "created_at": created_at, "created": True}
    def start(self, template_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        # Metadata belongs to this execution, while declarations belong to the
        # reusable study. This is what lets a parameter sweep share variables.
        run = {"run_id": str(uuid4()), "template_id": template_id, "status": "running", "metadata": metadata, "started_at": _now()}
        with self.store._lock, self.store._write_conn() as conn:
            self._execute(conn, "INSERT INTO experiment_runs (run_id,template_id,status,metadata_json,started_at) VALUES (?,?,?,?,?)", [run["run_id"], template_id, "running", _json(metadata), _database_timestamp(run["started_at"])])
        return run
    def observe(self, run_id: str, variable_id: str, *, value: Any = None, occurred_at: datetime | None = None, artifact_digest: str | None = None, ref_uri: str | None = None, interval: tuple[datetime,datetime] | None = None) -> dict[str, Any]:
        with self.store._lock, self.store._write_conn() as conn:
            self._active(conn, run_id)
            # Wall clocks can collide or arrive out of order. Sequence is the
            # authoritative order of observations within one experiment.
            seq = int(self._execute(conn, "SELECT coalesce(max(sequence),0)+1 FROM experiment_observations WHERE run_id=?", [run_id]).fetchone()[0])
            recorded = _now()
            self._execute(conn, "INSERT INTO experiment_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, variable_id, seq, _database_timestamp(recorded), _database_timestamp(occurred_at) if occurred_at else None, _json(value) if value is not None else None, artifact_digest, ref_uri, _database_timestamp(interval[0]) if interval else None, _database_timestamp(interval[1]) if interval else None])
        return {"sequence": seq, "recorded_at": recorded}
    def finish(self, run_id: str, status: str, error: Any = None) -> dict[str, Any]:
        if status not in {"succeeded", "failed"}: raise ValueError("status must be succeeded or failed")
        with self.store._lock, self.store._write_conn() as conn:
            self._active(conn, run_id)
            finished = _now(); self._execute(conn, "UPDATE experiment_runs SET status=?, finished_at=?, error_json=? WHERE run_id=?", [status, _database_timestamp(finished), _json(error) if error else None, run_id])
        return {"run_id": run_id, "status": status, "finished_at": finished}
    def attach_file(self, run_id: str, variable_id: str, filename: str, media_type: str | None, data: bytes) -> dict[str, Any]:
        digest = sha256(data).hexdigest(); target = self.artifact_dir / digest
        # The digest is both an integrity check and deduplication key. Never
        # overwrite an existing artifact: identical bytes are the same object.
        if not target.exists(): target.write_bytes(data)
        with self.store._lock, self.store._write_conn() as conn:
            self._active(conn, run_id)
            self._execute(conn, "INSERT INTO experiment_artifacts VALUES (?, ?, ?, ?, ?) ON CONFLICT(digest) DO NOTHING", [digest, filename, media_type, len(data), _database_timestamp(_now())])
        self.observe(run_id, variable_id, artifact_digest=digest)
        return {"digest": digest, "filename": filename, "byte_length": len(data)}

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
    ) -> list[dict[str, Any]]:
        clauses = ["template_id=?"]
        parameters: list[Any] = [template_id]
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
    ) -> list[dict[str, Any]]:
        clauses = ["r.template_id=?"]
        parameters: list[Any] = [template_id]
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
    def timeseries(self, *, observed: str | Point, unit: str | None = None, **metadata: Any): return self._make("timeseries", observed=str(observed.uri if isinstance(observed, Point) else observed), unit=unit, **metadata)

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
        source = f"experiment/{self.run_id}"; ref_name = self.label
        if hasattr(rows, "to_pylist"):
            rows = [(item.get("time", item.get("ts")), item["value"]) for item in rows.to_pylist()]
        elif hasattr(rows, "to_dicts"):
            rows = [(item.get("time", item.get("ts")), item["value"]) for item in rows.to_dicts()]
        rows = [(timestamp(when), value) for when, value in rows]
        if not rows: return None
        # Register graph metadata before writing data. This makes the output
        # discoverable by its observed plant property immediately.
        self.study.ac.register_streams([{
            "source_id": source,
            "ref_name": ref_name,
            "point_uri": observed,
            "unit": metadata.get("unit"),
            "label": self.label,
            "value_kind": "numeric",
        }])
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
        self.output = ExperimentOutputs(self)

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

    def finish(self):
        result = self.study.client.finish_experiment(self.run_id)
        self.status = result["status"]
        self.finished_at = timestamp(result["finished_at"])
        self.study._experiment = None
        return result

    def fail(self, error: BaseException | Any):
        result = self.study.client.finish_experiment(self.run_id, failed=True, error={"type": type(error).__name__, "message": str(error)})
        self.status = result["status"]
        self.finished_at = timestamp(result["finished_at"])
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
    def __init__(self, study: "Study", variables: list[ExperimentVariable] | None = None):
        self.study = study
        self._selection = variables

    def _items(self) -> list[ExperimentVariable]:
        if self._selection is not None:
            return list(self._selection)
        variables = [
            variable
            for collection in self.study._collections.values()
            for variable in collection.values()
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

    def where(self, *, role: str | None = None, kind: str | None = None) -> "VariableCatalog":
        return VariableCatalog(
            self.study,
            [
                variable
                for variable in self._items()
                if (role is None or variable.role == role)
                and (kind is None or variable.kind == kind)
            ],
        )

    def frame(self) -> pl.DataFrame:
        variables = self._items()
        if not variables:
            return pl.DataFrame()

        base_columns = {
            "variable_id", "label", "role", "kind", "metadata", "created_at",
        }
        metadata_columns = sorted({
            key
            for variable in variables
            for key, value in variable.metadata.items()
            if value is not None and key not in base_columns
        })
        return pl.from_dicts(
            [
                {
                    "variable_id": variable.variable_id,
                    "label": variable.label,
                    "role": variable.role,
                    "kind": variable.kind,
                    "metadata": variable.metadata,
                    "created_at": variable.created_at,
                    **{key: variable.metadata.get(key) for key in metadata_columns},
                }
                for variable in variables
            ],
            strict=False,
            infer_schema_length=None,
        )


class ExperimentCollection:
    """A lazily filtered selection of a Study's Experiments."""
    def __init__(self, study: "Study", **filters: Any):
        self.study = study
        self._filters = filters

    def where(
        self,
        *,
        status: str | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        metadata: dict[str, Any] | None = None,
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
        return ExperimentCollection(self.study, **filters)

    def all(self) -> list[Experiment]:
        return [
            Experiment(self.study, item)
            for item in self.study.client.list_experiments(self.study.study_id, **self._filters)
        ]

    def frame(self) -> pl.DataFrame:
        items = [experiment.as_dict() for experiment in self.all()]
        return pl.from_dicts(items, strict=False) if items else pl.DataFrame()

    def get(self, experiment_id: str) -> Experiment:
        item = self.study.client.get_experiment(experiment_id)
        if item["template_id"] != self.study.study_id:
            raise KeyError(experiment_id)
        return Experiment(self.study, item)

    def observations(self, variable: ExperimentVariable | str | None = None) -> "ObservationCollection":
        return ObservationCollection(self.study, variable=variable, **self._filters)


class ObservationCollection:
    """A selection of persisted observations materialized as objects or data."""
    def __init__(
        self,
        study: "Study",
        *,
        run_id: str | None = None,
        variable: ExperimentVariable | str | None = None,
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

    def _rows(self) -> list[dict[str, Any]]:
        return self.study.client.list_experiment_observations(
            self.study.study_id,
            run_id=self.run_id,
            variable_id=self.variable.variable_id if self.variable else None,
            **self.experiment_filters,
        )

    def all(self) -> list[Observation]:
        observations = []
        for item in self._rows():
            variable = self.study.variables[item["variable_id"]]
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
        items = [observation.as_dict() for observation in self.all()]
        return pl.from_dicts(items, strict=False) if items else pl.DataFrame()

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
    def start(self, metadata: dict | None = None) -> Experiment:
        # Variable objects deliberately route through one active experiment;
        # nested/concurrent experiments on the same Study are not ambiguous.
        if self._experiment is not None: raise RuntimeError("finish the active experiment before starting another")
        self._experiment = Experiment(self, self.client.start_experiment(self.study_id, metadata)); return self._experiment

class StudyService:
    """Top-level `ac.study` entry point; `define` is idempotent by study name."""
    def __init__(self, ac: Any): self.ac = ac
    def _hydrate(self, item: dict[str, Any]) -> Study:
        variables = self.ac.client.list_experiment_variables(item["template_id"])
        return Study(self.ac, item, variables)
    def define(self, name: str) -> Study:
        return self._hydrate(self.ac.client.define_experiment(name))
    def get(self, name_or_id: str) -> Study:
        return self._hydrate(self.ac.client.get_experiment_study(name_or_id))
    def all(self) -> list[Study]:
        return [self._hydrate(item) for item in self.ac.client.list_experiment_studies()]
