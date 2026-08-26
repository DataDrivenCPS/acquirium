"""Reusable, variable-centric experiment tracking."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any
from uuid import uuid4
from dateutil import parser as dtparser

UTC = timezone.utc

# Server receipt time is deliberately distinct from a caller's occurred_at.
# The former answers when Acquirium learned something; the latter answers when
# the outside-world event happened.
def _now() -> datetime: return datetime.now(UTC)
def timestamp(value: datetime | str) -> datetime:
    """Parse an ISO timestamp, treating a timezone-less value as UTC."""
    parsed = dtparser.isoparse(value) if isinstance(value, str) else value
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
def _json(value: Any) -> str:
    try: return json.dumps(value, sort_keys=True, default=str)
    except TypeError as error: raise ValueError("experiment values must be JSON serializable") from error

class ExperimentStore:
    """DuckDB persistence used by the manager and intentionally small HTTP API."""
    def __init__(self, store: Any, artifact_dir: Path) -> None:
        self.store, self.artifact_dir = store, artifact_dir
        artifact_dir.mkdir(parents=True, exist_ok=True)
        # Keep the experiment ledger beside canonical time-series storage.  The
        # ledger stores small provenance records; time-series samples continue
        # to live in the normal Acquirium tables.
        with store._lock, store._write_conn() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS experiment_templates (template_id VARCHAR PRIMARY KEY, name VARCHAR UNIQUE NOT NULL, created_at TIMESTAMP NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS experiment_variables (variable_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, label VARCHAR NOT NULL, role VARCHAR NOT NULL, kind VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, UNIQUE(template_id, label))")
            conn.execute("CREATE TABLE IF NOT EXISTS experiment_runs (run_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, status VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, error_json VARCHAR)")
            conn.execute("CREATE TABLE IF NOT EXISTS experiment_observations (run_id VARCHAR NOT NULL, variable_id VARCHAR NOT NULL, sequence BIGINT NOT NULL, recorded_at TIMESTAMP NOT NULL, occurred_at TIMESTAMP, value_json VARCHAR, artifact_digest VARCHAR, ref_uri VARCHAR, range_start TIMESTAMP, range_end TIMESTAMP, PRIMARY KEY(run_id, sequence))")
            conn.execute("CREATE TABLE IF NOT EXISTS experiment_artifacts (digest VARCHAR PRIMARY KEY, filename VARCHAR NOT NULL, media_type VARCHAR, byte_length BIGINT NOT NULL, created_at TIMESTAMP NOT NULL)")

    def _active(self, conn: Any, run_id: str) -> None:
        # All observations are append-only while running.  A terminal run is a
        # durable scientific record, not a mutable dashboard row.
        row = conn.execute("SELECT status FROM experiment_runs WHERE run_id=?", [run_id]).fetchone()
        if row is None: raise KeyError(run_id)
        if row[0] != "running": raise ValueError("experiment run is terminal")
    def define(self, name: str) -> dict[str, Any]:
        if not name.strip(): raise ValueError("experiment name is required")
        with self.store._lock, self.store._write_conn() as conn:
            # Defining a study repeatedly is normal for scripts imported more
            # than once. The name therefore identifies a reusable declaration.
            row = conn.execute("SELECT template_id, created_at FROM experiment_templates WHERE name=?", [name]).fetchone()
            if row: return {"template_id": row[0], "name": name, "created_at": row[1]}
            item = {"template_id": str(uuid4()), "name": name, "created_at": _now()}
            conn.execute("INSERT INTO experiment_templates VALUES (?, ?, ?)", list(item.values()))
            return item
    def declare(self, template_id: str, label: str, role: str, kind: str, metadata: dict[str, Any]) -> dict[str, Any]:
        if not label.strip() or role not in {"input", "output", "annotation"}: raise ValueError("a non-empty label and valid role are required")
        with self.store._lock, self.store._write_conn() as conn:
            row = conn.execute("SELECT variable_id, role, kind, metadata_json FROM experiment_variables WHERE template_id=? AND label=?", [template_id, label]).fetchone()
            if row:
                # A label cannot silently change shape: an old experiment must
                # remain interpretable after the script evolves.
                if (row[1], row[2], row[3]) != (role, kind, _json(metadata)): raise ValueError(f"variable {label!r} was already declared differently")
                return {"variable_id": row[0], "label": label, "role": role, "kind": kind, "metadata": metadata}
            variable_id = str(uuid4())
            conn.execute("INSERT INTO experiment_variables VALUES (?, ?, ?, ?, ?, ?, ?)", [variable_id, template_id, label, role, kind, _json(metadata), _now()])
            return {"variable_id": variable_id, "label": label, "role": role, "kind": kind, "metadata": metadata}
    def start(self, template_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        # Metadata belongs to this execution, while declarations belong to the
        # reusable study. This is what lets a parameter sweep share variables.
        run = {"run_id": str(uuid4()), "template_id": template_id, "status": "running", "metadata": metadata, "started_at": _now()}
        with self.store._lock, self.store._write_conn() as conn:
            conn.execute("INSERT INTO experiment_runs (run_id,template_id,status,metadata_json,started_at) VALUES (?,?,?,?,?)", [run["run_id"], template_id, "running", _json(metadata), run["started_at"]])
        return run
    def observe(self, run_id: str, variable_id: str, *, value: Any = None, occurred_at: datetime | None = None, artifact_digest: str | None = None, ref_uri: str | None = None, interval: tuple[datetime,datetime] | None = None) -> dict[str, Any]:
        with self.store._lock, self.store._write_conn() as conn:
            self._active(conn, run_id)
            # Wall clocks can collide or arrive out of order. Sequence is the
            # authoritative order of observations within one experiment.
            seq = int(conn.execute("SELECT coalesce(max(sequence),0)+1 FROM experiment_observations WHERE run_id=?", [run_id]).fetchone()[0])
            recorded = _now()
            conn.execute("INSERT INTO experiment_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, variable_id, seq, recorded, occurred_at, _json(value) if value is not None else None, artifact_digest, ref_uri, interval[0] if interval else None, interval[1] if interval else None])
        return {"sequence": seq, "recorded_at": recorded}
    def finish(self, run_id: str, status: str, error: Any = None) -> dict[str, Any]:
        if status not in {"succeeded", "failed"}: raise ValueError("status must be succeeded or failed")
        with self.store._lock, self.store._write_conn() as conn:
            self._active(conn, run_id)
            finished = _now(); conn.execute("UPDATE experiment_runs SET status=?, finished_at=?, error_json=? WHERE run_id=?", [status, finished, _json(error) if error else None, run_id])
        return {"run_id": run_id, "status": status, "finished_at": finished}
    def attach_file(self, run_id: str, variable_id: str, filename: str, media_type: str | None, data: bytes) -> dict[str, Any]:
        digest = sha256(data).hexdigest(); target = self.artifact_dir / digest
        # The digest is both an integrity check and deduplication key. Never
        # overwrite an existing artifact: identical bytes are the same object.
        if not target.exists(): target.write_bytes(data)
        with self.store._lock, self.store._write_conn() as conn:
            self._active(conn, run_id)
            conn.execute("INSERT INTO experiment_artifacts VALUES (?, ?, ?, ?, ?) ON CONFLICT(digest) DO NOTHING", [digest, filename, media_type, len(data), _now()])
        self.observe(run_id, variable_id, artifact_digest=digest)
        return {"digest": digest, "filename": filename, "byte_length": len(data)}

@dataclass(frozen=True)
class Point:
    """A deliberately small semantic handle; storage only needs its URI."""
    uri: str

class _Builder:
    """Delay declaration until the caller selects the variable's value kind."""
    def __init__(self, study: "Study", label: str, role: str): self.study, self.label, self.role = study, label, role
    def _make(self, kind: str, **metadata: Any) -> "ExperimentVariable":
        item = self.study.client.declare_experiment_variable(self.study.study_id, label=self.label, role=self.role, kind=kind, metadata=metadata)
        self.study._metadata[item["variable_id"]] = metadata
        return ExperimentVariable(self.study, item["variable_id"], self.label, kind)
    def json(self, **metadata: Any): return self._make("json", **metadata)
    def text(self, **metadata: Any): return self._make("text", **metadata)
    def scalar(self, *, unit: str | None = None, **metadata: Any): return self._make("scalar", unit=unit, **metadata)
    def file(self, *, media_type: str | None = None, **metadata: Any): return self._make("file", media_type=media_type, **metadata)
    def timeseries(self, *, observed: str | Point, unit: str | None = None, **metadata: Any): return self._make("timeseries", observed=str(observed.uri if isinstance(observed, Point) else observed), unit=unit, **metadata)

class ExperimentVariable:
    def __init__(self, study: "Study", variable_id: str, label: str, kind: str): self.study, self.variable_id, self.label, self.kind = study, variable_id, label, kind
    @property
    def run_id(self) -> str:
        if self.study._experiment is None: raise RuntimeError("start an experiment before mutating variables")
        return self.study._experiment.run_id
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
        self.study.ac.register_streams([{ "source_id": source, "ref_name": ref_name, "point_uri": observed, "unit": metadata.get("unit"), "label": self.label }])
        self.study.ac.insert_timeseries(source, ref_name, rows, point_uri=observed)
        return self.use(self.study.ac.reference_uri(source, ref_name), interval=(min(x[0] for x in rows), max(x[0] for x in rows)))

class Experiment:
    """One execution of a Study; variables route writes to this active object."""
    def __init__(self, study: "Study", run_id: str): self.study, self.run_id = study, run_id
    def finish(self):
        result = self.study.client.finish_experiment(self.run_id); self.study._experiment = None; return result
    def fail(self, error: BaseException | Any):
        result = self.study.client.finish_experiment(self.run_id, failed=True, error={"type": type(error).__name__, "message": str(error)}); self.study._experiment = None; return result

class Study:
    """Reusable variable declarations plus a single active Experiment."""
    def __init__(self, ac: Any, item: dict): self.ac, self.client, self.study_id, self.name, self._experiment, self._metadata = ac, ac.client, item["template_id"], item["name"], None, {}
    def input(self, label: str): return _Builder(self, label, "input")
    def output(self, label: str): return _Builder(self, label, "output")
    def log(self, label: str, **metadata: Any):
        """Declare an append-only, timestamped log variable."""
        return _Builder(self, label, "annotation")._make("log", **metadata)
    def start(self, metadata: dict | None = None) -> Experiment:
        # Variable objects deliberately route through one active experiment;
        # nested/concurrent experiments on the same Study are not ambiguous.
        if self._experiment is not None: raise RuntimeError("finish the active experiment before starting another")
        self._experiment = Experiment(self, self.client.start_experiment(self.study_id, metadata)["run_id"]); return self._experiment

class StudyService:
    """Top-level `ac.study` entry point; `define` is idempotent by study name."""
    def __init__(self, ac: Any): self.ac = ac
    def define(self, name: str) -> Study: return Study(self.ac, self.ac.client.define_experiment(name))
