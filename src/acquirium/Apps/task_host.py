"""One Ray actor hosting every registered task.

Tasks are the light tier: a query plus ``fn(ctx) -> list[Output]``, no
build phase, no state, no dependencies beyond the acquirium package. That
contract is what lets all of them share a single process instead of paying
an actor (~160MB of imports) each.

Execution model — deliberately simple:

- Task bodies run **inline on the actor's event loop**. No threads, no
  per-run Ray tasks. Bodies are meant to be tiny (fetch latest, compare,
  emit); a slow one delays its neighbours' ticks, and skip-on-overrun
  (:class:`IntervalScheduler`) keeps each task from piling up on itself.
  Anything heavier belongs in an App with its own actor.
- One host-level graph poll (10s floor) refreshes every task's query via
  :meth:`Query.from_dict` when the data generation advances — one
  ``graph_status`` call per cadence for all tasks, not one per task.
- Registration persists each task under ``<app_storage_root>/<name>/``
  (``task.json`` = spec sans blob, ``fn.pkl`` = blob) and writes the shared
  registration graph via :func:`app_spec_graph`. On construction the host
  reloads every persisted task from disk, so a host restart
  (``max_restarts``) or a server restart heals without a round-trip.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ray

from acquirium.internals._log import configure_logging
from acquirium.internals.app_utils import (
    app_deregister_update,
    app_source_id,
    app_spec_graph,
)
from acquirium.internals.models import AppContext, TaskSpec
from acquirium.internals.scheduling import IntervalScheduler
from acquirium.internals.read_recorder import recording_reads
from acquirium.Apps.provenance import ProvenanceWriter

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium

logger = logging.getLogger("acquirium.apps.task_host")

TASK_META_FILE = "task.json"
TASK_BLOB_FILE = "fn.pkl"
DEFAULT_GRAPH_POLL_INTERVAL = 10.0
RUN_HISTORY = 50


class _Task:
    """Per-task live state on the host (not a Ray object)."""

    def __init__(self, spec: TaskSpec):
        self.spec = spec
        self.fn = None
        self.query = None
        self.load_error: str | None = None
        self.scheduler: IntervalScheduler | None = None
        self.loop_task: asyncio.Task | None = None
        self.stop_event = asyncio.Event()
        self.runs: dict[str, dict[str, Any]] = {}
        self.run_counter = 0
        self.keep_alive = False
        self.provenance: ProvenanceWriter | None = None


def _task_dir(root: Path, name: str) -> Path:
    return Path(root) / name


def persist_task(root: Path, spec: TaskSpec) -> None:
    """Write a task's spec (sans blob) and blob under the app storage dir."""
    d = _task_dir(root, spec.name)
    d.mkdir(parents=True, exist_ok=True)
    meta = spec.model_dump(mode="json", exclude={"fn_blob"})
    (d / TASK_META_FILE).write_text(json.dumps(meta, ensure_ascii=True, sort_keys=True))
    blob = d / TASK_BLOB_FILE
    if spec.fn_blob:
        blob.write_bytes(spec.fn_blob)
    elif blob.exists():
        blob.unlink()


def load_persisted_task(root: Path, name: str) -> TaskSpec | None:
    d = _task_dir(root, name)
    meta = d / TASK_META_FILE
    if not meta.is_file():
        return None
    data = json.loads(meta.read_text())
    blob = d / TASK_BLOB_FILE
    if blob.is_file():
        data["fn_blob"] = blob.read_bytes()
    return TaskSpec(**data)


def persisted_task_names(root: Path) -> list[str]:
    root = Path(root)
    if not root.is_dir():
        return []
    return sorted(p.name for p in root.iterdir() if (p / TASK_META_FILE).is_file())


@ray.remote(max_restarts=-1)
class TaskHost:
    """The single shared task actor; see module docstring."""

    def __init__(self, app_storage_root: Path, acquirium_cli: "Acquirium"):
        configure_logging()
        self.app_storage_root = Path(app_storage_root)
        self.acquirium_cli = acquirium_cli
        self.logger = logging.getLogger("acquirium.task_host")
        self._tasks: dict[str, _Task] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self.graph_poll_interval = DEFAULT_GRAPH_POLL_INTERVAL
        self._last_graph_poll = 0.0
        self.data_version = 0
        # Restart self-heal: reload every persisted task; a bad one is
        # recorded (needs re-registration), never lets the host fail.
        for name in persisted_task_names(self.app_storage_root):
            try:
                spec = load_persisted_task(self.app_storage_root, name)
                if spec is not None:
                    self._load(spec)
            except Exception as exc:
                self.logger.exception("task '%s' failed to reload", name)
                t = _Task(TaskSpec(name=name, fn_name="?", fn_source=""))
                t.load_error = f"reload failed: {exc}"
                self._tasks[name] = t

    # ─────────────────────── loading ───────────────────────

    def _load(self, spec: TaskSpec) -> _Task:
        from acquirium.Apps.task_fn import load_function
        from acquirium.Client.explore.core import Query

        t = _Task(spec)
        t.provenance = ProvenanceWriter(spec.name, self.acquirium_cli)
        t.provenance.set_outputs(o.point_uri for o in spec.outputs)
        try:
            t.fn = load_function(
                fn_name=spec.fn_name, fn_source=spec.fn_source,
                fn_blob=spec.fn_blob, blob_python_version=spec.python_version,
            )
            t.query = (
                Query.from_dict(spec.query, client=self.acquirium_cli.client)
                if spec.query else None
            )
        except Exception as exc:
            t.load_error = str(exc)
            self.logger.exception("task '%s' failed to load", spec.name)
        self._tasks[spec.name] = t
        self._record_declared_provenance(t)
        return t

    def _record_declared_provenance(self, t: _Task) -> None:
        """acq:mayUse — every stream the task's query resolves to."""
        if t.query is None or t.provenance is None:
            return
        try:
            refs = {p["ref_uri"] for p in t.query.provenance()["points"]}
        except Exception:
            self.logger.debug("task '%s': declared provenance unavailable", t.spec.name, exc_info=True)
            return
        t.provenance.set_declared(refs)
        t.provenance.flush()

    def _source_id(self, name: str) -> str:
        return app_source_id(name)

    # ─────────────────────── registration ───────────────────────

    def register(self, spec: TaskSpec) -> dict[str, Any]:
        """Persist, write the registration graph, load. Called via ray.get."""
        existing = self._tasks.get(spec.name)
        if existing is not None:
            self._stop_task(existing)
        persist_task(self.app_storage_root, spec)
        source_id = self._source_id(spec.name)
        self.acquirium_cli.register_datasource(source_id)
        graph = app_spec_graph(spec.to_app_spec())
        self.acquirium_cli.insert_graph(
            graph.serialize(format="turtle"), format="turtle",
            replace=False, source_id=source_id,
        )
        t = self._load(spec)
        self.logger.info("Registered task '%s' (%d output(s))", spec.name, len(spec.outputs))
        return {"name": spec.name, "outputs": [o.point_uri for o in spec.outputs],
                "load_error": t.load_error}

    def restore(self, spec: TaskSpec) -> dict[str, Any]:
        """Load an already-registered task (graph + disk exist)."""
        persist_task(self.app_storage_root, spec)
        t = self._load(spec)
        return {"name": spec.name, "load_error": t.load_error}

    def deregister(self, name: str, *, remove_source: bool = True) -> dict[str, Any]:
        t = self._tasks.pop(name, None)
        if t is not None:
            self._stop_task(t)
        self.acquirium_cli.sparql_update(
            app_deregister_update(name), source_id=self._source_id(name),
        )
        try:
            self.acquirium_cli.insert_graph(
                "", format="turtle", replace=True,
                source_id=ProvenanceWriter(name, self.acquirium_cli).source_id,
            )
        except Exception:
            self.logger.warning("provenance graph cleanup failed for task '%s'", name, exc_info=True)
        if remove_source:
            import shutil
            d = _task_dir(self.app_storage_root, name)
            if d.is_dir():
                shutil.rmtree(d, ignore_errors=True)
        return {"name": name}

    # ─────────────────────── running ───────────────────────

    def _make_context(self, t: _Task, params: dict[str, Any], start=None, end=None) -> AppContext:
        return AppContext(
            app_id=t.spec.name,
            started_at=datetime.now(timezone.utc),
            start=start, end=end,
            query=t.query,
            params={**t.spec.params, **(params or {})},
            queries={"default": t.query} if t.query is not None else {},
        )

    async def _run_once(self, t: _Task, params: dict[str, Any], start=None, end=None) -> str:
        """Execute the body inline and emit outputs. Records the run."""
        from acquirium.Apps.output_emission import emit_outputs

        t.run_counter += 1
        run_id = f"{t.spec.name}-{t.run_counter}"
        record: dict[str, Any] = {
            "run_id": run_id, "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None, "outputs": None, "error": None,
        }
        t.runs[run_id] = record
        self._trim_runs(t)
        try:
            if t.fn is None:
                raise RuntimeError(t.load_error or "task function not loaded")
            with recording_reads() as reads:
                outputs = t.fn(self._make_context(t, params, start, end)) or []
            if t.provenance is not None:
                t.provenance.add_observed(reads)
            # Output emission does HTTP; keep it off the loop so other tasks'
            # ticks and stop() stay responsive during a slow insert.
            await asyncio.to_thread(
                emit_outputs, self._source_id(t.spec.name), list(outputs),
                insert_timeseries=self.acquirium_cli.client.insert_timeseries,
                logger=self.logger,
            )
            record["status"] = "done"
            record["outputs"] = len(outputs)
            if t.provenance is not None:
                await asyncio.to_thread(t.provenance.flush)
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = str(exc)
            self.logger.exception("task run %s failed", run_id)
        finally:
            record["finished_at"] = datetime.now(timezone.utc).isoformat()
        return run_id

    def _maybe_refresh_queries(self) -> None:
        """Rebuild every task's query when the data generation advances.

        One poll for the whole host, on its own cadence — never per tick.
        Failures never skip a run; a rebuild failure keeps the old query.
        """
        now = time.monotonic()
        if now - self._last_graph_poll < self.graph_poll_interval:
            return
        self._last_graph_poll = now
        try:
            status = self.acquirium_cli.graph_status()
            version = int(status.get("data_version", status.get("source_version", 0)))
        except Exception:
            return
        if version == self.data_version:
            return
        self.data_version = version
        from acquirium.Client.explore.core import Query
        for t in self._tasks.values():
            if t.spec.query:
                try:
                    t.query = Query.from_dict(t.spec.query, client=self.acquirium_cli.client)
                    self._record_declared_provenance(t)
                except Exception:
                    self.logger.exception("task '%s': query refresh failed; keeping previous", t.spec.name)

    async def _tick(self, t: _Task, params: dict[str, Any], start, end) -> None:
        self._maybe_refresh_queries()
        await self._run_once(t, params, start, end)

    async def run(
        self, name: str, *, start=None, end=None, params: dict[str, Any] | None = None,
        keep_alive: bool = False, interval: float | None = None,
        max_in_flight: int = 1,
    ) -> dict[str, Any]:
        self._loop = asyncio.get_running_loop()
        t = self._tasks.get(name)
        if t is None:
            raise KeyError(f"Unknown task: {name}")
        params = params or {}
        if not keep_alive:
            run_id = await self._run_once(t, params, start, end)
            return {"name": name, "run_id": run_id}

        if t.loop_task is not None and not t.loop_task.done():
            raise RuntimeError(f"Task '{name}' is already running keep-alive")
        interval = float(interval if interval is not None else (t.spec.interval or 10.0))
        t.stop_event.clear()
        t.keep_alive = True
        t.scheduler = IntervalScheduler(
            interval, lambda: self._tick(t, params, start, end),
            max_in_flight=max_in_flight, name=f"task:{name}", stop_event=t.stop_event,
        )

        async def _loop():
            try:
                await t.scheduler.run()
            finally:
                t.keep_alive = False

        t.loop_task = asyncio.create_task(_loop())
        return {"name": name, "keep_alive": True, "interval": interval}

    def _stop_task(self, t: _Task) -> None:
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(t.stop_event.set)
        else:
            t.stop_event.set()

    def stop(self, name: str) -> dict[str, Any]:
        t = self._tasks.get(name)
        if t is None:
            raise KeyError(f"Unknown task: {name}")
        self._stop_task(t)
        return {"name": name, "stopped": True}

    def _trim_runs(self, t: _Task, keep: int = RUN_HISTORY) -> None:
        if len(t.runs) <= keep:
            return
        for run_id in list(t.runs)[: len(t.runs) - keep]:
            if t.runs[run_id]["status"] != "running":
                t.runs.pop(run_id, None)

    # ─────────────────────── reporting ───────────────────────

    def status(self, name: str) -> dict[str, Any]:
        t = self._tasks.get(name)
        if t is None:
            raise KeyError(f"Unknown task: {name}")
        sched = t.scheduler.status() if t.scheduler is not None else {}
        return {
            "name": name,
            "kind": "task",
            "build": "failed" if t.load_error else "ready",
            "load_error": t.load_error,
            "keep_alive": t.keep_alive,
            "in_flight": sched.get("in_flight", 0),
            "dispatched": sched.get("dispatched", 0),
            "skipped": sched.get("skipped", 0),
            "last_duration": sched.get("last_duration"),
            "provenance": t.provenance.status() if t.provenance is not None else None,
            "runs": list(t.runs.values()),
        }

    def list_tasks(self) -> list[dict[str, Any]]:
        return [
            {"name": n, "kind": "task", "keep_alive": t.keep_alive,
             "load_error": t.load_error}
            for n, t in self._tasks.items()
        ]

    def has(self, name: str) -> bool:
        return name in self._tasks
