from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import shutil
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

import ray

from acquirium.Apps.base import app_source_id
from acquirium.internals._log import configure_logging, timed_debug as _timed_debug
from acquirium.internals.app_utils import app_deregister_update, app_spec_graph
from acquirium.internals.internals_namespaces import *
from acquirium.internals.models import AppSpec
from acquirium.internals.scheduling import IntervalScheduler
from acquirium.Apps.provenance import ProvenanceWriter

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium
    from acquirium.Drivers.Driver import Driver

logger = logging.getLogger("acquirium.apps.runner")

# Floor for graph-change polling when an app runs faster than this. Mirrors
# the driver runner: graph mutations are rare, so a fast run cadence must not
# turn into a fast poll of the server.
DEFAULT_GRAPH_POLL_INTERVAL = 10.0


@ray.remote
def _app_run_task(app: Any, ctx: Any) -> tuple[list, list[str]]:
    """Execute one app run. Stateless: reads ``ctx`` (which carries the built
    state) and returns ``(outputs, observed_reads)`` without mutating the
    actor. Dispatched by :class:`AppRunner` so runs can execute in parallel
    off the actor thread. The read-recording scope opens *here*, in the
    worker where the values are actually fetched.
    """
    from acquirium.internals.read_recorder import recording_reads

    with recording_reads() as reads:
        outputs = app.run(ctx)
    return outputs, sorted(reads)


@ray.remote
class AppRunner:
    """One Ray actor per registered app; owns that app's lifecycle.

    Constructed with the app's :class:`AppSpec` (which carries the app's
    Python source). ``register()`` persists the source under the app storage
    dir and writes the app's registration graph back to the server. Query
    building and execution land here later.
    """

    def __init__(
        self,
        spec: AppSpec,
        app_storage_root: Path,
        acquirium_cli: "Acquirium",
    ):
        # Ray workers don't inherit the server process's logging config.
        configure_logging()
        self.spec = spec
        # The app's RDF registration/build state has a separate owner from
        # output stream data. Expose it on both this actor and loaded App.
        self.source_id = app_source_id(spec.name)
        self.app_storage_root = Path(app_storage_root)
        self.acquirium_cli = acquirium_cli
        self.logger = logging.getLogger(f"acquirium.app.{spec.name}")

        # Populated by setup(): the loaded App instance, its resolved query
        # bundle, and whatever build_app() returns (e.g. a trained model).
        self.app: Any | None = None
        self.query: Any | None = None
        self.queries: dict[str, Any] = {}
        self.state: Any | None = None
        self.source_version = 0
        self._params: dict[str, Any] = {}
        self._build_status = "pending"

        # Run scheduling / monitoring, all owned by this actor.
        self._runs: dict[str, dict[str, Any]] = {}
        self._run_counter = 0
        self._keep_alive = False
        self._loop_task: asyncio.Task | None = None
        # Set by stop() to break the keep-alive loop.
        self._stop_event = asyncio.Event()
        # Captured when run() starts so the sync stop() can flip the event on
        # the loop thread via call_soon_threadsafe.
        self._loop: asyncio.AbstractEventLoop | None = None
        # Declared + observed provenance, written loop-safely to the app's
        # own provenance graph (see Apps.provenance).
        self.provenance = ProvenanceWriter(spec.name, acquirium_cli)
        self.provenance.set_outputs(o.point_uri for o in spec.outputs)
        # Overrun policy (set per run request) and the live scheduler.
        self._max_in_flight = 1
        self._run_timeout: float | None = None
        self._scheduler: IntervalScheduler | None = None
        # Graph-change polling runs on its own cadence, not per tick: a fast
        # data cadence must not become a fast poll of the server. None means
        # "derive from the interval" (max(interval, DEFAULT_GRAPH_POLL_INTERVAL)).
        self.graph_poll_interval: float | None = None
        self._last_graph_poll = 0.0

    @staticmethod
    def _safe_entry_file(entry_file: str | None) -> str:
        ef = (entry_file or "app.py").replace("\\", "/")
        if ef.startswith("/") or ".." in ef.split("/"):
            ef = "app.py"
        return ef

    def _persist_source(self) -> None:
        """Write the shipped app source (and load metadata) under the app dir."""
        entry_file = self._safe_entry_file(self.spec.entry_file)
        app_dir = self.app_storage_root / self.spec.name
        app_dir.mkdir(parents=True, exist_ok=True)
        if self.spec.source_code:
            (app_dir / entry_file).write_text(self.spec.source_code)
        meta = {"entry_file": entry_file, "app_class": self.spec.app_class}
        (app_dir / "app.json").write_text(
            json.dumps(meta, ensure_ascii=True, sort_keys=True)
        )

    def register(self) -> dict[str, Any]:
        """Persist the app's source and write its registration graph.

        Called synchronously (``ray.get``) by :class:`AppSupervisor` so the
        graph write completes — and races with other apps' writes are
        serialized by the supervisor's lock — before registration returns.
        """
        self._persist_source()
        # The app owns its graph and output streams under this source id —
        # register it like every driver does, so the server's datasource
        # registry knows the owner (the task host already does the same).
        self.acquirium_cli.register_datasource(self.source_id)
        graph = app_spec_graph(self.spec)
        self.insert_graph(
            graph.serialize(format="turtle"),
            format="turtle",
            replace=False,
        )
        self.logger.info(
            "Registered app '%s' (%d output stream(s))",
            self.spec.name, len(self.spec.outputs),
        )
        return {
            "name": self.spec.name,
            "outputs": [o.point_uri for o in self.spec.outputs],
        }

    def deregister(self) -> dict[str, Any]:
        """Inverse of :meth:`register`: strip this app's registration triples.

        Removes every triple describing the app node, the virtual points it
        produces, and those points' external references, then (server-side)
        advances the source generation so keep-alive workers rebuild. Driven only by
        the app URI, so it also cleans up triples the build phase may have
        added on the points, not just what register() wrote.
        """
        self.sparql_update(app_deregister_update(self.spec.name))
        # The provenance graph is a separate source; the DELETE above only
        # knows the registration triples.
        try:
            self.acquirium_cli.insert_graph(
                "", format="turtle", replace=True, source_id=self.provenance.source_id,
            )
        except Exception:
            self.logger.warning("provenance graph cleanup failed for '%s'", self.spec.name, exc_info=True)
        self.logger.info("Deregistered app '%s' from the graph", self.spec.name)
        return {"name": self.spec.name}

    def insert_graph(self, rdf_graph: str, *, format: str = "turtle", replace: bool = False) -> None:
        """Write RDF to this app's graph; ownership is never caller-selected."""
        self.acquirium_cli.insert_graph(
            rdf_graph,
            format=format,
            replace=replace,
            source_id=self.source_id,
        )

    def insert_graph_file(
        self,
        path: str | Path,
        *,
        format: str | None = None,
        replace: bool = False,
    ) -> None:
        """Read an RDF file into this app's graph; ownership is fixed."""
        self.acquirium_cli.insert_graph_file(
            path,
            format=format,
            replace=replace,
            source_id=self.source_id,
        )

    def sparql_update(self, update: str) -> dict[str, Any]:
        """Apply a SPARQL update only to this app's graph."""
        return self.acquirium_cli.sparql_update(update, source_id=self.source_id)

    # ─────────────────────── build phase ───────────────────────

    def _load_app(self):
        """Load the App class from the persisted source and instantiate it.

        The client ships ``source_code`` + ``app_class``; ``register()`` wrote
        both to the app dir. We import that file and pick the class by name
        (falling back to the sole App subclass if no name was recorded).
        """
        from acquirium.Apps.base import App

        app_dir = self.app_storage_root / self.spec.name
        entry_file = self.spec.entry_file
        app_class = self.spec.app_class
        meta_path = app_dir / "app.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                entry_file = entry_file or meta.get("entry_file")
                app_class = app_class or meta.get("app_class")
            except Exception:
                self.logger.warning("Failed to read %s", meta_path, exc_info=True)

        path = app_dir / self._safe_entry_file(entry_file)
        # Make the app dir importable so multi-file apps resolve siblings.
        if str(app_dir) not in sys.path:
            sys.path.insert(0, str(app_dir))

        module_spec = importlib.util.spec_from_file_location(
            f"acquirium_app_{self.spec.name}", str(path)
        )
        if module_spec is None or module_spec.loader is None:
            raise ValueError(f"Unable to load app file {path}")
        module = importlib.util.module_from_spec(module_spec)
        # register_pickle_by_value (below) requires the module to be reachable
        # through sys.modules under its own name.
        sys.modules[module_spec.name] = module
        module_spec.loader.exec_module(module)
        # The app class is defined in this dynamically-loaded module, which the
        # run-task worker can't import by name. Pin it to pickle by value so the
        # class (and the app instance) ships intact to _app_run_task.
        ray.cloudpickle.register_pickle_by_value(module)

        if app_class:
            cls = getattr(module, app_class, None)
            if cls is None:
                raise ValueError(f"App class {app_class!r} not found in {path}")
        else:
            candidates = [
                obj for obj in vars(module).values()
                if isinstance(obj, type) and issubclass(obj, App) and obj is not App
            ]
            if not candidates:
                raise ValueError(f"No App subclass found in {path}")
            cls = candidates[0]

        self.app = cls()
        self.app._bind_graph_api(self.acquirium_cli, self.source_id)
        self.logger.info("Loaded app '%s' (%s)", self.spec.name, cls.__name__)
        return self.app

    def _make_context(self, *, params: dict[str, Any], start=None, end=None):
        from acquirium.internals.models import AppContext

        return AppContext(
            app_id=self.spec.name,
            started_at=datetime.now(timezone.utc),
            start=start,
            end=end,
            query=self.query,
            params=params or {},
            queries=self.queries,
            state=self.state,
        )

    def build_query(self) -> None:
        """Resolve the app's query bundle against the current graph and cache it."""
        if self.app is None:
            raise RuntimeError("build_query called before the app was loaded")
        bundle = self.app.build_query(self.acquirium_cli)
        if isinstance(bundle, dict):
            self.queries = bundle
            self.query = bundle.get("default") or (
                next(iter(bundle.values())) if bundle else None
            )
        else:
            self.query = bundle
            self.queries = {"default": bundle}
        self.logger.info(
            "Built %d query/queries for app '%s'", len(self.queries), self.spec.name
        )
        self._record_declared_provenance()

    def _record_declared_provenance(self) -> None:
        """acq:mayUse — every stream the query bundle resolves to.

        Uses Query.provenance() (executes the pattern, cached); a query that
        can't be resolved right now simply contributes nothing this time.
        Written through the loop-safe writer, so this never wakes pollers.
        """
        refs: set[str] = set()
        for q in self.queries.values():
            prov = getattr(q, "provenance", None)
            if prov is None:
                continue
            try:
                refs.update(p["ref_uri"] for p in prov()["points"])
            except Exception:
                self.logger.debug("declared provenance unavailable for a query", exc_info=True)
        self.provenance.set_declared(refs)
        self.provenance.flush()

    def build_app(self) -> None:
        """Run the app's one-time build phase and cache whatever it returns.

        This is where a stateful app does expensive setup (e.g. training a
        model). The return value is held on the actor as ``self.state`` for
        the run phase to consume.
        """
        if self.app is None:
            raise RuntimeError("build_app called before the app was loaded")
        ctx = self._make_context(params=self._params)
        with _timed_debug(self.logger, "build_app app=%s", self.spec.name):
            self.state = self.app.build_app(ctx)
        self.logger.info(
            "build_app complete for '%s' (state=%s)",
            self.spec.name,
            type(self.state).__name__ if self.state is not None else "None",
        )

    def setup(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Load the app and run its build phase (build_query + build_app).

        Mirrors :meth:`DriverRunner.setup`: called once and serialized under
        the supervisor lock so build-time graph reads/writes don't race. The
        resolved query bundle and any state produced by ``build_app`` are
        cached on the actor for the run phase.

        Params default to those registered with the app (``spec.params``), so
        the build phase sees the same configuration after a server restart
        restores the app from the graph.
        """
        self._params = params if params is not None else dict(self.spec.params)
        try:
            self._load_app()
            self.build_query()
            self.build_app()
        except Exception:
            # Surface the failure in status() instead of leaving "pending"
            # forever; the supervisor logs the exception and keeps the app
            # registered so a later run can retry the build.
            self._build_status = "failed"
            raise
        # Seed the source generation the query was built against so the run
        # phase can detect a stale query after later graph mutations.
        try:
            self.source_version = self._data_generation()
        except Exception:
            self.source_version = 0
        self._build_status = "ready"
        return {
            "name": self.spec.name,
            "queries": list(self.queries.keys()),
            "state": type(self.state).__name__ if self.state is not None else None,
        }

    # ─────────────────────── run phase ───────────────────────

    async def run(
        self,
        start=None,
        end=None,
        params: dict[str, Any] | None = None,
        keep_alive: bool = False,
        interval: float = 10.0,
        max_in_flight: int = 1,
        run_timeout: float | None = None,
    ) -> dict[str, Any]:
        """Schedule execution and return immediately.

        One-shot: dispatch a single run task and return its ``run_id``.
        Keep-alive: start a background loop that dispatches a run every
        ``interval`` seconds until :meth:`stop`, keeping at most
        ``max_in_flight`` runs in flight — an interval tick that would exceed
        that is skipped and counted, never queued. ``run_timeout`` bounds one
        run's wall clock; an overrunning task is cancelled and recorded as
        "timeout". In both cases the actual ``app.run`` executes in a
        stateless Ray task; this actor only schedules and monitors it.
        """
        self._loop = asyncio.get_running_loop()
        params = params or {}
        self._max_in_flight = max(1, int(max_in_flight))
        self._run_timeout = run_timeout
        if self.app is None:
            # Build never completed (e.g. failed at registration); retry once.
            self.setup(params)

        if keep_alive:
            if self._loop_task is not None and not self._loop_task.done():
                raise RuntimeError(f"App '{self.spec.name}' is already running keep-alive")
            self._keep_alive = True
            self._stop_event.clear()
            self._loop_task = asyncio.create_task(self._run_loop(interval, start, end, params))
            return {"name": self.spec.name, "keep_alive": True, "interval": interval}

        run_id = self._dispatch_run(start, end, params)
        return {"name": self.spec.name, "run_id": run_id}

    def _dispatch_run(self, start, end, params: dict[str, Any]) -> str:
        """Launch a stateless run task and monitor it in the background."""
        self._run_counter += 1
        run_id = f"{self.spec.name}-{self._run_counter}"
        ctx = self._make_context(params=params, start=start, end=end)
        ref = _app_run_task.remote(self.app, ctx)
        record: dict[str, Any] = {
            "run_id": run_id,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "outputs": None,
            "error": None,
        }
        self._runs[run_id] = record
        record["_monitor"] = asyncio.create_task(self._monitor_run(run_id, ref))
        self._trim_runs()
        return run_id

    async def _monitor_run(self, run_id: str, ref) -> None:
        """Await a run task, emit its outputs, and record the outcome."""
        from acquirium.Apps.output_emission import emit_outputs

        record = self._runs[run_id]
        try:
            if self._run_timeout is not None:
                result = await asyncio.wait_for(ref, timeout=self._run_timeout)
            else:
                result = await ref
            outputs, reads = result
            self.provenance.add_observed(reads)
            # emit_outputs does blocking I/O (timeseries inserts, webhook
            # posts); run it off the event-loop thread so the actor's loop
            # stays responsive to other runs and to stop().
            await asyncio.to_thread(
                emit_outputs,
                self.source_id,
                outputs,
                insert_timeseries=self.acquirium_cli.client.insert_timeseries,
                logger=self.logger,
            )
            record["status"] = "done"
            record["outputs"] = len(outputs)
            # Off the loop: a provenance write is an HTTP call.
            await asyncio.to_thread(self.provenance.flush)
        except asyncio.TimeoutError:
            record["status"] = "timeout"
            record["error"] = f"run exceeded run_timeout={self._run_timeout}s"
            # force=True: a stuck user function never reaches a cancellation
            # point, so only killing the worker actually frees the slot.
            try:
                ray.cancel(ref, force=True)
            except Exception:
                self.logger.debug("ray.cancel failed for %s", run_id, exc_info=True)
            self.logger.warning(
                "run %s cancelled after %.1fs run_timeout", run_id, self._run_timeout,
            )
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = str(exc)
            self.logger.exception("run %s failed", run_id)
        finally:
            record["finished_at"] = datetime.now(timezone.utc).isoformat()

    def _data_generation(self) -> int:
        """The write generation this app watches: data_version — bumped by
        every graph write *except* provenance-graph writes — so an app
        (this one included) writing provenance never triggers a rebuild.
        Falls back to source_version against an older server."""
        status = self.acquirium_cli.graph_status()
        return int(status.get("data_version", status.get("source_version", 0)))

    def _maybe_refresh_query(self) -> None:
        """Rebuild the query when the server's source generation advances.

        Runs on its own cadence (``graph_poll_interval``), not per tick.
        A poll or rebuild failure must never skip the run, so everything is
        guarded; a rebuild failure keeps the previous query.
        """
        now = time.monotonic()
        if now - self._last_graph_poll < (self.graph_poll_interval or 0.0):
            return
        self._last_graph_poll = now
        try:
            source_version = self._data_generation()
            if source_version != self.source_version:
                self.source_version = source_version
                self.build_query()
        except Exception:
            self.logger.exception("query refresh failed; keeping previous query")

    async def _scheduled_dispatch(self, start, end, params: dict[str, Any]) -> None:
        """One keep-alive tick: refresh the query if stale, run, await the end.

        Awaiting the monitor task makes the scheduler's in-flight count span
        the run's full duration (including output emission) — that is what
        skip-on-overrun measures against.
        """
        self._maybe_refresh_query()
        run_id = self._dispatch_run(start, end, params)
        monitor = self._runs.get(run_id, {}).get("_monitor")
        if monitor is not None:
            await monitor

    async def _run_loop(self, interval: float, start, end, params: dict[str, Any]) -> None:
        """Keep-alive loop, driven by :class:`IntervalScheduler`.

        The scheduler owns the deadline grid and the overrun policy (skip and
        count, bounded in-flight); this method wires it to the app's dispatch
        and to the actor's stop event. The state from build_app is reused, so
        the model trains once and each dispatched run is an inference.
        """
        self.logger.info(
            "keep-alive loop start '%s' (interval=%.1fs, max_in_flight=%d)",
            self.spec.name, interval, self._max_in_flight,
        )
        if self.graph_poll_interval is None:
            self.graph_poll_interval = max(float(interval), DEFAULT_GRAPH_POLL_INTERVAL)
        scheduler = IntervalScheduler(
            interval,
            lambda: self._scheduled_dispatch(start, end, params),
            max_in_flight=self._max_in_flight,
            name=f"app:{self.spec.name}",
            stop_event=self._stop_event,
        )
        self._scheduler = scheduler
        await scheduler.run()
        self._keep_alive = False
        self.logger.info("keep-alive loop exit '%s'", self.spec.name)

    def _trim_runs(self, keep: int = 50) -> None:
        """Bound the run history so a long keep-alive app doesn't grow forever."""
        if len(self._runs) <= keep:
            return
        for run_id in list(self._runs)[: len(self._runs) - keep]:
            rec = self._runs[run_id]
            if rec["status"] != "running":
                self._runs.pop(run_id, None)

    def stop(self) -> dict[str, Any]:
        """Signal the keep-alive loop to exit after its current iteration.

        Sync methods run off the actor's event-loop thread, so the
        asyncio.Event must be set via the loop rather than touched directly.
        """
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(self._stop_event.set)
        else:
            # run() hasn't captured the loop yet; no coroutine is waiting.
            self._stop_event.set()
        return {"name": self.spec.name, "stopped": True}

    def status(self) -> dict[str, Any]:
        """Report build/run status for this app (the actor answers directly)."""
        sched = self._scheduler.status() if self._scheduler is not None else {}
        return {
            "name": self.spec.name,
            "build": self._build_status,
            "queries": list(self.queries.keys()),
            "state": type(self.state).__name__ if self.state is not None else None,
            "keep_alive": self._keep_alive,
            # Overrun visibility: skipped rises when runs outlast the interval.
            "in_flight": sched.get("in_flight", 0),
            "dispatched": sched.get("dispatched", 0),
            "skipped": sched.get("skipped", 0),
            "last_duration": sched.get("last_duration"),
            "provenance": self.provenance.status(),
            "runs": [
                {k: v for k, v in r.items() if not k.startswith("_")}
                for r in self._runs.values()
            ],
        }
