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
from rdflib import Graph, Literal, URIRef

from acquirium.Apps.base import app_source_id
from acquirium.internals._log import configure_logging, timed_debug as _timed_debug
from acquirium.internals.app_utils import app_uri_for, app_type_uri, add_literal_or_uri
from acquirium.internals.internals_namespaces import *
from acquirium.internals.models import AppOutputSpec, AppRunRequest, AppSpec, compute_ref_uri

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium
    from acquirium.Drivers.Driver import Driver

logger = logging.getLogger("acquirium.apps.runner")


@ray.remote
def _app_run_task(app: Any, ctx: Any) -> list:
    """Execute one app run. Stateless: reads ``ctx`` (which carries the built
    state) and returns outputs without mutating the actor. Dispatched by
    :class:`AppRunner` so runs can execute in parallel off the actor thread.
    """
    return app.run(ctx)


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
        graph = self._app_spec_graph(self.spec)
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
        app_uri = app_uri_for(self.spec.name)
        query = f"""
        DELETE {{
          ?app ?ap ?ao .
          ?point ?pp ?po .
          ?ref ?rp ?ro .
        }} WHERE {{
          VALUES ?app {{ <{app_uri}> }}
          {{ ?app ?ap ?ao . }}
          UNION {{ ?app <{PRODUCES}> ?point . ?point ?pp ?po . }}
          UNION {{
            ?app <{PRODUCES}> ?point .
            ?point <{HAS_EXTERNAL_REFERENCE}> ?ref .
            ?ref ?rp ?ro .
          }}
        }}
        """
        self.sparql_update(query)
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

    def _app_spec_graph(self, spec: AppSpec) -> Graph:
        app_uri = URIRef(app_uri_for(spec.name))
        source_id = app_source_id(spec.name)
        graph = Graph()

        graph.add((app_uri, RDF.type, APP))
        graph.add((app_uri, RDFS.label, Literal(spec.name)))
        if spec.app_type:
            graph.add((app_uri, RDF.type, app_type_uri(spec.app_type)))

        if spec.version:
            graph.add((app_uri, HAS_VERSION, Literal(spec.version)))
        if spec.queries:
            graph.add((app_uri, APP_QUERY, Literal(json.dumps(spec.queries, sort_keys=True, ensure_ascii=True))))
        if spec.params:
            graph.add((app_uri, APP_PARAMS, Literal(json.dumps(spec.params, sort_keys=True, ensure_ascii=True))))

        for dep in spec.depends_on:
            graph.add((app_uri, DEPENDS_ON, URIRef(dep)))

        for out in spec.outputs:
            point_uri = URIRef(out.point_uri)
            ref_uri = compute_ref_uri(source_id, out.point_uri)

            graph.add((app_uri, PRODUCES, point_uri))
            graph.add((point_uri, RDF.type, VIRTUAL_POINT))
            graph.add((point_uri, HAS_EXTERNAL_REFERENCE, ref_uri))
            graph.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(source_id)))
            graph.add((ref_uri, ACQUIRIUM_REF_NAME, Literal(out.point_uri)))
            graph.add((ref_uri, RDF.type, STREAM))
            if out.kind in {"event", "trigger"}:
                graph.add((ref_uri, RDF.type, EVENT_STREAM))
                graph.add((ref_uri, ACQUIRIUM_VALUE_KIND, Literal("text")))
            else:
                graph.add((ref_uri, RDF.type, TIMESERIES_STREAM))
                graph.add((ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")))

            graph.add((ref_uri, STORAGE_BACKEND, Literal(out.storage_backend or "timescale")))

            add_literal_or_uri(graph, point_uri, HAS_QUANTITY_KIND, out.quantity_kind)
            add_literal_or_uri(graph, point_uri, HAS_UNIT, out.unit)
            add_literal_or_uri(graph, point_uri, DATA_SOURCE, out.data_source)
            for dep in spec.depends_on:
                graph.add((point_uri, IS_CALCULATED_FROM, URIRef(dep)))
        return graph

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
        self._load_app()
        self.build_query()
        self.build_app()
        # Seed the source generation the query was built against so the run
        # phase can detect a stale query after later graph mutations.
        try:
            self.source_version = int(self.acquirium_cli.graph_status()["source_version"])
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
    ) -> dict[str, Any]:
        """Schedule execution and return immediately.

        One-shot: dispatch a single run task and return its ``run_id``.
        Keep-alive: start a background loop that dispatches a run every
        ``interval`` seconds until :meth:`stop`. In both cases the actual
        ``app.run`` executes in a stateless Ray task; this actor only
        schedules and monitors it.
        """
        self._loop = asyncio.get_running_loop()
        params = params or {}
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
            outputs = await ref
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
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = str(exc)
            self.logger.exception("run %s failed", run_id)
        finally:
            record["finished_at"] = datetime.now(timezone.utc).isoformat()

    async def _run_loop(self, interval: float, start, end, params: dict[str, Any]) -> None:
        """Keep-alive loop: dispatch a run each interval, rebuilding the query
        when the server's source generation advances (mirrors DriverRunner.run).
        The state from build_app is reused, so the model trains once and each
        dispatched run is an inference.
        """
        self.logger.info("keep-alive loop start '%s' (interval=%.1fs)", self.spec.name, interval)
        self._dispatch_run(start, end, params)
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass
            # A version poll failure must not skip the run, so guard it apart.
            try:
                source_version = int(self.acquirium_cli.graph_status()["source_version"])
                if source_version != self.source_version:
                    self.source_version = source_version
                    self.build_query()
            except Exception:
                self.logger.exception("query refresh failed; keeping previous query")
            self._dispatch_run(start, end, params)
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
        return {
            "name": self.spec.name,
            "build": self._build_status,
            "queries": list(self.queries.keys()),
            "state": type(self.state).__name__ if self.state is not None else None,
            "keep_alive": self._keep_alive,
            "runs": [
                {k: v for k, v in r.items() if not k.startswith("_")}
                for r in self._runs.values()
            ],
        }
