from __future__ import annotations

import asyncio
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ray
import json
import shutil
import sys
import time
import importlib.util

from urllib.parse import unquote

from acquirium.internals._log import configure_logging, timed_debug as _timed_debug
from acquirium.internals.models import AppSpec, AppOutputSpec, AppRunRequest, compute_ref_uri
from acquirium.internals.internals_namespaces import *
from acquirium.internals.app_utils import app_uri_for, app_type_uri, add_literal_or_uri
from rdflib import URIRef, Graph, Literal

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium
    from acquirium.Driver import Driver

logger = logging.getLogger("acquirium.ray")


def _worker_pythonpath(source_dir: str) -> str:
    """Prepend ``source_dir`` to this process's PYTHONPATH for a Ray worker.

    runtime_env env_vars replace rather than extend, so the inherited value has
    to be carried over explicitly or the worker loses it.
    """
    inherited = os.environ.get("PYTHONPATH", "")
    if not inherited:
        return source_dir
    if source_dir in inherited.split(os.pathsep):
        return inherited
    return source_dir + os.pathsep + inherited


@ray.remote
class DriverRunner:
    """Run one driver's tick loop in its own Ray actor.

    The driver is constructed inside the actor so its state (DriverState,
    client connections) never crosses the process boundary. Lifecycle:

        runner = DriverRunner.remote(driver_cls, cfg, aq, interval)
        ray.get(runner.setup.remote())   # serially across actors
        run_ref = runner.run.remote()
        ...
        runner.stop.remote()             # loop exits, driver.stop() runs
        ray.get(run_ref)                 # join
    """

    def __init__(
        self,
        driver_cls: type[Driver],
        driver_cfg: dict,
        acquirium_cli: Acquirium,
        interval: float,
    ):
        # Ray workers don't inherit the server process's logging config.
        configure_logging()
        self.driver: Driver = driver_cls(acquirium_cli, driver_cfg)
        self.acquirium_cli = acquirium_cli
        self.interval = interval
        self.graph_version = 0
        self.logger = logging.getLogger(
            f"acquirium.driver.{type(self.driver).__name__}"
        )
        self._stop_event = asyncio.Event()

    def setup(self) -> None:
        """One-time driver setup.

        The caller must ray.get() these one actor at a time so setup-time
        graph writes cannot race each other (DriverSupervisor holds its lock
        across setup for exactly this reason).
        """
        self.driver.setup()
        # Seed after setup so the loop doesn't fire on_graph_change() for the
        # pre-existing graph or this driver's own setup insertions.
        try:
            self.graph_version = self.acquirium_cli.graph_version()
        except Exception:
            pass

    async def run(self) -> None:
        name = type(self.driver).__name__
        self.logger.info("Starting driver runner for %s", name)
        self._tick()
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval)
                break
            except asyncio.TimeoutError:
                pass
            # Version check failures (e.g. server briefly unreachable) must
            # not skip the tick, so they are guarded separately.
            try:
                v = self.acquirium_cli.graph_version()
                if v != self.graph_version:
                    self.graph_version = v
                    try:
                        self.driver.on_graph_change()
                    except Exception:
                        self.logger.exception("on_graph_change error")
            except Exception:
                pass
            self._tick()
        self.logger.debug("driver loop exit: %s", name)
        try:
            self.driver.stop()
        except Exception:
            self.logger.exception("stop error")

    def stop(self) -> None:
        """Signal run() to exit; driver.stop() cleanup happens there."""
        self._stop_event.set()

    def _tick(self) -> None:
        try:
            with _timed_debug(self.logger, "tick"):
                self.driver.tick()
        except Exception:
            self.logger.exception("tick error")


class DriverSupervisor:
    """Owns the DriverRunner actors of one server process, keyed by name.

    Lives in the FastAPI process. start_driver() imports the driver class,
    spawns a DriverRunner actor that connects back to this server over HTTP,
    runs setup, and starts the tick loop. The internal lock is held across
    setup so two drivers' setup-time graph writes can never race.
    """

    def __init__(self, server_url: str, server_port: int, use_ssl: bool = False):
        self.server_url = server_url
        self.server_port = int(server_port)
        self.use_ssl = bool(use_ssl)
        self._drivers: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    @property
    def base_url(self) -> str:
        return f"{'https' if self.use_ssl else 'http'}://{self.server_url}:{self.server_port}"

    def start_driver(
        self,
        *,
        spec: str,
        config: dict,
        interval: float | None = None,
        name: str | None = None,
    ) -> dict[str, Any]:
        from acquirium.cli import _import_driver_class
        from acquirium.Client.acquirium import Acquirium

        driver_section = config.get("driver", {})
        effective_interval = float(
            interval if interval is not None else driver_section.get("interval", 10.0)
        )
        driver_name = name or spec.rsplit(":", 1)[-1]

        with self._lock:
            if driver_name in self._drivers:
                raise ValueError(f"Driver '{driver_name}' is already running")

            base_dir = Path(config.get("__config_dir", Path.cwd()))
            driver_cls, source_dir = _import_driver_class(spec, base_dir=base_dir)
            aq = Acquirium(
                server_url=self.server_url,
                server_port=self.server_port,
                use_ssl=self.use_ssl,
                insert_batch_rows=int(driver_section.get("insert_batch_rows", 50_000)),
            )
            runner_cls = DriverRunner
            if source_dir is not None:
                runner_cls = DriverRunner.options(
                    runtime_env={"env_vars": {"PYTHONPATH": _worker_pythonpath(source_dir)}}
                )
            runner = runner_cls.remote(driver_cls, config, aq, effective_interval)
            try:
                ray.get(runner.setup.remote())
            except Exception:
                ray.kill(runner)
                raise
            run_ref = runner.run.remote()

            record = {
                "name": driver_name,
                "spec": spec,
                "interval": effective_interval,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "actor": runner,
                "run_ref": run_ref,
            }
            self._drivers[driver_name] = record
            logger.info("Started driver '%s' (%s, interval=%.1fs)", driver_name, spec, effective_interval)
            return self._public_info(record)

    def stop_driver(self, name: str, *, timeout: float = 10.0) -> dict[str, Any]:
        with self._lock:
            record = self._drivers.pop(name, None)
        if record is None:
            raise KeyError(f"No running driver named '{name}'")

        record["actor"].stop.remote()
        try:
            ray.get(record["run_ref"], timeout=timeout)
        except Exception:
            logger.warning("Driver '%s' did not exit within %.1fs; killing actor", name, timeout)
        ray.kill(record["actor"])
        logger.info("Stopped driver '%s'", name)
        return {"name": name, "stopped": True}

    def list_drivers(self) -> list[dict[str, Any]]:
        with self._lock:
            records = list(self._drivers.values())
        return [self._public_info(r) for r in records]

    def stop_all(self, *, timeout: float = 10.0) -> None:
        with self._lock:
            records = list(self._drivers.values())
            self._drivers.clear()
        if not records:
            return

        # Signal every driver to exit first, then join them in one window, so
        # N drivers' shutdown timeouts overlap instead of stacking serially the
        # way a loop over stop_driver() would.
        for record in records:
            try:
                record["actor"].stop.remote()
            except Exception:
                logger.exception("Failed to signal stop for driver '%s'", record["name"])

        by_ref = {record["run_ref"]: record for record in records}
        _, not_ready = ray.wait(list(by_ref), num_returns=len(by_ref), timeout=timeout)
        for ref in not_ready:
            logger.warning(
                "Driver '%s' did not exit within %.1fs; killing actor",
                by_ref[ref]["name"], timeout,
            )

        for record in records:
            ray.kill(record["actor"])
            logger.info("Stopped driver '%s'", record["name"])

    def _public_info(self, record: dict[str, Any]) -> dict[str, Any]:
        ready, _ = ray.wait([record["run_ref"]], timeout=0)
        if not ready:
            status = "running"
        else:
            # run() returned: clean exit, or the loop/actor died with an error.
            try:
                ray.get(record["run_ref"])
                status = "stopped"
            except Exception as exc:
                status = f"failed: {exc}"
        return {
            "name": record["name"],
            "spec": record["spec"],
            "interval": record["interval"],
            "started_at": record["started_at"],
            "status": status,
        }

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
        self.app_storage_root = Path(app_storage_root)
        self.acquirium_cli = acquirium_cli
        self.logger = logging.getLogger(f"acquirium.app.{spec.name}")

        # Populated by setup(): the loaded App instance, its resolved query
        # bundle, and whatever build_app() returns (e.g. a trained model).
        self.app: Any | None = None
        self.query: Any | None = None
        self.queries: dict[str, Any] = {}
        self.state: Any | None = None
        self.graph_version = 0
        self._params: dict[str, Any] = {}
        self._build_status = "pending"

        # Run scheduling / monitoring, all owned by this actor.
        self._runs: dict[str, dict[str, Any]] = {}
        self._run_counter = 0
        self._keep_alive = False
        self._loop_task: asyncio.Task | None = None
        # Set by stop() to break the keep-alive loop.
        self._stop_event = asyncio.Event()

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
        self.acquirium_cli.insert_graph(
            graph.serialize(format="turtle"), format="turtle", replace=False
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
        bumps the graph version so keep-alive workers rebuild. Driven only by
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
        self.acquirium_cli.client.sparql_update(query)
        self.logger.info("Deregistered app '%s' from the graph", self.spec.name)
        return {"name": self.spec.name}

    def _app_spec_graph(self, spec: AppSpec) -> Graph:
        app_uri = URIRef(app_uri_for(spec.name))
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
            ref_uri = compute_ref_uri(spec.name, out.point_uri)

            graph.add((app_uri, PRODUCES, point_uri))
            graph.add((point_uri, RDF.type, VIRTUAL_POINT))
            graph.add((point_uri, HAS_EXTERNAL_REFERENCE, ref_uri))
            graph.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(spec.name)))
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
        # Seed the version the query was built against so the run phase can
        # detect a stale query after later graph mutations.
        try:
            self.graph_version = self.acquirium_cli.graph_version()
        except Exception:
            self.graph_version = 0
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
            emit_outputs(
                self.spec.name,
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
        when the server's graph version advances (mirrors DriverRunner.run).
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
                v = self.acquirium_cli.graph_version()
                if v != self.graph_version:
                    self.graph_version = v
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
        """Signal the keep-alive loop to exit after its current iteration."""
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


def _app_type_name(type_uri: URIRef) -> str:
    """Inverse of :func:`app_type_uri` for the graph→spec restore path."""
    known = {SOFT_SENSOR: "soft_sensor", THRESHOLD: "threshold", ALARM: "alarm", REPORT: "report"}
    if type_uri in known:
        return known[type_uri]
    ns = str(ACQUIRIUM_NS)
    uri = str(type_uri)
    return uri[len(ns):] if uri.startswith(ns) else uri


def restore_app_specs(manager) -> list[AppSpec]:
    """Rebuild the AppSpecs of apps registered by a previous server run.

    Inverts :meth:`AppRunner._app_spec_graph`: enumerates ``?app a acq:App``
    in the persistent graph and reads scalar fields, outputs, and
    dependencies back out. Source code is not in the graph — it lives under
    the app storage dir (with app.json carrying entry_file/app_class), where
    ``AppRunner._load_app`` reads it — so specs are returned without source
    and apps whose storage dir is gone are skipped.

    One lossy corner: registration writes ``event`` and ``trigger`` outputs
    identically (both as EventStream), so a restored trigger output comes
    back as kind="event". Output kind is only used when writing the
    registration graph, which the restore path skips, so behavior is
    unaffected.
    """
    def rows(q: str) -> list:
        start = time.perf_counter()
        result = manager.graph_store.sparql_query(q, use_union=False).get("rows", [])
        logger.debug(
            "App restore: SPARQL query returned %d row(s) in %.1f ms",
            len(result), (time.perf_counter() - start) * 1000.0,
        )
        return result

    logger.debug("App restore: reading registered apps from the persistent graph")
    apps: dict[str, dict[str, Any]] = {}
    for app_uri, label, version, queries, params, rdf_type in rows(f"""
        SELECT ?app ?label ?version ?queries ?params ?type WHERE {{
          ?app a <{APP}> .
          OPTIONAL {{ ?app <{RDFS.label}> ?label }}
          OPTIONAL {{ ?app <{HAS_VERSION}> ?version }}
          OPTIONAL {{ ?app <{APP_QUERY}> ?queries }}
          OPTIONAL {{ ?app <{APP_PARAMS}> ?params }}
          OPTIONAL {{ ?app a ?type . FILTER(?type != <{APP}>) }}
        }}"""):
        entry = apps.setdefault(str(app_uri), {
            "name": None, "version": None, "app_type": None,
            "queries": {}, "params": {}, "outputs": [], "depends_on": set(),
        })
        if label is not None:
            entry["name"] = str(label)
        if version is not None:
            entry["version"] = str(version)
        if rdf_type is not None:
            entry["app_type"] = _app_type_name(URIRef(str(rdf_type)))
        if queries is not None:
            try:
                entry["queries"] = json.loads(str(queries))
            except json.JSONDecodeError:
                logger.warning("App %s: unparseable querySpec in graph; dropped", app_uri)
        if params is not None:
            try:
                entry["params"] = json.loads(str(params))
            except json.JSONDecodeError:
                logger.warning("App %s: unparseable paramSpec in graph; dropped", app_uri)

    for app_uri, dep in rows(
        f"SELECT ?app ?dep WHERE {{ ?app a <{APP}> ; <{DEPENDS_ON}> ?dep }}"
    ):
        if str(app_uri) in apps:
            apps[str(app_uri)]["depends_on"].add(str(dep))

    # Refs carry Stream plus exactly one of EventStream/TimeseriesStream, so
    # the FILTER IN yields one row per output.
    for app_uri, point, ref, qk, unit, ds, backend, rtype in rows(f"""
        SELECT ?app ?point ?ref ?qk ?unit ?ds ?backend ?rtype WHERE {{
          ?app a <{APP}> ; <{PRODUCES}> ?point .
          ?point <{HAS_EXTERNAL_REFERENCE}> ?ref .
          ?ref a ?rtype . FILTER(?rtype IN (<{EVENT_STREAM}>, <{TIMESERIES_STREAM}>))
          OPTIONAL {{ ?point <{HAS_QUANTITY_KIND}> ?qk }}
          OPTIONAL {{ ?point <{HAS_UNIT}> ?unit }}
          OPTIONAL {{ ?point <{DATA_SOURCE}> ?ds }}
          OPTIONAL {{ ?ref <{STORAGE_BACKEND}> ?backend }}
        }}"""):
        if str(app_uri) not in apps:
            continue
        apps[str(app_uri)]["outputs"].append(AppOutputSpec(
            kind="event" if URIRef(str(rtype)) == EVENT_STREAM else "timeseries",
            point_uri=str(point),
            ref_uri=str(ref),
            quantity_kind=str(qk) if qk is not None else None,
            unit=str(unit) if unit is not None else None,
            data_source=str(ds) if ds is not None else None,
            storage_backend=str(backend) if backend is not None else None,
        ))

    specs: list[AppSpec] = []
    for app_uri, entry in apps.items():
        name = entry["name"] or unquote(app_uri.rsplit("/", 1)[-1])
        app_dir = Path(manager.app_storage_root) / name
        if not app_dir.is_dir():
            logger.error(
                "App '%s' is registered in the graph but has no source under %s; not restored",
                name, app_dir,
            )
            continue
        specs.append(AppSpec(
            name=name,
            version=entry["version"] or "0.0",
            app_type=entry["app_type"] or "soft_sensor",
            queries=entry["queries"],
            outputs=entry["outputs"],
            depends_on=sorted(entry["depends_on"]),
            params=entry["params"],
        ))
    logger.debug(
        "App restore: rebuilt %d spec(s) from %d registered app record(s)",
        len(specs), len(apps),
    )
    return specs


class AppAlreadyRegistered(ValueError):
    """Raised when registering a name that is already registered without replace."""

    def __init__(self, name: str):
        super().__init__(
            f"App '{name}' is already registered; "
            f"pass replace=True to overwrite it."
        )
        self.name = name


class AppSupervisor:
    """Owns the AppRunner actors of one server process, keyed by app name.

    Lives in the FastAPI process. ``register_app()`` spawns an AppRunner actor
    for the app, connects it back to this server over HTTP, and runs its
    ``register()`` step synchronously under a lock so apps' registration graph
    writes cannot race each other.
    """

    def __init__(
        self,
        app_storage_root: Path,
        server_url: str,
        server_port: int,
        use_ssl: bool = False,
    ):
        self.app_storage_root = Path(app_storage_root)
        self.server_url = server_url
        self.server_port = int(server_port)
        self.use_ssl = bool(use_ssl)
        self._lock = threading.Lock()
        self._apps: dict[str, dict[str, Any]] = {}

    def register_app(self, spec: AppSpec, *, replace: bool = False) -> dict[str, Any]:
        from acquirium.Client.acquirium import Acquirium

        with self._lock:
            replaced = spec.name in self._apps
            if replaced:
                if not replace:
                    raise AppAlreadyRegistered(spec.name)
                # Gracefully dispose the existing app (stop, clean its graph
                # registration, kill the actor) before spawning the new one.
                self._teardown_app(self._apps.pop(spec.name), remove_source=False)

            aq = Acquirium(
                server_url=self.server_url,
                server_port=self.server_port,
                use_ssl=self.use_ssl,
            )
            actor = AppRunner.remote(spec, self.app_storage_root, aq)
            try:
                info = ray.get(actor.register.remote())
            except Exception:
                ray.kill(actor)
                raise

            # Build phase (load app, build_query, build_app) runs serially
            # under this lock so build-time graph reads/writes can't race.
            # A build failure is non-fatal — the app stays registered and the
            # run phase can rebuild — so registration itself stays robust.
            try:
                info = {**info, **ray.get(actor.setup.remote())}
            except Exception:
                logger.exception("Build phase failed for app '%s'", spec.name)

            self._apps[spec.name] = {
                "name": spec.name,
                "spec": spec,
                "actor": actor,
                "registered_at": datetime.now(timezone.utc).isoformat(),
                "started_at": None,
                "stopped_at": None,
                "running" : False
            }
            logger.info("%s app '%s'", "Replaced" if replaced else "Registered", spec.name)
            return {**info, "replaced": replaced}

    def restore_app(self, spec: AppSpec) -> dict[str, Any]:
        """Respawn the actor of an app registered by a previous server run.

        The registration graph and the persisted source already exist (see
        :func:`restore_app_specs`), so this skips ``register()`` and goes
        straight to the build phase. Like register_app, a build failure is
        non-fatal: the app stays listed and the run phase can rebuild.
        """
        from acquirium.Client.acquirium import Acquirium

        with self._lock:
            if spec.name in self._apps:
                self._teardown_app(self._apps.pop(spec.name), remove_source=False)

            aq = Acquirium(
                server_url=self.server_url,
                server_port=self.server_port,
                use_ssl=self.use_ssl,
            )
            actor = AppRunner.remote(spec, self.app_storage_root, aq)
            info: dict[str, Any] = {"name": spec.name}
            try:
                with _timed_debug(logger, "restore_app setup app=%s", spec.name):
                    info = {**info, **ray.get(actor.setup.remote())}
            except Exception:
                logger.exception("Build phase failed for restored app '%s'", spec.name)

            self._apps[spec.name] = {
                "name": spec.name,
                "spec": spec,
                "actor": actor,
                "registered_at": datetime.now(timezone.utc).isoformat(),
                "started_at": None,
                "stopped_at": None,
                "running": False,
            }
            return info

    def _teardown_app(self, record: dict[str, Any], *, remove_source: bool = False) -> None:
        """Gracefully dispose one app's actor. Does NOT touch ``self._apps`` or
        the lock — the caller owns removing the record. Every step is
        best-effort so a partial failure still frees the name.

        Order: stop a running keep-alive loop, strip the app's registration
        triples from the graph, kill the actor, then (optionally) delete its
        persisted source.
        """
        actor = record["actor"]
        name = record["name"]
        if record.get("running"):
            try:
                ray.get(actor.stop.remote())
            except Exception:
                logger.exception("Failed to stop app '%s' before teardown", name)
        try:
            ray.get(actor.deregister.remote())
        except Exception:
            logger.exception("Failed to deregister app '%s' from the graph", name)
        try:
            ray.kill(actor)
        except Exception:
            logger.exception("Failed to kill actor for app '%s'", name)
        if remove_source:
            app_dir = self.app_storage_root / name
            try:
                if app_dir.is_dir():
                    shutil.rmtree(app_dir)
            except Exception:
                logger.exception("Failed to remove source dir for app '%s'", name)

    def delete_app(self, app_id: str, *, remove_source: bool = True) -> dict[str, Any]:
        """Stop, deregister, and dispose an app, cleaning up its graph triples."""
        with self._lock:
            record = self._apps.pop(app_id, None)
        if record is None:
            raise KeyError(f"Unknown app: {app_id}")
        self._teardown_app(record, remove_source=remove_source)
        logger.info("Deleted app '%s'", app_id)
        return {"name": app_id, "deleted": True}

    def list_apps(self) -> list[dict[str, Any]]:
        with self._lock:
            records = list(self._apps.values())
        return [
            {"name": r["name"],"running": r["running"], "started_at": r["started_at"],"stopped_at":r["stopped_at"]} for r in records
        ]

    def _actor(self, app_id: str):
        with self._lock:
            record = self._apps.get(app_id)
        if record is None:
            raise KeyError(f"Unknown app: {app_id}")
        return record["actor"]

    def run_app(self, req: AppRunRequest) -> dict[str, Any]:
        """Route a run request to the app's actor, which schedules it."""
        actor = self._actor(req.app_id)
        with _timed_debug(logger, "run_app actor.run app=%s", req.app_id):
            run_message = ray.get(
                actor.run.remote(req.start, req.end, req.params, req.keep_alive, req.interval)
            )
        with self._lock:
            record = self._apps.get(req.app_id)
            if record is None:
                raise KeyError(f"Unknown app: {req.app_id}")
            else:
                record["running"] = True
                record["started_at"] = datetime.now(timezone.utc).isoformat()
                record["stopped_at"] = None
        logger.info(
            "App '%s' started (keep_alive=%s, interval=%s)",
            req.app_id, req.keep_alive, req.interval,
        )
        return run_message

    def stop_app(self, app_id: str) -> dict[str, Any]:
        """Ask the app's actor to stop its keep-alive loop."""
        actor = self._actor(app_id)
        with _timed_debug(logger, "stop_app actor.stop app=%s", app_id):
            stop_message = ray.get(actor.stop.remote())
        with self._lock:
            record = self._apps.get(app_id)
            if record is None:
                raise KeyError(f"Unknown app: {app_id}")
            else:
                record["running"] = False
                record["stopped_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("App '%s' stopped", app_id)
        return stop_message

    def app_status(self, app_id: str) -> dict[str, Any]:
        """Ask the app's actor to report its build/run status."""
        actor = self._actor(app_id)
        return ray.get(actor.status.remote())

    def stop_all_apps(self) -> None:
        with self._lock:
            records = list(self._apps.values())
            self._apps.clear()
        for record in records:
            try:
                ray.kill(record["actor"])
                logger.info("Stopped app '%s'", record["name"])
            except Exception:
                logger.exception("Failed to stop app '%s'", record["name"])




