from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import ray
import shutil

from rdflib import URIRef

from acquirium.internals._log import timed_debug as _timed_debug
from acquirium.internals.env_spec import DEFAULT_ENV_STORAGE_ROOT, build_runtime_env, ensure_env
from acquirium.internals.models import AppSpec, AppOutputSpec, AppRunRequest, EnvSpec, TaskSpec
from acquirium.internals.internals_namespaces import *
from acquirium.Apps.runner import AppRunner
from acquirium.Apps.task_host import TaskHost, load_persisted_task



logger = logging.getLogger("acquirium.apps.supervisor")


class AppSupervisor:
    """Owns the AppRunner actors of one server process, keyed by app name.

    Lives in the FastAPI process. ``register_app()`` spawns an AppRunner actor
    for the app and connects it back to this server over HTTP.

    Locking discipline: ``_lock`` protects only the ``_apps`` dict and is
    **never held across an actor call**. Actor calls (``register``/``setup``/
    ``stop``/``deregister``) re-enter this server over HTTP; every sync
    endpoint shares one bounded request threadpool, so a thread holding
    ``_lock`` while waiting on such a call can deadlock the server the moment
    other requests pile up on the same lock. Registration reserves the app
    name with a placeholder record instead, and concurrent registrations
    serialize their build-time graph writes via ``_build_lock`` — which no
    request path ever takes.
    """

    #: Bound on teardown-time actor calls; a wedged actor is killed instead
    #: of stalling delete/replace forever.
    TEARDOWN_TIMEOUT = 10.0

    def __init__(
        self,
        app_storage_root: Path,
        server_url: str,
        server_port: int,
        use_ssl: bool = False,
        env_storage_root: Path | str | None = None,
    ):
        self.app_storage_root = Path(app_storage_root)
        self.server_url = server_url
        self.server_port = int(server_port)
        self.use_ssl = bool(use_ssl)
        self.env_storage_root = Path(env_storage_root) if env_storage_root else DEFAULT_ENV_STORAGE_ROOT
        self._lock = threading.Lock()
        self._build_lock = threading.Lock()
        self._apps: dict[str, dict[str, Any]] = {}
        # The single shared task actor, created on first use (tasks share
        # the app name space and the app routes; records carry kind="task").
        self._task_host = None
        self._task_host_lock = threading.Lock()

    def _placeholder(self, spec: AppSpec) -> dict[str, Any]:
        """A record that reserves an app name while its actor is being built."""
        return {
            "name": spec.name,
            "kind": "app",
            "spec": spec,
            "actor": None,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "started_at": None,
            "stopped_at": None,
            "running": False,
        }

    # ─────────────────────── task host ───────────────────────

    def _host(self):
        """The shared TaskHost actor, spawned lazily (never under _lock)."""
        with self._task_host_lock:
            if self._task_host is None:
                from acquirium.Client.acquirium import Acquirium

                aq = Acquirium(
                    server_url=self.server_url,
                    server_port=self.server_port,
                    use_ssl=self.use_ssl,
                )
                self._task_host = TaskHost.remote(self.app_storage_root, aq)
                logger.info("Spawned the shared task host")
            return self._task_host

    def _task_record(self, spec: TaskSpec) -> dict[str, Any]:
        return {
            "name": spec.name,
            "kind": "task",
            "spec": spec,
            "actor": None,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "started_at": None,
            "stopped_at": None,
            "running": False,
        }

    def register_task(self, spec: TaskSpec, *, replace: bool = False) -> dict[str, Any]:
        """Register a class-less task on the shared host.

        Same name space as apps: a task cannot take a registered app's name
        (or vice versa) without ``replace=True``.
        """
        record = self._task_record(spec)
        with self._lock:
            existing = self._apps.get(spec.name)
            if existing is not None and not replace:
                raise AppAlreadyRegistered(spec.name)
            self._apps[spec.name] = record
        replaced = existing is not None
        try:
            if existing is not None:
                self._teardown_app(existing, remove_source=False)
            with self._build_lock:
                info = ray.get(self._host().register.remote(spec), timeout=self.TEARDOWN_TIMEOUT * 6)
        except Exception:
            with self._lock:
                if self._apps.get(spec.name) is record:
                    del self._apps[spec.name]
            raise
        logger.info("%s task '%s'", "Replaced" if replaced else "Registered", spec.name)
        return {**info, "replaced": replaced}

    def restore_task(self, spec: AppSpec) -> dict[str, Any]:
        """Re-attach a persisted task after a server restart.

        The registration graph gave us the AppSpec view; the function itself
        lives on disk (task.json + fn.pkl), which is what the host loads.
        """
        task = load_persisted_task(self.app_storage_root, spec.name)
        if task is None:
            raise FileNotFoundError(f"task '{spec.name}' has no persisted function under {self.app_storage_root}")
        record = self._task_record(task)
        with self._lock:
            self._apps[spec.name] = record
        try:
            with self._build_lock:
                info = ray.get(self._host().restore.remote(task), timeout=self.TEARDOWN_TIMEOUT * 6)
        except Exception:
            with self._lock:
                if self._apps.get(spec.name) is record:
                    del self._apps[spec.name]
            raise
        return info

    def register_app(self, spec: AppSpec, *, replace: bool = False) -> dict[str, Any]:
        from acquirium.Client.acquirium import Acquirium

        placeholder = self._placeholder(spec)
        with self._lock:
            existing = self._apps.get(spec.name)
            if existing is not None and not replace:
                raise AppAlreadyRegistered(spec.name)
            self._apps[spec.name] = placeholder
        replaced = existing is not None

        try:
            if existing is not None:
                # Gracefully dispose the replaced app (stop, clean its graph
                # registration, kill the actor) before spawning the new one.
                self._teardown_app(existing, remove_source=False)

            aq = Acquirium(
                server_url=self.server_url,
                server_port=self.server_port,
                use_ssl=self.use_ssl,
            )
            # A declared env gives this app's actor (and its run workers,
            # which inherit the actor's env) its own dependencies; undeclared
            # apps keep the zero-cost inherit path. Materialized OUTSIDE both
            # locks — a cold build must never stall other registrations.
            py_executable = ensure_env(spec.env, self.env_storage_root)
            runtime_env = build_runtime_env(spec.env, py_executable=py_executable)
            runner_cls = (
                AppRunner if runtime_env is None
                else AppRunner.options(runtime_env=runtime_env)
            )
            # Build-time graph reads/writes of concurrent registrations
            # serialize on the build lock (not the record lock — see class
            # docstring).
            with self._build_lock:
                actor = runner_cls.remote(spec, self.app_storage_root, aq)
                try:
                    info = ray.get(actor.register.remote())
                except Exception:
                    ray.kill(actor)
                    raise
                # A build failure is non-fatal — the app stays registered and
                # the run phase can rebuild — so registration itself stays
                # robust.
                try:
                    info = {**info, **ray.get(actor.setup.remote())}
                except Exception:
                    logger.exception("Build phase failed for app '%s'", spec.name)
        except Exception:
            with self._lock:
                if self._apps.get(spec.name) is placeholder:
                    del self._apps[spec.name]
            raise

        record = {**placeholder, "actor": actor}
        with self._lock:
            superseded = self._apps.get(spec.name) is not placeholder
            if not superseded:
                self._apps[spec.name] = record
        if superseded:
            # A concurrent replace won the name while we were building;
            # dispose what we built and report the conflict.
            self._teardown_app(record, remove_source=False)
            raise AppAlreadyRegistered(spec.name)

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

        placeholder = self._placeholder(spec)
        with self._lock:
            existing = self._apps.get(spec.name)
            self._apps[spec.name] = placeholder
        info: dict[str, Any] = {"name": spec.name}

        try:
            if existing is not None:
                self._teardown_app(existing, remove_source=False)

            aq = Acquirium(
                server_url=self.server_url,
                server_port=self.server_port,
                use_ssl=self.use_ssl,
            )
            py_executable = ensure_env(spec.env, self.env_storage_root)
            runtime_env = build_runtime_env(spec.env, py_executable=py_executable)
            runner_cls = (
                AppRunner if runtime_env is None
                else AppRunner.options(runtime_env=runtime_env)
            )
            with self._build_lock:
                actor = runner_cls.remote(spec, self.app_storage_root, aq)
                try:
                    with _timed_debug(logger, "restore_app setup app=%s", spec.name):
                        info = {**info, **ray.get(actor.setup.remote())}
                except Exception:
                    logger.exception("Build phase failed for restored app '%s'", spec.name)
        except Exception:
            with self._lock:
                if self._apps.get(spec.name) is placeholder:
                    del self._apps[spec.name]
            raise

        record = {**placeholder, "actor": actor}
        with self._lock:
            superseded = self._apps.get(spec.name) is not placeholder
            if not superseded:
                self._apps[spec.name] = record
        if superseded:
            # The name was re-registered (or shut down) while the restore was
            # in flight; the newer owner wins — dispose what we built.
            self._teardown_app(record, remove_source=False)
        return info

    def _teardown_app(self, record: dict[str, Any], *, remove_source: bool = False) -> None:
        """Gracefully dispose one app's actor. Does NOT touch ``self._apps`` or
        the lock — the caller owns removing the record. Every step is
        best-effort so a partial failure still frees the name.

        Order: stop a running keep-alive loop, strip the app's registration
        triples from the graph, kill the actor, then (optionally) delete its
        persisted source.
        """
        actor = record.get("actor")
        name = record["name"]
        if record.get("kind") == "task":
            # Tasks live on the shared host: stop + deregister there; the
            # host itself outlives any single task.
            try:
                ray.get(
                    self._host().deregister.remote(name, remove_source=remove_source),
                    timeout=self.TEARDOWN_TIMEOUT,
                )
            except Exception:
                logger.exception("Failed to deregister task '%s'", name)
            return
        if actor is None:
            # A reservation placeholder — nothing was built yet.
            return
        wedged = False
        if record.get("running"):
            try:
                ray.get(actor.stop.remote(), timeout=self.TEARDOWN_TIMEOUT)
            except ray.exceptions.GetTimeoutError:
                wedged = True
                logger.warning(
                    "App '%s' did not stop within %.0fs; killing its actor "
                    "(graph registration is left in place)",
                    name, self.TEARDOWN_TIMEOUT,
                )
            except Exception:
                logger.exception("Failed to stop app '%s' before teardown", name)
        if not wedged:
            # A wedged actor cannot serve deregister either — skip straight
            # to the kill rather than stalling another timeout.
            try:
                ray.get(actor.deregister.remote(), timeout=self.TEARDOWN_TIMEOUT)
            except ray.exceptions.GetTimeoutError:
                logger.warning(
                    "App '%s' deregister timed out after %.0fs; its "
                    "registration triples remain in the graph",
                    name, self.TEARDOWN_TIMEOUT,
                )
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
            {"name": r["name"], "kind": r.get("kind", "app"), "running": r["running"],
             "started_at": r["started_at"], "stopped_at": r["stopped_at"]}
            for r in records
        ]

    def _record(self, app_id: str) -> dict[str, Any]:
        with self._lock:
            record = self._apps.get(app_id)
        if record is None:
            raise KeyError(f"Unknown app: {app_id}")
        return record

    def _actor(self, app_id: str):
        record = self._record(app_id)
        if record.get("kind") == "task":
            raise RuntimeError(f"'{app_id}' is a task; it has no actor of its own")
        if record.get("actor") is None:
            raise RuntimeError(f"App '{app_id}' is still registering; retry shortly")
        return record["actor"]

    def run_app(self, req: AppRunRequest) -> dict[str, Any]:
        """Route a run request to the app's actor (or the task host)."""
        record = self._record(req.app_id)
        now = datetime.now(timezone.utc).isoformat()
        with _timed_debug(logger, "run_app actor.run app=%s", req.app_id):
            if record.get("kind") == "task":
                run_message = ray.get(
                    self._host().run.remote(
                        req.app_id, start=req.start, end=req.end, params=req.params,
                        keep_alive=req.keep_alive, interval=req.interval,
                        max_in_flight=req.max_in_flight,
                    )
                )
            else:
                actor = self._actor(req.app_id)
                run_message = ray.get(
                    actor.run.remote(
                        req.start, req.end, req.params, req.keep_alive, req.interval,
                        req.max_in_flight, req.run_timeout,
                    )
                )
        with self._lock:
            record = self._apps.get(req.app_id)
            if record is not None:
                # A one-shot run (keep_alive=False) has already been dispatched
                # by the time actor.run returns, so the app isn't in a running
                # state — only a keep-alive loop keeps it "running".
                record["started_at"] = now
                if req.keep_alive:
                    record["running"] = True
                    record["stopped_at"] = None
                else:
                    record["running"] = False
                    record["stopped_at"] = datetime.now(timezone.utc).isoformat()
        if record is None:
            # Deleted concurrently after the run was already dispatched; the
            # run itself happened, only the bookkeeping is moot.
            logger.warning("App '%s' was deleted while run_app was in flight", req.app_id)
        logger.info(
            "App '%s' started (keep_alive=%s, interval=%s)",
            req.app_id, req.keep_alive, req.interval,
        )
        return run_message

    def stop_app(self, app_id: str) -> dict[str, Any]:
        """Ask the app's actor to stop its keep-alive loop.

        A no-op for an app that isn't in a running state (e.g. one that only
        ever did a one-shot run): the actor has no keep-alive loop to stop, so
        we return early without clobbering ``stopped_at`` or logging a stop.
        """
        with self._lock:
            record = self._apps.get(app_id)
            if record is None:
                raise KeyError(f"Unknown app: {app_id}")
            actor = record["actor"]
            is_task = record.get("kind") == "task"
            if not record.get("running"):
                logger.info("App '%s' is not running; stop is a no-op", app_id)
                return {"name": app_id, "stopped": False, "running": False}

        with _timed_debug(logger, "stop_app actor.stop app=%s", app_id):
            if is_task:
                stop_message = ray.get(self._host().stop.remote(app_id))
            else:
                stop_message = ray.get(actor.stop.remote())
        with self._lock:
            record = self._apps.get(app_id)
            if record is not None:
                record["running"] = False
                record["stopped_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("App '%s' stopped", app_id)
        return stop_message

    def app_status(self, app_id: str) -> dict[str, Any]:
        """Ask the app's actor (or the task host) to report build/run status."""
        record = self._record(app_id)
        if record.get("kind") == "task":
            return ray.get(self._host().status.remote(app_id))
        return ray.get(self._actor(app_id).status.remote())

    def stop_all_apps(self) -> None:
        with self._lock:
            records = list(self._apps.values())
            self._apps.clear()
        for record in records:
            if record.get("actor") is None:
                continue
            try:
                ray.kill(record["actor"])
                logger.info("Stopped app '%s'", record["name"])
            except Exception:
                logger.exception("Failed to stop app '%s'", record["name"])
        with self._task_host_lock:
            host, self._task_host = self._task_host, None
        if host is not None:
            try:
                # Plain kill, no deregister: like apps, task registrations
                # stay in the graph so the next start restores them.
                ray.kill(host, no_restart=True)
                logger.info("Stopped the shared task host")
            except Exception:
                logger.exception("Failed to stop the task host")


def _app_type_name(type_uri: URIRef) -> str:
    """Inverse of :func:`app_type_uri` for the graph→spec restore path."""
    known = {SOFT_SENSOR: "soft_sensor", THRESHOLD: "threshold", ALARM: "alarm",
             REPORT: "report", TASK: "task"}
    if type_uri in known:
        return known[type_uri]
    ns = str(ACQUIRIUM_NS)
    uri = str(type_uri)
    return uri[len(ns):] if uri.startswith(ns) else uri


def restore_app_specs(manager) -> list[AppSpec]:
    """Rebuild the AppSpecs of apps registered by a previous server run.

    Inverts :func:`acquirium.internals.app_utils.app_spec_graph`: enumerates ``?app a acq:App``
    in the persistent graph and reads scalar fields, outputs, and
    dependencies back out. Source code is not in the graph — it lives under
    the app storage dir (with app.json carrying entry_file/app_class), where
    ``AppRunner._load_app`` reads it — so specs are returned without source
    and apps whose storage dir is gone are skipped.

    Legacy corner: graphs written before ``acq:outputKind`` existed encode
    ``event`` and ``trigger`` outputs identically (both as EventStream), so
    a trigger output from such a graph restores as kind="event". Newly
    registered specs round-trip the declared kind exactly.
    """
    def rows(q: str) -> list:
        start = time.perf_counter()
        result = manager.graph_store.sparql_query(q, include_dependencies=False).get("rows", [])
        logger.debug(
            "App restore: SPARQL query returned %d row(s) in %.1f ms",
            len(result), (time.perf_counter() - start) * 1000.0,
        )
        return result

    logger.debug("App restore: reading registered apps from the persistent graph")
    apps: dict[str, dict[str, Any]] = {}
    for app_uri, label, version, queries, params, rdf_type, run_mode, interval, env in rows(f"""
        SELECT ?app ?label ?version ?queries ?params ?type ?run_mode ?interval ?env WHERE {{
          ?app a <{APP}> .
          OPTIONAL {{ ?app <{RDFS.label}> ?label }}
          OPTIONAL {{ ?app <{HAS_VERSION}> ?version }}
          OPTIONAL {{ ?app <{APP_QUERY}> ?queries }}
          OPTIONAL {{ ?app <{APP_PARAMS}> ?params }}
          OPTIONAL {{ ?app a ?type . FILTER(?type NOT IN (<{APP}>, <{TASK}>)) }}
          OPTIONAL {{ ?app <{RUN_MODE}> ?run_mode }}
          OPTIONAL {{ ?app <{RUN_INTERVAL}> ?interval }}
          OPTIONAL {{ ?app <{ENV_SPEC}> ?env }}
        }}"""):
        entry = apps.setdefault(str(app_uri), {
            "name": None, "version": None, "app_type": None, "kind": "app",
            "queries": {}, "params": {}, "outputs": [],
            "run_mode": None, "interval": None, "env": None,
        })
        if label is not None:
            entry["name"] = str(label)
        if version is not None:
            entry["version"] = str(version)
        if rdf_type is not None:
            entry["app_type"] = _app_type_name(URIRef(str(rdf_type)))
        if run_mode is not None:
            entry["run_mode"] = str(run_mode)
        if interval is not None:
            try:
                entry["interval"] = float(str(interval))
            except ValueError:
                logger.warning("App %s: unparseable runInterval in graph; dropped", app_uri)
        if env is not None:
            try:
                entry["env"] = EnvSpec(**json.loads(str(env)))
            except (json.JSONDecodeError, TypeError, ValueError):
                logger.warning("App %s: unparseable envSpec in graph; dropped", app_uri)
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

    for (app_uri,) in rows(f"SELECT ?app WHERE {{ ?app a <{TASK}> }}"):
        if str(app_uri) in apps:
            apps[str(app_uri)]["kind"] = "task"
            # acq:Task doubles as the app_type (excluded from the ?type
            # column above so it doesn't shadow a real app's type).
            apps[str(app_uri)]["app_type"] = apps[str(app_uri)]["app_type"] or "task"

    # Refs carry Stream plus exactly one of EventStream/TimeseriesStream, so
    # the FILTER IN yields one row per output.
    for app_uri, point, ref, qk, unit, ds, backend, rtype, okind in rows(f"""
        SELECT ?app ?point ?ref ?qk ?unit ?ds ?backend ?rtype ?okind WHERE {{
          ?app a <{APP}> ; <{PRODUCES}> ?point .
          ?point <{HAS_EXTERNAL_REFERENCE}> ?ref .
          ?ref a ?rtype . FILTER(?rtype IN (<{EVENT_STREAM}>, <{TIMESERIES_STREAM}>))
          OPTIONAL {{ ?ref <{OUTPUT_KIND}> ?okind }}
          OPTIONAL {{ ?point <{HAS_QUANTITY_KIND}> ?qk }}
          OPTIONAL {{ ?point <{HAS_UNIT}> ?unit }}
          OPTIONAL {{ ?point <{DATA_SOURCE}> ?ds }}
          OPTIONAL {{ ?ref <{STORAGE_BACKEND}> ?backend }}
        }}"""):
        if str(app_uri) not in apps:
            continue
        if okind is not None and str(okind) in ("timeseries", "event", "trigger"):
            kind = str(okind)
        else:
            # Legacy graph without acq:outputKind — fall back to the stream
            # type, which cannot distinguish trigger from event.
            kind = "event" if URIRef(str(rtype)) == EVENT_STREAM else "timeseries"
        apps[str(app_uri)]["outputs"].append(AppOutputSpec(
            kind=kind,
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
            kind=entry["kind"],
            version=entry["version"] or "0.0",
            app_type=entry["app_type"] or "soft_sensor",
            queries=entry["queries"],
            outputs=entry["outputs"],
            params=entry["params"],
            run_mode=(entry["run_mode"]
                      if entry["run_mode"] in ("manual", "interval", "on_change")
                      else "manual"),
            interval=entry["interval"],
            env=entry["env"],
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
