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
from acquirium.internals.models import AppSpec, AppOutputSpec, AppRunRequest
from acquirium.internals.internals_namespaces import *
from acquirium.Apps.runner import AppRunner



logger = logging.getLogger("acquirium.apps.supervisor")


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
            {
                "name": r["name"],
                "spec": r["spec"].source_spec,
                "running": r["running"],
                "started_at": r["started_at"],
                "stopped_at": r["stopped_at"],
            }
            for r in records
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
        now = datetime.now(timezone.utc).isoformat()
        with _timed_debug(logger, "run_app actor.run app=%s", req.app_id):
            run_message = ray.get(
                actor.run.remote(req.start, req.end, req.params, req.keep_alive, req.interval)
            )
        with self._lock:
            record = self._apps.get(req.app_id)
            if record is None:
                raise KeyError(f"Unknown app: {req.app_id}")
            # A one-shot run (keep_alive=False) has already been dispatched by
            # the time actor.run returns, so the app isn't in a running state —
            # only a keep-alive loop keeps it "running".
            record["started_at"] = now
            if req.keep_alive:
                record["running"] = True
                record["stopped_at"] = None
            else:
                record["running"] = False
                record["stopped_at"] = datetime.now(timezone.utc).isoformat()
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
            if not record.get("running"):
                logger.info("App '%s' is not running; stop is a no-op", app_id)
                return {"name": app_id, "stopped": False, "running": False}

        with _timed_debug(logger, "stop_app actor.stop app=%s", app_id):
            stop_message = ray.get(actor.stop.remote())
        with self._lock:
            record = self._apps.get(app_id)
            if record is None:
                raise KeyError(f"Unknown app: {app_id}")
            record["running"] = False
            record["stopped_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("App '%s' stopped", app_id)
        return stop_message

    def app_status(self, app_id: str) -> dict[str, Any]:
        """Ask the app's actor to report its build/run status."""
        actor = self._actor(app_id)
        return ray.get(actor.status.remote())

    async def dispatch_pending(self, app_id: str) -> dict[str, Any]:
        """Run one continuous-batch turn on *app_id*'s actor.

        The router's dispatch callback (Server/router.py): called with no
        Ray dependency of its own, so the router can be unit-tested without
        a Ray cluster. Awaiting the actor's ``process_pending.remote()``
        ObjectRef directly works because this coroutine always runs inside
        the FastAPI event loop, where Ray's asyncio integration is active.
        """
        actor = self._actor(app_id)
        return await actor.process_pending.remote()

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
    the app storage dir. Load metadata, including the original copy-pasteable
    source spec, is restored from ``app.json``. Apps whose storage directory is
    gone are skipped.

    One lossy corner: registration writes ``event`` and ``trigger`` outputs
    identically (both as EventStream), so a restored trigger output comes
    back as kind="event". Output kind is only used when writing the
    registration graph, which the restore path skips, so behavior is
    unaffected.
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
    output_by_point: dict[tuple[str, str], AppOutputSpec] = {}
    for app_uri, point, ref, ref_name, value_kind, qk, unit, ds, backend, rtype in rows(f"""
        SELECT ?app ?point ?ref ?refName ?valueKind ?qk ?unit ?ds ?backend ?rtype WHERE {{
          ?app a <{APP}> ; <{PRODUCES}> ?point .
          ?point <{HAS_EXTERNAL_REFERENCE}> ?ref .
          ?ref a ?rtype . FILTER(?rtype IN (<{EVENT_STREAM}>, <{TIMESERIES_STREAM}>))
          OPTIONAL {{ ?ref <{ACQUIRIUM_REF_NAME}> ?refName }}
          OPTIONAL {{ ?ref <{ACQUIRIUM_VALUE_KIND}> ?valueKind }}
          OPTIONAL {{ ?point <{HAS_QUANTITY_KIND}> ?qk }}
          OPTIONAL {{ ?point <{HAS_UNIT}> ?unit }}
          OPTIONAL {{ ?point <{DATA_SOURCE}> ?ds }}
          OPTIONAL {{ ?ref <{STORAGE_BACKEND}> ?backend }}
        }}"""):
        if str(app_uri) not in apps:
            continue
        output = AppOutputSpec(
            kind="event" if URIRef(str(rtype)) == EVENT_STREAM else "timeseries",
            point_uri=str(point),
            ref_name=str(ref_name) if ref_name is not None else None,
            ref_uri=str(ref),
            value_kind=str(value_kind) if value_kind is not None else None,
            quantity_kind=str(qk) if qk is not None else None,
            unit=str(unit) if unit is not None else None,
            data_source=str(ds) if ds is not None else None,
            storage_backend=str(backend) if backend is not None else None,
        )
        apps[str(app_uri)]["outputs"].append(output)
        output_by_point[(str(app_uri), str(point))] = output

    for app_uri, point, dep in rows(f"""
        SELECT ?app ?point ?dep WHERE {{
          ?app a <{APP}> ; <{PRODUCES}> ?point .
          ?point <{IS_CALCULATED_FROM}> ?dep .
        }}"""):
        output = output_by_point.get((str(app_uri), str(point)))
        if output is not None and str(dep) not in output.depends_on:
            output.depends_on.append(str(dep))

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
        load_meta: dict[str, Any] = {}
        meta_path = app_dir / "app.json"
        if meta_path.exists():
            try:
                load_meta = json.loads(meta_path.read_text())
            except (json.JSONDecodeError, OSError):
                logger.warning("App '%s': could not read %s", name, meta_path)
        run_state: dict[str, Any] = {}
        run_path = app_dir / "run.json"
        if run_path.exists():
            try:
                run_state = json.loads(run_path.read_text())
            except (json.JSONDecodeError, OSError):
                logger.warning("App '%s': could not read %s", name, run_path)
        specs.append(AppSpec(
            name=name,
            version=entry["version"] or "0.0",
            app_type=entry["app_type"] or "soft_sensor",
            source_spec=load_meta.get("source_spec"),
            app_class=load_meta.get("app_class"),
            entry_file=load_meta.get("entry_file"),
            queries=entry["queries"],
            outputs=entry["outputs"],
            depends_on=sorted(entry["depends_on"]),
            params=entry["params"],
            resume_keep_alive=bool(run_state.get("keep_alive", False)),
            run_interval=float(run_state.get("interval", 10.0)),
            run_start=run_state.get("start"),
            run_end=run_state.get("end"),
            run_params=run_state.get("params", {}),
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
