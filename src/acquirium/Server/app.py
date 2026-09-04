from __future__ import annotations

import asyncio
import contextlib
import importlib
import io
import json
import logging
import os
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Iterator, Optional

# When the server is launched via `uv run`, Ray's uv hook would give every
# worker a fresh env resolved from pyproject.toml — dropping optional extras
# like watertap/pyomo. Disable it so actors inherit this process's environment.
# Ray reads this flag once at import time, so it must be set before `import ray`.
os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")
import ray
import pyoxigraph as ox

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, Response
from dateutil import parser as dtparser
from pydantic import BaseModel, Field
from datetime import datetime

from acquirium.Server.manager import Manager
from acquirium.Materialization import App as MaterializationApp
from acquirium.Materialization.planner import Deployment

from acquirium.internals.models import (
    LogEntry,
    TimeIntervalModel,
    StreamInsert,
    RegisterDatasourceRequest,
)
from acquirium.internals.internals_namespaces import PLANT_URI

import pyarrow.ipc as ipc
import pyarrow as pa
import polars as pl

from acquirium.Server.insert_stats import insert_stats, start_insert_summary_thread
from acquirium.Drivers.supervisor import DriverSupervisor



log = logging.getLogger("acquirium.api")


def _sparql_results_to_rows(serialized: bytes) -> dict[str, Any]:
    """Preserve Acquirium's SPARQL response contract without RDFLib terms."""
    payload = json.loads(serialized)
    if "boolean" in payload:
        return {"columns": [], "rows": [[bool(payload["boolean"])]]}
    columns = payload["head"].get("vars", [])
    rows = [
        [binding.get(column, {}).get("value") for column in columns]
        for binding in payload["results"].get("bindings", [])
    ]
    return {"columns": columns, "rows": rows}


_SPARQL_RESULT_FORMATS = {
    "application/sparql-results+json": ox.QueryResultsFormat.JSON,
    "application/sparql-results+xml": ox.QueryResultsFormat.XML,
    "text/csv": ox.QueryResultsFormat.CSV,
    "text/tab-separated-values": ox.QueryResultsFormat.TSV,
}
_SPARQL_GRAPH_FORMATS = {
    "text/turtle": ox.RdfFormat.TURTLE,
    "application/n-triples": ox.RdfFormat.N_TRIPLES,
    "application/rdf+xml": ox.RdfFormat.RDF_XML,
    "application/ld+json": ox.RdfFormat.JSON_LD,
}


def _accepted_sparql_formats(accept: str) -> tuple[ox.QueryResultsFormat, ox.RdfFormat]:
    """Choose supported protocol formats from an HTTP Accept header.

    SELECT and ASK use the first result format accepted by the client;
    CONSTRUCT and DESCRIBE use the first accepted RDF graph format. The
    defaults are SPARQL Results JSON and Turtle, respectively.
    """
    accepted: list[tuple[float, int, str]] = []
    for position, item in enumerate(accept.lower().split(",")):
        media_type, *parameters = item.strip().split(";")
        quality = 1.0
        for parameter in parameters:
            name, separator, value = parameter.strip().partition("=")
            if separator and name == "q":
                try:
                    quality = float(value)
                except ValueError:
                    quality = 0.0
        if quality:
            accepted.append((-quality, position, media_type.strip()))
    for _, _, media_type in sorted(accepted):
        if media_type in _SPARQL_RESULT_FORMATS:
            return _SPARQL_RESULT_FORMATS[media_type], _SPARQL_GRAPH_FORMATS["text/turtle"]
        if media_type in _SPARQL_GRAPH_FORMATS:
            return _SPARQL_RESULT_FORMATS["application/sparql-results+json"], _SPARQL_GRAPH_FORMATS[media_type]
        if media_type in {"*/*", "application/*", "text/*"}:
            break
    return _SPARQL_RESULT_FORMATS["application/sparql-results+json"], _SPARQL_GRAPH_FORMATS["text/turtle"]


def _self_connect_cfg(cfg: dict) -> tuple[str, int, bool]:
    """Return (host, port, use_ssl) that driver actors use to reach this server."""
    driver_cfg = cfg.get("driver", {})
    host = driver_cfg.get("server_url", "localhost")
    if host == "0.0.0.0":
        host = "localhost"
    port = int(
        driver_cfg.get("server_port")
        or os.environ.get("ACQUIRIUM_SELF_PORT")
        or cfg.get("server", {}).get("port", 8000)
    )
    use_ssl = bool(driver_cfg.get("use_ssl", False))
    return host, port, use_ssl


async def _wait_until_healthy(base_url: str) -> bool:
    """Poll /health until the server answers. The lifespan startup hook runs
    before uvicorn begins serving, so startup work whose actors connect back
    over HTTP must wait for this first."""
    import requests

    health_url = f"{base_url}/health"
    for _ in range(300):
        try:
            r = await asyncio.to_thread(requests.get, health_url, timeout=2)
            if r.ok:
                return True
        except Exception:
            pass
        await asyncio.sleep(2)
    return False


async def _start_config_drivers(supervisor: DriverSupervisor, cfg: dict) -> None:
    """Start every [[drivers]] entry from the config as a Ray actor.

    The caller must wait for /health first (_wait_until_healthy) — driver
    actors talk to this server over HTTP. Setup ordering stays serial because
    DriverSupervisor.start_driver holds its lock across setup.
    """
    entries = cfg.get("drivers", [])
    if not entries:
        return

    for entry in entries:
        spec = entry.get("spec")
        if not spec:
            log.warning("[[drivers]] entry missing 'spec'; skipping")
            continue
        overrides = {k: v for k, v in entry.items() if k not in ("spec", "name")}
        merged_cfg = {**cfg, "driver": {**cfg.get("driver", {}), **overrides}}
        interval = float(overrides.get("interval", cfg.get("driver", {}).get("interval", 10.0)))
        try:
            info = await asyncio.to_thread(
                supervisor.start_driver,
                spec=spec,
                config=merged_cfg,
                interval=interval,
                name=entry.get("name"),
            )
            log.info("Started config driver '%s' (%s)", info["name"], spec)
        except Exception:
            log.exception("Config driver %s failed to start", spec)


def _load_config_app_target(spec: str, *, base_dir: Path) -> object:
    """Load a transformation class or registrar from a config ``spec``."""
    if ":" not in spec:
        raise ValueError("app spec must be module_or_file:target")
    module_part, target_name = spec.rsplit(":", 1)
    file_path = Path(module_part)
    is_file = "/" in module_part or file_path.suffix == ".py" or file_path.exists()
    if is_file:
        # Config files may name a project-local module without packaging it.
        # Resolve relative paths against the config, never the server's cwd.
        if not file_path.is_absolute():
            file_path = (base_dir / file_path).resolve()
        if not file_path.is_file():
            raise ValueError(f"app file not found: {module_part}")
        if not file_path.stem.isidentifier():
            raise ValueError(f"app file name must be a Python module name: {file_path.name}")
        module_dir = str(file_path.parent)
        if module_dir not in sys.path:
            sys.path.insert(0, module_dir)
        module = importlib.import_module(file_path.stem)
    else:
        module = importlib.import_module(module_part)
    try:
        return getattr(module, target_name)
    except AttributeError as error:
        raise ValueError(f"app target {target_name!r} was not found in {module.__name__!r}") from error


def _deploy_config_apps(cfg: dict) -> None:
    """Deploy the materialization applications declared in ``[[apps]]``."""
    entries = cfg.get("apps", [])
    if not entries:
        return
    from acquirium.Client.acquirium import Acquirium

    host, port, use_ssl = _self_connect_cfg(cfg)
    aq = Acquirium(server_url=host, server_port=port, use_ssl=use_ssl)
    base_dir = Path(cfg.get("__config_dir", Path.cwd()))
    for entry in entries:
        spec = entry.get("spec")
        if not isinstance(spec, str) or not spec:
            log.warning("[[apps]] entry missing 'spec'; skipping")
            continue
        try:
            target = _load_config_app_target(spec, base_dir=base_dir)
            # Only non-reserved fields become constructor parameters. ``name``
            # remains display/config metadata and cannot alter binding identity.
            parameters = {key: value for key, value in entry.items() if key not in {"spec", "name"}}
            if isinstance(target, type) and issubclass(target, MaterializationApp):
                deployments = (target,)
                deployment_parameters = parameters
            elif callable(target):
                result = target(aq, parameters)
                if result is None:
                    deployments = ()
                elif isinstance(result, type) and issubclass(result, MaterializationApp):
                    deployments = (result,)
                else:
                    deployments = tuple(result)
                    if not all(isinstance(item, type) and issubclass(item, MaterializationApp) for item in deployments):
                        raise TypeError("an app registrar must return App classes or None")
                deployment_parameters = {}
            else:
                raise TypeError("app spec target must be an App class or callable registrar")
            for app_class in deployments:
                aq.deploy_app(app_class, parameters=deployment_parameters)
            log.info("Deployed config app '%s' (%s): %d app(s)", entry.get("name", spec), spec, len(deployments))
        except Exception:
            log.exception("Config app %s failed to deploy", spec)


class Health(BaseModel):
    ok: bool


class EmbeddingIndexStatus(BaseModel):
    state: str          # "idle" | "building" | "ready" | "error"
    concepts: int
    surfaces: int
    error: str | None
    last_built: str | None
    duration_s: float | None

class EmbeddingStatus(BaseModel):
    graph: EmbeddingIndexStatus
    qudt: EmbeddingIndexStatus

class InsertGraphRequest(BaseModel):
    rdf_graph: str = Field(..., description="File path or RDF text")
    format: str = "turtle"
    replace: bool = True
    source_id: str = Field(
        min_length=1,
        description="Deployment data-graph owner; use 'plant' for the shared plant model",
    )


class TimeseriesInfoRequest(BaseModel):
    uris: list[str]


class RecordFieldSpec(BaseModel):
    name: str
    text: str
    kind: Optional[str] = None


class ResolveRecordRequest(BaseModel):
    fields: list[RecordFieldSpec]
    top_k: int = 5
    min_score: float = 0.5


@asynccontextmanager
async def lifespan(app: FastAPI):
    from acquirium.internals._log import configure_logging
    configure_logging()  # honors ACQUIRIUM_VERBOSE env var set by `acquirium server -v`

    from acquirium.cli import _load_config
    _config_path = os.environ.get("ACQUIRIUM_CONFIG")
    _cfg = _load_config(Path(_config_path) if _config_path else None)
    config_dir = str(Path(_cfg.get("__config_dir", Path.cwd())).resolve())
    if config_dir not in sys.path:
        sys.path.insert(0, config_dir)
    server_cfg = _cfg.get("server", {})
    idle_seconds = float(server_cfg.get("materialization_poll_seconds", 0.25))
    error_log_seconds = float(server_cfg.get("materialization_error_log_seconds", 30.0))
    worker_count = int(server_cfg.get("materialization_workers", 2))
    if idle_seconds <= 0 or error_log_seconds <= 0:
        raise ValueError("materialization polling and error log intervals must be positive")
    if worker_count < 1:
        raise ValueError("server.materialization_workers must be positive")

    # v1 runs materialization in a bounded local worker pool. Ray remains an
    # optional driver runtime, but is not part of the materialization API.
    m = Manager.from_env()
    app.state.manager = m
    app.state.read_batch_size = int(server_cfg.get("read_batch_size", 50_000))

    try:
        m._sync_stream_refs_from_graph()
        m.recover_materialization_state()
    except Exception as e:
        log.exception("Startup failed: %s", e)
        try:
            m.close()
        finally:
            raise

    # Shutdown signal for the insert summary logger thread.
    summary_stop = threading.Event()
    start_insert_summary_thread(summary_stop, interval=10.0)

    # Drivers run as Ray actors that connect back over HTTP.
    _host, _port, _use_ssl = _self_connect_cfg(_cfg)
    supervisor = DriverSupervisor(server_url=_host, server_port=_port, use_ssl=_use_ssl)
    app.state.drivers = supervisor

    async def _durable_worker(label: str, operation) -> None:
        """Run one failure-isolated durable control-plane worker."""
        next_error_log = 0.0
        while True:
            try:
                ran = await asyncio.to_thread(operation)
            except asyncio.CancelledError:
                raise
            except Exception:
                # A bad deployment must not stop ingestion or other durable
                # workers. Rate-limit repeats until its configuration changes.
                now = asyncio.get_running_loop().time()
                if now >= next_error_log:
                    log.exception("%s worker failed", label)
                    next_error_log = now + error_log_seconds
                ran = False
            await asyncio.sleep(0 if ran else idle_seconds)

    durable_tasks = [
        asyncio.create_task(_durable_worker(
            f"materialization-{index}",
            lambda index=index: m.run_materialization_once(),
        ))
        for index in range(worker_count)
    ]
    app.state.durable_tasks = durable_tasks

    # Once the server answers /health, start configured drivers, then deploy
    # configured transformations. Drivers load their plant models during setup,
    # so application queries resolve against the intended graph.
    async def _startup_actors() -> None:
        if not await _wait_until_healthy(supervisor.base_url):
            log.error("Server never became healthy at %s; skipping configured drivers", supervisor.base_url)
            return
        await _start_config_drivers(supervisor, _cfg)
        await asyncio.to_thread(_deploy_config_apps, _cfg)

    startup_task = asyncio.create_task(_startup_actors())

    try:
        yield
    finally:
        summary_stop.set()
        startup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await startup_task
        for task in durable_tasks:
            task.cancel()
        await asyncio.gather(*durable_tasks, return_exceptions=True)
        try:
            await asyncio.to_thread(supervisor.stop_all)
        except Exception:
            log.exception("Error stopping drivers during shutdown")
        try:
            m.close()
        except Exception:
            log.exception("Error during shutdown")


app = FastAPI(title="Acquirium API", version="0.1", lifespan=lifespan)


@app.middleware("http")
async def _debug_log_requests(request: Request, call_next):
    """One DEBUG line per HTTP request with status + elapsed ms (off at INFO)."""
    if not log.isEnabledFor(logging.DEBUG):
        return await call_next(request)
    import time as _time
    start = _time.perf_counter()
    log.debug("→ %s %s", request.method, request.url.path)
    try:
        response = await call_next(request)
    except Exception:
        log.debug("← %s %s -> ERROR (%.1f ms)", request.method, request.url.path,
                  (_time.perf_counter() - start) * 1000.0)
        raise
    log.debug("← %s %s -> %d (%.1f ms)", request.method, request.url.path,
              response.status_code, (_time.perf_counter() - start) * 1000.0)
    return response


@app.get("/health", response_model=Health)
def health():
    # If we got here, the app is up
    return Health(ok=True)

@app.get("/embedding_status", response_model=EmbeddingStatus)
def embedding_status():
    manager = app.state.manager
    status = manager.embedding_status()
    return EmbeddingStatus(
        graph=EmbeddingIndexStatus(**status["graph"]),
        qudt=EmbeddingIndexStatus(**status["qudt"]),
    )


@app.get("/graph_version")
def graph_version() -> dict[str, int | bool]:
    """Return source and derived-query generations.

    ``published_version`` identifies the source generation represented by the
    last complete query cache.
    """
    return app.state.manager.graph_status()



#### GRAPH API ENDPOINTS ####


@app.post("/insert_graph")
def insert_graph(req: InsertGraphRequest) -> dict[str, Any]:
    try:
        app.state.manager.insert_graph(
            rdf_graph=req.rdf_graph,
            format=req.format,
            replace=req.replace,
            source_id=req.source_id,
        )
        return {"ok": True, "embedding_ready": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/validate_graph")
def validate_graph() -> dict[str, str | bool]:
    try:
        return app.state.manager.validate_graph()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/export_graph")
def export_graph(include_dependencies: bool = True, format: str = "turtle"):
    """Export the RDF graph in the specified format.

    Args:
        include_dependencies: If True, includes deployment data plus all imported
                      ontology/shape triples. If False, returns all registered
                      deployment/source graphs without those dependencies.
        format: Serialization format - turtle, n3, xml, trig, etc.
    """
    try:
        content = app.state.manager.graph_store.export_graph(
            include_dependencies=include_dependencies,
            format=format,
        )
        media_types = {
            "turtle": "text/turtle",
            "n3": "text/n3",
            "xml": "application/rdf+xml",
            "trig": "application/trig",
            "nquads": "application/n-quads",
        }
        media_type = media_types.get(format.lower(), "text/plain")
        return Response(content=content, media_type=media_type)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/namespace/list")
def list_namespaces() -> dict[str, str]:
    """List all namespaces in the union graph as a mapping of prefix to URI."""
    try:
        manager = app.state.manager
        ns_manager = manager.namespace_manager()
        return {prefix: str(uri) for prefix, uri in ns_manager.namespaces()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
\
#### MATERIALIZATION API ENDPOINTS ####


class AppRegistration(BaseModel):
    name: str
    executable_digest: str
    entrypoint: str
    outputs: dict[str, Any]
    # Durations travel as whole microseconds; lookback may be the string "all".
    lookback: int | str
    lookback_after: int = 0
    backfill: bool = False
    coalesce: int = 0
    max_delay: int | None = None
    min_interval: int | None = None
    parameters: dict[str, Any] = {}


@app.put("/apps/{name}")
def deploy_app(name: str, request: AppRegistration) -> dict[str, Any]:
    """Validate and select an immutable definition for a named deployment."""
    try:
        if name != request.name:
            raise ValueError("deployment name must match the definition name")
        deployment = Deployment.from_json(request.model_dump_json())
        return {"ok": True, **app.state.manager.deploy_app(deployment)}
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.post("/apps/check")
def check_app(request: AppRegistration, limit: int | None = None) -> dict[str, Any]:
    """Run an app against stored data and return its output without saving it.

    Every computed row is returned unless ``limit`` keeps only the first few
    of each output.
    """
    try:
        deployment = Deployment.from_json(request.model_dump_json())
        return {"ok": True, **app.state.manager.check_app(deployment, limit=limit)}
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.delete("/apps/{name}")
def remove_app(name: str) -> dict[str, Any]:
    try:
        return {"ok": True, **app.state.manager.remove_app(name)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown app {name!r}")


@app.get("/materialization/dag")
def materialization_dag() -> dict[str, Any]:
    """Observational node-link representation of the active compiled DAG."""
    return {"ok": True, **app.state.manager.materializer.dag()}


#### DRIVERS API ENDPOINTS ####


class DriverStartRequest(BaseModel):
    spec: str = Field(..., description="Driver spec: 'my.module:ClassName' or 'path/to/file.py:ClassName'")
    config: dict = Field(default_factory=dict, description="Full merged acquirium config for the driver")
    name: Optional[str] = Field(None, description="Registry name; defaults to the spec's class name")
    interval: Optional[float] = Field(None, description="Tick interval in seconds")


class DriverStopRequest(BaseModel):
    name: str


@app.post("/drivers/start")
def start_driver(req: DriverStartRequest) -> dict[str, Any]:
    """Start a driver as a Ray actor on this server.

    The driver spec must be importable/resolvable on the server host; file
    paths are resolved against the config's __config_dir. Setup runs before
    this returns, so a slow driver setup means a slow response.
    """
    try:
        info = app.state.drivers.start_driver(
            spec=req.spec,
            config=req.config,
            interval=req.interval,
            name=req.name,
        )
        return {"ok": True, "driver": info}
    except Exception as e:
        log.exception("start_driver failed")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/drivers/stop")
def stop_driver(req: DriverStopRequest) -> dict[str, Any]:
    try:
        result = app.state.drivers.stop_driver(req.name)
        return {"ok": True, **result}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log.exception("stop_driver failed")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/drivers/list")
def list_drivers() -> dict[str, Any]:
    try:
        return {"ok": True, "drivers": app.state.drivers.list_drivers()}
    except Exception as e:
        log.exception("list_drivers failed")
        raise HTTPException(status_code=400, detail=str(e))


##### TIMESERIES INGESTION API ENDPOINTS ####


@app.post("/register_datasource")
def register_datasource(req: RegisterDatasourceRequest) -> dict[str, Any]:
    """Register a named datasource in the knowledge graph.

    The source_id is a user-provided string that scopes stream ref_names so
    two sources with the same ref_name never collide in the timeseries store.
    """
    try:
        source_id = app.state.manager.register_datasource(req.source_id)
        return {"ok": True, "source_id": source_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class ResolveStorageKeysRequest(BaseModel):
    uris: list[str]


@app.post("/resolve_storage_keys")
def resolve_storage_keys(req: ResolveStorageKeysRequest) -> dict[str, str]:
    """Map each semantic point_uri (or already-canonical ref_uri) in *uris*
    to its canonical storage key. Callers resolve declared input point_uris
    to ref_uris with this before reading or subscribing to them."""
    try:
        return app.state.manager.timescale.resolve_storage_keys(req.uris)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/insert_timeseries")
def insert_timeseries(streams: Annotated[list[StreamInsert], Body()]) -> dict[str, Any]:
    """Insert timeseries data for one or more streams.

    Each element specifies ``source_id``/``ref_name``, an optional semantic
    ``point_uri``, an optional ``replace`` flag, and a list of
    ``[timestamp, value]`` pairs. A single-stream insert is a one-element list.
    """
    try:
        total = 0
        bulk_streams: dict[str, dict[str, list[tuple[datetime, Any]]]] = {}
        individual_streams: list[StreamInsert] = []
        for s in streams:
            # A stream needing its own publication_id, point_uri registration,
            # or replace semantics can't share one bulk publication with its
            # source-id siblings, so it always takes the individual path.
            if s.point_uri is None and not s.replace and s.publication_id is None:
                bulk_streams.setdefault(s.source_id, {})[s.ref_name] = s.values
            else:
                individual_streams.append(s)

        for source_id, source_streams in bulk_streams.items():
            total += app.state.manager.insert_timeseries_batch(source_id, source_streams).row_count
        for s in individual_streams:
            total += app.state.manager.insert_timeseries(
                source_id=s.source_id,
                ref_name=s.ref_name,
                rows=s.values,
                point_uri=s.point_uri,
                replace=s.replace,
                publication_id=s.publication_id,
            ).row_count
        insert_stats.record(
            origin="http",
            rows=sum(len(s.values) for s in streams),
            streams=[s.ref_name or str(s.point_uri) for s in streams],
        )
        return {"ok": True, "rows_inserted": total}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/insert_timeseries_arrow")
async def insert_timeseries_arrow(request: Request):
    try:
        body = await request.body()
        log.debug("/insert_timeseries_arrow: received %d bytes", len(body))
        reader = ipc.RecordBatchStreamReader(pa.BufferReader(body))
        df = pl.from_arrow(reader.read_all())
        log.debug("/insert_timeseries_arrow: parsed Arrow stream into df rows=%d", len(df))

        # A caller-supplied publication_id makes a retried flush idempotent:
        # it is reused verbatim against PublicationStore.publish, whose
        # id-plus-hash check returns the original receipt instead of
        # re-applying the mutation. One request can span multiple source_ids,
        # each its own atomic publication, so the base id is namespaced per
        # source.
        base_publication_id = request.headers.get("X-Acquirium-Publication-Id")

        total = 0
        for key, source_df in df.partition_by("source_id", as_dict=True).items():
            sid = key[0] if isinstance(key, tuple) else key
            log.debug("/insert_timeseries_arrow: dispatching source=%s rows=%d", sid, len(source_df))
            pub_id = f"{base_publication_id}:{sid}" if base_publication_id else None
            total += app.state.manager.insert_timeseries_arrow(
                str(sid), source_df.drop("source_id").to_arrow(), publication_id=pub_id
            ).row_count

        return {"ok": True, "rows_inserted": total}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    # Accept ISO strings like "2025-12-17T10:30:00Z" or "2025-12-17 10:30:00"
    return dtparser.isoparse(s)


#### TIMESERIES API ENDPOINTS ####
@app.get("/timeseries")
def get_timeseries(
    request: Request,
    uri: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: Optional[int] = None,
    order: str = "asc",
    value_mode: str = "default",
    ):
    try:
        if order not in ("asc", "desc"):
            raise ValueError("order must be 'asc' or 'desc'")
    
        start_dt = _parse_dt(start)
        end_dt = _parse_dt(end)

        batches: Iterator[pa.RecordBatch] = app.state.manager.timeseries_batch(
            uri=uri,
            start=start_dt,
            end=end_dt,
            limit=limit,
            order=order,        # type: ignore[arg-type]
            batch_size=request.app.state.read_batch_size,
            value_mode=value_mode,
        )

        accept = request.headers.get("accept", "")

        def arrow_stream() -> Iterator[bytes]:
            buf = io.BytesIO()
            writer: ipc.RecordBatchStreamWriter | None = None
            try:
                for batch in batches:
                    if writer is None:
                        writer = ipc.new_stream(buf, batch.schema)
                    writer.write_batch(batch)
                    data = buf.getvalue()
                    if data:
                        yield data
                        buf.seek(0)
                        buf.truncate(0)
                if writer is None:
                    empty_schema = pa.schema([
                        ("ts", pa.timestamp("us", tz="UTC")),
                        ("value", pa.float64()),
                        ("uri", pa.string()),
                    ])
                    writer = ipc.new_stream(buf, empty_schema)
            finally:
                if writer is not None:
                    writer.close()
            data = buf.getvalue()
            if data:
                yield data

        return StreamingResponse(
            arrow_stream(),
            media_type="application/vnd.apache.arrow.stream",
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/timeseries_info")
def timeseries_info(req: TimeseriesInfoRequest) -> dict[str, Any]:
    try:
        result = app.state.manager.timeseries_info_batch(req.uris)
        return {uri: info.model_dump() for uri, info in result.items()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/sparql_json")
def sparql_json(query: str, include_dependencies: bool = True, wait_for_fresh: bool = False):
    try:
        serialized = app.state.manager.sparql_json(
            query, include_dependencies=include_dependencies, wait_for_fresh=wait_for_fresh,
        )
        if serialized is not None:
            return _sparql_results_to_rows(serialized)
        result = app.state.manager.sparql_dict(
            query, include_dependencies=include_dependencies, wait_for_fresh=wait_for_fresh,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/sparql")
def sparql(
    request: Request,
    query: Annotated[str, Query(description="SPARQL SELECT, ASK, CONSTRUCT, or DESCRIBE query")],
    include_dependencies: bool = True,
    wait_for_fresh: bool = False,
) -> Response:
    """Read-only SPARQL 1.1 Protocol endpoint over Acquirium's derived graph.

    The default dataset is inferred deployment data plus resolved ontology and
    shape triples. ``include_dependencies=false`` omits that closure while retaining
    inferred deployment data. Dataset-selection and update protocol parameters
    are deliberately not exposed by this read-only first version.
    """
    results_format, graph_format = _accepted_sparql_formats(request.headers.get("accept", "*/*"))
    try:
        content, media_type = app.state.manager.sparql_serialized(
            query,
            include_dependencies=include_dependencies,
            wait_for_fresh=wait_for_fresh,
            results_format=results_format,
            graph_format=graph_format,
        )
        return Response(content=content, media_type=media_type)
    except Exception as exc:
        # A GET endpoint accepts only SPARQL Query forms; parser and query
        # failures are request errors under the SPARQL Protocol.
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class SparqlQueryRequest(BaseModel):
    query: str = Field(..., description="SPARQL SELECT/ASK/CONSTRUCT query")
    include_dependencies: bool = Field(True, description="Include ontology/shape triples")
    wait_for_fresh: bool = Field(
        False,
        description="Wait for pending inference; default returns the last complete graph",
    )


@app.post("/sparql_json")
def sparql_json_post(req: SparqlQueryRequest):
    """POST form of /sparql_json: VALUES-heavy queries (e.g. resolved
    traversal edges) exceed URL length limits, so the client posts."""
    try:
        serialized = app.state.manager.sparql_json(
            req.query,
            include_dependencies=req.include_dependencies,
            wait_for_fresh=req.wait_for_fresh,
        )
        if serialized is not None:
            return _sparql_results_to_rows(serialized)
        return app.state.manager.sparql_dict(
            req.query,
            include_dependencies=req.include_dependencies,
            wait_for_fresh=req.wait_for_fresh,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class SparqlUpdateRequest(BaseModel):
    update: str = Field(..., description="SPARQL UPDATE (INSERT/DELETE) statement")
    source_id: str = Field(
        min_length=1,
        description="Owner of the data graph to update; use 'plant' for the shared plant model",
    )


@app.post("/sparql_update")
def sparql_update(req: SparqlUpdateRequest) -> dict[str, Any]:
    """Run a SPARQL UPDATE against plant or source-owned data."""
    try:
        return {
            "ok": True,
            **app.state.manager.sparql_update(req.update, source_id=req.source_id),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/resolve_text")
def resolve_text(
    text: str,
    kind: Optional[str] = None,
    top_k: int = 5,
    min_score: float = 0.5,
    context: Optional[list[str]] = Query(None),
) -> dict[str, Any]:
    matches = app.state.manager.resolve_text(
        text=text, kind=kind, top_k=top_k, min_score=min_score, context=context
    )
    return {"matches": matches}


@app.post("/resolve_record")
def resolve_record(req: ResolveRecordRequest) -> dict[str, Any]:
    fields = {f.name: (f.text, f.kind) for f in req.fields}
    matches = app.state.manager.resolve_record(
        fields, top_k=req.top_k, min_score=req.min_score
    )
    return {"matches": matches}


@app.post("/insert_log")
def insert_log(
        point_uri: Optional[str] = None,
        log_timestamp: str = "",
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        message: str = "",
    ) -> dict[str, Any]:
    try:
        uri = point_uri if point_uri else PLANT_URI
        log_time = dtparser.isoparse(log_timestamp)
        obs_start = _parse_dt(observation_start)
        obs_end = _parse_dt(observation_end)
        log_entry = LogEntry(
            point_uri=uri,
            timestamp=log_time,
            period=TimeIntervalModel(start=obs_start, end=obs_end),
            message=message,
        )
        log.info(f"Inserting log entry: {log_entry}")
        app.state.manager.insert_log(log_entry)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/query_logs", response_model=list[LogEntry])
def query_logs(
        point_uri: Optional[str] = None,
        log_time_start: Optional[str] = None,
        log_time_end: Optional[str] = None,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
    ) -> list[LogEntry]:
    try:
        log_time_interval = None
        if log_time_start is not None or log_time_end is not None:
            log_time_interval = TimeIntervalModel(
                start=_parse_dt(log_time_start) if log_time_start is not None else None,
                end=_parse_dt(log_time_end) if log_time_end is not None else None,
            )
        obs_time_interval = None
        if observation_start is not None or observation_end is not None:
            obs_time_interval = TimeIntervalModel(
                start=_parse_dt(observation_start) if observation_start is not None else None,
                end=_parse_dt(observation_end) if observation_end is not None else None,
            )
        uri = point_uri if point_uri else PLANT_URI
        logs = app.state.manager.query_logs(
            point_uri=uri,
            log_time_interval=log_time_interval,
            obs_time_interval=obs_time_interval,
        )
        return logs
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/delete_logs")
def delete_logs(
        point_uri: Optional[str] = None,
    ) -> dict[str, Any]:
    try:
        uri = point_uri if point_uri else PLANT_URI
        if app.state.manager.delete_logs(uri):
            return {"ok": True}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete logs")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# -------------------- Unit conversion --------------------

class ResolveUnitRequest(BaseModel):
    identifier: str = Field(..., description="Unit URI, label, symbol, or UCUM code")

class ConversionFactorsRequest(BaseModel):
    from_unit: str = Field(..., description="Source unit identifier")
    to_unit: str = Field(..., description="Target unit identifier")


class ResolveConversionRequest(BaseModel):
    from_unit: str = Field(..., description="Source unit: URI or free text")
    to_unit: str = Field(..., description="Target unit: URI or free text")
    top_k: int = Field(5, description="Candidates considered per side")
    min_score: float = Field(0.5, description="Minimum resolver score")


@app.post("/resolve_conversion")
def resolve_conversion(req: ResolveConversionRequest) -> dict[str, Any]:
    try:
        return app.state.manager.resolve_conversion_info(
            req.from_unit, req.to_unit, top_k=req.top_k, min_score=req.min_score
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/resolve_unit")
def resolve_unit(req: ResolveUnitRequest) -> dict[str, Any]:
    try:
        return app.state.manager.resolve_unit_info(req.identifier)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/conversion_factors")
def conversion_factors(req: ConversionFactorsRequest) -> dict[str, Any]:
    try:
        return app.state.manager.get_conversion_factors(req.from_unit, req.to_unit)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
