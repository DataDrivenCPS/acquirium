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

import pyoxigraph as ox

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, Response
from dateutil import parser as dtparser
from pydantic import BaseModel, Field
from datetime import datetime

from acquirium.Server.config import load_active_config
from acquirium.Server.manager import Manager
from acquirium.apps import App
from escapement import PendingError, ExecutionError, CapacityError

from acquirium.internals.models import (
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
    """Return (host, port, use_ssl) that driver processes use to reach this server."""
    if local_host := os.environ.get("ACQUIRIUM_SELF_HOST"):
        port = int(
            os.environ.get("ACQUIRIUM_SELF_PORT")
            or cfg.get("server", {}).get("port", 8000)
        )
        return local_host, port, False

    driver_cfg = cfg.get("driver", {})
    # 127.0.0.1 rather than "localhost": see AcquiriumClient (issue #85).
    host = driver_cfg.get("server_url", "127.0.0.1")
    if host == "0.0.0.0":
        host = "127.0.0.1"
    port = int(
        driver_cfg.get("server_port")
        or os.environ.get("ACQUIRIUM_SELF_PORT")
        or cfg.get("server", {}).get("port", 8000)
    )
    use_ssl = bool(driver_cfg.get("use_ssl", False))
    return host, port, use_ssl


async def _wait_until_healthy(base_url: str) -> bool:
    """Poll /health until the server answers. The lifespan startup hook runs
    before uvicorn begins serving, so startup work whose drivers connect back
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
    """Start every [[drivers]] entry from the config as a driver process.

    The caller must wait for /health first (_wait_until_healthy) — driver
    processes talk to this server over HTTP. Setup ordering stays serial because
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
            parameters = {key: value for key, value in entry.items() if key not in {"spec", "name"}}
            result = target(aq, parameters) if callable(target) else target
            deployments = [result] if isinstance(result, App) else list(result or [])
            for declaration in deployments:
                aq.deploy_app(declaration)
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
    # False in exact-only mode: the indexes are built and ready, but hold no
    # embeddings, so they answer exact matches only.
    semantic: bool = True
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

    loaded_config = load_active_config()
    _cfg = loaded_config.data
    config_dir = str(loaded_config.directory)
    if config_dir not in sys.path:
        sys.path.insert(0, config_dir)
    server_cfg = _cfg.get("server", {})
    m = Manager.from_env()
    app.state.manager = m
    app.state.read_batch_size = int(server_cfg.get("read_batch_size", 50_000))

    try:
        m._sync_stream_refs_from_graph()
        m.recover_apps()
    except Exception as e:
        log.exception("Startup failed: %s", e)
        try:
            m.close()
        finally:
            raise

    # Shutdown signal for the insert summary logger thread.
    summary_stop = threading.Event()
    start_insert_summary_thread(summary_stop, interval=10.0)

    # Drivers run as child processes that connect back over HTTP.
    _host, _port, _use_ssl = _self_connect_cfg(_cfg)
    supervisor = DriverSupervisor(server_url=_host, server_port=_port, use_ssl=_use_ssl)
    app.state.drivers = supervisor

    # Once the server answers /health, start configured drivers, then deploy
    # configured transformations. Drivers load their plant models during setup,
    # so application queries resolve against the intended graph.
    async def _startup_drivers() -> None:
        if not await _wait_until_healthy(supervisor.base_url):
            log.error("Server never became healthy at %s; skipping configured drivers", supervisor.base_url)
            return
        await _start_config_drivers(supervisor, _cfg)
        await asyncio.to_thread(_deploy_config_apps, _cfg)

    startup_task = asyncio.create_task(_startup_drivers())

    try:
        yield
    finally:
        summary_stop.set()
        startup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await startup_task
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
        semantic=status.get("semantic", True),
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


@app.get("/apps")
def list_apps() -> dict[str, Any]:
    """List all deployments, including unmatched and failed apps."""
    return {"ok": True, "apps": app.state.manager.apps.list()}


@app.get("/apps/{name}")
def inspect_app(name: str) -> dict[str, Any]:
    """Read a deployment's declarations, output schemas, and binding progress."""
    try:
        return {"ok": True, "app": app.state.manager.apps.inspect(name)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown app {name!r}")


@app.put("/apps/{name}")
def deploy_app(
    name: str, definition: Annotated[dict, Body()], dry_run: bool = False
) -> dict[str, Any]:
    """Deploy an app, or with ``dry_run`` report what deploying it would do."""
    try:
        if definition.get("name") != name:
            raise ValueError("app name must match the route")
        declaration = App.from_dict(definition)
        if dry_run:
            return {"ok": True, "app": app.state.manager.apps.preview(declaration)}
        return {"ok": True, "app": app.state.manager.deploy_app(declaration)}
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.delete("/apps/{name}")
def remove_app(name: str):
    try:
        return {"ok": True, **app.state.manager.remove_app(name)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown app {name!r}")


@app.get("/engine/status")
def engine_status():
    status = app.state.manager.core.processor.status()
    status["python_workers"] = app.state.manager.core.engine.python_worker_stats()
    # Operator IDs are "<app>:<group digest>"; roll their totals up per app.
    # compute_ms sums operators that may have run in parallel.
    apps = {}
    for operator, totals in status["stats"]["operators"].items():
        rollup = apps.setdefault(
            operator.split(":", 1)[0],
            {"operators": 0, "groups": 0, "input_rows": 0, "changed_rows": 0, "compute_ms": 0.0},
        )
        rollup["operators"] += 1
        for key in ("groups", "input_rows", "changed_rows", "compute_ms"):
            rollup[key] += totals[key]
    status["apps"] = apps
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_bytes = int(peak if sys.platform == "darwin" else peak * 1024)
    except ImportError:
        peak_bytes = None
    status["process"] = {"pid": os.getpid(), "peak_rss_bytes": peak_bytes}
    return status


@app.get("/engine/snapshot")
def engine_snapshot():
    snapshot = app.state.manager.core.engine.snapshot()
    return {"epoch": snapshot.epoch, "applied": snapshot.applied, "plan": snapshot.plan}


@app.post("/engine/retry")
def engine_retry():
    app.state.manager.core.processor.retry()
    return engine_status()


@app.get("/engine/changes")
def engine_changes(
    epoch: int = Query(0, ge=0, lt=2**64),
    ordinal: int = Query(0, ge=0, lt=2**64),
    limit: int = Query(1000, ge=1, le=10_000),
):
    try:
        return app.state.manager.core.changes(epoch=epoch, ordinal=ordinal, limit=limit)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


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
    """Start a driver as a child process of this server.

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
    to its canonical ref_uri. Callers do this before reading or subscribing;
    the database's internal integer ref_id is never exposed."""
    try:
        return app.state.manager.resolve_storage_keys(req.uris)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _receipt(receipt, source_id, request_id, wait):
    core = app.state.manager.core
    error = None
    try:
        if wait > 0:
            core.wait(receipt, timeout=wait)
    except (PendingError, ExecutionError) as exception:
        error = str(exception)
    status = core.processor.status()
    return {"source_id": source_id, "request_id": request_id, "sequence": receipt.sequence,
            "accepted": True, "published": status["engine"]["applied"] >= receipt.sequence,
            "error": error or status["error"]}


@app.post("/insert_timeseries")
def insert_timeseries(streams: Annotated[list[StreamInsert], Body()], wait: float = Query(30, ge=0)):
    try:
        receipts, total = [], 0
        groups = {}
        for stream in streams:
            group = groups.setdefault((stream.source_id, stream.request_id), {})
            group.setdefault(stream.ref_name, []).extend(stream.values)
            total += len(stream.values)
        for (source_id, request_id), observations in groups.items():
            receipt = app.state.manager.insert_timeseries_batch(source_id, observations,
                                                               request_id=request_id)
            receipts.append(_receipt(receipt, source_id, request_id, wait))
        return {"ok": True, "rows_inserted": total, "receipts": receipts}
    except CapacityError as error:
        raise HTTPException(status_code=503, detail=str(error), headers={"Retry-After": "1"}) from error
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.post("/insert_timeseries_arrow")
async def insert_timeseries_arrow(request: Request, wait: float = Query(30, ge=0)):
    try:
        body = await request.body()
        table = ipc.RecordBatchStreamReader(pa.BufferReader(body)).read_all()
        frame = pl.from_arrow(table)
        request_id = request.headers.get("X-Acquirium-Request-Id")
        if not request_id:
            raise ValueError("X-Acquirium-Request-Id is required")
        receipts = []
        for key, source_frame in frame.partition_by("source_id", as_dict=True).items():
            source_id = str(key[0])
            receipt = await asyncio.to_thread(app.state.manager.insert_timeseries_arrow,
                source_id, source_frame.drop("source_id").to_arrow(), request_id=request_id)
            receipts.append(await asyncio.to_thread(_receipt, receipt, source_id, request_id, wait))
        return {"ok": True, "rows_inserted": len(frame), "receipts": receipts}
    except CapacityError as error:
        raise HTTPException(status_code=503, detail=str(error), headers={"Retry-After": "1"}) from error
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


class DeleteObservations(BaseModel):
    source_id: str
    ref_name: str
    timestamps: list[int]
    request_id: str


@app.post("/delete_timeseries")
def delete_timeseries(request: DeleteObservations, wait: float = Query(30, ge=0)):
    try:
        receipt = app.state.manager.delete_timeseries(request.source_id, request.ref_name,
            request.timestamps, request_id=request.request_id)
        return _receipt(receipt, request.source_id, request.request_id, wait)
    except CapacityError as error:
        raise HTTPException(status_code=503, detail=str(error), headers={"Retry-After": "1"}) from error
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


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
    epoch: int | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    ):
    try:
        if order not in ("asc", "desc"):
            raise ValueError("order must be 'asc' or 'desc'")
    
        batches: Iterator[pa.RecordBatch] = app.state.manager.timeseries_batch(
            uri=uri,
            start=start_ns if start_ns is not None else start,
            end=end_ns if end_ns is not None else end,
            epoch=epoch,
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
                        ("ts", pa.timestamp("ns", tz="UTC")),
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
        return result
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
