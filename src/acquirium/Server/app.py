from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Optional, Iterator
import ray

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, Response
from dateutil import parser as dtparser
from pydantic import BaseModel, Field
from datetime import datetime

from acquirium.Server.manager import Manager

from acquirium.internals.models import (
    LogEntry,
    TimeIntervalModel,
    AppSpec,
    AppRunRequest,
    AppStopRequest,
    StreamInsert,
    RegisterDatasourceRequest,
)
from acquirium.internals.internals_namespaces import PLANT_URI

import pyarrow.ipc as ipc
import pyarrow as pa
import polars as pl

from acquirium.Server.insert_stats import insert_stats, start_insert_summary_thread
from acquirium.Server.ray_backend import DriverSupervisor, AppSupervisor

log = logging.getLogger("acquirium.api")


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


async def _start_config_drivers(supervisor: DriverSupervisor, cfg: dict) -> None:
    """Start every [[drivers]] entry from the config as a Ray actor.

    Driver actors talk to this server over HTTP, so this waits until the
    server answers /health before starting them — the lifespan startup hook
    runs before uvicorn begins serving. Setup ordering stays serial because
    DriverSupervisor.start_driver holds its lock across setup.
    """
    import requests

    entries = cfg.get("drivers", [])
    if not entries:
        return

    health_url = f"{supervisor.base_url}/health"
    for _ in range(300):
        try:
            r = await asyncio.to_thread(requests.get, health_url, timeout=2)
            if r.ok:
                break
        except Exception:
            pass
        await asyncio.sleep(2)
    else:
        log.error("Server never became healthy at %s; not starting [[drivers]]", health_url)
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

    m = Manager.from_env()
    app.state.manager = m
    app.state.read_batch_size = int(_cfg.get("server", {}).get("read_batch_size", 50_000))

    try:
        m._sync_stream_refs_from_graph()
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
    ray.init(ignore_reinit_error=True)
    _host, _port, _use_ssl = _self_connect_cfg(_cfg)
    supervisor = DriverSupervisor(server_url=_host, server_port=_port, use_ssl=_use_ssl)
    app.state.drivers = supervisor
    # Apps also run as Ray actors (one AppRunner per app) that connect back
    # over HTTP; the supervisor spawns and tracks them.
    app_supervisor = AppSupervisor(
        app_storage_root=m.app_storage_root,
        server_url=_host, server_port=_port, use_ssl=_use_ssl,
    )
    app.state.apps = app_supervisor
    # [[drivers]] from the config start once the server answers /health;
    # the lifespan startup hook runs before uvicorn begins serving.
    startup_task = asyncio.create_task(_start_config_drivers(supervisor, _cfg))

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
            await asyncio.to_thread(app_supervisor.stop_all_apps)
        except Exception:
            log.exception("Error stopping apps during shutdown")
        ray.shutdown()
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
def graph_version() -> dict[str, int]:
    """Return a counter that the server increments on every graph mutation.

    Long-running clients (e.g. keep-alive app workers) can poll this to
    decide when to rebuild cached queries that depend on the graph.
    """
    return {"version": app.state.manager.graph_version()}



#### GRAPH API ENDPOINTS ####


@app.post("/insert_graph")
def insert_graph(req: InsertGraphRequest) -> dict[str, Any]:
    try:
        app.state.manager.insert_graph(
            rdf_graph=req.rdf_graph,
            format=req.format,
            replace=req.replace,
        )
        return {"ok": True, "embedding_ready": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/export_graph")
def export_graph(include_union: bool = True, format: str = "turtle"):
    """Export the RDF graph in the specified format.

    Args:
        include_union: If True, includes the union graph with all imports resolved.
                      If False, returns only the main graph.
        format: Serialization format - turtle, n3, xml, trig, etc.
    """
    try:
        content = app.state.manager.graph_store.export_graph(
            include_union=include_union,
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
#### APPS API ENDPOINTS ####


@app.post("/apps/register")
def register_app(spec: AppSpec) -> dict[str, Any]:
    try:
        info = app.state.apps.register_app(spec)
        return {"ok": True, **info}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/apps/run")
def run_app(req: AppRunRequest) -> dict[str, Any]:
    try:
        result = app.state.apps.run_app(req)
        return {"ok": True, **result}
    except Exception as e:
        log.exception("run_app failed")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/apps/stop")
def stop_app(req: AppStopRequest) -> dict[str, Any]:
    try:
        result = app.state.apps.stop_app(req.app_id)
        return {"ok": True, **result}
    except Exception as e:
        log.exception("stop_app failed")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/apps/list")
def list_app_runs(app_id: Optional[str] = None) -> dict[str, Any]:
    try:
        if app_id:
            return {"ok": True, **app.state.apps.app_status(app_id)}
        return {"ok": True, "apps": app.state.apps.list_apps()}
    except Exception as e:
        log.exception("list_app_runs failed")
        raise HTTPException(status_code=400, detail=str(e))


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
            if s.point_uri is None and not s.replace:
                bulk_streams.setdefault(s.source_id, {})[s.ref_name] = s.values
            else:
                individual_streams.append(s)

        for source_id, source_streams in bulk_streams.items():
            total += app.state.manager.insert_timeseries_batch(source_id, source_streams)
        for s in individual_streams:
            total += app.state.manager.insert_timeseries(
                source_id=s.source_id,
                ref_name=s.ref_name,
                rows=s.values,
                point_uri=s.point_uri,
                replace=s.replace,
            )
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

        total = 0
        for key, source_df in df.partition_by("source_id", as_dict=True).items():
            sid = key[0] if isinstance(key, tuple) else key
            log.debug("/insert_timeseries_arrow: dispatching source=%s rows=%d", sid, len(source_df))
            total += app.state.manager.insert_timeseries_arrow(str(sid), source_df.drop("source_id").to_arrow())

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
def sparql_json(query: str, use_union: bool = True) -> dict[str, Any]:
    try:
        result = app.state.manager.sparql_dict(query, use_union=use_union)
        return result
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
