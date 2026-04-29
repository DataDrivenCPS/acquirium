from __future__ import annotations

import io
import logging
import os
import threading
import time
import tomllib
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Optional, Iterator

from fastapi import Body, FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import StreamingResponse, Response
from dateutil import parser as dtparser
from pydantic import BaseModel, Field
from datetime import datetime

from acquirium.Server.manager import Manager

from acquirium.internals.models import (
    Order,
    LogEntry,
    TimeInterval,
    TimeIntervalModel,
    TimeseriesInfo,
    AppSpec,
    AppRunRequest,
    AppStopRequest,
    StreamInsert,
    RegisterDatasourceRequest,
)
from acquirium.internals.internals_namespaces import PLANT_URI

import pyarrow.ipc as ipc
import pyarrow as pa

from acquirium.Server.insert_stats import insert_stats, start_insert_summary_thread

log = logging.getLogger("acquirium.api")

def _start_insert_summary_thread(stop_event: threading.Event, interval: float = 30.0) -> threading.Thread:
    return start_insert_summary_thread(stop_event, interval)


# ---------------------------------------------------------------------------
# In-process driver helpers
# ---------------------------------------------------------------------------

def _run_inprocess_driver(
    entry: dict,
    manager: Manager,
    cfg: dict,
    stop_event: threading.Event,
) -> None:
    """Thread target: run a single [[drivers]] entry using DirectAcquirium."""
    from acquirium.Server.direct_client import DirectAcquirium
    from acquirium.cli import _import_driver_class

    spec = entry["spec"]
    driver_overrides = {k: v for k, v in entry.items() if k != "spec"}
    merged_cfg = {**cfg, "driver": {**cfg.get("driver", {}), **driver_overrides}}
    interval = float(driver_overrides.get("interval", cfg.get("driver", {}).get("interval", 10.0)))
    config_dir = Path(cfg.get("__config_dir", Path.cwd()))

    try:
        driver_cls = _import_driver_class(spec, base_dir=config_dir)
        direct_aq = DirectAcquirium(
            manager,
            origin=spec,
            insert_batch_rows=int(merged_cfg.get("driver", {}).get("insert_batch_rows", 50_000)),
        )
        driver = driver_cls(direct_aq, merged_cfg)
        driver.setup()
        known_version = manager.graph_version()
        log.info("In-process driver ready: %s", driver_cls.__name__)
    except Exception:
        log.exception("In-process driver %s setup failed; thread exiting", spec)
        return

    try:
        while not stop_event.wait(timeout=interval):
            try:
                v = manager.graph_version()
                if v != known_version:
                    known_version = v
                    try:
                        driver.on_graph_change()
                    except Exception:
                        log.exception("In-process driver %s on_graph_change error", spec)
                driver.loop()
            except Exception:
                log.exception("In-process driver %s loop error", spec)
    finally:
        try:
            driver.stop()
        except Exception:
            log.exception("In-process driver %s stop error", spec)


def _start_inprocess_drivers(
    manager: Manager,
    stop_event: threading.Event,
) -> list[threading.Thread]:
    """Read [[drivers]] from ACQUIRIUM_CONFIG and start each as a daemon thread."""
    from acquirium.cli import _load_config

    config_path = os.environ.get("ACQUIRIUM_CONFIG")
    if not config_path:
        return []

    try:
        cfg = _load_config(Path(config_path))
    except Exception:
        log.warning("Could not load config from ACQUIRIUM_CONFIG=%s; skipping in-process drivers", config_path)
        return []

    threads: list[threading.Thread] = []
    for entry in cfg.get("drivers", []):
        spec = entry.get("spec")
        if not spec:
            log.warning("[[drivers]] entry missing 'spec'; skipping")
            continue
        t = threading.Thread(
            target=_run_inprocess_driver,
            args=(entry, manager, cfg, stop_event),
            daemon=True,
            name=f"acquirium-driver-{spec.rsplit(':', 1)[-1]}",
        )
        t.start()
        threads.append(t)
        log.info("Started in-process driver: %s", spec)
    return threads


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

class IngestStatus(BaseModel):
    scheduled: int
    done: int
    error: int
    total: int


class InsertGraphRequest(BaseModel):
    rdf_graph: str = Field(..., description="File path or RDF text")
    format: str = "turtle"
    replace: bool = True
    wait_for_embedding: bool = False


class TimeseriesInfoRequest(BaseModel):
    uris: list[str]


class FindDataRequest(BaseModel):
    from_: Optional[str] = None
    path: Optional[str] = None
    class_: Optional[str] = None
    quantity_kind: Optional[str] = None
    enumeration_kind: Optional[str] = None
    unit: Optional[str] = None
    data_source: Optional[str] = None
    substance: Optional[str] = None
    medium: Optional[str] = None
    alias: Optional[str] = None
    hops: int = 3


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    m = Manager.from_env()
    app.state.manager = m

    try:
        m._sync_stream_handles_from_graph()
    except Exception as e:
        log.exception("Startup failed: %s", e)
        try:
            m.close()
        finally:
            raise

    # Shared shutdown signal for all server-owned background threads.
    # In-process drivers listed in [[drivers]] run alongside the API server,
    # and the insert summary logger also runs in a background thread. They both
    # poll this event so FastAPI lifespan shutdown can stop them before closing
    # the Manager and its storage connections.
    driver_stop = threading.Event()
    _start_inprocess_drivers(m, driver_stop)
    _start_insert_summary_thread(driver_stop, interval=10.0)

    try:
        yield
    finally:
        # Tell background driver/summary threads to exit, then close the manager.
        # Driver threads are daemons, so shutdown should not block indefinitely;
        # the event mainly prevents them from continuing to use Manager after it
        # starts closing.
        driver_stop.set()
        try:
            m.close()
        except Exception:
            log.exception("Error during shutdown")


app = FastAPI(title="Acquirium API", version="0.1", lifespan=lifespan)


@app.get("/health", response_model=Health)
def health():
    # If we got here, the app is up
    return Health(ok=True)

@app.get("/ingest_status", response_model=IngestStatus)
def ingest_status():
    manager = app.state.manager
    status = manager.ingest_status()
    return IngestStatus(
        scheduled=status["scheduled_tasks"],
        done=status["done_tasks"],
        error=status["error_tasks"],
        total=status["total_tasks"],
    )

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
            wait_for_embedding=req.wait_for_embedding,
        )
        return {"ok": True, "embedding_ready": req.wait_for_embedding}
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


#### APPS API ENDPOINTS ####


@app.post("/apps/register")
def register_app(spec: AppSpec) -> dict[str, Any]:
    try:
        app.state.manager.register_app_spec(spec)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/apps/run")
def run_app(req: AppRunRequest) -> dict[str, Any]:
    try:
        run_id = app.state.manager.run_app(req)
        print(f"Started app run with ID: {run_id}")
        return {"ok": True, "run_id": run_id}
    except Exception as e:
        log.exception("run_app failed") 
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/apps/stop")
def stop_app(req: AppStopRequest) -> dict[str, Any]:
    try:
        result = app.state.manager.stop_app(run_id=req.run_id, app_id=req.app_id)
        return {"ok": True, **result}
    except Exception as e:
        log.exception("stop_app failed")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/apps/list")
def list_app_runs(app_id: Optional[str] = None) -> dict[str, Any]:
    try:
        runs = app.state.manager.list_app_runs(app_id=app_id)
        return {"ok": True, "runs": runs}
    except Exception as e:
        log.exception("list_app_runs failed")
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

    Each element specifies a ``ref_uri``, an optional ``point_uri`` (defaults
    to ``ref_uri``), an optional ``replace`` flag, and a list of
    ``[timestamp, value]`` pairs.  A single-stream insert is just a
    one-element list.
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


@app.post("/ingest_external_reference")
async def ingest_external_reference(
    data_uri: str = Form(...),
    ref_uri: str = Form(...),
    time_column: str | None = Form(None),
    value_column: str | None = Form(None),
    file: UploadFile = File(...),
) -> dict[str, Any]:
    try:
        content = await file.read()
        n = app.state.manager.ingest_reference_bytes(
            data_uri=data_uri,
            ref_uri=ref_uri,
            content=content,
            time_column=time_column,
            value_column=value_column,
            filename=file.filename or "upload",
        )
        return {"ok": True, "rows_ingested": n}
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
            batch_size=50_000,
        )

        accept = request.headers.get("accept", "")

        # Default: Arrow IPC stream (best for Polars)
        schema = pa.schema([
            ("ts", pa.timestamp("us", tz="UTC")),
            # set value type explicitly if you know it; string shown as safe default
            ("value", pa.string()),
            ("uri", pa.string()),
        ])

        def arrow_stream() -> Iterator[bytes]:
            buf = io.BytesIO()
            writer = ipc.new_stream(buf, schema)
            for batch in batches:
                writer.write_batch(batch)
                data = buf.getvalue()
                if data:
                    yield data
                    buf.seek(0)
                    buf.truncate(0)
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
) -> dict[str, Any]:
    matches = app.state.manager.resolve_text(
        text=text, kind=kind, top_k=top_k, min_score=min_score
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
