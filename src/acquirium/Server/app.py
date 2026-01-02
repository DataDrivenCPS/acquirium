from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Optional, Iterator

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from dateutil import parser as dtparser
from pydantic import BaseModel, Field
from datetime import datetime

from acquirium.Server.manager import Manager

from acquirium.internals.models import Order

import pyarrow.ipc as ipc
import pyarrow as pa

log = logging.getLogger("acquirium.api")


class Health(BaseModel):
    ok: bool
    mqtt_subscriptions: int | None = None


class IngestStatus(BaseModel):
    scheduled: int
    done: int
    error: int
    total: int


class InsertGraphRequest(BaseModel):
    rdf_graph: str = Field(..., description="File path or RDF text")
    format: str = "turtle"
    replace: bool = True


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

    # Start ingestion services at startup
    try:
        # one time file ingestion kickoff (async threads inside Manager)
        m._ingest_external_references_async()

        # start mqtt subscribers from graph
        n = m._connect_mqtt_streams_from_graph()
        app.state.mqtt_subscriptions = n
        log.info("Started %d MQTT subscriptions from graph", n)
    except Exception as e:
        log.exception("Startup failed: %s", e)
        # If startup fails, ensure we close and crash so Docker restart policy can help
        try:
            m.close()
        finally:
            raise

    try:
        yield
    finally:
        # FastAPI shutdown
        try:
            m.close()
        except Exception:
            log.exception("Error during shutdown")


app = FastAPI(title="Acquirium API", version="0.1", lifespan=lifespan)


@app.get("/health", response_model=Health)
def health():
    # If we got here, the app is up
    mqtt_n = getattr(app.state, "mqtt_subscriptions", None)
    return Health(ok=True, mqtt_subscriptions=mqtt_n)

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



#### GRAPH API ENDPOINTS ####


@app.post("/insert_graph")
def insert_graph(req: InsertGraphRequest) -> dict[str, Any]:
    try:
        app.state.manager.insert_graph(rdf_graph = req.rdf_graph, format=req.format, replace=req.replace)
        return {"ok": True}
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
    point_uri: str,
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
            point_uri=point_uri,
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
            ("point_uri", pa.string()),
        ])

        def arrow_stream() -> Iterator[bytes]:
            sink = pa.BufferOutputStream()
            writer = ipc.new_stream(sink, schema)
            try:
                for batch in batches:
                    writer.write_batch(batch)
                    # emit whatever has been buffered so far
                    buf = sink.getvalue()
                    if buf.size > 0:
                        yield buf.to_pybytes()
                        sink = pa.BufferOutputStream()
                        writer = ipc.new_stream(sink, schema)  # restart stream per chunk
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        return StreamingResponse(
            arrow_stream(),
            media_type="application/vnd.apache.arrow.stream",
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    

@app.get("/sparql_json")
def sparql_json(query: str, use_union: bool = True) -> dict[str, Any]:
    try:
        result = app.state.manager.sparql_dict(query, use_union=use_union)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))