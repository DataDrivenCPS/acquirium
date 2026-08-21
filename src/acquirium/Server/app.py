from __future__ import annotations

import asyncio
import base64
import contextlib
import io
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Iterator, Literal, Optional

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
from acquirium.Materialization.definitions import MaterializationDefinition
from acquirium.Materialization.impact import ImpactPolicy
from acquirium.Materialization.impact import TimeRange
from acquirium.Materialization.state import ArtifactCandidate, ArtifactRequest
from acquirium.Materialization.experiments import ExperimentArtifact, ExperimentRunRequest, ExperimentRunner

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
    if _cfg.get("apps"):
        raise RuntimeError(
            "[[apps]] configuration is no longer supported. "
            "Deploy durable @transform definitions through the transformations API "
            "or use @service for long-running reactive work."
        )

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
    if os.getenv("ACQUIRIUM_MATERIALIZATION_EXECUTOR", "local").lower() == "ray":
        m.use_ray_materialization_executor(int(os.getenv("ACQUIRIUM_MATERIALIZATION_RAY_WORKERS", "2")))
    _host, _port, _use_ssl = _self_connect_cfg(_cfg)
    supervisor = DriverSupervisor(server_url=_host, server_port=_port, use_ssl=_use_ssl)
    app.state.drivers = supervisor
    server_cfg = _cfg.get("server", {})

    async def _materialization_loop() -> None:
        """Drain active durable transformation work without blocking requests."""
        owner = f"server-{os.getpid()}"
        idle_seconds = float(server_cfg.get("materialization_poll_seconds", 0.25))
        safety_scan_seconds = float(server_cfg.get("service_safety_scan_seconds", 1.0))
        next_service_scan = 0.0
        while True:
            try:
                rebound = await asyncio.to_thread(m.run_rebind_once, owner)
                ran_work = await asyncio.to_thread(m.run_materialization_once, owner)
                ran_service = await asyncio.to_thread(m.run_service_once)
                ran_effect = await asyncio.to_thread(m.run_effect_once, owner)
                now = asyncio.get_running_loop().time()
                if now >= next_service_scan:
                    await asyncio.to_thread(m.service_safety_scan)
                    next_service_scan = now + safety_scan_seconds
            except asyncio.CancelledError:
                raise
            except Exception:
                # The scheduler recorded the failed attempt as retryable; keep
                # the server healthy and retry after the normal idle delay.
                log.exception("Materialization execution failed")
                rebound = ran_work = ran_service = ran_effect = False
            await asyncio.sleep(0 if rebound or ran_work or ran_service or ran_effect else idle_seconds)

    materialization_task = asyncio.create_task(_materialization_loop())
    app.state.materialization_task = materialization_task

    # Once the server answers /health, start configured drivers. Durable
    # transformations and services are restored by their own storage-backed
    # schedulers rather than a configuration-time app actor.
    async def _startup_actors() -> None:
        if not await _wait_until_healthy(supervisor.base_url):
            log.error("Server never became healthy at %s; skipping configured drivers", supervisor.base_url)
            return
        await _start_config_drivers(supervisor, _cfg)

    startup_task = asyncio.create_task(_startup_actors())

    try:
        yield
    finally:
        summary_stop.set()
        startup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await startup_task
        materialization_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await materialization_task
        try:
            await asyncio.to_thread(supervisor.stop_all)
        except Exception:
            log.exception("Error stopping drivers during shutdown")
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
#### APPS API ENDPOINTS ####


class TransformationRegistration(BaseModel):
    name: str
    source_digest: str
    entrypoint: str
    inputs: dict[str, Any] | None = None
    bind: dict[str, Any] | None = None
    outputs: dict[str, Any]
    impact: dict[str, Any]
    parameters_schema: dict[str, Any] = Field(default_factory=dict)


class ServiceRegistration(BaseModel):
    name: str
    source_digest: str
    entrypoint: str
    parameters_schema: dict[str, Any] = Field(default_factory=dict)


class StateRevisionPromotion(BaseModel):
    policy: Literal["prospective", "recompute_all", "recompute_from"] = "prospective"
    effective_from: datetime | None = None


class ArtifactRequestSubmission(BaseModel):
    request_id: str
    kind: str
    deployment_name: str
    binding_id: str
    input_versions: dict[str, int] = Field(default_factory=dict)
    start: datetime
    end: datetime
    previous_revision: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactLeaseRequest(BaseModel):
    owner: str


class ArtifactCompletion(BaseModel):
    owner: str
    attempt: int
    data_base64: str
    media_type: str = "application/octet-stream"
    metadata: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)


class ArtifactFailure(BaseModel):
    owner: str
    attempt: int
    error: dict[str, Any]


class ExperimentStart(BaseModel):
    run_id: str
    definition_id: str
    graph_revision: int
    start: datetime
    end: datetime
    params: dict[str, Any] = Field(default_factory=dict)
    params_schema: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    input_versions: dict[str, int] = Field(default_factory=dict)
    binding_snapshot: list[dict[str, Any]] = Field(default_factory=list)
    state_revision: str | None = None


class ExperimentFinish(BaseModel):
    status: Literal["succeeded", "failed", "cancelled"]
    error: dict[str, Any] | None = None


class ExperimentMetric(BaseModel):
    value: Any


class ExperimentKeep(BaseModel):
    reason: str


class ExperimentArtifactAttachment(BaseModel):
    name: str
    digest: str
    metadata: dict[str, Any] = Field(default_factory=dict)


@app.post("/transformations/register")
def register_transformation(request: TransformationRegistration) -> dict[str, Any]:
    """Register trusted local transformation metadata; execution is scheduled separately."""
    try:
        definition = MaterializationDefinition(
            name=request.name, source_digest=request.source_digest, entrypoint=request.entrypoint,
            inputs=request.inputs, bind=request.bind, outputs=request.outputs,
            impact=ImpactPolicy.from_json(request.impact), parameters_schema=request.parameters_schema,
        )
        return {"ok": True, **app.state.manager.register_transformation(definition)}
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


def _service_response(service) -> dict[str, Any]:
    return {"name": service.name, "definition_id": service.definition_id, "status": service.status,
            "health": service.health, "updated_at": service.updated_at}


@app.post("/services/register")
def register_service(request: ServiceRegistration) -> dict[str, Any]:
    try:
        definition = MaterializationDefinition(name=request.name, source_digest=request.source_digest,
            entrypoint=request.entrypoint, kind="service", parameters_schema=request.parameters_schema)
        return {"ok": True, **app.state.manager.register_service(definition)}
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.post("/services/{name}/start")
def start_service(name: str) -> dict[str, Any]:
    try:
        return {"ok": True, **_service_response(app.state.manager.start_service(name))}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown service {name!r}")
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error))


@app.post("/services/{name}/stop")
def stop_service(name: str) -> dict[str, Any]:
    try:
        return {"ok": True, **_service_response(app.state.manager.service_supervisor.stop(name))}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown service {name!r}")


@app.get("/services/{name}")
def service_status(name: str) -> dict[str, Any]:
    try:
        return {"ok": True, **_service_response(app.state.manager.materialization.service(name))}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown service {name!r}")


@app.post("/artifact-requests")
def create_artifact_request(request: ArtifactRequestSubmission) -> dict[str, Any]:
    try:
        artifact_request = ArtifactRequest(request.request_id, request.kind, request.deployment_name,
            request.binding_id, request.input_versions, TimeRange(request.start, request.end),
            request.previous_revision, request.metadata)
        return {"ok": True, "request_id": app.state.manager.materialization.create_artifact_request(artifact_request)}
    except (TypeError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.post("/artifact-requests/lease")
def lease_artifact_request(request: ArtifactLeaseRequest) -> dict[str, Any]:
    lease = app.state.manager.materialization.lease_artifact_request(request.owner)
    if lease is None:
        return {"ok": True, "lease": None}
    item = lease.request
    return {"ok": True, "lease": {"request_id": item.request_id, "kind": item.kind,
        "deployment_name": item.deployment_name, "binding_id": item.binding_id,
        "input_versions": dict(item.input_versions), "start": item.interval.start,
        "end": item.interval.end, "previous_revision": item.previous_revision,
        "metadata": dict(item.metadata), "owner": lease.owner, "attempt": lease.attempt,
        "expires_at": lease.expires_at}}


@app.post("/artifact-requests/{request_id}/complete")
def complete_artifact_request(request_id: str, request: ArtifactCompletion) -> dict[str, Any]:
    try:
        payload = base64.b64decode(request.data_base64, validate=True)
        lease = app.state.manager.materialization.leased_artifact_request(request_id, request.owner, request.attempt)
        candidate = ArtifactCandidate(payload, request.media_type, request.metadata, request.metrics)
        artifact = app.state.manager.materialization_artifacts.put(payload, media_type=request.media_type,
            metadata=request.metadata)
        revision = app.state.manager.materialization.complete_artifact_request(lease, artifact, candidate)
        return {"ok": True, "revision_id": revision.revision_id, "status": revision.status}
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=409, detail=str(error))


@app.post("/artifact-requests/{request_id}/fail")
def fail_artifact_request(request_id: str, request: ArtifactFailure) -> dict[str, Any]:
    try:
        lease = app.state.manager.materialization.leased_artifact_request(request_id, request.owner, request.attempt)
        app.state.manager.materialization.fail_artifact_request(lease, request.error)
        return {"ok": True}
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=409, detail=str(error))


def _experiment_response(run) -> dict[str, Any]:
    return {"run_id": run.run_id, "definition_id": run.definition_id, "graph_revision": run.graph_revision,
            "status": run.status, "start": run.interval.start, "end": run.interval.end,
            "params": dict(run.params), "params_schema": dict(run.params_schema),
            "metadata": dict(run.metadata), "input_versions": dict(run.input_versions),
            "binding_snapshot": list(run.binding_snapshot), "state_revision": run.state_revision,
            "started_at": run.started_at, "finished_at": run.finished_at, "error": run.error,
            "keep_reason": run.keep_reason, "collected_at": run.collected_at}


@app.post("/experiments/runs")
def start_experiment(request: ExperimentStart) -> dict[str, Any]:
    try:
        run = app.state.manager.materialization.start_experiment(ExperimentRunRequest(
            request.run_id, request.definition_id, request.graph_revision, TimeRange(request.start, request.end),
            request.params, request.params_schema, request.metadata, request.input_versions,
            request.binding_snapshot, request.state_revision))
        return {"ok": True, **_experiment_response(run)}
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.get("/experiments/runs/{run_id}")
def experiment_run(run_id: str) -> dict[str, Any]:
    try: return {"ok": True, **_experiment_response(app.state.manager.materialization.experiment_run(run_id))}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")


@app.get("/experiments/runs")
def list_experiments(status: str | None = None, metadata_key: str | None = None,
                     metadata_value: str | None = None) -> dict[str, Any]:
    metadata = {metadata_key: json.loads(metadata_value) if metadata_value else None} if metadata_key else None
    try:
        runs = app.state.manager.materialization.list_experiments(status=status, metadata=metadata)
        return {"ok": True, "runs": [_experiment_response(run) for run in runs]}
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=400, detail=f"metadata_value must be JSON: {error}")


@app.post("/experiments/runs/{run_id}/rerun/{new_run_id}")
def rerun_experiment(run_id: str, new_run_id: str) -> dict[str, Any]:
    try: return {"ok": True, **_experiment_response(app.state.manager.materialization.rerun_experiment(run_id, new_run_id))}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")
    except ValueError as error: raise HTTPException(status_code=409, detail=str(error))


@app.post("/experiments/runs/{run_id}/execute")
def execute_experiment(run_id: str) -> dict[str, Any]:
    try:
        result = ExperimentRunner(app.state.manager.materialization, app.state.manager.materialization_executor).run(run_id)
        return {"ok": True, "result": result, **_experiment_response(app.state.manager.materialization.experiment_run(run_id))}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")
    except ValueError as error: raise HTTPException(status_code=409, detail=str(error))
    except Exception as error: raise HTTPException(status_code=500, detail=str(error))


@app.post("/experiments/runs/{run_id}/finish")
def finish_experiment(run_id: str, request: ExperimentFinish) -> dict[str, Any]:
    try: return {"ok": True, **_experiment_response(app.state.manager.materialization.finish_experiment(run_id, status=request.status, error=request.error))}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")
    except ValueError as error: raise HTTPException(status_code=400, detail=str(error))


@app.post("/experiments/runs/{run_id}/metrics/{name}")
def record_experiment_metric(run_id: str, name: str, request: ExperimentMetric) -> dict[str, Any]:
    try:
        app.state.manager.materialization.record_experiment_metric(run_id, name, request.value)
        return {"ok": True}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")
    except ValueError as error: raise HTTPException(status_code=400, detail=str(error))


@app.get("/experiments/runs/{run_id}/metrics")
def experiment_metrics(run_id: str) -> dict[str, Any]:
    try: return {"ok": True, "metrics": app.state.manager.materialization.experiment_metrics(run_id)}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")


@app.get("/experiments/runs/{run_id}/artifacts")
def experiment_artifacts(run_id: str) -> dict[str, Any]:
    try:
        artifacts = app.state.manager.materialization.experiment_artifacts(run_id)
        return {"ok": True, "artifacts": [{"name": item.name, "digest": item.digest, "metadata": dict(item.metadata)} for item in artifacts]}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")


@app.post("/experiments/runs/{run_id}/artifacts")
def attach_experiment_artifact(run_id: str, request: ExperimentArtifactAttachment) -> dict[str, Any]:
    try:
        app.state.manager.materialization.attach_experiment_artifact(
            run_id, ExperimentArtifact(request.name, request.digest, request.metadata)
        )
        return {"ok": True}
    except KeyError as error: raise HTTPException(status_code=404, detail=f"unknown run or artifact {error.args[0]!r}")


@app.post("/experiments/runs/{run_id}/keep")
def keep_experiment(run_id: str, request: ExperimentKeep) -> dict[str, Any]:
    try: return {"ok": True, **_experiment_response(app.state.manager.materialization.keep_experiment(run_id, request.reason))}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")
    except ValueError as error: raise HTTPException(status_code=400, detail=str(error))


@app.post("/experiments/runs/{run_id}/collect")
def collect_experiment(run_id: str) -> dict[str, Any]:
    try: return {"ok": True, **_experiment_response(app.state.manager.materialization.collect_experiment(run_id))}
    except KeyError: raise HTTPException(status_code=404, detail=f"unknown experiment run {run_id!r}")
    except ValueError as error: raise HTTPException(status_code=409, detail=str(error))


def _set_transformation_status(name: str, status: str) -> dict[str, Any]:
    try:
        app.state.manager.materialization.set_deployment_status(name, status)
        return {"ok": True, **app.state.manager.materialization.deployment_status(name)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown transformation {name!r}")


@app.post("/transformations/{name}/start")
def start_transformation(name: str) -> dict[str, Any]:
    return _set_transformation_status(name, "active")


@app.post("/transformations/{name}/pause")
def pause_transformation(name: str) -> dict[str, Any]:
    return _set_transformation_status(name, "paused")


@app.post("/transformations/{name}/rebind")
def rebind_transformation(name: str) -> dict[str, Any]:
    deployment = app.state.manager.materialization.deployment_status(name)
    if deployment is None:
        raise HTTPException(status_code=404, detail=f"unknown transformation {name!r}")
    revision = int(app.state.manager.graph_store.graph_status()["published_version"])
    if revision < 0:
        raise HTTPException(status_code=409, detail="no published query graph is available")
    app.state.manager.materialization.request_rebind(name, revision)
    return {"ok": True, "name": name, "graph_revision": revision}


@app.post("/transformations/{name}/reconcile")
def reconcile_transformation(name: str) -> dict[str, Any]:
    """Force a fresh staged topology resolution against the published graph."""
    deployment = app.state.manager.materialization.deployment_status(name)
    if deployment is None:
        raise HTTPException(status_code=404, detail=f"unknown transformation {name!r}")
    revision = int(app.state.manager.graph_store.graph_status()["published_version"])
    if revision < 0:
        raise HTTPException(status_code=409, detail="no published query graph is available")
    app.state.manager.materialization.request_rebind(name, revision, force=True)
    return {"ok": True, "name": name, "graph_revision": revision, "reconcile": True}


@app.get("/transformations")
def transformations() -> dict[str, Any]:
    return {"ok": True, "transformations": app.state.manager.materialization.deployments()}


@app.get("/transformations/{name}")
def transformation_status(name: str) -> dict[str, Any]:
    result = app.state.manager.materialization.deployment_status(name)
    if result is None:
        raise HTTPException(status_code=404, detail=f"unknown transformation {name!r}")
    return {"ok": True, **result}


@app.post("/transformations/{name}/preview")
def preview_transformation(name: str):
    """Run the next pending partition without committing any output."""
    if app.state.manager.materialization.deployment_status(name) is None:
        raise HTTPException(status_code=404, detail=f"unknown transformation {name!r}")
    try:
        result = app.state.manager.preview_transformation(name)
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))
    if result is None:
        raise HTTPException(status_code=409, detail="no pending partition is available for preview")
    table, metadata = result
    return _arrow_response(table, "acquirium-materialization-preview", {f"x-acquirium-{key.replace('_', '-')}": str(value) for key, value in metadata.items()})


@app.post("/state-revisions/{revision_id}/promote")
def promote_state_revision(revision_id: str, request: StateRevisionPromotion) -> dict[str, Any]:
    try:
        revision = app.state.manager.materialization.promote_state_revision(
            revision_id, policy=request.policy, effective_from=request.effective_from
        )
        return {"ok": True, "revision_id": revision.revision_id, "status": revision.status,
                "policy": revision.policy, "effective_from": revision.effective_from}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown state revision {revision_id!r}")
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


def _arrow_response(table: "pa.Table", metadata_key: str, metadata: dict[str, Any]) -> Response:
    """Serialize a materialization table and metadata as an Arrow IPC stream."""
    tagged = table.replace_schema_metadata(
        {**(table.schema.metadata or {}), metadata_key.encode("utf-8"): json.dumps(metadata).encode("utf-8")}
    )
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, tagged.schema) as writer:
        writer.write_table(tagged)
    return Response(
        content=sink.getvalue().to_pybytes(),
        media_type="application/vnd.apache.arrow.stream",
    )


class MaterializationLeaseRequest(BaseModel):
    owner: str


class MaterializationSnapshotRequest(BaseModel):
    owner: str
    attempt: int


@app.post("/internal/materializations/lease")
def internal_materialization_lease(request: MaterializationLeaseRequest) -> dict[str, Any]:
    lease = app.state.manager.materialization.lease_partition(request.owner)
    if lease is None:
        return {"lease": None}
    return {"lease": {"partition_id": lease.partition.partition_id, "plan_id": lease.partition.plan_id,
            "start": lease.partition.interval.start.isoformat(), "end": lease.partition.interval.end.isoformat(),
            "attempt": lease.attempt, "expires_at": lease.expires_at.isoformat()}}


@app.post("/internal/materializations/{partition_id}/snapshot")
def internal_materialization_snapshot(partition_id: str, request: MaterializationSnapshotRequest):
    try:
        storage = app.state.manager.materialization
        lease = storage.leased_partition(partition_id, request.owner, request.attempt)
        inputs, outputs = storage.partition_refs(partition_id)
        snapshot = storage.snapshot_partition(lease, inputs)
        return _arrow_response(snapshot.inputs, "acquirium-materialization-snapshot", {
            "partition_id": partition_id, "plan_id": lease.partition.plan_id, "attempt": lease.attempt,
            "input_versions": snapshot.input_versions, "input_refs": inputs, "output_refs": outputs,
            "start": lease.partition.interval.start.isoformat(), "end": lease.partition.interval.end.isoformat(),
        })
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=409, detail=str(error))


@app.post("/internal/materializations/{partition_id}/commit")
async def internal_materialization_commit(partition_id: str, request: Request, owner: str, attempt: int):
    try:
        body = await request.body()
        replacement = ipc.open_stream(pa.py_buffer(body)).read_all()
        storage = app.state.manager.materialization
        lease = storage.leased_partition(partition_id, owner, attempt)
        inputs, outputs = storage.partition_refs(partition_id)
        snapshot = storage.snapshot_partition(lease, inputs)
        output_publication_id = storage.commit_replacement(snapshot, input_refs=inputs, output_refs=outputs, replacement=replacement)
        return {"ok": True, "output_publication_id": output_publication_id}
    except Exception as error:
        raise HTTPException(status_code=409, detail=str(error))


class MaterializationFailureRequest(BaseModel):
    owner: str
    attempt: int
    error: dict[str, Any] = Field(default_factory=dict)


@app.post("/internal/materializations/{partition_id}/fail")
def internal_materialization_fail(partition_id: str, request: MaterializationFailureRequest) -> dict[str, Any]:
    # A failed attempt becomes pending again; the attempt count remains durable.
    try:
        storage = app.state.manager.materialization
        lease = storage.leased_partition(partition_id, request.owner, request.attempt)
        if hasattr(storage, "fail_partition"):
            storage.fail_partition(lease, request.error)
        else:
            raise RuntimeError("materialization backend lacks failure transitions")
        return {"ok": True}
    except Exception as error:
        raise HTTPException(status_code=409, detail=str(error))
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
    to its canonical storage key. An app resolves its declared input
    point_uris to ref_uris with this before subscribing to them (used by
    the actor's default, non-MappedApp mapping resolution)."""
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

        # A caller-supplied publication_id makes a retried flush idempotent
        # (see continuous_batch_plan.md Decision 5/1d): reused verbatim
        # against ContinuousStore.publish, whose id-plus-hash check returns
        # the original receipt instead of re-applying the mutation. One
        # request can span multiple source_ids, each its own atomic
        # publication, so the base id is namespaced per source.
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
