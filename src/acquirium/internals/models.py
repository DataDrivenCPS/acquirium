"""Pydantic models for the streaming API."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Literal, Any, TYPE_CHECKING
from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict, Field, RootModel
from acquirium.internals.internals_namespaces import ACQUIRIUM_NS
from rdflib import URIRef
# Fixed UUID namespace for deterministic reference URI generation.
# All (source_id, ref_name) pairs are hashed within this namespace so ref URIs
# are globally unique and reproducible without any state.
_REF_URI_NAMESPACE = uuid.UUID("6a8f3c2e-4b1d-5e7f-9012-3a4b5c6d7e8f")


def looks_like_uri(value: object) -> bool:
    """True if *value* is a string already in URI form.

    The single canonical check (strict prefix form). The looser
    ``"://" in value`` variant scattered elsewhere also matches arbitrary
    text containing ``://`` and should converge here.
    """
    return isinstance(value, str) and value.startswith(
        ("http://", "https://", "urn:")
    )


def compute_ref_uri(source_id: str, ref_name: str) -> URIRef:
    """Return a deterministic UUID5 ref URI for a (source_id, ref_name) pair.

    The ref URI is used as the TimescaleDB storage key and stored as
    ``ref:hasTimeseriesId`` in the RDF graph.  It is stable across restarts
    and can be recomputed at any time from the same inputs.
    """
    ref_uri_str = str(uuid.uuid5(_REF_URI_NAMESPACE, f"{source_id}:{ref_name}"))
    return ACQUIRIUM_NS[ref_uri_str]


compute_handle = compute_ref_uri


if TYPE_CHECKING:
    from acquirium.Client.explore.core import Query

class TimeseriesInfo(BaseModel):
    table: str
    row_count: int
    earliest: datetime | None = None
    latest: datetime | None = None



class Point(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    uri: str
    handle: str | None = None
    ref_uri: str | URIRef | list[str] | list[URIRef] | None = None
    types: list[str] = Field(default_factory=list)
    unit: str | None = None
    last_reported: datetime | None = None
    stream: TimeseriesInfo | None = None


class PointCreateRequest(BaseModel):
    uri: str
    types: list[str] = Field(default_factory=list)
    unit: str | None = None


class RegisterDatasourceRequest(BaseModel):
    """Request to register a named datasource."""
    source_id: str


class StreamInsert(BaseModel):
    """A single stream's data payload for the unified insert endpoint.

    ``source_id`` identifies the registered datasource (e.g. ``"mybox-metrics"``).
    ``ref_name`` is the source-local stream identifier (e.g. ``"cpu_percent"``).
    The TimescaleDB storage key (ref URI) is computed deterministically from
    both via :func:`compute_ref_uri` — so two sources with the same ``ref_name``
    never collide.
    """

    source_id: str
    ref_name: str
    point_uri: str | None = None
    replace: bool = False
    values: list[tuple[datetime, float | int | str | None]]


Order = Literal["asc", "desc"]

@dataclass(frozen=True)
class TimeInterval:
    start: datetime
    end: datetime

    def __init__(self, start: datetime | None = None, end: datetime | None = None, duration: float | None = None):
        if start is None and end is None:
            raise ValueError("Either start or end must be provided")

        if duration is not None:
            if start is not None and end is None:
                end = start + timedelta(seconds=duration)
            elif end is not None and start is None:
                start = end - timedelta(seconds=duration)
            else:
                raise ValueError("If duration is provided, only one of start or end should be provided")

        # If duration is not provided, require both bounds
        if start is None or end is None:
            raise ValueError("Both start and end must be provided (or provide duration)")

        if end <= start:
            raise ValueError("end must be after start")

        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)

    def serialize(self) -> dict[str, str]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}

    def to_str(self) -> str:
        return f"{self.start.isoformat()}/{self.end.isoformat()}"

    @classmethod
    def from_str(cls, s: str) -> "TimeInterval":
        start_str, end_str = s.split("/")
        return cls(start=datetime.fromisoformat(start_str), end=datetime.fromisoformat(end_str))
        
class TimeIntervalModel(BaseModel):
    start: datetime | None = None
    end: datetime | None = None

    def to_interval(self) -> TimeInterval:
        return TimeInterval(start=self.start, end=self.end)

class LogEntry(BaseModel):
    point_uri: str
    timestamp: datetime
    period: TimeIntervalModel
    message: str | None = None

    def to_dict(self) -> dict:
        return {
            "point_uri": self.point_uri,
            "log_time": self.timestamp.isoformat(),
            "observation_start": self.period.start.isoformat() if self.period.start else None,
            "observation_end": self.period.end.isoformat() if self.period.end else None,
            "message": self.message,
        }
    

@dataclass
class AppContext:
    app_id: str
    started_at: datetime
    start: datetime | None
    end: datetime | None
    query: Query | None
    params: dict[str, Any]
    queries: dict[str, Query] | None = None
    data: Any | None = None
    state: Any | None = None


class AppOutputSpec(BaseModel):
    kind: Literal["timeseries", "event", "trigger"]
    point_uri: str
    ref_uri: str | None = None
    quantity_kind: str | None = None
    unit: str | None = None
    data_source: str | None = None
    storage_backend: str | None = None


class EnvSpec(BaseModel):
    """Per-app/driver execution environment, resolved to a Ray runtime_env.

    Declared at registration and persisted with the spec so a restart
    rebuilds the same environment. ``setup_commands`` run once per node via
    the worker setup hook (guarded by a marker file) — for prerequisites
    like ``idaes get-extensions`` that install outside the venv.
    """

    pip: list[str] = Field(default_factory=list)
    env_vars: dict[str, str] = Field(default_factory=dict)
    setup_commands: list[str] = Field(default_factory=list)
    py_modules: list[str] = Field(default_factory=list)


class AppSpec(BaseModel):
    name: str
    kind: Literal["app", "task"] = "app"
    version: str = "0.0"
    app_type: str = "soft_sensor"
    app_class: str | None = None
    source_code: str | None = None
    entry_file: str | None = None
    queries: dict[str, dict] = Field(default_factory=dict)
    outputs: list[AppOutputSpec] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    run_mode: Literal["manual", "interval", "on_change"] = "manual"
    interval: float | None = None
    env: EnvSpec | None = None


class TaskSpec(BaseModel):
    """A class-less registered function: one query, one ``fn(ctx)``.

    Tasks are the light tier: no build phase, no state, and — by contract —
    no dependencies beyond the acquirium package, so every task shares one
    host actor. ``fn_source`` is the authoritative persisted form (exec'd on
    restore); ``fn_blob`` (cloudpickle) is a fast path used only when the
    server's Python matches ``python_version`` — cloudpickle is not portable
    across interpreter versions and is not meant for long-term storage.
    """

    # bytes serialize as base64 in JSON (pydantic's default assumes UTF-8,
    # which a pickle is not) — the wire and the on-disk task.json share this.
    model_config = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")

    name: str
    query: dict = Field(default_factory=dict)          # Query.to_dict() form
    fn_name: str
    fn_source: str
    fn_blob: bytes | None = None
    python_version: str | None = None                  # "3.12"
    outputs: list[AppOutputSpec] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    run_mode: Literal["manual", "interval", "on_change"] = "manual"
    interval: float | None = None
    version: str = "0.0"

    def to_app_spec(self) -> "AppSpec":
        """The registration-graph view of this task (shared graph shape)."""
        return AppSpec(
            name=self.name,
            kind="task",
            version=self.version,
            app_type="task",
            queries={"default": self.query} if self.query else {},
            outputs=self.outputs,
            params=self.params,
            run_mode=self.run_mode,
            interval=self.interval,
        )


class AppRunRequest(BaseModel):
    app_id: str
    start: datetime | None = None
    end: datetime | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    keep_alive: bool = False
    interval: float = 10.0
    # Overrun policy: at most this many runs in flight; an interval tick that
    # would exceed it is skipped and counted, never queued.
    max_in_flight: int = Field(default=1, ge=1)
    # Optional per-run wall-clock bound; an overrunning task is cancelled and
    # its run recorded as "timeout".
    run_timeout: float | None = Field(default=None, gt=0)


class AppStopRequest(BaseModel):
    app_id: str
