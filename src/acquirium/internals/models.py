"""Pydantic models for the streaming API."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Literal, Any, TYPE_CHECKING
from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict, Field, RootModel
from acquirium.internals.internals_namespaces import ACQUIRIUM_NS
from rdflib import URIRef
# Fixed UUID namespace for deterministic handle generation.
# All (source_id, ref_name) pairs are hashed within this namespace so handles
# are globally unique and reproducible without any state.
_HANDLE_NAMESPACE = uuid.UUID("6a8f3c2e-4b1d-5e7f-9012-3a4b5c6d7e8f")


def compute_handle(source_id: str, ref_name: str) -> URIRef:
    """Return a deterministic UUID5 handle for a (source_id, ref_name) pair.

    The handle is used as the TimescaleDB storage key and stored as
    ``ref:hasTimeseriesId`` in the RDF graph.  It is stable across restarts
    and can be recomputed at any time from the same inputs.
    """
    handle_str = str(uuid.uuid5(_HANDLE_NAMESPACE, f"{source_id}:{ref_name}"))
    return ACQUIRIUM_NS[handle_str]

if TYPE_CHECKING:
    from acquirium.Client.query import Query

class TimeseriesInfo(BaseModel):
    table: str
    row_count: int
    earliest: datetime | None = None
    latest: datetime | None = None



class Point(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    uri: str
    handle: str | URIRef | list[str] | list[URIRef] | None = None
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
    The TimescaleDB storage key (handle) is computed deterministically from
    both via :func:`compute_handle` — so two sources with the same ``ref_name``
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


class AppOutputSpec(BaseModel):
    kind: Literal["timeseries", "event", "trigger"]
    point_uri: str
    quantity_kind: str | None = None
    unit: str | None = None
    data_source: str | None = None
    storage_backend: str | None = None


class AppSpec(BaseModel):
    name: str
    version: str = "0.0"
    app_type: str = "soft_sensor"
    docker_image: str | None = None
    module: str | None = None
    app_class: str | None = None
    entrypoint: str | None = None
    command: str | None = None
    source_code: str | None = None
    entry_file: str | None = None
    queries: dict[str, dict] = Field(default_factory=dict)
    outputs: list[AppOutputSpec] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)


class AppRunRequest(BaseModel):
    app_id: str
    start: datetime | None = None
    end: datetime | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    keep_alive: bool = False
    interval: float = 10.0


class AppStopRequest(BaseModel):
    run_id: str | None = None
    app_id: str | None = None
