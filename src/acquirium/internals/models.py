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
    publication_id: str | None = None


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
