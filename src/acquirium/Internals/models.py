"""Pydantic models for the streaming API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from dataclasses import dataclass
from pydantic import BaseModel, Field, RootModel
from datetime import timedelta

class TimeseriesInfo(BaseModel):
    table: str
    row_count: int
    earliest: datetime | None = None
    latest: datetime | None = None



class Point(BaseModel):
    uri: str
    handle: str | None = None
    types: list[str] = Field(default_factory=list)
    unit: str | None = None
    last_reported: datetime | None = None
    stream: TimeseriesInfo | None = None


class PointCreateRequest(BaseModel):
    uri: str
    types: list[str] = Field(default_factory=list)
    unit: str | None = None


class InsertTimeseriesRequest(BaseModel):
    values: list[tuple[datetime, float | int | str]]


class InsertBatchRequest(RootModel[dict[str, list[tuple[datetime, float | int | str]]]]):
    """Batch insert where keys are point URIs and values are lists of (ts, value)."""

    @property
    def streams(self) -> dict[str, list[tuple[datetime, float | int | str]]]:
        return self.root


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