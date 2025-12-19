"""Pydantic models for the streaming API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, RootModel


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


class SoftSensorSpec(BaseModel):
    uri: str
    sources: list[str] = Field(default_factory=list)
    module_path: str = Field(..., description="Import path like module:function")
    params: dict | None = None
    schedule: str | None = None  # optional cron/interval string for future use


class SoftSensorRunRequest(BaseModel):
    uris: list[str] | None = None  # if None, run all


class SoftSensorRunResult(BaseModel):
    uri: str
    inserted: int
    rows: int
    status: str


Order = Literal["asc", "desc"]
