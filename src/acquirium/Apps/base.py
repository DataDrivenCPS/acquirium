from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from acquirium.internals.models import AppContext
from acquirium.Client.explore.core import Query





@dataclass
class Output:
    kind: str
    payload: dict[str, Any]

    @staticmethod
    def timeseries(
        *,
        point_uri: str,
        rows: Iterable[tuple[datetime, Any]] | None = None,
        series: Any | None = None,
        time_index: Iterable[datetime] | None = None,
    ) -> "Output":
        if rows is None:
            if series is None:
                raise ValueError("timeseries output requires rows or series")
            series_list = list(series) if isinstance(series, Iterable) else None
            if series_list is not None and series_list and all(isinstance(v, tuple) and len(v) == 2 for v in series_list):
                rows = series_list
            else:
                values = series.to_list() if hasattr(series, "to_list") else (series_list or list(series))
                if time_index is None:
                    if hasattr(series, "index"):
                        time_index = list(series.index)
                    else:
                        raise ValueError("series outputs require time_index or a Series index")
                times = list(time_index)
                if len(times) != len(values):
                    raise ValueError("time_index length must match series length")
                rows = list(zip(times, values))
        return Output(kind="timeseries", payload={"point_uri": point_uri, "rows": list(rows)})

    @staticmethod
    def event(
        *,
        point_uri: str,
        severity: str,
        message: str,
        ts: datetime | None = None,
        data: dict[str, Any] | None = None,
    ) -> "Output":
        ts = ts or datetime.now(timezone.utc)
        return Output(
            kind="event",
            payload={
                "point_uri": point_uri,
                "severity": severity,
                "message": message,
                "ts": ts,
                "data": data or {},
            },
        )

    @staticmethod
    def trigger(
        *,
        url: str,
        message: Any,
        point_uri: str | None = None,
        ts: datetime | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> "Output":
        return Output(
            kind="trigger",
            payload={
                "url": url,
                "message": message,
                "point_uri": point_uri,
                "ts": ts,
                "headers": headers or {},
                "timeout": timeout,
            },
        )


class App(ABC):
    name: str
    version: str = "0.0"
    app_type: str = "soft_sensor"
    outputs: list[Any] = []
    source_code: str | None = None
    entry_file: str | None = None

    @abstractmethod
    def build_query(self, aq: Any) -> Query | dict[str, Query]:
        raise NotImplementedError
    
    def build_app(self, ctx:AppContext) -> Any:
        return None

    @abstractmethod
    def run(self, ctx: AppContext) -> list[Output]:
        raise NotImplementedError
