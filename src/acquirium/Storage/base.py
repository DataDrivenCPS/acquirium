from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable

from acquirium.internals.models import Order, TimeseriesInfo


@runtime_checkable
class TimeseriesStore(Protocol):
    """Minimal storage contract for timeseries bookkeeping."""

    db_path: str | Path | None

    # table management
    def ensure_table(self) -> str: ...

    # timeseries mutations and reads
    def upsert_rows(self, point_uri: str, rows: Iterable[tuple[datetime, Any]]) -> int: ...
    def replace_rows(self, point_uri: str, rows: Iterable[tuple[datetime, Any]]) -> int: ...
    def timeseries(
        self,
        point_uri: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: Order = "asc",
    ) -> list[dict[str, Any]]: ...
    def timeseries_info(self, point_uri: str) -> TimeseriesInfo: ...
    def timeseries_info_batch(self, point_uris: list[str]) -> dict[str, TimeseriesInfo]: ...

    # transaction-ish hooks
    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

    # utility
    def sql_query(self, query: str) -> dict[str, Any]: ...
    def close(self) -> None: ...
