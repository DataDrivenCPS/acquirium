from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Protocol, runtime_checkable

import pyarrow as pa
import polars as pl

from acquirium.internals.models import LogEntry, Order, TimeIntervalModel, TimeseriesInfo


@runtime_checkable
class TimeseriesStore(Protocol):
    """Full storage contract for timeseries bookkeeping.

    Both TimescaleStore (Postgres/TimescaleDB) and DuckDBStore implement
    this protocol. Choose the backend at startup via the
    ACQUIRIUM_TIMESERIES_BACKEND environment variable.
    """

    db_path: str | Path | None

    # ---- table management ----
    def ensure_table(self) -> str: ...

    # ---- timeseries mutations ----
    def upsert_rows(
        self,
        ref_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "text",
    ) -> int: ...
    def replace_rows(
        self,
        ref_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "text",
    ) -> int: ...
    def bulk_insert_polars(self, df: pl.DataFrame) -> int: ...

    # ---- stream reference registry ----
    def ensure_stream_ref(
        self,
        point_uri: str | None,
        source_id: str,
        ref_name: str,
        ref_uri: str | None = None,
        value_kind: str = "text",
    ) -> str: ...
    def stream_value_kind(self, ref_uri: str) -> str | None: ...
    def resolve_storage_key(self, point_uri: str) -> str: ...
    def resolve_storage_keys(self, point_uris: list[str]) -> dict[str, str]: ...

    # ---- timeseries reads ----
    def timeseries(
        self,
        ref_uri: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: Order = "asc",
        batch_size: int = 50_000,
    ) -> Iterator[pa.RecordBatch]: ...
    def timeseries_info(self, ref_uri: str) -> TimeseriesInfo: ...
    def timeseries_info_batch(self, ref_uris: list[str]) -> dict[str, TimeseriesInfo]: ...

    # ---- logs ----
    def insert_log(self, log: LogEntry) -> None: ...
    def query_logs(
        self,
        point_uri: str,
        log_time_interval: TimeIntervalModel | None = None,
        obs_time_interval: TimeIntervalModel | None = None,
    ) -> list[LogEntry]: ...
    def delete_logs(self, point_uri: str) -> bool: ...

    # ---- transaction hooks ----
    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

    # ---- utility ----
    def sql_query(self, query: str) -> dict[str, Any]: ...
    def close(self) -> None: ...
