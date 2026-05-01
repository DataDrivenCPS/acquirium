from __future__ import annotations

"""DuckDB-backed timeseries store.

Implements the same interface as TimescaleStore so the two backends are
interchangeable.  Select this backend at startup via:

    ACQUIRIUM_TIMESERIES_BACKEND=duckdb  (or "timescale" for Postgres/TimescaleDB)
    ACQUIRIUM_DUCKDB_PATH=/path/to/timeseries.duckdb  (default: {data_dir}/timeseries.duckdb)

**Concurrency:** DuckDB allows only one read-write connection per file at a
time. This class opens one connection in the constructor and protects all
write operations with a ``threading.Lock`` for in-process thread safety.
Multi-process write access to the same ``.duckdb`` file is *not* supported.
Use the Postgres backend when running multiple server workers.

**Timestamps:** All timestamps are stored as ``TIMESTAMP`` (microseconds, UTC)
rather than ``TIMESTAMPTZ``, avoiding a DuckDB Python-API dependency on
``pytz``. The ``_to_utc`` helper normalises every input value before storage,
and ``_add_utc`` re-attaches ``tzinfo=UTC`` to values read back.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator
import threading
import logging

import pyarrow as pa
import pyarrow.compute as pc
import polars as pl

from acquirium.internals.models import (
    LogEntry,
    Order,
    TimeIntervalModel,
    TimeseriesInfo,
    compute_ref_uri,
)
from acquirium.Storage.values import normalize_value_kind, prepare_value_columns, split_value

logger = logging.getLogger(__name__)

TIMESERIES_TABLE = "timeseries"
STREAMS_TABLE = "streams"
LOGS_TABLE = "logs"
TIMESERIES_STREAMS_VIEW = "timeseries_streams"


class DuckDBStore:
    """DuckDB implementation of the TimeseriesStore protocol.

    Uses plain DuckDB tables without TimescaleDB-specific features
    (no hypertables, no compression policies).  The ``logs`` table
    stores the observation period as two separate TIMESTAMP columns
    instead of a Postgres ``tstzrange``.
    """

    def __init__(self, db_path: str | Path, *, recreate: bool = False) -> None:
        import duckdb  # lazy — not required unless duckdb backend is selected

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = duckdb.connect(str(self.db_path))
        self._in_tx = False

        if recreate:
            self._conn.execute(f"DROP VIEW IF EXISTS {TIMESERIES_STREAMS_VIEW}")
            for tbl in (TIMESERIES_TABLE, STREAMS_TABLE, LOGS_TABLE):
                self._conn.execute(f"DROP TABLE IF EXISTS {tbl}")
        self.ensure_table()

    # ---- table management ----

    def ensure_table(self) -> str:
        """Create tables and indexes if they do not exist. Returns a status string."""
        # Use TIMESTAMP (not TIMESTAMPTZ) to avoid a DuckDB/pytz interop issue.
        # All values are normalised to UTC before insertion via _to_utc().
        stmts = [
            f"""
            CREATE TABLE IF NOT EXISTS {TIMESERIES_TABLE} (
                ref_uri VARCHAR NOT NULL,
                ts      TIMESTAMP NOT NULL,
                numeric_value DOUBLE,
                text_value    VARCHAR,
                CHECK (numeric_value IS NULL OR text_value IS NULL),
                UNIQUE (ref_uri, ts)
            )
            """,
            f"CREATE INDEX IF NOT EXISTS idx_ts_ref ON {TIMESERIES_TABLE} (ref_uri, ts)",
            f"CREATE INDEX IF NOT EXISTS idx_ts_numeric_ref ON {TIMESERIES_TABLE} (ref_uri, ts, numeric_value)",
            f"CREATE INDEX IF NOT EXISTS idx_ts_text_ref ON {TIMESERIES_TABLE} (ref_uri, ts, text_value)",
            f"CREATE INDEX IF NOT EXISTS idx_ts_numeric_value ON {TIMESERIES_TABLE} (ref_uri, numeric_value)",
            f"CREATE INDEX IF NOT EXISTS idx_ts_text_value ON {TIMESERIES_TABLE} (ref_uri, text_value)",
            f"""
            CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                ref_uri   VARCHAR PRIMARY KEY,
                point_uri VARCHAR UNIQUE,
                source_id VARCHAR NOT NULL,
                ref_name  VARCHAR NOT NULL,
                value_kind VARCHAR NOT NULL DEFAULT 'numeric'
            )
            """,
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_source_ref_name ON {STREAMS_TABLE} (source_id, ref_name)",
            f"CREATE INDEX IF NOT EXISTS idx_streams_point_uri ON {STREAMS_TABLE} (point_uri)",
            f"""
            CREATE OR REPLACE VIEW {TIMESERIES_STREAMS_VIEW} AS
            SELECT
                t.ref_uri,
                s.point_uri,
                s.source_id,
                s.ref_name,
                COALESCE(s.value_kind, 'numeric') AS value_kind,
                t.ts,
                t.numeric_value AS value_numeric,
                t.text_value AS value_text
            FROM {TIMESERIES_TABLE} AS t
            LEFT JOIN {STREAMS_TABLE} AS s
                ON t.ref_uri = s.ref_uri
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {LOGS_TABLE} (
                point_uri      VARCHAR NOT NULL,
                timestamp      TIMESTAMP NOT NULL,
                observed_start TIMESTAMP,
                observed_end   TIMESTAMP,
                message        VARCHAR NOT NULL,
                UNIQUE (point_uri, timestamp)
            )
            """,
            f"CREATE INDEX IF NOT EXISTS idx_logs_point ON {LOGS_TABLE} (point_uri, timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_logs_obs ON {LOGS_TABLE} (observed_start, observed_end)",
        ]
        with self._lock:
            for stmt in stmts:
                self._conn.execute(stmt)
        return "ok"

    # ---- timeseries mutations ----

    def upsert_rows(
        self,
        ref_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "numeric",
    ) -> int:
        rows_list = [
            (ref_uri, self._to_utc_naive(ts), *split_value(v, value_kind))
            for ts, v in rows
        ]
        if not rows_list:
            return 0
        with self._lock:
            self._conn.executemany(
                f"""
                INSERT INTO {TIMESERIES_TABLE} (ref_uri, ts, numeric_value, text_value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (ref_uri, ts) DO UPDATE SET
                    numeric_value = excluded.numeric_value,
                    text_value = excluded.text_value
                """,
                rows_list,
            )
        return len(rows_list)

    def replace_rows(
        self,
        ref_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "numeric",
    ) -> int:
        rows_list = list(rows)
        with self._lock:
            self._conn.execute(
                f"DELETE FROM {TIMESERIES_TABLE} WHERE ref_uri = ?", [ref_uri]
            )
        return self.upsert_rows(ref_uri, rows_list, value_kind=value_kind)

    def bulk_insert_polars(self, df: pl.DataFrame) -> int:
        """Bulk-insert a Polars DataFrame with canonical or split value columns.

        Uses DuckDB's native Polars bridge — no ADBC required.
        """
        if df.is_empty():
            return 0
        df = prepare_value_columns(df).with_columns(
            pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        )
        df = df.unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
        with self._lock:
            # DuckDB resolves the local name 'df' via its Arrow bridge
            self._conn.execute(
                f"""
                INSERT INTO {TIMESERIES_TABLE} (ref_uri, ts, numeric_value, text_value)
                SELECT ref_uri, ts, numeric_value, text_value FROM df
                ON CONFLICT (ref_uri, ts) DO UPDATE SET
                    numeric_value = excluded.numeric_value,
                    text_value = excluded.text_value
                """
            )
        return len(df)

    # ---- stream reference registry ----

    def ensure_stream_ref(
        self,
        point_uri: str | None,
        source_id: str,
        ref_name: str,
        ref_uri: str | None = None,
        value_kind: str = "numeric",
    ) -> str:
        """Register a stream reference and return its canonical ref URI."""
        if ref_uri is None:
            ref_uri = compute_ref_uri(source_id, ref_name)
        ref_uri_str = str(ref_uri)
        value_kind = normalize_value_kind(value_kind)
        with self._lock:
            self._conn.execute(
                f"""
                INSERT INTO {STREAMS_TABLE} (ref_uri, point_uri, source_id, ref_name, value_kind)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (ref_uri) DO UPDATE SET
                    source_id = excluded.source_id,
                    ref_name  = excluded.ref_name,
                    point_uri = COALESCE(excluded.point_uri, {STREAMS_TABLE}.point_uri),
                    value_kind = excluded.value_kind
                """,
                [ref_uri_str, point_uri, source_id, ref_name, value_kind],
            )
        return ref_uri_str

    def resolve_storage_key(self, point_uri: str) -> str:
        """Return the storage ref URI for point_uri, or point_uri itself if unregistered."""
        row = self._conn.execute(
            f"SELECT ref_uri FROM {STREAMS_TABLE} WHERE point_uri = ?", [point_uri]
        ).fetchone()
        return point_uri if row is None else row[0]

    def resolve_storage_keys(self, point_uris: list[str]) -> dict[str, str]:
        """Batch-resolve point_uris to storage keys."""
        if not point_uris:
            return {}
        placeholders = ", ".join("?" * len(point_uris))
        d = self._conn.execute(
            f"SELECT point_uri, ref_uri FROM {STREAMS_TABLE} WHERE point_uri IN ({placeholders})",
            point_uris,
        ).to_arrow_table().to_pydict()
        registered = dict(zip(d["point_uri"], d["ref_uri"]))
        return {uri: registered.get(uri, uri) for uri in point_uris}

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
    ) -> Iterator[pa.RecordBatch]:
        """Yield PyArrow RecordBatches with schema [ts: timestamp[us,UTC], value, uri]."""
        clauses = ["ref_uri = ?"]
        params: list[Any] = [ref_uri]

        if start:
            clauses.append("ts >= ?")
            params.append(self._to_utc_naive(start))
        if end:
            clauses.append("ts <= ?")
            params.append(self._to_utc_naive(end))

        where = " AND ".join(clauses)
        order_sql = "ASC" if order == "asc" else "DESC"
        limit_sql = f" LIMIT {int(limit)}" if limit else ""

        query = f"""
            SELECT
                ts,
                numeric_value,
                text_value
            FROM {TIMESERIES_TABLE}
            WHERE {where}
            ORDER BY ts {order_sql}{limit_sql}
        """

        value_kind = self._stream_value_kind(ref_uri)
        table = self._conn.execute(query, params).to_arrow_table()
        for batch in table.to_batches(max_chunksize=batch_size):
            numeric_col = batch.column("numeric_value")
            text_col = batch.column("text_value")
            if value_kind == "text":
                val_col = text_col.cast(pa.string())
            elif value_kind == "numeric":
                val_col = numeric_col.cast(pa.float64())
            elif numeric_col.null_count < len(numeric_col):
                val_col = numeric_col.cast(pa.float64())
            else:
                val_col = text_col.cast(pa.string())
            ts_col = pc.assume_timezone(batch.column("ts"), timezone="UTC")
            uri_col = pa.array([ref_uri] * len(batch), type=pa.string())
            yield pa.record_batch([ts_col, val_col, uri_col], names=["ts", "value", "uri"])

    def timeseries_info(self, ref_uri: str) -> TimeseriesInfo:
        row = self._conn.execute(
            f"SELECT COUNT(*), MIN(ts), MAX(ts) FROM {TIMESERIES_TABLE} WHERE ref_uri = ?",
            [ref_uri],
        ).fetchone()
        cnt, earliest_raw, latest_raw = (row[0] or 0, row[1], row[2]) if row else (0, None, None)
        return TimeseriesInfo(
            table=TIMESERIES_TABLE,
            row_count=cnt,
            earliest=self._add_utc(earliest_raw),
            latest=self._add_utc(latest_raw),
        )

    def timeseries_info_batch(self, ref_uris: list[str]) -> dict[str, TimeseriesInfo]:
        if not ref_uris:
            return {}
        placeholders = ", ".join("?" * len(ref_uris))
        d = self._conn.execute(
            f"""
            SELECT ref_uri,
                   COUNT(*)   AS row_count,
                   MIN(ts)    AS earliest,
                   MAX(ts)    AS latest
            FROM {TIMESERIES_TABLE}
            WHERE ref_uri IN ({placeholders})
            GROUP BY ref_uri
            """,
            ref_uris,
        ).to_arrow_table().to_pydict()
        result: dict[str, TimeseriesInfo] = {}
        for uri, cnt, earliest_raw, latest_raw in zip(
            d["ref_uri"], d["row_count"], d["earliest"], d["latest"]
        ):
            result[uri] = TimeseriesInfo(
                table=TIMESERIES_TABLE,
                row_count=cnt or 0,
                earliest=self._add_utc(earliest_raw),
                latest=self._add_utc(latest_raw),
            )
        for uri in ref_uris:
            if uri not in result:
                result[uri] = TimeseriesInfo(table=TIMESERIES_TABLE, row_count=0)
        return result

    # ---- logs ----

    def insert_log(self, log: LogEntry) -> None:
        obs_start = self._to_utc_naive(log.period.start) if log.period and log.period.start else None
        obs_end = self._to_utc_naive(log.period.end) if log.period and log.period.end else None
        with self._lock:
            self._conn.execute(
                f"""
                INSERT INTO {LOGS_TABLE} (point_uri, timestamp, observed_start, observed_end, message)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (point_uri, timestamp) DO UPDATE SET
                    message        = excluded.message,
                    observed_start = excluded.observed_start,
                    observed_end   = excluded.observed_end
                """,
                [log.point_uri, self._to_utc_naive(log.timestamp), obs_start, obs_end, log.message],
            )

    def query_logs(
        self,
        point_uri: str,
        log_time_interval: TimeIntervalModel | None = None,
        obs_time_interval: TimeIntervalModel | None = None,
    ) -> list[LogEntry]:
        start = log_time_interval.start if log_time_interval else None
        end = log_time_interval.end if log_time_interval else None
        obs_start = obs_time_interval.start if obs_time_interval else None
        obs_end = obs_time_interval.end if obs_time_interval else None

        clauses = ["point_uri = ?"]
        params: list[Any] = [point_uri]

        if start is not None:
            clauses.append("timestamp >= ?")
            params.append(self._to_utc_naive(start))
        if end is not None:
            clauses.append("timestamp <= ?")
            params.append(self._to_utc_naive(end))

        # Overlap check for [obs_start, obs_end) vs [query_start, query_end)
        if obs_start is not None and obs_end is not None:
            clauses.append("observed_start < ? AND (observed_end IS NULL OR observed_end > ?)")
            params.extend([self._to_utc_naive(obs_end), self._to_utc_naive(obs_start)])
        elif obs_start is not None:
            clauses.append("(observed_end IS NULL OR observed_end > ?)")
            params.append(self._to_utc_naive(obs_start))
        elif obs_end is not None:
            clauses.append("observed_start < ?")
            params.append(self._to_utc_naive(obs_end))

        where = " AND ".join(clauses)
        query = f"""
            SELECT point_uri, timestamp, observed_start, observed_end, message
            FROM {LOGS_TABLE}
            WHERE {where}
            ORDER BY timestamp ASC
        """
        try:
            tbl = self._conn.execute(query, params).to_arrow_table()
        except Exception as exc:
            logger.error("query_logs failed: %s", exc)
            return []

        d = tbl.to_pydict()
        result: list[LogEntry] = []
        for uri_val, ts_raw, o_start_raw, o_end_raw, msg in zip(
            d["point_uri"], d["timestamp"], d["observed_start"], d["observed_end"], d["message"]
        ):
            period = TimeIntervalModel(start=self._add_utc(o_start_raw), end=self._add_utc(o_end_raw))
            result.append(LogEntry(point_uri=uri_val, timestamp=self._add_utc(ts_raw), period=period, message=msg))
        return result

    def delete_logs(self, point_uri: str) -> bool:
        with self._lock:
            self._conn.execute(
                f"DELETE FROM {LOGS_TABLE} WHERE point_uri = ?", [point_uri]
            )
        return True

    # ---- transaction helpers ----

    def begin(self) -> None:
        with self._lock:
            if not self._in_tx:
                self._conn.execute("BEGIN TRANSACTION")
                self._in_tx = True

    def commit(self) -> None:
        with self._lock:
            if self._in_tx:
                self._conn.execute("COMMIT")
                self._in_tx = False

    def rollback(self) -> None:
        with self._lock:
            if self._in_tx:
                self._conn.execute("ROLLBACK")
                self._in_tx = False

    # ---- utility ----

    def sql_query(self, query: str) -> dict[str, Any]:
        tbl = self._conn.execute(query).to_arrow_table()
        d = tbl.to_pydict()
        cols = tbl.schema.names
        return {
            "columns": cols,
            "rows": [list(row) for row in zip(*[d[c] for c in cols])] if cols else [],
        }

    def close(self) -> None:
        self._conn.close()

    # ---- helpers ----

    @staticmethod
    def _to_utc_naive(ts: datetime) -> datetime:
        """Return a naive UTC datetime (timezone info stripped) for storage as TIMESTAMP."""
        if ts.tzinfo is None:
            return ts  # assume UTC
        return ts.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _add_utc(ts: datetime | None) -> datetime | None:
        """Re-attach UTC tzinfo to a naive datetime read back from DuckDB TIMESTAMP."""
        if ts is None:
            return None
        if ts.tzinfo is not None:
            return ts
        return ts.replace(tzinfo=timezone.utc)

    @staticmethod
    def _to_str(val: Any) -> str | None:
        return None if val is None else str(val)

    def _stream_value_kind(self, ref_uri: str) -> str | None:
        row = self._conn.execute(
            f"SELECT value_kind FROM {STREAMS_TABLE} WHERE ref_uri = ?",
            [ref_uri],
        ).fetchone()
        return row[0] if row else None
