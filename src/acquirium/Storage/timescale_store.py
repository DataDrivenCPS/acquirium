from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Iterator
import hashlib
import random
import string

import psycopg
from psycopg import sql
from psycopg.types.json import Json

from acquirium.internals.models import Order, TimeseriesInfo, TimeInterval, LogEntry, TimeIntervalModel, compute_handle
from acquirium.Storage.base import TimeseriesStore
import logging
import pyarrow as pa
import polars as pl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TIMESERIES_TABLE = "timeseries"
STREAMS_TABLE = "streams"
LOGS_TABLE = "logs"


class TimescaleStore(TimeseriesStore):
    def __init__(
        self,
        *,
        dsn: str | None = None,
        connect_timeout: int | None = None,
        recreate: bool = False,
    ):
        self.dsn = dsn
        self.db_path = self.dsn
        # print(f"Connecting to TimescaleDB at {self.dsn}...")
        # default autocommit so reads don't hold open transactions; explicit begin toggles off
        self.conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=connect_timeout)
        self._in_tx = False
        if recreate:
            with self.conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(TIMESERIES_TABLE)))
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(STREAMS_TABLE)))
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(LOGS_TABLE)))
        self.ensure_table()

    # -------------------- table management --------------------
    def ensure_table(self) -> str:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TIMESERIES_TABLE} (
                    point_uri TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    value TEXT
                );
                """
            )
            # create hypertable if not already
            cur.execute(
                sql.SQL(
                    "SELECT create_hypertable(%s, %s, if_not_exists => TRUE, migrate_data => TRUE);"
                ),
                (TIMESERIES_TABLE, "ts"),
            )
            # index to support lookups by id + time
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_timeseries_point_ts ON {TIMESERIES_TABLE} (point_uri, ts);"
            )
            # enable compression, segment by point_uri and order by ts for efficient scans
            cur.execute(
                f"ALTER TABLE {TIMESERIES_TABLE} SET (timescaledb.compress, timescaledb.compress_segmentby = 'point_uri', timescaledb.compress_orderby = 'ts');"
            )
            cur.execute(
                "SELECT add_compression_policy('timeseries', INTERVAL '7 days', if_not_exists => TRUE);"
            )

            # add unique constraint on (point_uri, ts) pairs
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_timeseries_point_ts_unique ON {TIMESERIES_TABLE} (point_uri, ts);"
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                    handle TEXT PRIMARY KEY,
                    point_uri TEXT UNIQUE NOT NULL,
                    source_id TEXT NOT NULL,
                    ref_name TEXT NOT NULL
                );
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {LOGS_TABLE} (
                    point_uri TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    observed tstzrange,
                    message TEXT NOT NULL
                );
                """
            )
            # index to support lookups by point_uri, timestamp
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_point_time ON {LOGS_TABLE} (point_uri, timestamp);"
            )
            # index to support lookups by observed time range
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_logs_observed ON {LOGS_TABLE} USING GIST (observed);"
            )

        if not self._in_tx:
            self.conn.commit()
        return TIMESERIES_TABLE

    # -------------------- mutations --------------------
    def upsert_rows(self, point_uri: str, rows: Iterable[tuple[datetime, Any]]) -> int:
        rows_list = list(rows)
        if not rows_list:
            return 0
        payload = [(point_uri, self._to_utc(ts), self._to_str(val)) for ts, val in rows_list]
        with self.conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {TIMESERIES_TABLE} (point_uri, ts, value) VALUES (%s, %s, %s) ON CONFLICT (point_uri, ts) DO UPDATE SET value = EXCLUDED.value",
                payload,
            )
        logger.debug("acquirium: upserted %d rows into %s", len(rows_list), TIMESERIES_TABLE)
        return len(rows_list)

    def replace_rows(self, point_uri: str, rows: Iterable[tuple[datetime, Any]]) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("DELETE FROM {} WHERE point_uri=%s").format(sql.Identifier(TIMESERIES_TABLE)), [point_uri])
        return self.upsert_rows(point_uri, rows)

    def bulk_insert_polars(self, df: pl.DataFrame) -> int:
        # Using polars to write to database via ADBC
        # df format: columns: ["point_uri", "time", "value"]
        random_string = ''.join(random.choice(string.ascii_lowercase) for _ in range(15))
        with self.conn.cursor() as cur:
            cur.execute(
                f"""DROP TABLE IF EXISTS {random_string};"""
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {random_string} (
                point_uri TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                value TEXT
            );""")
        try:
            rows_affected = df.write_database(
                table_name=random_string,
                connection=self.dsn,
                engine="adbc",
                if_table_exists="append" # Use 'replace' to drop/create the table
            )
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {TIMESERIES_TABLE} (point_uri, ts, value)
                    SELECT point_uri, ts, value FROM {random_string}
                    ON CONFLICT (point_uri, ts) DO UPDATE SET value = EXCLUDED.value;
                    """
                )
                cur.execute(f"DROP TABLE IF EXISTS {random_string};")
            logger.info(f"acquirium: bulk inserted {rows_affected} rows into {TIMESERIES_TABLE}")
            return rows_affected
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return -1

    # -------------------- stream handles --------------------
    def ensure_stream_handle(self, point_uri: str, source_id: str, ref_name: str, handle: str | None = None) -> str:
        """Register a (point_uri, source_id, ref_name) mapping in the streams table.

        The handle is computed deterministically from (source_id, ref_name) via
        :func:`compute_handle`, so two sources with the same ref_name never
        produce the same storage key.  The handle is also used as the
        TimescaleDB row key for the stream's data.  Pass a precomputed handle
        to avoid recomputing it when already available.

        Returns the handle.
        """
        if handle is None:
            handle = compute_handle(source_id, ref_name)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {STREAMS_TABLE} (handle, point_uri, source_id, ref_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (point_uri) DO UPDATE
                    SET handle = EXCLUDED.handle,
                        source_id = EXCLUDED.source_id,
                        ref_name = EXCLUDED.ref_name
                """,
                (handle, point_uri, source_id, ref_name),
            )
        return handle

    def resolve_handle(self, handle: str) -> tuple[str | None, str | None, str | None]:
        """Resolve a handle to its (point_uri, source_id, ref_name) triple.

        Returns (None, None, None) if the handle is not found.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT point_uri, source_id, ref_name FROM {STREAMS_TABLE} WHERE handle = %s",
                (handle,),
            )
            row = cur.fetchone()
            return (row[0], row[1], row[2]) if row else (None, None, None)

    def resolve_storage_key(self, point_uri: str) -> str:
        """Return the storage key (handle) for a point_uri, or point_uri itself if not registered.

        Streams inserted via insert_timeseries are stored under their handle (UUID).
        This resolves the semantic URI → handle so reads find the right rows.
        Falls back to the URI itself for data inserted directly (e.g. bulk CSV ingest).
        """
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT handle FROM {STREAMS_TABLE} WHERE point_uri = %s",
                (point_uri,),
            )
            row = cur.fetchone()
            return row[0] if row else point_uri

    def resolve_storage_keys(self, point_uris: list[str]) -> dict[str, str]:
        """Batch-resolve point_uris to storage keys in a single query.

        Returns a mapping of point_uri → handle (or point_uri itself for
        unregistered URIs, preserving the single-URI fallback behaviour).
        """
        if not point_uris:
            return {}
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT point_uri, handle FROM {STREAMS_TABLE} WHERE point_uri = ANY(%s)",
                (point_uris,),
            )
            rows = cur.fetchall()
        registered = {row[0]: row[1] for row in rows}
        return {uri: registered.get(uri, uri) for uri in point_uris}

    # -------------------- queries --------------------
    def timeseries(
        self,
        point_uri: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: Order = "asc",
        batch_size: int = 50_000,
    ) -> Iterator[pa.RecordBatch]:
        '''
        Returns an iterator over the time series data for the given point URI.
        '''
        clauses = ["point_uri = %s"]
        params: list[Any] = [point_uri]

        if start:
            clauses.append("ts >= %s")
            params.append(self._to_utc(start))
        if end:
            clauses.append("ts <= %s")
            params.append(self._to_utc(end))

        where = " AND ".join(clauses)
        order_sql = "ASC" if order == "asc" else "DESC"
        limit_sql = " LIMIT %s" if limit else ""
        if limit:
            params.append(limit)

        query = f"""
            SELECT ts, value
            FROM {TIMESERIES_TABLE}
            WHERE {where}
            ORDER BY ts {order_sql}{limit_sql}
        """

        with self.conn.cursor() as cur:
            cur.execute(query, params)

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                ts_col = [r[0] for r in rows]
                val_col = [r[1] for r in rows]
                point_uri_col = [point_uri] * len(ts_col)
                if not ts_col or not val_col or not point_uri_col:
                    break

                batch = pa.record_batch(
                    [
                        pa.array(ts_col, type=pa.timestamp("us", tz="UTC")),
                        pa.array(val_col, type=pa.string()),
                        pa.array(point_uri_col, type=pa.string()),
                    ],
                    names=["ts", "value", "uri"],
                )
                yield batch

    def timeseries_info(self, point_uri: str) -> TimeseriesInfo:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*), MIN(ts), MAX(ts) FROM {TIMESERIES_TABLE} WHERE point_uri=%s",
                (point_uri,),
            )
            cnt, earliest, latest = cur.fetchone()
        return TimeseriesInfo(table=TIMESERIES_TABLE, row_count=cnt, earliest=earliest, latest=latest)

    def timeseries_info_batch(self, point_uris: list[str]) -> dict[str, TimeseriesInfo]:
        """Return stats (row_count, earliest, latest) for multiple point URIs in one query."""
        if not point_uris:
            return {}
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT point_uri, COUNT(*), MIN(ts), MAX(ts) FROM {TIMESERIES_TABLE} WHERE point_uri = ANY(%s) GROUP BY point_uri",
                (point_uris,),
            )
            rows = cur.fetchall()
        result: dict[str, TimeseriesInfo] = {}
        for uri, cnt, earliest, latest in rows:
            result[uri] = TimeseriesInfo(table=TIMESERIES_TABLE, row_count=cnt, earliest=earliest, latest=latest)
        for uri in point_uris:
            if uri not in result:
                result[uri] = TimeseriesInfo(table=TIMESERIES_TABLE, row_count=0)
        return result

    # -------------------- logging API --------------------

    def insert_log(self, log: LogEntry) -> None:
        point_uri: str = log.point_uri
        ts: datetime = log.timestamp
        message: str = log.message

        # if period is optional
        if log.period is None:
            observation_start = None
            observation_end = None
        else:
            observation_start = log.period.start
            observation_end = log.period.end

        if observation_start is not None and observation_end is not None:
            # Let Postgres build the range
            sql = f"""
                INSERT INTO {LOGS_TABLE} (point_uri, timestamp, observed, message)
                VALUES (%s, %s, tstzrange(%s, %s, '[)'), %s)
                ON CONFLICT (point_uri, timestamp) DO UPDATE SET message = EXCLUDED.message, observed = EXCLUDED.observed      
            """
            params = [point_uri, ts, observation_start, observation_end, message]
        elif observation_start is not None:
            sql = f"""
                INSERT INTO {LOGS_TABLE} (point_uri, timestamp, observed, message)
                VALUES (%s, %s, tstzrange(%s, 'infinity', '[)'), %s)
                ON CONFLICT (point_uri, timestamp) DO UPDATE SET message = EXCLUDED.message, observed = EXCLUDED.observed
            """
            params = [point_uri, ts, observation_start, message]
        elif observation_end is not None:
            sql = f"""
                INSERT INTO {LOGS_TABLE} (point_uri, timestamp, observed, message)
                VALUES (%s, %s, tstzrange('-infinity', %s, '[)'), %s)
                ON CONFLICT (point_uri, timestamp) DO UPDATE SET message = EXCLUDED.message, observed = EXCLUDED.observed
            """
            params = [point_uri, ts, observation_end, message]
        else:
            sql = f"""
                INSERT INTO {LOGS_TABLE} (point_uri, timestamp, observed, message)
                VALUES (%s, %s, NULL, %s)
                ON CONFLICT (point_uri, timestamp) DO UPDATE SET message = EXCLUDED.message, observed = EXCLUDED.observed
            """
            params = [point_uri, ts, message]
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
        except Exception as e:
            logger.error(f"An error occurred while inserting log: {e}")
            logger.error(f"SQL: {sql}")
            logger.error(f"Params: {params}")
            raise e
    def query_logs(
        self,
        point_uri: str,
        log_time_interval: TimeIntervalModel | None = None,
        obs_time_interval: TimeIntervalModel | None = None
    ) -> list[LogEntry]:
        start = log_time_interval.start if log_time_interval else None
        end = log_time_interval.end if log_time_interval else None
        observation_start = obs_time_interval.start if obs_time_interval else None
        observation_end = obs_time_interval.end if obs_time_interval else None
        
        clauses = ["point_uri = %s"]
        params: list[Any] = [point_uri]

        if start is not None:
            clauses.append("timestamp >= %s")
            params.append(start)
        if end is not None:
            clauses.append("timestamp <= %s")
            params.append(end)

        # observed overlap semantics with open ended windows
        if observation_start is not None and observation_end is not None:
            clauses.append("observed && tstzrange(%s, %s, '[)')")
            params.extend([observation_start, observation_end])
        elif observation_start is not None:
            clauses.append("observed && tstzrange(%s, 'infinity', '[)')")
            params.append(observation_start)
        elif observation_end is not None:
            clauses.append("observed && tstzrange('-infinity', %s, '[)')")
            params.append(observation_end)
        where = " AND ".join(clauses)
        query = f"""
            SELECT point_uri, timestamp, observed, message
            FROM {LOGS_TABLE}
            WHERE {where}
            ORDER BY timestamp ASC
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

            result: list[LogEntry] = []
            for point_uri, ts, observed_range, msg in rows:
                print(point_uri, ts, observed_range, msg)
                period = None
                if observed_range is not None:
                    period = TimeIntervalModel(start=observed_range.lower, end=observed_range.upper)
                else:
                    period = TimeIntervalModel(start=None, end=None)

                result.append(
                    LogEntry(
                        point_uri=point_uri,
                        timestamp=ts,
                        period=period,
                        message=msg,
                    )
                )
            return result
        except Exception as e:
            logger.error(f"An error occurred while querying logs: {e}")
            logger.error(f"Query: {query}")
            return []

    def delete_logs(self, point_uri: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {LOGS_TABLE} WHERE point_uri=%s", (point_uri,))
        return True

    # -------------------- transaction helpers --------------------
    def begin(self) -> None:
        if not self._in_tx:
            self.conn.autocommit = False
            self._in_tx = True
            self.conn.execute("BEGIN")

    def commit(self) -> None:
        if self._in_tx:
            self.conn.commit()
            self.conn.autocommit = True
            self._in_tx = False

    def rollback(self) -> None:
        if self._in_tx:
            self.conn.rollback()
            self.conn.autocommit = True
            self._in_tx = False

    # -------------------- utility --------------------
    def sql_query(self, query: str) -> dict[str, Any]:
        with self.conn.cursor() as cur:
            cur.execute(query)
            cols = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
        return {"columns": cols, "rows": rows}

    # -------------------- lifecycle --------------------
    def close(self) -> None:
        self.conn.close()

    # -------------------- helpers --------------------
    def _to_utc(self, ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    def _to_str(self, val: Any) -> str | None:
        if val is None:
            return None
        return str(val)
