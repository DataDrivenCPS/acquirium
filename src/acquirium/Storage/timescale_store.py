from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Iterator
import hashlib
import random
import string

import psycopg
from psycopg import sql
from psycopg.types.json import Json

from acquirium.internals.models import Order, TimeseriesInfo 
from acquirium.Storage.base import TimeseriesStore
import logging
import pyarrow as pa
import polars as pl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TIMESERIES_TABLE = "timeseries"
STREAMS_TABLE = "streams"
SOFT_SENSOR_TABLE = "soft_sensors"
SOFT_SENSOR_RUNS = "soft_sensor_runs"


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
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(SOFT_SENSOR_TABLE)))
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(SOFT_SENSOR_RUNS)))
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
            # add unique constraint on (point_uri, ts) pairs
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_timeseries_point_ts_unique ON {TIMESERIES_TABLE} (point_uri, ts);"
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                    handle TEXT PRIMARY KEY,
                    uri TEXT UNIQUE NOT NULL
                );
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SOFT_SENSOR_TABLE} (
                    uri TEXT PRIMARY KEY,
                    module_path TEXT NOT NULL,
                    sources TEXT[] NOT NULL,
                    params JSONB,
                    schedule TEXT
                );
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SOFT_SENSOR_RUNS} (
                    uri TEXT,
                    started TIMESTAMPTZ,
                    duration_ms DOUBLE PRECISION,
                    rows_out BIGINT,
                    status TEXT,
                    message TEXT
                );
                """
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
            logging.info(f"acquirium: bulk inserted {rows_affected} rows into {TIMESERIES_TABLE}")
            return rows_affected
        except Exception as e:
            print(f"An error occurred: {e}")
            return -1

    # -------------------- stream handles --------------------
    def ensure_stream_handle(self, uri: str, handle: str | None = None) -> str:
        if handle is None:
            handle = hashlib.sha1(uri.encode("utf-8")).hexdigest()[:10]
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {STREAMS_TABLE} (handle, uri)
                VALUES (%s, %s)
                ON CONFLICT (uri) DO UPDATE SET handle = EXCLUDED.handle
                """,
                (handle, uri),
            )
        return handle

    def resolve_handle(self, handle_or_uri: str) -> str:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT uri FROM {STREAMS_TABLE} WHERE handle = %s", (handle_or_uri,))
            row = cur.fetchone()
            return row[0] if row else handle_or_uri

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
                    names=["ts", "value", "point_uri"],
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

    # -------------------- soft sensor catalog --------------------
    def upsert_soft_sensor(self, *, uri: str, module_path: str, sources: list[str], params: dict | None, schedule: str | None) -> None:
        with self.conn.cursor() as cur:
            encoded_params = Json(params) if params is not None else None
            cur.execute(
                f"""
                INSERT INTO {SOFT_SENSOR_TABLE} (uri, module_path, sources, params, schedule)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (uri) DO UPDATE SET
                    module_path = EXCLUDED.module_path,
                    sources = EXCLUDED.sources,
                    params = EXCLUDED.params,
                    schedule = EXCLUDED.schedule
                """,
                (uri, module_path, sources, encoded_params, schedule),
            )

    def list_soft_sensors(self) -> list[dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT uri, module_path, sources, params, schedule FROM {SOFT_SENSOR_TABLE}")
            rows = cur.fetchall()
        return [
            {
                "uri": r[0],
                "module_path": r[1],
                "sources": r[2],
                "params": r[3],
                "schedule": r[4],
            }
            for r in rows
        ]

    def get_soft_sensor(self, uri: str) -> dict[str, Any] | None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT uri, module_path, sources, params, schedule FROM {SOFT_SENSOR_TABLE} WHERE uri=%s",
                (uri,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "uri": row[0],
            "module_path": row[1],
            "sources": row[2],
            "params": row[3],
            "schedule": row[4],
        }

    def record_soft_run(self, uri: str, started: datetime, duration_ms: float, rows_out: int, status: str, message: str | None = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {SOFT_SENSOR_RUNS} VALUES (%s, %s, %s, %s, %s, %s)",
                (uri, self._to_utc(started), duration_ms, rows_out, status, message),
            )

    def delete_soft_sensor(self, uri: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SOFT_SENSOR_TABLE} WHERE uri=%s", (uri,))
            cur.execute(f"DELETE FROM {SOFT_SENSOR_RUNS} WHERE uri=%s", (uri,))
            cur.execute(f"DELETE FROM {TIMESERIES_TABLE} WHERE point_uri=%s", (uri,))
            cur.execute(f"DELETE FROM {STREAMS_TABLE} WHERE uri=%s", (uri,))

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
