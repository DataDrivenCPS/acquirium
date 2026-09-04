from __future__ import annotations

"""TimescaleDB-backed timeseries store, driven through DuckDB.

The Postgres/TimescaleDB database is attached to an in-process DuckDB
instance with DuckDB's ``postgres`` extension, and every read and write goes
through DuckDB connections exactly as in :class:`DuckDBStore`, which this
class extends. No psycopg, no ADBC. Select this backend at startup via:

    ACQUIRIUM_TIMESERIES_BACKEND=timescale
    PG_DSN=postgresql://user:pass@host:5432/dbname

**Reads** are the inherited DuckDB read path, run against the attached
catalog: DuckDB pushes the ``ref_id``/``ts`` filters into the Postgres scan
and streams the result over the binary COPY protocol. Parallel ctid-range
scanning is disabled because Timescale chunks reuse ctids, and LIMIT is not
pushed down, so a ``LIMIT n`` read still pulls the whole filtered range from
Postgres before DuckDB cuts it.

**Writes** cannot be the inherited DuckDB statements for the ``timeseries``
hypertable: DuckDB executes its own DELETE/UPDATE/ON CONFLICT against an
attached Postgres table by ctid, and on a hypertable a ctid identifies one
row *per chunk*. Mutations are therefore merged server-side: the frame is
staged into a per-store Postgres staging table by DuckDB (one COPY), then a
single ``INSERT ... ON CONFLICT DO UPDATE`` runs on Postgres through
``postgres_execute`` inside the same DuckDB transaction. The ``streams`` and
``logs`` tables are plain tables and use the same staging path for
uniformity. Staging tables are named ``_acquirium_stg_<kind>_<suffix>`` and
dropped on :meth:`close`; ``recreate=True`` also sweeps any left behind.

**Schema** mirrors :class:`DuckDBStore`: ``timeseries`` keyed by an integer
``ref_id`` assigned from ``ref_ids``, naive-UTC ``TIMESTAMP`` columns, and
``logs`` with two observation columns. It is a hypertable on ``ts`` with
compression segmented by ``ref_id``. Databases written by the previous
psycopg store (``ref_uri`` text keys, ``TIMESTAMPTZ``) are not readable;
recreate them.

**Concurrency** follows :class:`DuckDBStore`: an anchor connection keeps the
named in-memory DuckDB instance (and its attached Postgres pool) alive,
every operation opens its own DuckDB connection to it, writes are serialised
by a lock, and ``begin()``/``commit()``/``rollback()`` span a DuckDB
transaction, which the extension maps onto one Postgres transaction.
"""

from typing import Any
from uuid import uuid4
import logging
import threading

import polars as pl

from acquirium.Storage.duckdb_store import (
    DuckDBStore,
    LOGS_TABLE,
    REF_IDS_SEQ,
    REF_IDS_TABLE,
    STREAMS_TABLE,
    TIMESERIES_STREAMS_VIEW,
    TIMESERIES_TABLE,
)
from acquirium.internals._log import timed_debug

logger = logging.getLogger(__name__)

CATALOG = "pg"
SCHEMA = "public"
_STAGING_PREFIX = "_acquirium_stg_"

# Statement heads that produce a result set and go through postgres_query;
# anything else runs through postgres_execute.
_QUERY_HEADS = ("SELECT", "WITH", "VALUES", "TABLE", "SHOW", "EXPLAIN")


class TimescaleStore(DuckDBStore):
    """TimescaleDB implementation of the TimeseriesStore protocol via DuckDB."""

    def __init__(
        self,
        *,
        dsn: str | None = None,
        connect_timeout: int | None = None,
        recreate: bool = False,
    ):
        import duckdb  # lazy, as in DuckDBStore

        if not dsn:
            raise ValueError("TimescaleStore requires a dsn")
        self._duckdb = duckdb
        self.dsn = dsn
        self.db_path = dsn
        self._bind_table_names(f"{CATALOG}.{SCHEMA}.")
        self._lock = threading.Lock()
        self._tx_conn = None
        suffix = uuid4().hex[:12]
        # Named in-memory instance: connections opened with the same name share
        # it, so the attached Postgres catalog and its connection pool are set up
        # once and every per-operation connection reuses them.
        self._instance = f":memory:acquirium-timescale-{suffix}"
        self._stg_timeseries = f"{CATALOG}.{SCHEMA}.{_STAGING_PREFIX}timeseries_{suffix}"
        self._stg_streams = f"{CATALOG}.{SCHEMA}.{_STAGING_PREFIX}streams_{suffix}"
        self._stg_logs = f"{CATALOG}.{SCHEMA}.{_STAGING_PREFIX}logs_{suffix}"

        logger.debug("TimescaleStore.__init__: attaching (recreate=%s)", recreate)
        with timed_debug(logger, "TimescaleStore attach"):
            self._anchor_conn = duckdb.connect(self._instance)
            self._anchor_conn.execute("INSTALL postgres; LOAD postgres;")
            attach_dsn = _with_connect_timeout(dsn, connect_timeout).replace("'", "''")
            self._anchor_conn.execute(f"ATTACH '{attach_dsn}' AS {CATALOG} (TYPE postgres)")
            # ctid-range parallel scans assume one heap; a hypertable is many.
            self._anchor_conn.execute("SET GLOBAL pg_use_ctid_scan = false")

        if recreate:
            logger.debug("TimescaleStore.__init__: dropping existing tables/views (recreate=True)")
            staging_like = _STAGING_PREFIX.replace("_", "\\_")
            with self._lock, self._own_conn() as conn:
                self._pg_execute(
                    conn,
                    f"""
                    DROP VIEW IF EXISTS {TIMESERIES_STREAMS_VIEW} CASCADE;
                    DROP TABLE IF EXISTS {TIMESERIES_TABLE} CASCADE;
                    DROP TABLE IF EXISTS {STREAMS_TABLE} CASCADE;
                    DROP TABLE IF EXISTS {LOGS_TABLE} CASCADE;
                    DROP TABLE IF EXISTS {REF_IDS_TABLE} CASCADE;
                    DROP SEQUENCE IF EXISTS {REF_IDS_SEQ};
                    DO $sweep$
                    DECLARE r record;
                    BEGIN
                        FOR r IN SELECT tablename FROM pg_tables
                                 WHERE schemaname = '{SCHEMA}'
                                   AND tablename LIKE '{staging_like}%'
                        LOOP
                            EXECUTE format('DROP TABLE IF EXISTS {SCHEMA}.%I', r.tablename);
                        END LOOP;
                    END
                    $sweep$;
                    """,
                )
                conn.execute("CALL pg_clear_cache()")
        self.ensure_table()
        logger.debug("TimescaleStore.__init__: ready")

    # ---- connections ----

    def _connect(self):
        return self._duckdb.connect(self._instance)

    # ---- Postgres passthrough ----

    @staticmethod
    def _pg_execute(conn, sql: str) -> None:
        """Run *sql* verbatim on Postgres, on the connection DuckDB has bound to
        *conn*'s transaction (so it commits or rolls back with it)."""
        conn.execute(f"CALL postgres_execute('{CATALOG}', $acq${sql}$acq$)")

    def _stage(self, conn, df: pl.DataFrame, staging_table: str) -> None:
        """Load *df* into *staging_table* through DuckDB (a single binary COPY)."""
        cols = ", ".join(df.columns)
        conn.register("_acquirium_staging_frame", df)
        try:
            conn.execute(f"INSERT INTO {staging_table} ({cols}) SELECT {cols} FROM _acquirium_staging_frame")
        finally:
            conn.unregister("_acquirium_staging_frame")

    # ---- table management ----

    def ensure_table(self) -> str:
        """Create tables, hypertable, indexes and staging tables if missing."""
        stg_ts = _unqualified(self._stg_timeseries)
        stg_streams = _unqualified(self._stg_streams)
        stg_logs = _unqualified(self._stg_logs)
        ddl = f"""
            CREATE EXTENSION IF NOT EXISTS timescaledb;
            CREATE SEQUENCE IF NOT EXISTS {REF_IDS_SEQ};
            CREATE TABLE IF NOT EXISTS {REF_IDS_TABLE} (
                ref_id  INTEGER PRIMARY KEY DEFAULT nextval('{REF_IDS_SEQ}'),
                ref_uri TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS {TIMESERIES_TABLE} (
                ref_id  INTEGER NOT NULL,
                ts      TIMESTAMP NOT NULL,
                numeric_value DOUBLE PRECISION,
                text_value    TEXT,
                CHECK (numeric_value IS NULL OR text_value IS NULL)
            );
            SELECT create_hypertable('{TIMESERIES_TABLE}', 'ts', if_not_exists => TRUE);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_timeseries_ref_ts_unique ON {TIMESERIES_TABLE} (ref_id, ts);
            ALTER TABLE {TIMESERIES_TABLE} SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'ref_id',
                timescaledb.compress_orderby = 'ts DESC'
            );
            SELECT add_compression_policy('{TIMESERIES_TABLE}', INTERVAL '7 days', if_not_exists => TRUE);
            CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                ref_uri    TEXT PRIMARY KEY,
                point_uri  TEXT,
                source_id  TEXT NOT NULL,
                ref_name   TEXT NOT NULL,
                value_kind TEXT NOT NULL DEFAULT 'text'
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_source_ref_name ON {STREAMS_TABLE} (source_id, ref_name);
            CREATE INDEX IF NOT EXISTS idx_streams_point_uri ON {STREAMS_TABLE} (point_uri) WHERE point_uri IS NOT NULL;
            CREATE OR REPLACE VIEW {TIMESERIES_STREAMS_VIEW} AS
            SELECT
                r.ref_uri,
                s.point_uri,
                s.source_id,
                s.ref_name,
                COALESCE(s.value_kind, 'text') AS value_kind,
                t.ts,
                t.numeric_value AS value_numeric,
                t.text_value AS value_text
            FROM {TIMESERIES_TABLE} AS t
            JOIN {REF_IDS_TABLE} AS r ON t.ref_id = r.ref_id
            LEFT JOIN {STREAMS_TABLE} AS s ON r.ref_uri = s.ref_uri;
            CREATE TABLE IF NOT EXISTS {LOGS_TABLE} (
                point_uri      TEXT NOT NULL,
                timestamp      TIMESTAMP NOT NULL,
                observed_start TIMESTAMP,
                observed_end   TIMESTAMP,
                message        TEXT NOT NULL,
                UNIQUE (point_uri, timestamp)
            );
            CREATE INDEX IF NOT EXISTS idx_logs_obs ON {LOGS_TABLE} (observed_start, observed_end);
            CREATE UNLOGGED TABLE IF NOT EXISTS {stg_ts} (
                ref_uri TEXT, ts TIMESTAMP, numeric_value DOUBLE PRECISION, text_value TEXT
            );
            CREATE UNLOGGED TABLE IF NOT EXISTS {stg_streams} (
                ref_uri TEXT, point_uri TEXT, source_id TEXT, ref_name TEXT, value_kind TEXT
            );
            CREATE UNLOGGED TABLE IF NOT EXISTS {stg_logs} (
                point_uri TEXT, timestamp TIMESTAMP, observed_start TIMESTAMP, observed_end TIMESTAMP, message TEXT
            );
        """
        with self._lock, timed_debug(logger, "ensure_table"), self._own_conn() as conn:
            self._check_schema(conn)
            self._pg_execute(conn, ddl)
            # DDL ran on Postgres directly; refresh DuckDB's view of the catalog.
            conn.execute("CALL pg_clear_cache()")
        return "ok"

    def _check_schema(self, conn) -> None:
        """Refuse to run against a ``timeseries`` table from the psycopg-era schema."""
        cols = {
            r[0]
            for r in conn.execute(
                f"""
                SELECT column_name FROM {CATALOG}.information_schema.columns
                WHERE table_schema = '{SCHEMA}' AND table_name = '{TIMESERIES_TABLE}'
                """
            ).fetchall()
        }
        if cols and "ref_id" not in cols:
            raise RuntimeError(
                f"Postgres table {TIMESERIES_TABLE!r} uses the old ref_uri-keyed schema; "
                "this store keys rows by ref_id. Start with recreate=True to rebuild it "
                "(this drops all stored timeseries, streams and logs)."
            )

    # ---- timeseries mutations ----

    def _insert_frame(self, conn, df: pl.DataFrame) -> None:
        """Stage the prepared frame, then assign ids, delete colliding rows and insert.

        Same shape as the DuckDB path, run on Postgres. Delete-then-insert
        measured 2-4x faster than ``ON CONFLICT DO UPDATE`` on the hypertable
        (100k rows: ~1.1s empty / ~1.9s all colliding, versus ~4.4s either way).
        """
        stg = _unqualified(self._stg_timeseries)
        df = df.select(["ref_uri", "ts", "numeric_value", "text_value"])
        self._stage(conn, df, self._stg_timeseries)
        self._pg_execute(
            conn,
            f"""
            INSERT INTO {REF_IDS_TABLE} (ref_uri)
            SELECT DISTINCT ref_uri FROM {stg}
            ON CONFLICT (ref_uri) DO NOTHING;
            """,
        )
        # Constant predicates on the segmentby column and the orderby range:
        # Timescale prunes compressed batches on these, but not on join
        # conditions, so without them a DELETE touching a compressed chunk
        # decompresses the whole chunk (and trips its per-transaction limit).
        ids = self._ref_ids(conn, df["ref_uri"].unique().to_list())
        id_list = ", ".join(str(int(i)) for i in ids.values()) or "NULL"
        ts_min, ts_max = df["ts"].min(), df["ts"].max()
        self._pg_execute(
            conn,
            f"""
            DELETE FROM {TIMESERIES_TABLE} AS t
            USING {stg} AS s
            JOIN {REF_IDS_TABLE} AS r USING (ref_uri)
            WHERE t.ref_id = r.ref_id AND t.ts = s.ts
              AND t.ref_id IN ({id_list})
              AND t.ts BETWEEN '{ts_min.isoformat()}' AND '{ts_max.isoformat()}';
            INSERT INTO {TIMESERIES_TABLE} (ref_id, ts, numeric_value, text_value)
            SELECT r.ref_id, s.ts, s.numeric_value, s.text_value
            FROM {stg} AS s
            JOIN {REF_IDS_TABLE} AS r USING (ref_uri);
            TRUNCATE {stg};
            """,
        )

    def _delete_stream_rows(self, conn, ref_uri: str) -> None:
        ref_id = self._ref_id(conn, ref_uri)
        if ref_id is None:
            return
        self._pg_execute(conn, f"DELETE FROM {TIMESERIES_TABLE} WHERE ref_id = {int(ref_id)}")

    # ---- stream reference registry ----

    def _upsert_streams_frame(self, conn, df: pl.DataFrame) -> None:
        stg = _unqualified(self._stg_streams)
        self._stage(conn, df, self._stg_streams)
        self._pg_execute(
            conn,
            f"""
            INSERT INTO {STREAMS_TABLE} (ref_uri, point_uri, source_id, ref_name, value_kind)
            SELECT ref_uri, point_uri, source_id, ref_name, value_kind FROM {stg}
            ON CONFLICT (ref_uri) DO UPDATE SET
                source_id  = EXCLUDED.source_id,
                ref_name   = EXCLUDED.ref_name,
                point_uri  = COALESCE(EXCLUDED.point_uri, {STREAMS_TABLE}.point_uri),
                value_kind = EXCLUDED.value_kind;
            TRUNCATE {stg};
            """,
        )

    # ---- logs ----

    def _upsert_log_row(self, conn, row: list[Any]) -> None:
        stg = _unqualified(self._stg_logs)
        point_uri, timestamp, obs_start, obs_end, message = row
        df = pl.DataFrame(
            {
                "point_uri": pl.Series([point_uri], dtype=pl.Utf8),
                "timestamp": pl.Series([timestamp], dtype=pl.Datetime("us")),
                "observed_start": pl.Series([obs_start], dtype=pl.Datetime("us")),
                "observed_end": pl.Series([obs_end], dtype=pl.Datetime("us")),
                "message": pl.Series([message], dtype=pl.Utf8),
            }
        )
        self._stage(conn, df, self._stg_logs)
        self._pg_execute(
            conn,
            f"""
            INSERT INTO {LOGS_TABLE} (point_uri, timestamp, observed_start, observed_end, message)
            SELECT point_uri, timestamp, observed_start, observed_end, message FROM {stg}
            ON CONFLICT (point_uri, timestamp) DO UPDATE SET
                message        = EXCLUDED.message,
                observed_start = EXCLUDED.observed_start,
                observed_end   = EXCLUDED.observed_end;
            TRUNCATE {stg};
            """,
        )

    # ---- utility ----

    def sql_query(self, query: str) -> dict[str, Any]:
        """Run *query* on Postgres in Postgres SQL.

        Statements are passed through rather than planned by DuckDB, so
        Timescale functions work and a DELETE against the hypertable is safe.
        Result-producing statements return columns and rows; others return
        empty ones.
        """
        logger.debug("sql_query: %s", query.replace("\n", " ")[:200])
        stripped = query.strip().rstrip(";").strip()
        head = stripped.lstrip("(").split(None, 1)[0].upper() if stripped else ""
        with self._own_conn() as conn, timed_debug(logger, "sql_query"):
            if head in _QUERY_HEADS:
                tbl = conn.execute(
                    f"SELECT * FROM postgres_query('{CATALOG}', $acq${stripped}$acq$)"
                ).to_arrow_table()
            else:
                self._pg_execute(conn, stripped)
                return {"columns": [], "rows": []}
        d = tbl.to_pydict()
        cols = tbl.schema.names
        return {
            "columns": cols,
            "rows": [list(row) for row in zip(*[d[c] for c in cols])] if cols else [],
        }

    def close(self) -> None:
        logger.debug("TimescaleStore.close")
        with self._lock:
            if self._tx_conn is not None:
                self._tx_conn.rollback()
                self._tx_conn.close()
                self._tx_conn = None
            try:
                with self._own_conn() as conn:
                    self._pg_execute(
                        conn,
                        f"""
                        DROP TABLE IF EXISTS {_unqualified(self._stg_timeseries)};
                        DROP TABLE IF EXISTS {_unqualified(self._stg_streams)};
                        DROP TABLE IF EXISTS {_unqualified(self._stg_logs)};
                        """,
                    )
            except Exception as exc:  # the database may already be gone
                logger.debug("TimescaleStore.close: staging cleanup skipped: %s", exc)
        self._anchor_conn.close()


def _unqualified(name: str) -> str:
    """``pg.public.x`` -> ``x``, for statements that run on Postgres itself."""
    return name.rsplit(".", 1)[-1]


def _with_connect_timeout(dsn: str, connect_timeout: int | None) -> str:
    if connect_timeout is None:
        return dsn
    if dsn.startswith(("postgresql://", "postgres://")):
        sep = "&" if "?" in dsn else "?"
        return f"{dsn}{sep}connect_timeout={int(connect_timeout)}"
    return f"{dsn} connect_timeout={int(connect_timeout)}"
