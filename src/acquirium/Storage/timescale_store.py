from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Iterator
from contextlib import contextmanager
import hashlib
import threading

import psycopg
from psycopg import sql

from acquirium.internals.models import Order, TimeseriesInfo, TimeInterval, LogEntry, TimeIntervalModel, compute_stream_uri
from acquirium.Storage.base import TimeseriesStore
import logging
import pyarrow as pa
import polars as pl
from rdflib import URIRef
from acquirium.Storage.migrations import TIMESERIES_MIGRATIONS, migrate
from acquirium.Storage.publication.revision import RevisionPublisher
from acquirium.Storage.values import (
    normalize_value_kind,
    normalize_value_mode,
    prepare_value_columns,
    typed_value_series,
)
from acquirium.internals._log import timed_debug

logger = logging.getLogger(__name__)

TIMESERIES_TABLE = "timeseries"
STREAMS_TABLE = "streams"
LOGS_TABLE = "logs"
STREAM_IDS_TABLE = "stream_ids"
TIMESERIES_STREAMS_VIEW = "timeseries_streams"
SYSTEM_STATE_TABLE = "system_state"
BINDING_PROGRESS_TABLE = "binding_progress"


# Storage keys: the API speaks ``stream_uri`` strings throughout, but the
# ``timeseries`` hypertable keys rows by an ``INTEGER stream_id`` -- the rows are
# narrower, the unique index and compressed-chunk segmentby work on a 4-byte
# key instead of a TEXT one, and it matches the DuckDB backend. The
# ``stream_ids`` table maps each ``stream_uri`` to its ``stream_id``; ids are assigned
# on first write and resolved inside this module, never exposed. The
# ``timeseries_streams`` view joins the string back in, so SQL against the
# view is unaffected. Databases whose ``timeseries`` table is still keyed by
# a text ``ref_uri`` column are not migrated in place: ``ensure_table`` refuses them, and
# they should be recreated.


class TimescaleStore(TimeseriesStore):
    materialization_backend = "postgres"
    def __init__(
        self,
        *,
        dsn: str | None = None,
        connect_timeout: int | None = None,
        recreate: bool = False,
    ):
        self.dsn = dsn
        self.db_path = self.dsn
        # Snapshot reads use independent short-lived connections. Serialized
        # writes use this persistent connection so the public transaction API
        # can govern revision updates and rows together.
        self._lock = threading.Lock()
        # default autocommit so reads don't hold open transactions; explicit begin toggles off
        logger.debug("TimescaleStore.__init__: connecting (recreate=%s)", recreate)
        with timed_debug(logger, "psycopg.connect"):
            self.conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=connect_timeout)
        self._in_tx = False
        if recreate:
            logger.debug("TimescaleStore.__init__: dropping existing tables/views")
            with self.conn.cursor() as cur:
                cur.execute(sql.SQL("DROP VIEW IF EXISTS {} CASCADE").format(sql.Identifier(TIMESERIES_STREAMS_VIEW)))
                for table in (
                    "experiment_metadata_outbox",
                    "experiment_metadata_state",
                    "experiment_observations",
                    "experiment_artifacts",
                    "experiment_artifact_chunks",
                    "experiment_runs",
                    "experiment_variables",
                    "experiment_templates",
                    "experiment_metadata_resolutions",
                    "schema_migrations", "stream_resets", "materialization_work", "materialization_lineage", "materialization_deployments",
                    BINDING_PROGRESS_TABLE, SYSTEM_STATE_TABLE, TIMESERIES_TABLE, STREAM_IDS_TABLE, STREAMS_TABLE, LOGS_TABLE,
                ):
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))
        self.ensure_table()
        logger.debug("TimescaleStore.__init__: ready")

    # -------------------- table management --------------------
    def ensure_table(self) -> str:
        with timed_debug(logger, "ensure_table"), self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            # A timeseries table from before stream_id keys would survive the
            # CREATE IF NOT EXISTS below and fail later on the missing column;
            # refuse it here with a message that says what to do.
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s AND column_name = 'ref_uri'
                """,
                (TIMESERIES_TABLE,),
            )
            if cur.fetchone():
                raise RuntimeError(
                    f"{TIMESERIES_TABLE} is keyed by ref_uri text; this version keys it by "
                    f"stream_id. Recreate the store (recreate=True) instead of opening it in place."
                )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {STREAM_IDS_TABLE} (
                    stream_id INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
                    stream_uri TEXT NOT NULL UNIQUE
                );
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TIMESERIES_TABLE} (
                    stream_id INTEGER NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    numeric_value DOUBLE PRECISION,
                    text_value TEXT,
                    deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    last_revision BIGINT NOT NULL DEFAULT 0,
                    CHECK (numeric_value IS NULL OR text_value IS NULL)
                );
                """
            )
            with self._write_conn() as migration_conn:
                migrate(migration_conn, backend="postgres", component="timeseries",
                        migrations=TIMESERIES_MIGRATIONS)
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_timeseries_last_revision ON {TIMESERIES_TABLE} (last_revision);")
            # Every query filters by stream_id first, so the unique (stream_id, ts)
            # index below serves the read path. Avoid Timescale's redundant
            # default ts index and its extra write cost.
            cur.execute(
                sql.SQL(
                    "SELECT public.create_hypertable(%s, %s, if_not_exists => TRUE, create_default_indexes => FALSE);"
                ),
                (TIMESERIES_TABLE, "ts"),
            )
            # Unique index supports idempotent upserts and scans by stream/time.
            # Timescale unique indexes on hypertables must include the time
            # partition column; (stream_id, ts) satisfies that requirement.
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_timeseries_ref_ts_unique ON {TIMESERIES_TABLE} (stream_id, ts);"
            )
            # Segment compressed chunks by stream and order newest-first within
            # each stream. TimescaleDB 2.11+ supports the targeted historical
            # updates used by materialization on compressed chunks.
            cur.execute(
                f"""
                ALTER TABLE {TIMESERIES_TABLE}
                SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'stream_id',
                    timescaledb.compress_orderby = 'ts DESC'
                );
                """
            )
            cur.execute(
                sql.SQL("SELECT public.add_compression_policy({}, INTERVAL '7 days', if_not_exists => TRUE);").format(
                    sql.Literal(TIMESERIES_TABLE)
                )
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                    stream_uri TEXT PRIMARY KEY,
                    property_uri TEXT,
                    source_id TEXT NOT NULL,
                    stream_name TEXT NOT NULL,
                    value_kind TEXT NOT NULL DEFAULT 'text'
                );
                """
            )
            cur.execute(
                f"ALTER TABLE {STREAMS_TABLE} ALTER COLUMN property_uri DROP NOT NULL;"
            )
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_source_ref_name ON {STREAMS_TABLE} (source_id, stream_name);"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_streams_point_uri ON {STREAMS_TABLE} (property_uri) WHERE property_uri IS NOT NULL;"
            )
            cur.execute(
                f"""
                CREATE OR REPLACE VIEW {TIMESERIES_STREAMS_VIEW} AS
                SELECT
                    r.stream_uri,
                    s.property_uri,
                    s.source_id,
                    s.stream_name,
                    COALESCE(s.value_kind, 'text') AS value_kind,
                    t.ts,
                    t.numeric_value AS value_numeric,
                    t.text_value AS value_text
                FROM {TIMESERIES_TABLE} AS t
                JOIN {STREAM_IDS_TABLE} AS r
                    ON t.stream_id = r.stream_id
                LEFT JOIN {STREAMS_TABLE} AS s
                    ON r.stream_uri = s.stream_uri
                WHERE NOT t.deleted;
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {LOGS_TABLE} (
                    property_uri TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    observed tstzrange,
                    message TEXT NOT NULL
                );
                """
            )
            # index to support lookups by property_uri, timestamp
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_point_time ON {LOGS_TABLE} (property_uri, timestamp);"
            )
            # index to support lookups by observed time range
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_logs_observed ON {LOGS_TABLE} USING GIST (observed);"
            )
            cur.execute("CREATE TABLE IF NOT EXISTS stream_resets (stream_uri TEXT PRIMARY KEY, last_revision BIGINT NOT NULL)")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {SYSTEM_STATE_TABLE} (current_revision BIGINT NOT NULL);")
            cur.execute(f"INSERT INTO {SYSTEM_STATE_TABLE} SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM {SYSTEM_STATE_TABLE});")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {BINDING_PROGRESS_TABLE} (progress_key TEXT PRIMARY KEY, consumed_revision BIGINT NOT NULL);")
        if not self._in_tx:
            self.conn.commit()
        return TIMESERIES_TABLE

    # -------------------- materialization transaction hooks --------------------
    # These deliberately mirror the small private interface DuckDBStore offers
    # to RevisionStore.  Keeping it here lets the materializer remain backend
    # neutral instead of maintaining two schedulers.
    def _connect(self):
        return psycopg.connect(self.dsn, autocommit=True)

    @contextmanager
    def _own_conn(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write_conn(self):
        # ``begin()`` is the public transaction API.  Its writes must use the
        # same connection so commit and rollback govern the revision update as
        # well as the inserted rows.
        if self._in_tx:
            yield self.conn
            return
        with self.conn.transaction():
            yield self.conn

    @staticmethod
    def _next_revision(conn) -> int:
        # Match DuckDB's global frontier: a revision identifies one committed
        # visibility boundary even when the write touches several streams.
        return int(conn.execute(
            f"UPDATE {SYSTEM_STATE_TABLE} SET current_revision = current_revision + 1 RETURNING current_revision"
        ).fetchone()[0])

    # -------------------- storage keys --------------------
    def _stream_id(self, cur, stream_uri: str) -> int | None:
        """Return the stream_id assigned to stream_uri, or None if it was never written."""
        cur.execute(
            f"SELECT stream_id FROM {STREAM_IDS_TABLE} WHERE stream_uri = %s",
            (stream_uri,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    # -------------------- mutations --------------------
    # Every write goes through one path: the rows become a Polars frame, the
    # frame is COPYed into a TEMP staging table on this connection, ids are
    # assigned for unseen uris, and one INSERT ... SELECT ... ON CONFLICT
    # merges into the hypertable. All of that runs in a single transaction,
    # so a failure leaves neither ids without rows nor a leaked staging
    # table, and inside a begin()/commit() span the write joins that span.
    STAGING_TABLE = "_acquirium_incoming_timeseries"

    def upsert_rows(
        self,
        stream_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "text",
    ) -> int:
        rows_list = list(rows)
        logger.debug("upsert_rows stream_uri=%s rows=%d kind=%s", stream_uri, len(rows_list), value_kind)
        if not rows_list:
            return 0
        return self.bulk_insert_polars(self._rows_frame(stream_uri, rows_list, value_kind))

    def replace_rows(
        self,
        stream_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "text",
    ) -> int:
        value_kind = normalize_value_kind(value_kind)
        frame = self._prepare_frame(self._rows_frame(stream_uri, list(rows), value_kind))
        if frame["ts"].null_count():
            raise ValueError("replacement timestamps must not be null")
        return self._replace_frame(stream_uri, frame)

    def _replace_frame(self, stream_uri: str, frame: pl.DataFrame) -> int:
        """Physically replace a stream and record its latest reset atomically."""
        with self._lock, self._write_conn() as conn:
            revision = RevisionPublisher(self).reset(conn, [stream_uri])
            self._insert_frame(conn, frame, revision)
        return frame.height

    def bulk_insert_polars(self, df: pl.DataFrame) -> int:
        """Upsert a Polars frame with columns ``stream_uri, ts, value[, value_kind]``
        or the already split ``stream_uri, ts, numeric_value, text_value``.

        Returns the number of rows merged (inserted or updated).
        """
        if df.is_empty():
            logger.debug("bulk_insert_polars: empty DataFrame, skipping")
            return 0
        in_rows = len(df)
        with timed_debug(logger, "bulk_insert_polars prepare/dedupe rows=%d", in_rows):
            df = self._prepare_frame(df)
        logger.debug("bulk_insert_polars: %d rows after dedupe (was %d)", len(df), in_rows)
        # Assigning the revision and writing the rows in this one transaction
        # is the materialization visibility contract.  It also makes ordinary
        # writes usable as materializer inputs.
        try:
            with self._lock, timed_debug(
                logger, "bulk_insert_polars COPY+merge rows=%d", len(df)
            ), self._write_conn() as conn:
                self._insert_frame(conn, df, self._next_revision(conn))
        except Exception:
            logger.exception("acquirium: bulk insert into %s failed", TIMESERIES_TABLE)
            raise
        logger.info("acquirium: bulk inserted %d rows into %s", len(df), TIMESERIES_TABLE)
        return len(df)

    @staticmethod
    def _rows_frame(stream_uri: str, rows_list: list[tuple[datetime, Any]], value_kind: str) -> pl.DataFrame:
        n = len(rows_list)
        return pl.DataFrame(
            {
                "stream_uri": pl.Series("stream_uri", [stream_uri] * n, dtype=pl.Utf8),
                "ts": pl.Series("ts", [ts for ts, _ in rows_list], dtype=pl.Datetime("us", "UTC")),
                "value": typed_value_series([value for _, value in rows_list]),
                "value_kind": pl.Series("value_kind", [value_kind] * n, dtype=pl.Utf8),
            }
        )

    @staticmethod
    def _prepare_frame(df: pl.DataFrame) -> pl.DataFrame:
        """Split value columns, normalise ts to tz-aware UTC, dedupe on (stream_uri, ts).

        Dedupe is required: ON CONFLICT DO UPDATE rejects a source that names
        the same row twice. Last occurrence wins, matching a sequence of
        individual upserts.
        """
        df = prepare_value_columns(df)
        ts = pl.col("ts")
        if df.schema["ts"].time_zone is None:
            # Naive timestamps are taken as UTC, matching _to_utc on the read side.
            ts = ts.dt.replace_time_zone("UTC")
        else:
            ts = ts.dt.convert_time_zone("UTC")
        df = df.with_columns(ts.cast(pl.Datetime("us", "UTC")))
        return df.unique(subset=["stream_uri", "ts"], keep="last", maintain_order=True)

    def _insert_frame(self, conn, df: pl.DataFrame, revision: int) -> None:
        """COPY and merge a prepared frame at one materialization revision."""
        staging = self.STAGING_TABLE
        with conn.cursor() as cur:
            # ON COMMIT DROP fires at the outer commit, so a second write in
            # one materialization transaction needs the previous table removed.
            cur.execute(f"DROP TABLE IF EXISTS {staging}")
            cur.execute(
                f"""
                CREATE TEMP TABLE {staging} (
                    stream_uri TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    numeric_value DOUBLE PRECISION,
                    text_value TEXT
                ) ON COMMIT DROP
                """
            )
            payload = df.select(["stream_uri", "ts", "numeric_value", "text_value"]).write_csv(include_header=False)
            with timed_debug(logger, "COPY rows=%d bytes=%d", len(df), len(payload)):
                with cur.copy(
                    f"COPY {staging} (stream_uri, ts, numeric_value, text_value) FROM STDIN WITH (FORMAT csv)"
                ) as copy:
                    copy.write(payload)
            cur.execute(
                f"""
                INSERT INTO {STREAM_IDS_TABLE} (stream_uri)
                SELECT DISTINCT i.stream_uri FROM {staging} AS i
                WHERE NOT EXISTS (
                    SELECT 1 FROM {STREAM_IDS_TABLE} AS r WHERE r.stream_uri = i.stream_uri
                )
                ON CONFLICT (stream_uri) DO NOTHING
                """
            )
            # Sort by the unique index key so wide time-major input writes
            # sequentially per stream and keeps the hot b-tree pages cached.
            with timed_debug(logger, "merge into %s rows=%d", TIMESERIES_TABLE, len(df)):
                cur.execute(
                    f"""
                    INSERT INTO {TIMESERIES_TABLE} (stream_id, ts, numeric_value, text_value, deleted, last_revision)
                    SELECT r.stream_id, i.ts, i.numeric_value, i.text_value, FALSE, %s
                    FROM {staging} AS i
                    JOIN {STREAM_IDS_TABLE} AS r USING (stream_uri)
                    ORDER BY r.stream_id, i.ts
                    ON CONFLICT (stream_id, ts) DO UPDATE SET
                        numeric_value = EXCLUDED.numeric_value,
                        text_value = EXCLUDED.text_value,
                        deleted = FALSE,
                        last_revision = EXCLUDED.last_revision
                    """,
                    (revision,),
                )

    # -------------------- stream references --------------------
    def ensure_stream_ref(
        self,
        property_uri: str | None,
        source_id: str,
        stream_name: str,
        stream_uri: URIRef | None = None,
        value_kind: str = "text",
    ) -> URIRef:
        """Register a stream reference in the streams table.

        The stream URI is computed deterministically from (source_id, stream_name) via
        :func:`compute_stream_uri`, so two sources with the same stream_name never
        produce the same canonical identity. Timeseries rows use the integer
        ``stream_id`` assigned through ``stream_ids``. Pass a precomputed stream_uri to
        avoid recomputing it when already available.

        Returns the stream URI.
        """
        return URIRef(self.ensure_stream_refs([(property_uri, source_id, stream_name, str(stream_uri) if stream_uri is not None else None, value_kind)])[0])

    def ensure_stream_refs(self, refs) -> list[str]:
        """Register a batch using its own transaction."""
        with self._lock, self._write_conn() as conn:
            return self._ensure_stream_refs(conn, refs)

    def _ensure_stream_refs(self, conn, refs) -> list[str]:
        """Register references in the caller's transaction; never commit it."""
        prepared: dict[str, tuple[Any, ...]] = {}
        for property_uri, source_id, stream_name, stream_uri, value_kind in refs:
            key = str(stream_uri if stream_uri is not None else compute_stream_uri(source_id, stream_name))
            # Last occurrence wins, matching a sequence of individual upserts.
            # ON CONFLICT DO UPDATE also refuses a source naming the same row
            # twice, and the caller's SPARQL can legitimately yield repeats.
            prepared[key] = (key, property_uri, source_id, stream_name, normalize_value_kind(value_kind))
        if not prepared:
            return []
        cols = "(stream_uri, property_uri, source_id, stream_name, value_kind)"
        # The connection runs autocommit, so an explicit transaction is what keeps
        # the staging table alive from COPY through to the upsert (and drops it).
        # ``self.conn`` is the persistent public-transaction connection.  The
        # staging table name is fixed, so concurrent graph synchronizations must
        # serialize just like timeseries writers; otherwise one request can
        # nest a transaction or collide with the other request's temp table.
        with conn.cursor() as cur:
            # A caller-controlled transaction can synchronize references
            # more than once before commit; remove the prior staging table
            # because ON COMMIT DROP has not fired yet.
            cur.execute("DROP TABLE IF EXISTS _acquirium_incoming_refs")
            cur.execute(
                f"CREATE TEMP TABLE _acquirium_incoming_refs "
                f"(LIKE {STREAMS_TABLE}) ON COMMIT DROP"
            )
            with cur.copy(f"COPY _acquirium_incoming_refs {cols} FROM STDIN") as copy:
                for row in prepared.values():
                    copy.write_row(row)
            cur.execute(
                f"""
                INSERT INTO {STREAMS_TABLE} {cols}
                SELECT stream_uri, property_uri, source_id, stream_name, value_kind
                FROM _acquirium_incoming_refs
                ON CONFLICT (stream_uri) DO UPDATE
                    SET
                        property_uri = COALESCE(EXCLUDED.property_uri, {STREAMS_TABLE}.property_uri),
                        source_id = EXCLUDED.source_id,
                        stream_name = EXCLUDED.stream_name,
                        value_kind = EXCLUDED.value_kind
                """
            )
        return list(prepared.keys())

    def resolve_storage_key(self, property_uri: str) -> str:
        """Return the canonical stream URI for a property URI, or pass it through.

        This resolves the semantic URI to the public stream identity used by
        reads. Internally, ``stream_ids`` maps that identity to an integer key.
        An unregistered URI is returned unchanged.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT stream_uri FROM {STREAMS_TABLE} WHERE property_uri = %s",
                (property_uri,),
            )
            row = cur.fetchone()
            return row[0] if row else property_uri

    def resolve_storage_keys(self, property_uris: list[str]) -> dict[str, str]:
        """Batch-resolve property URIs to canonical stream URIs in one query.

        Returns a mapping of property_uri → stream_uri (or property_uri itself for
        unregistered URIs, preserving the single-URI fallback behaviour).
        """
        if not property_uris:
            return {}
        with timed_debug(logger, "resolve_storage_keys n=%d", len(property_uris)), self.conn.cursor() as cur:
            cur.execute(
                f"SELECT property_uri, stream_uri FROM {STREAMS_TABLE} WHERE property_uri = ANY(%s)",
                (property_uris,),
            )
            rows = cur.fetchall()
        registered = {row[0]: row[1] for row in rows}
        return {uri: registered.get(uri, uri) for uri in property_uris}

    # -------------------- queries --------------------
    def timeseries(
        self,
        stream_uri: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: Order = "asc",
        batch_size: int = 50_000,
        value_mode: str = "default",
    ) -> Iterator[pa.RecordBatch]:
        '''
        Returns an iterator over the time series data for the given stream URI.
        '''
        mode = normalize_value_mode(value_mode)
        with self.conn.cursor() as cur:
            stream_id = self._stream_id(cur, stream_uri)
        if stream_id is None:
            # Never written to: no id, no rows.
            logger.debug("timeseries stream_uri=%s has no stream_id; yielding nothing", stream_uri)
            return
        clauses = ["stream_id = %s", "NOT deleted"]
        params: list[Any] = [stream_id]

        if start:
            clauses.append("ts >= %s")
            params.append(self._to_utc(start))
        if end:
            clauses.append("ts <= %s")
            params.append(self._to_utc(end))
        if mode == "numeric":
            clauses.append("numeric_value IS NOT NULL")
        elif mode == "text":
            clauses.append("text_value IS NOT NULL")

        where = " AND ".join(clauses)
        order_sql = "ASC" if order == "asc" else "DESC"
        limit_sql = " LIMIT %s" if limit else ""
        if limit:
            params.append(limit)

        query = f"""
            SELECT
                ts,
                numeric_value,
                text_value
            FROM {TIMESERIES_TABLE}
            WHERE {where}
            ORDER BY ts {order_sql}{limit_sql}
        """

        value_kind = self.stream_value_kind(str(stream_uri))
        logger.debug(
            "timeseries stream_uri=%s start=%s end=%s limit=%s order=%s mode=%s",
            stream_uri, start, end, limit, order, mode,
        )
        with self.conn.cursor() as cur:
            with timed_debug(logger, "timeseries cur.execute stream_uri=%s", stream_uri):
                cur.execute(query, params)

            total = 0
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    logger.debug("timeseries stream_uri=%s yielded total rows=%d", stream_uri, total)
                    break
                total += len(rows)

                ts_col = [r[0] for r in rows]
                numeric_col = [r[1] for r in rows]
                text_col = [r[2] for r in rows]
                stream_uri_col = [stream_uri] * len(ts_col)
                if not ts_col or not stream_uri_col:
                    break
                if mode == "coalesce":
                    values = [
                        text if text is not None else (str(numeric) if numeric is not None else None)
                        for numeric, text in zip(numeric_col, text_col)
                    ]
                    val_array = pa.array(values, type=pa.string())
                elif mode == "text" or value_kind == "text":
                    val_array = pa.array(text_col, type=pa.string())
                elif mode == "numeric" or value_kind == "numeric":
                    val_array = pa.array(numeric_col, type=pa.float64())
                elif any(v is not None for v in numeric_col):
                    val_array = pa.array(numeric_col, type=pa.float64())
                else:
                    val_array = pa.array(text_col, type=pa.string())

                batch = pa.record_batch(
                    [
                        pa.array(ts_col, type=pa.timestamp("us", tz="UTC")),
                        val_array,
                        pa.array(stream_uri_col, type=pa.string()),
                    ],
                    names=["ts", "value", "uri"],
                )
                yield batch


    def timeseries_info(self, stream_uri: str) -> TimeseriesInfo:
        with timed_debug(logger, "timeseries_info stream_uri=%s", stream_uri), self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*), MIN(ts), MAX(ts)
                FROM {TIMESERIES_TABLE} AS t
                JOIN {STREAM_IDS_TABLE} AS r USING (stream_id)
                WHERE r.stream_uri = %s AND NOT t.deleted
                """,
                (stream_uri,),
            )
            cnt, earliest, latest = cur.fetchone()
        return TimeseriesInfo(table=TIMESERIES_TABLE, row_count=cnt, earliest=earliest, latest=latest)

    def timeseries_info_batch(self, stream_uris: list[str]) -> dict[str, TimeseriesInfo]:
        """Return stats (row_count, earliest, latest) for multiple stream URIs in one query."""
        if not stream_uris:
            return {}
        with timed_debug(logger, "timeseries_info_batch n=%d", len(stream_uris)), self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT r.stream_uri, COUNT(*), MIN(ts), MAX(ts)
                FROM {TIMESERIES_TABLE} AS t
                JOIN {STREAM_IDS_TABLE} AS r USING (stream_id)
                WHERE r.stream_uri = ANY(%s) AND NOT t.deleted
                GROUP BY r.stream_uri
                """,
                (stream_uris,),
            )
            rows = cur.fetchall()
        result: dict[str, TimeseriesInfo] = {}
        for uri, cnt, earliest, latest in rows:
            result[uri] = TimeseriesInfo(table=TIMESERIES_TABLE, row_count=cnt, earliest=earliest, latest=latest)
        for uri in stream_uris:
            if uri not in result:
                result[uri] = TimeseriesInfo(table=TIMESERIES_TABLE, row_count=0)
        return result

    def timeseries_exists_batch(self, stream_uris: list[str]) -> set[str]:
        """The stream URIs among ``stream_uris`` that have at least one live row.

        EXISTS stops at each stream's first entry in the (stream_id, ts) index,
        so the cost does not grow with the stream's length as COUNT(*) does.
        """
        if not stream_uris:
            return set()
        with timed_debug(logger, "timeseries_exists_batch n=%d", len(stream_uris)), self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT r.stream_uri
                FROM {STREAM_IDS_TABLE} AS r
                WHERE r.stream_uri = ANY(%s)
                  AND EXISTS (SELECT 1 FROM {TIMESERIES_TABLE} AS t
                              WHERE t.stream_id = r.stream_id AND NOT t.deleted)
                """,
                (stream_uris,),
            )
            return {uri for (uri,) in cur.fetchall()}

    # -------------------- logging API --------------------

    def insert_log(self, log: LogEntry) -> None:
        property_uri: str = log.property_uri
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
                INSERT INTO {LOGS_TABLE} (property_uri, timestamp, observed, message)
                VALUES (%s, %s, tstzrange(%s, %s, '[)'), %s)
                ON CONFLICT (property_uri, timestamp) DO UPDATE SET message = EXCLUDED.message, observed = EXCLUDED.observed      
            """
            params = [property_uri, ts, observation_start, observation_end, message]
        elif observation_start is not None:
            sql = f"""
                INSERT INTO {LOGS_TABLE} (property_uri, timestamp, observed, message)
                VALUES (%s, %s, tstzrange(%s, 'infinity', '[)'), %s)
                ON CONFLICT (property_uri, timestamp) DO UPDATE SET message = EXCLUDED.message, observed = EXCLUDED.observed
            """
            params = [property_uri, ts, observation_start, message]
        elif observation_end is not None:
            sql = f"""
                INSERT INTO {LOGS_TABLE} (property_uri, timestamp, observed, message)
                VALUES (%s, %s, tstzrange('-infinity', %s, '[)'), %s)
                ON CONFLICT (property_uri, timestamp) DO UPDATE SET message = EXCLUDED.message, observed = EXCLUDED.observed
            """
            params = [property_uri, ts, observation_end, message]
        else:
            sql = f"""
                INSERT INTO {LOGS_TABLE} (property_uri, timestamp, observed, message)
                VALUES (%s, %s, NULL, %s)
                ON CONFLICT (property_uri, timestamp) DO UPDATE SET message = EXCLUDED.message, observed = EXCLUDED.observed
            """
            params = [property_uri, ts, message]
        
        logger.debug("insert_log property_uri=%s ts=%s", property_uri, ts)
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
        property_uri: str,
        log_time_interval: TimeIntervalModel | None = None,
        obs_time_interval: TimeIntervalModel | None = None
    ) -> list[LogEntry]:
        start = log_time_interval.start if log_time_interval else None
        end = log_time_interval.end if log_time_interval else None
        observation_start = obs_time_interval.start if obs_time_interval else None
        observation_end = obs_time_interval.end if obs_time_interval else None
        
        clauses = ["property_uri = %s"]
        params: list[Any] = [property_uri]

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
            SELECT property_uri, timestamp, observed, message
            FROM {LOGS_TABLE}
            WHERE {where}
            ORDER BY timestamp ASC
        """
        try:
            with timed_debug(logger, "query_logs property_uri=%s clauses=%d", property_uri, len(clauses)), self.conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

            result: list[LogEntry] = []
            for property_uri, ts, observed_range, msg in rows:
                print(property_uri, ts, observed_range, msg)
                period = None
                if observed_range is not None:
                    period = TimeIntervalModel(start=observed_range.lower, end=observed_range.upper)
                else:
                    period = TimeIntervalModel(start=None, end=None)

                result.append(
                    LogEntry(
                        property_uri=property_uri,
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

    def delete_logs(self, property_uri: str) -> None:
        logger.debug("delete_logs property_uri=%s", property_uri)
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {LOGS_TABLE} WHERE property_uri=%s", (property_uri,))
        return True

    # -------------------- transaction helpers --------------------
    def begin(self) -> None:
        if not self._in_tx:
            logger.debug("BEGIN")
            self.conn.autocommit = False
            self._in_tx = True
            self.conn.execute("BEGIN")

    def commit(self) -> None:
        if self._in_tx:
            logger.debug("COMMIT")
            self.conn.commit()
            self.conn.autocommit = True
            self._in_tx = False

    def rollback(self) -> None:
        if self._in_tx:
            logger.debug("ROLLBACK")
            self.conn.rollback()
            self.conn.autocommit = True
            self._in_tx = False

    # -------------------- utility --------------------
    def sql_query(self, query: str) -> dict[str, Any]:
        logger.debug("sql_query: %s", query.replace("\n", " ")[:200])
        with timed_debug(logger, "sql_query"), self.conn.cursor() as cur:
            cur.execute(query)
            cols = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
        return {"columns": cols, "rows": rows}

    # -------------------- lifecycle --------------------
    def close(self) -> None:
        logger.debug("TimescaleStore.close")
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

    def stream_value_kind(self, stream_uri: str) -> str | None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT value_kind FROM {STREAMS_TABLE} WHERE stream_uri = %s",
                (stream_uri,),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def stream_value_kinds(self, stream_uris: Iterable[str]) -> dict[str, str]:
        """Value kinds of the registered streams among *stream_uris*, in one query."""
        uris = list(dict.fromkeys(stream_uris))
        if not uris:
            return {}
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT stream_uri, value_kind FROM {STREAMS_TABLE} WHERE stream_uri = ANY(%s)",
                (uris,),
            )
            rows = cur.fetchall()
        return {uri: kind for uri, kind in rows}
