from __future__ import annotations

"""DuckDB-backed timeseries store.

Implements the same interface as TimescaleStore so the two backends are
interchangeable.  Select this backend at startup via:

    ACQUIRIUM_TIMESERIES_BACKEND=duckdb  (or "timescale" for Postgres/TimescaleDB)
    ACQUIRIUM_DUCKDB_PATH=/path/to/timeseries.duckdb  (default: {data_dir}/timeseries.duckdb)

**Concurrency:** Every operation opens its own connection against the shared
in-process database instance (DuckDB's Python client caches the instance per
path, and the store holds an anchor connection so it stays cached). Reads
therefore never wait on writes. Writes are serialised by a ``threading.Lock``
and each runs in its own transaction; ``begin()``/``commit()``/``rollback()``
span multiple write calls via a dedicated transaction connection. Uncommitted
transaction writes are not visible to any read. Multi-process access to the
same ``.duckdb`` file is *not* supported — use the Postgres backend when
running multiple server workers.

**Timestamps:** All timestamps are stored as ``TIMESTAMP`` (microseconds, UTC)
rather than ``TIMESTAMPTZ``, avoiding a DuckDB Python-API dependency on
``pytz``. The ``_to_utc`` helper normalises every input value before storage,
and ``_add_utc`` re-attaches ``tzinfo=UTC`` to values read back.

**Storage keys:** The API speaks ``stream_uri`` strings throughout, but the
``timeseries`` table keys rows by an ``INTEGER stream_id`` — integer columns get
far more effective zonemap (min-max) pruning than VARCHAR, and the rows are
narrower. The ``stream_ids`` table maps each ``stream_uri`` to its ``stream_id``; ids
are assigned from a sequence on first write and resolved inside this module,
never exposed. The ``timeseries_streams`` view joins the string back in, so
    SQL against the view is unaffected. The schema expects the integer-keyed
    ``timeseries`` table described above.
"""

from contextlib import contextmanager
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
    compute_stream_uri,
)
from acquirium.Storage.migrations import DUCKDB_TIMESERIES_MIGRATIONS, migrate
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
REF_IDS_SEQ = "ref_ids_seq"
TIMESERIES_STREAMS_VIEW = "timeseries_streams"
SYSTEM_STATE_TABLE = "system_state"
BINDING_PROGRESS_TABLE = "binding_progress"



class DuckDBStore:
    """DuckDB implementation of the TimeseriesStore protocol.

    Uses plain DuckDB tables without TimescaleDB-specific features
    (no hypertables, no compression policies).  The ``logs`` table
    stores the observation period as two separate TIMESTAMP columns
    instead of a Postgres ``tstzrange``.
    """

    def __init__(self, db_path: str | Path, *, recreate: bool = False) -> None:
        import duckdb  # lazy — not required unless duckdb backend is selected

        # Kept on the instance so operations can open their own connections
        # without importing duckdb at module scope.
        self._duckdb = duckdb
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        logger.debug("DuckDBStore.__init__: connecting to %s (recreate=%s)", self.db_path, recreate)
        # Never used for queries: held open so the in-process database instance
        # stays cached, making per-operation connects cheap attachments instead
        # of full file opens (and avoiding a WAL checkpoint each time the last
        # connection closes).
        with timed_debug(logger, "duckdb.connect(%s)", self.db_path):
            self._anchor_conn = duckdb.connect(str(self.db_path))
        # Dedicated connection carrying a caller-scoped begin()/commit() span;
        # None outside such a span. Guarded by self._lock.
        self._tx_conn = None

        if recreate:
            logger.debug("DuckDBStore.__init__: dropping existing tables/views (recreate=True)")
            with self._lock, self._own_conn() as conn:
                conn.execute(f"DROP VIEW IF EXISTS {TIMESERIES_STREAMS_VIEW}")
                for tbl in (
                    "experiment_metadata_outbox",
                    "experiment_metadata_state",
                    "experiment_observations",
                    "experiment_artifacts",
                    "experiment_artifact_chunks",
                    "experiment_runs",
                    "experiment_variables",
                    "experiment_templates",
                    "experiment_metadata_resolutions",
                    "materialization_work",
                    "materialization_lineage",
                    "materialization_deployments",
                    "schema_migrations",
                    "stream_resets",
                    BINDING_PROGRESS_TABLE,
                    SYSTEM_STATE_TABLE,
                    TIMESERIES_TABLE,
                    STREAMS_TABLE,
                    LOGS_TABLE,
                    STREAM_IDS_TABLE,
                ):
                    conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                conn.execute(f"DROP SEQUENCE IF EXISTS {REF_IDS_SEQ}")
        self.ensure_table()
        logger.debug("DuckDBStore.__init__: ready at %s", self.db_path)

    # ---- connections ----

    def _connect(self):
        """Open a connection to the shared in-process database instance."""
        return self._duckdb.connect(str(self.db_path))

    @contextmanager
    def _own_conn(self):
        """A private autocommit connection, used for reads and DDL.

        Reads never block behind the write lock and see the last committed
        state: writes inside an open begin()/commit() span are not visible
        until that span commits. DDL also runs here, statement-by-statement:
        DuckDB's catalog dependency tracking rejects re-creating a dropped
        index inside an explicit transaction.
        """
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write_conn(self):
        """A connection with an open transaction. Call with ``self._lock`` held.

        Inside a caller-scoped begin()/commit() span this yields the span's
        connection and leaves commit/rollback to the span owner. Otherwise it
        opens a private connection and wraps the block in its own transaction.
        """
        if self._tx_conn is not None:
            yield self._tx_conn
            return
        conn = self._connect()
        try:
            conn.begin()
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

    # ---- table management ----

    def ensure_table(self) -> str:
        """Create tables and indexes if they do not exist. Returns a status string."""
        # Use TIMESTAMP (not TIMESTAMPTZ) to avoid a DuckDB/pytz interop issue.
        # All values are normalised to UTC before insertion via _to_utc().
        stmts = [
            f"CREATE SEQUENCE IF NOT EXISTS {REF_IDS_SEQ}",
            f"""
            CREATE TABLE IF NOT EXISTS {STREAM_IDS_TABLE} (
                stream_id  INTEGER PRIMARY KEY DEFAULT nextval('{REF_IDS_SEQ}'),
                stream_uri VARCHAR NOT NULL UNIQUE
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {TIMESERIES_TABLE} (
                stream_id  INTEGER NOT NULL,
                ts      TIMESTAMP NOT NULL,
                numeric_value DOUBLE,
                text_value    VARCHAR,
                -- Canonical publication columns.
                -- ``deleted`` marks a physical
                -- tombstone: the row is kept (not removed) so its
                -- last_stream_version remains resolvable by a batch reader.
                -- ``last_stream_version`` is the stream_heads version at which
                -- this row was last written; the batch reader uses it to defer
                -- a key to a later batch when a newer write has already
                -- superseded it within the same snapshot.
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                last_stream_version BIGINT NOT NULL DEFAULT 0,
                -- A global discovery revision used by the incremental
                -- materializer.  It is deliberately independent from the
                -- per-stream publication protocol above.
                last_revision BIGINT NOT NULL DEFAULT 0,
                -- No UNIQUE (stream_id, ts): _insert_frame keeps one row per key
                -- by deleting colliding rows before it inserts.
                CHECK (numeric_value IS NULL OR text_value IS NULL)
            )
            """,
        ]
        dependent_stmts = [
            f"""
            CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                stream_uri   VARCHAR PRIMARY KEY,
                property_uri VARCHAR,
                source_id VARCHAR NOT NULL,
                stream_name  VARCHAR NOT NULL,
                value_kind VARCHAR NOT NULL DEFAULT 'text'
            )
            """,
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_source_ref_name ON {STREAMS_TABLE} (source_id, stream_name)",
            f"CREATE INDEX IF NOT EXISTS idx_streams_point_uri ON {STREAMS_TABLE} (property_uri)",
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
            WHERE NOT t.deleted
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {LOGS_TABLE} (
                property_uri      VARCHAR NOT NULL,
                timestamp      TIMESTAMP NOT NULL,
                observed_start TIMESTAMP,
                observed_end   TIMESTAMP,
                message        VARCHAR NOT NULL,
                UNIQUE (property_uri, timestamp)
            )
            """,
            f"CREATE INDEX IF NOT EXISTS idx_logs_point ON {LOGS_TABLE} (property_uri, timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_logs_obs ON {LOGS_TABLE} (observed_start, observed_end)",
            "CREATE TABLE IF NOT EXISTS stream_resets (stream_uri VARCHAR PRIMARY KEY, last_revision BIGINT NOT NULL)",
            f"CREATE TABLE IF NOT EXISTS {SYSTEM_STATE_TABLE} (current_revision BIGINT NOT NULL)",
            f"INSERT INTO {SYSTEM_STATE_TABLE} SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM {SYSTEM_STATE_TABLE})",
            f"CREATE TABLE IF NOT EXISTS {BINDING_PROGRESS_TABLE} (progress_key VARCHAR PRIMARY KEY, consumed_revision BIGINT NOT NULL)",
            f"CREATE INDEX IF NOT EXISTS idx_timeseries_last_revision ON {TIMESERIES_TABLE} (last_revision)",
        ]
        with self._lock, timed_debug(logger, "ensure_table"), self._own_conn() as conn:
            for stmt in stmts:
                conn.execute(stmt)
            with self._write_conn() as migration_conn:
                migrate(migration_conn, backend="duckdb", component="timeseries",
                        migrations=DUCKDB_TIMESERIES_MIGRATIONS)
            for stmt in dependent_stmts:
                conn.execute(stmt)
        return "ok"

    # ---- timeseries mutations ----

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
            return self._insert_frame(conn, frame, revision)

    def bulk_insert_polars(self, df: pl.DataFrame) -> int:
        """Bulk-insert a Polars DataFrame with canonical or split value columns.

        Uses DuckDB's native Polars bridge — no ADBC required.
        """
        if df.is_empty():
            logger.debug("bulk_insert_polars: empty DataFrame, skipping")
            return 0
        in_rows = len(df)
        with timed_debug(logger, "bulk_insert_polars prepare rows=%d", in_rows):
            df = self._prepare_frame(df)
        with self._lock, timed_debug(logger, "bulk_insert_polars DELETE+INSERT rows=%d", in_rows), self._write_conn() as conn:
            written = self._insert_frame(conn, df, self._next_revision(conn))
        logger.debug("bulk_insert_polars: %d rows after dedupe (was %d)", written, in_rows)
        return written

    @staticmethod
    def _prepare_frame(df: pl.DataFrame) -> pl.DataFrame:
        """Split value columns and normalise ts to naive UTC."""
        return prepare_value_columns(df).with_columns(
            pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        )

    @staticmethod
    def _next_revision(conn) -> int:
        """Allocate one revision inside the caller's write transaction."""
        # This is global rather than per-stream so one materialization frontier
        # can describe a coherent multi-input snapshot.
        return int(conn.execute(
            f"UPDATE {SYSTEM_STATE_TABLE} SET current_revision = current_revision + 1 RETURNING current_revision"
        ).fetchone()[0])

    STAGING_TABLE = "_acquirium_staging_timeseries"

    @classmethod
    def _insert_frame(cls, conn, df: pl.DataFrame, revision: int) -> int:
        """Upsert a prepared frame on *conn* through a temporary staging table.

        The frame carries stream_uri strings. Ids are assigned for unseen uris and
        the rows staged by stream_id. Colliding (stream_id, ts) rows are then deleted
        from the timeseries table and the staged rows inserted in
        (stream_id, ts) order, all inside the caller's transaction.

        The table has no unique constraint, so this is the only place that
        keeps one row per (stream_id, ts). A key repeated within the frame keeps
        its last row. Returns the number of rows written.
        """
        df = df.unique(subset=["stream_uri", "ts"], keep="last", maintain_order=True)
        staging = cls.STAGING_TABLE
        conn.register("_acquirium_incoming_timeseries", df)
        try:
            conn.execute(
                f"""
                INSERT INTO {STREAM_IDS_TABLE} (stream_uri)
                SELECT DISTINCT stream_uri FROM _acquirium_incoming_timeseries
                ON CONFLICT (stream_uri) DO NOTHING
                """
            )
            # OR REPLACE: a caller-scoped transaction can write more than once
            # on the same connection, and a failed write leaves its table behind.
            conn.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE {staging} AS
                SELECT r.stream_id, i.ts, i.numeric_value, i.text_value
                FROM _acquirium_incoming_timeseries AS i
                JOIN {STREAM_IDS_TABLE} AS r USING (stream_uri)
                """
            )
        finally:
            conn.unregister("_acquirium_incoming_timeseries")
        conn.execute(
            f"""
            DELETE FROM {TIMESERIES_TABLE}
            USING {staging} AS incoming
            WHERE {TIMESERIES_TABLE}.stream_id = incoming.stream_id
              AND {TIMESERIES_TABLE}.ts = incoming.ts
            """
        )
        # DuckDB appends in the order given, so each write lands as a run
        # sorted by stream and time.
        conn.execute(
            f"""
            INSERT INTO {TIMESERIES_TABLE} (stream_id, ts, numeric_value, text_value, last_revision)
            SELECT stream_id, ts, numeric_value, text_value, ?
            FROM {staging}
            ORDER BY stream_id, ts
            """, [revision]
        )
        conn.execute(f"DROP TABLE {staging}")
        return df.height

    # ---- stream reference registry ----

    def ensure_stream_ref(
        self,
        property_uri: str | None,
        source_id: str,
        stream_name: str,
        stream_uri: str | None = None,
        value_kind: str = "text",
    ) -> str:
        """Register a stream reference and return its canonical stream URI."""
        return self.ensure_stream_refs([(property_uri, source_id, stream_name, stream_uri, value_kind)])[0]

    def ensure_stream_refs(self, refs) -> list[str]:
        """Register a batch using its own transaction."""
        with self._lock, self._write_conn() as conn:
            return self._ensure_stream_refs(conn, refs)

    def _ensure_stream_refs(self, conn, refs) -> list[str]:
        """Register references in the caller's transaction; never commit it."""
        prepared: dict[str, list[Any]] = {}
        for property_uri, source_id, stream_name, stream_uri, value_kind in refs:
            key = str(stream_uri if stream_uri is not None else compute_stream_uri(source_id, stream_name))
            # Last occurrence wins, matching a sequence of individual upserts.
            # DuckDB's ON CONFLICT DO UPDATE rejects a source naming the same row
            # twice, and the caller's SPARQL can legitimately yield repeats.
            prepared[key] = [key, property_uri, source_id, stream_name, normalize_value_kind(value_kind)]
        if not prepared:
            return []
        df = pl.DataFrame(
            list(prepared.values()),
            schema=["stream_uri", "property_uri", "source_id", "stream_name", "value_kind"],
            orient="row",
        )
        conn.register("_acquirium_incoming_refs", df)
        try:
            conn.execute(
                f"""
                INSERT INTO {STREAMS_TABLE} (stream_uri, property_uri, source_id, stream_name, value_kind)
                SELECT stream_uri, property_uri, source_id, stream_name, value_kind
                FROM _acquirium_incoming_refs
                ON CONFLICT (stream_uri) DO UPDATE SET
                    source_id  = excluded.source_id,
                    stream_name   = excluded.stream_name,
                    property_uri  = COALESCE(excluded.property_uri, {STREAMS_TABLE}.property_uri),
                    value_kind = excluded.value_kind
                """
            )
        finally:
            conn.unregister("_acquirium_incoming_refs")
        return list(prepared.keys())

    def resolve_storage_key(self, property_uri: str) -> str:
        """Return the storage stream URI for property_uri, or property_uri itself if unregistered."""
        with self._own_conn() as conn:
            row = conn.execute(
                f"SELECT stream_uri FROM {STREAMS_TABLE} WHERE property_uri = ?", [property_uri]
            ).fetchone()
        return property_uri if row is None else row[0]

    def resolve_storage_keys(self, property_uris: list[str]) -> dict[str, str]:
        """Batch-resolve property_uris to storage keys."""
        if not property_uris:
            return {}
        placeholders = ", ".join("?" * len(property_uris))
        with self._own_conn() as conn, timed_debug(logger, "resolve_storage_keys n=%d", len(property_uris)):
            d = conn.sql(
                f"SELECT property_uri, stream_uri FROM {STREAMS_TABLE} WHERE property_uri IN ({placeholders})",
                params=property_uris,
            ).to_arrow_table().to_pydict()
        registered = dict(zip(d["property_uri"], d["stream_uri"]))
        return {uri: registered.get(uri, uri) for uri in property_uris}

    # ---- timeseries reads ----

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
        """Yield PyArrow RecordBatches with schema [ts: timestamp[us,UTC], value, uri].

        The value column's type is fixed for the whole read — every batch of
        one call has the same schema.
        """
        mode = normalize_value_mode(value_mode)

        # DuckDB connections are not thread-safe, and cursor() only returns another
        # handle on the same connection -- the documented pattern for parallel work
        # is an independent connection per concurrent reader. This generator owns
        # its connection for its whole life (not via _own_conn, whose scope would
        # end at the first yield), which keeps it correct no matter which
        # threadpool thread advances it.
        conn = self._connect()
        rows = 0
        try:
            with timed_debug(
                logger, "timeseries stream_uri=%s start=%s end=%s limit=%s order=%s mode=%s",
                stream_uri, start, end, limit, order, mode,
            ):
                # fetchall(), not fetchone(): a partly read execute() result
                # stays open, and DuckDB then refuses the relation below.
                stream_id_rows = conn.execute(
                    f"SELECT stream_id FROM {STREAM_IDS_TABLE} WHERE stream_uri = ? LIMIT 1", [stream_uri]
                ).fetchall()
                if not stream_id_rows:
                    # Never written to: no id, no rows.
                    return

                # Normal reads hide tombstones: a deleted row is kept
                # physically (its last_stream_version stays resolvable by a
                # batch reader) but is invisible to this API.
                clauses = ["stream_id = ?", "NOT deleted"]
                params: list[Any] = [stream_id_rows[0][0]]
                if start:
                    clauses.append("ts >= ?")
                    params.append(self._to_utc_naive(start))
                if end:
                    clauses.append("ts <= ?")
                    params.append(self._to_utc_naive(end))
                if mode == "numeric":
                    clauses.append("numeric_value IS NOT NULL")
                elif mode == "text":
                    clauses.append("text_value IS NOT NULL")

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

                value_kind = self._stream_value_kind(conn, stream_uri)
                # Resolve the value column once for the whole read so every
                # yielded batch has the same schema. An explicit mode wins over
                # the stream's registered kind: a numeric read of a text-kind
                # stream must return the numeric column it filtered on, not the
                # (all-NULL) text column. In default mode the registered kind
                # decides; an unregistered stream is probed over the queried
                # range (ignoring LIMIT, so the type cannot depend on which
                # rows a LIMIT happens to return): numeric-only reads as
                # float64, text-only as string, and a mixed stream coalesces to
                # string so no value is nulled out.
                resolved = mode
                if resolved == "default":
                    if value_kind in ("numeric", "text"):
                        resolved = value_kind
                    else:
                        has_numeric, has_text = conn.execute(
                            f"SELECT COUNT(numeric_value) > 0, COUNT(text_value) > 0 FROM {TIMESERIES_TABLE} WHERE {where}",
                            params,
                        ).fetchall()[0]
                        if has_numeric and has_text:
                            resolved = "coalesce"
                        else:
                            resolved = "numeric" if has_numeric else "text"
                # Streamed, not materialized: batches are pulled as the caller
                # consumes them, so an unbounded range need not fit in memory.
                reader = conn.sql(query, params=params).to_arrow_reader(batch_size)
            for batch in reader:
                rows += batch.num_rows
                numeric_col = batch.column("numeric_value")
                text_col = batch.column("text_value")
                if resolved == "coalesce":
                    numeric_values = numeric_col.to_pylist()
                    text_values = text_col.to_pylist()
                    values = [
                        text if text is not None else (str(numeric) if numeric is not None else None)
                        for numeric, text in zip(numeric_values, text_values)
                    ]
                    val_col = pa.array(values, type=pa.string())
                elif resolved == "numeric":
                    val_col = numeric_col.cast(pa.float64())
                else:
                    val_col = text_col.cast(pa.string())
                ts_col = pc.assume_timezone(batch.column("ts"), timezone="UTC")
                uri_col = pa.array([stream_uri] * len(batch), type=pa.string())
                yield pa.record_batch([ts_col, val_col, uri_col], names=["ts", "value", "uri"])
        finally:
            # Runs on early caller exit (GeneratorExit) as well as on exhaustion.
            conn.close()
            logger.debug("timeseries: stream_uri=%s rows=%d (batch_size=%d)", stream_uri, rows, batch_size)


    def timeseries_info(self, stream_uri: str) -> TimeseriesInfo:
        with self._own_conn() as conn, timed_debug(logger, "timeseries_info stream_uri=%s", stream_uri):
            row = conn.execute(
                f"""
                SELECT COUNT(*), MIN(ts), MAX(ts)
                FROM {TIMESERIES_TABLE} AS t
                JOIN {STREAM_IDS_TABLE} AS r USING (stream_id)
                WHERE r.stream_uri = ? AND NOT t.deleted
                """,
                [stream_uri],
            ).fetchone()
        cnt, earliest_raw, latest_raw = (row[0] or 0, row[1], row[2]) if row else (0, None, None)
        return TimeseriesInfo(
            table=TIMESERIES_TABLE,
            row_count=cnt,
            earliest=self._add_utc(earliest_raw),
            latest=self._add_utc(latest_raw),
        )

    def timeseries_info_batch(self, stream_uris: list[str]) -> dict[str, TimeseriesInfo]:
        if not stream_uris:
            return {}
        placeholders = ", ".join("?" * len(stream_uris))
        with self._own_conn() as conn, timed_debug(logger, "timeseries_info_batch n=%d", len(stream_uris)):
            d = conn.sql(
                f"""
                SELECT r.stream_uri,
                       COUNT(*)   AS row_count,
                       MIN(ts)    AS earliest,
                       MAX(ts)    AS latest
                FROM {TIMESERIES_TABLE} AS t
                JOIN {STREAM_IDS_TABLE} AS r USING (stream_id)
                WHERE r.stream_uri IN ({placeholders}) AND NOT t.deleted
                GROUP BY r.stream_uri
                """,
                params=stream_uris,
            ).to_arrow_table().to_pydict()
        result: dict[str, TimeseriesInfo] = {}
        for uri, cnt, earliest_raw, latest_raw in zip(
            d["stream_uri"], d["row_count"], d["earliest"], d["latest"]
        ):
            result[uri] = TimeseriesInfo(
                table=TIMESERIES_TABLE,
                row_count=cnt or 0,
                earliest=self._add_utc(earliest_raw),
                latest=self._add_utc(latest_raw),
            )
        for uri in stream_uris:
            if uri not in result:
                result[uri] = TimeseriesInfo(table=TIMESERIES_TABLE, row_count=0)
        return result

    def timeseries_exists_batch(self, stream_uris: list[str]) -> set[str]:
        """The stream URIs among ``stream_uris`` that have at least one live row.

        The join of ``timeseries_info_batch`` without its aggregates. DuckDB
        cannot stop a correlated EXISTS at a stream's first row (it hashes the
        whole table for the semi join), and one ``LIMIT 1`` probe per stream
        costs more to plan than it saves once there are many streams.
        """
        if not stream_uris:
            return set()
        placeholders = ", ".join("?" * len(stream_uris))
        with self._own_conn() as conn, timed_debug(logger, "timeseries_exists_batch n=%d", len(stream_uris)):
            rows = conn.execute(
                f"""
                SELECT DISTINCT r.stream_uri
                FROM {TIMESERIES_TABLE} AS t
                JOIN {STREAM_IDS_TABLE} AS r USING (stream_id)
                WHERE r.stream_uri IN ({placeholders}) AND NOT t.deleted
                """,
                stream_uris,
            ).fetchall()
        return {uri for (uri,) in rows}

    # ---- logs ----

    def insert_log(self, log: LogEntry) -> None:
        obs_start = self._to_utc_naive(log.period.start) if log.period and log.period.start else None
        obs_end = self._to_utc_naive(log.period.end) if log.period and log.period.end else None
        logger.debug("insert_log property_uri=%s ts=%s", log.property_uri, log.timestamp)
        with self._lock, self._write_conn() as conn:
            conn.execute(
                f"""
                INSERT INTO {LOGS_TABLE} (property_uri, timestamp, observed_start, observed_end, message)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (property_uri, timestamp) DO UPDATE SET
                    message        = excluded.message,
                    observed_start = excluded.observed_start,
                    observed_end   = excluded.observed_end
                """,
                [log.property_uri, self._to_utc_naive(log.timestamp), obs_start, obs_end, log.message],
            )

    def query_logs(
        self,
        property_uri: str,
        log_time_interval: TimeIntervalModel | None = None,
        obs_time_interval: TimeIntervalModel | None = None,
    ) -> list[LogEntry]:
        start = log_time_interval.start if log_time_interval else None
        end = log_time_interval.end if log_time_interval else None
        obs_start = obs_time_interval.start if obs_time_interval else None
        obs_end = obs_time_interval.end if obs_time_interval else None

        clauses = ["property_uri = ?"]
        params: list[Any] = [property_uri]

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
            # A log without an observation interval is not an open-ended
            # observation.  It has no event-time extent and must not match an
            # observation-window query (matching PostgreSQL tstzrange NULL
            # semantics).
            clauses.append("observed_end IS NOT NULL AND observed_end > ?")
            params.append(self._to_utc_naive(obs_start))
        elif obs_end is not None:
            clauses.append("observed_start < ?")
            params.append(self._to_utc_naive(obs_end))

        where = " AND ".join(clauses)
        query = f"""
            SELECT property_uri, timestamp, observed_start, observed_end, message
            FROM {LOGS_TABLE}
            WHERE {where}
            ORDER BY timestamp ASC
        """
        try:
            with self._own_conn() as conn, timed_debug(logger, "query_logs property_uri=%s clauses=%d", property_uri, len(clauses)):
                tbl = conn.sql(query, params=params).to_arrow_table()
        except Exception as exc:
            logger.error("query_logs failed: %s", exc)
            return []

        d = tbl.to_pydict()
        result: list[LogEntry] = []
        for uri_val, ts_raw, o_start_raw, o_end_raw, msg in zip(
            d["property_uri"], d["timestamp"], d["observed_start"], d["observed_end"], d["message"]
        ):
            period = TimeIntervalModel(start=self._add_utc(o_start_raw), end=self._add_utc(o_end_raw))
            result.append(LogEntry(property_uri=uri_val, timestamp=self._add_utc(ts_raw), period=period, message=msg))
        return result

    def delete_logs(self, property_uri: str) -> bool:
        logger.debug("delete_logs property_uri=%s", property_uri)
        with self._lock, self._write_conn() as conn:
            conn.execute(
                f"DELETE FROM {LOGS_TABLE} WHERE property_uri = ?", [property_uri]
            )
        return True

    # ---- transaction helpers ----

    def begin(self) -> None:
        """Open a transaction span: writes until commit()/rollback() are atomic.

        Reads do not see the span's writes until it commits (they run on their
        own connections against the last committed state).
        """
        with self._lock:
            if self._tx_conn is None:
                logger.debug("BEGIN TRANSACTION")
                conn = self._connect()
                conn.begin()
                self._tx_conn = conn

    def commit(self) -> None:
        with self._lock:
            if self._tx_conn is not None:
                logger.debug("COMMIT")
                try:
                    self._tx_conn.commit()
                finally:
                    self._tx_conn.close()
                    self._tx_conn = None

    def rollback(self) -> None:
        with self._lock:
            if self._tx_conn is not None:
                logger.debug("ROLLBACK")
                try:
                    self._tx_conn.rollback()
                finally:
                    self._tx_conn.close()
                    self._tx_conn = None

    # ---- utility ----

    def sql_query(self, query: str) -> dict[str, Any]:
        logger.debug("sql_query: %s", query.replace("\n", " ")[:200])
        with self._own_conn() as conn, timed_debug(logger, "sql_query"):
            tbl = conn.sql(query).to_arrow_table()
        d = tbl.to_pydict()
        cols = tbl.schema.names
        return {
            "columns": cols,
            "rows": [list(row) for row in zip(*[d[c] for c in cols])] if cols else [],
        }

    def close(self) -> None:
        logger.debug("DuckDBStore.close")
        with self._lock:
            if self._tx_conn is not None:
                # An uncommitted span at close is abandoned, not committed.
                self._tx_conn.rollback()
                self._tx_conn.close()
                self._tx_conn = None
        self._anchor_conn.close()

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

    def stream_value_kind(self, stream_uri: str) -> str | None:
        with self._own_conn() as conn:
            return self._stream_value_kind(conn, stream_uri)

    def stream_value_kinds(self, stream_uris: Iterable[str]) -> dict[str, str]:
        """Value kinds of the registered streams among *stream_uris*, in one query.

        One connection and one statement for the whole batch: a lookup per
        stream costs a connection attach plus a parameterised execute each,
        which dominated ingest when a batch carried a hundred streams.
        """
        uris = list(dict.fromkeys(stream_uris))
        if not uris:
            return {}
        with self._own_conn() as conn:
            rows = conn.execute(
                f"SELECT stream_uri, value_kind FROM {STREAMS_TABLE} "
                "WHERE stream_uri IN (SELECT unnest(?::VARCHAR[]))",
                [uris],
            ).fetchall()
        return {uri: kind for uri, kind in rows}

    def _stream_value_kind(self, conn, stream_uri: str) -> str | None:
        """Look up a stream's value kind on *conn* — the caller's connection, so
        a streaming read can resolve it without opening a second connection."""
        rows = conn.execute(
            f"SELECT value_kind FROM {STREAMS_TABLE} WHERE stream_uri = ? LIMIT 1",
            [stream_uri],
        ).fetchall()
        return rows[0][0] if rows else None
