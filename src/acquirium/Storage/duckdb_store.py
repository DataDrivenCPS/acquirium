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

**Storage keys:** The API speaks ``ref_uri`` strings throughout, but the
``timeseries`` table keys rows by an ``INTEGER ref_id`` — integer columns get
far more effective zonemap (min-max) pruning than VARCHAR, and the rows are
narrower. The ``ref_ids`` table maps each ``ref_uri`` to its ``ref_id``; ids
are assigned from a sequence on first write and resolved inside this module,
never exposed. The ``timeseries_streams`` view joins the string back in, so
SQL against the view is unaffected. This schema is not backward compatible
with databases whose ``timeseries`` table is keyed by ``ref_uri`` strings —
recreate those rather than opening them in place.
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
    compute_ref_uri,
)
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
REF_IDS_TABLE = "ref_ids"
REF_IDS_SEQ = "ref_ids_seq"
TIMESERIES_STREAMS_VIEW = "timeseries_streams"

# Continuous-batch tables (continuous_batch.md; DDL details in
# continuous_batch_plan.md Phase 1a). Keyed by the integer ``ref_id`` used
# throughout this backend, not by ``ref_uri`` strings -- see
# ``Storage/continuous/duckdb.py`` for the id-resolution boundary. Created
# here (rather than lazily by the continuous layer) so a plain DuckDBStore
# always has a schema the continuous layer can attach to without a separate
# migration step.
STREAM_HEADS_TABLE = "stream_heads"
STREAM_PUBLICATIONS_TABLE = "stream_publications"
STREAM_PUBLICATIONS_SEQ = "stream_publications_seq"
STREAM_CHANGE_KEYS_TABLE = "stream_change_keys"
STREAM_CHANGE_RANGES_TABLE = "stream_change_ranges"
APP_RUNTIME_TABLE = "app_runtime"
APP_SUBSCRIPTIONS_TABLE = "app_subscriptions"
APP_BATCH_COMMITS_TABLE = "app_batch_commits"
APP_BATCH_INPUTS_TABLE = "app_batch_inputs"
APP_BOOTSTRAPS_TABLE = "app_bootstraps"
APP_BOOTSTRAP_STREAMS_TABLE = "app_bootstrap_streams"
APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE = "app_bootstrap_output_targets"
APP_BOOTSTRAP_ROWS_TABLE = "app_bootstrap_rows"
APP_BOOTSTRAP_OUTPUTS_TABLE = "app_bootstrap_outputs"
APP_WEBHOOK_INTENTS_TABLE = "app_webhook_intents"

CONTINUOUS_BATCH_DDL = [
    f"CREATE SEQUENCE IF NOT EXISTS {STREAM_PUBLICATIONS_SEQ}",
    f"""
    CREATE TABLE IF NOT EXISTS {STREAM_HEADS_TABLE} (
        ref_id INTEGER PRIMARY KEY,
        current_version BIGINT NOT NULL,
        retained_from_version BIGINT NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STREAM_PUBLICATIONS_TABLE} (
        publication_seq BIGINT PRIMARY KEY DEFAULT nextval('{STREAM_PUBLICATIONS_SEQ}'),
        publication_id VARCHAR UNIQUE NOT NULL,
        payload_hash VARCHAR NOT NULL,
        row_count BIGINT NOT NULL,
        versions_json VARCHAR NOT NULL,
        committed_at TIMESTAMP NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STREAM_CHANGE_KEYS_TABLE} (
        publication_seq BIGINT NOT NULL,
        publication_row INTEGER NOT NULL,
        ref_id INTEGER NOT NULL,
        stream_version BIGINT NOT NULL,
        ts TIMESTAMP NOT NULL,
        PRIMARY KEY (publication_seq, publication_row)
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_stream_change_keys_ref_version ON {STREAM_CHANGE_KEYS_TABLE} (ref_id, stream_version)",
    f"""
    CREATE TABLE IF NOT EXISTS {STREAM_CHANGE_RANGES_TABLE} (
        ref_uri VARCHAR NOT NULL,
        stream_version BIGINT NOT NULL,
        publication_id VARCHAR NOT NULL,
        start_ts TIMESTAMP NOT NULL,
        end_ts TIMESTAMP NOT NULL,
        change_kind VARCHAR NOT NULL,
        row_count BIGINT NOT NULL,
        PRIMARY KEY (ref_uri, stream_version, start_ts, end_ts)
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_stream_change_ranges_ref_version ON {STREAM_CHANGE_RANGES_TABLE} (ref_uri, stream_version)",
    f"""
    CREATE TABLE IF NOT EXISTS {APP_RUNTIME_TABLE} (
        app_id VARCHAR PRIMARY KEY,
        generation BIGINT NOT NULL,
        status VARCHAR NOT NULL,
        topology_version BIGINT NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_SUBSCRIPTIONS_TABLE} (
        app_id VARCHAR NOT NULL,
        generation BIGINT NOT NULL,
        ref_id INTEGER NOT NULL,
        stream_version BIGINT NOT NULL,
        PRIMARY KEY (app_id, generation, ref_id)
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_app_subscriptions_ref ON {APP_SUBSCRIPTIONS_TABLE} (ref_id)",
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BATCH_COMMITS_TABLE} (
        app_id VARCHAR NOT NULL,
        generation BIGINT NOT NULL,
        batch_id VARCHAR NOT NULL,
        batch_kind VARCHAR NOT NULL,
        rows_inserted BIGINT NOT NULL,
        output_versions_json VARCHAR NOT NULL,
        committed_at TIMESTAMP NOT NULL,
        PRIMARY KEY (app_id, generation, batch_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BATCH_INPUTS_TABLE} (
        app_id VARCHAR NOT NULL,
        generation BIGINT NOT NULL,
        batch_id VARCHAR NOT NULL,
        ref_id INTEGER NOT NULL,
        from_version BIGINT NOT NULL,
        to_version BIGINT NOT NULL,
        PRIMARY KEY (app_id, generation, batch_id, ref_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAPS_TABLE} (
        bootstrap_id VARCHAR PRIMARY KEY,
        app_id VARCHAR NOT NULL,
        generation BIGINT NOT NULL,
        status VARCHAR NOT NULL,
        next_ordinal BIGINT NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAP_STREAMS_TABLE} (
        bootstrap_id VARCHAR NOT NULL,
        ref_id INTEGER NOT NULL,
        stream_version BIGINT NOT NULL,
        PRIMARY KEY (bootstrap_id, ref_id)
    )
    """,
    # Declared output targets for this bootstrap, captured at begin_bootstrap
    # time. finalize_bootstrap anti-joins this list against what actually
    # landed in app_bootstrap_outputs to tombstone an output stream a
    # narrower/changed selector no longer produces (continuous_batch.md
    # "The actor transforms staged history..."). Not part of the design
    # doc's logical schema list; added because finalize's reconciliation step
    # needs it.
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE} (
        bootstrap_id VARCHAR NOT NULL,
        output_ref_id INTEGER NOT NULL,
        PRIMARY KEY (bootstrap_id, output_ref_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAP_ROWS_TABLE} (
        bootstrap_id VARCHAR NOT NULL,
        ordinal BIGINT NOT NULL,
        ref_id INTEGER NOT NULL,
        ts TIMESTAMP NOT NULL,
        numeric_value DOUBLE,
        text_value VARCHAR,
        PRIMARY KEY (bootstrap_id, ordinal)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAP_OUTPUTS_TABLE} (
        bootstrap_id VARCHAR NOT NULL,
        ordinal BIGINT NOT NULL,
        output_ref_uri VARCHAR NOT NULL,
        ts TIMESTAMP NOT NULL,
        operation VARCHAR NOT NULL,
        numeric_value DOUBLE,
        text_value VARCHAR,
        PRIMARY KEY (bootstrap_id, ordinal)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_WEBHOOK_INTENTS_TABLE} (
        app_id VARCHAR NOT NULL,
        generation BIGINT NOT NULL,
        batch_id VARCHAR NOT NULL,
        seq INTEGER NOT NULL,
        url VARCHAR NOT NULL,
        payload_json VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMP,
        PRIMARY KEY (app_id, generation, batch_id, seq)
    )
    """,
]


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
                    APP_WEBHOOK_INTENTS_TABLE,
                    APP_BOOTSTRAP_OUTPUTS_TABLE,
                    APP_BOOTSTRAP_ROWS_TABLE,
                    APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE,
                    APP_BOOTSTRAP_STREAMS_TABLE,
                    APP_BOOTSTRAPS_TABLE,
                    APP_BATCH_INPUTS_TABLE,
                    APP_BATCH_COMMITS_TABLE,
                    APP_SUBSCRIPTIONS_TABLE,
                    APP_RUNTIME_TABLE,
                    STREAM_CHANGE_KEYS_TABLE,
                    STREAM_PUBLICATIONS_TABLE,
                    STREAM_HEADS_TABLE,
                    TIMESERIES_TABLE,
                    STREAMS_TABLE,
                    LOGS_TABLE,
                    REF_IDS_TABLE,
                ):
                    conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                conn.execute(f"DROP SEQUENCE IF EXISTS {REF_IDS_SEQ}")
                conn.execute(f"DROP SEQUENCE IF EXISTS {STREAM_PUBLICATIONS_SEQ}")
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
            CREATE TABLE IF NOT EXISTS {REF_IDS_TABLE} (
                ref_id  INTEGER PRIMARY KEY DEFAULT nextval('{REF_IDS_SEQ}'),
                ref_uri VARCHAR NOT NULL UNIQUE
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {TIMESERIES_TABLE} (
                ref_id  INTEGER NOT NULL,
                ts      TIMESTAMP NOT NULL,
                numeric_value DOUBLE,
                text_value    VARCHAR,
                -- Continuous-batch columns (see continuous_batch.md and
                -- Storage/continuous/duckdb.py). ``deleted`` marks a physical
                -- tombstone: the row is kept (not removed) so its
                -- last_stream_version remains resolvable by a batch reader.
                -- ``last_stream_version`` is the stream_heads version at which
                -- this row was last written; next_app_batch uses it to defer
                -- a key to a later batch when a newer write has already
                -- superseded it within the same snapshot.
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                last_stream_version BIGINT NOT NULL DEFAULT 0,
                CHECK (numeric_value IS NULL OR text_value IS NULL),
                UNIQUE (ref_id, ts)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                ref_uri   VARCHAR PRIMARY KEY,
                point_uri VARCHAR,
                source_id VARCHAR NOT NULL,
                ref_name  VARCHAR NOT NULL,
                value_kind VARCHAR NOT NULL DEFAULT 'text'
            )
            """,
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_source_ref_name ON {STREAMS_TABLE} (source_id, ref_name)",
            f"CREATE INDEX IF NOT EXISTS idx_streams_point_uri ON {STREAMS_TABLE} (point_uri)",
            f"""
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
            JOIN {REF_IDS_TABLE} AS r
                ON t.ref_id = r.ref_id
            LEFT JOIN {STREAMS_TABLE} AS s
                ON r.ref_uri = s.ref_uri
            WHERE NOT t.deleted
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
            *CONTINUOUS_BATCH_DDL,
        ]
        with self._lock, timed_debug(logger, "ensure_table"), self._own_conn() as conn:
            for stmt in stmts:
                conn.execute(stmt)
        return "ok"

    # ---- timeseries mutations ----

    def upsert_rows(
        self,
        ref_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "text",
    ) -> int:
        rows_list = list(rows)
        logger.debug("upsert_rows ref_uri=%s rows=%d kind=%s", ref_uri, len(rows_list), value_kind)
        if not rows_list:
            return 0
        return self.bulk_insert_polars(self._rows_frame(ref_uri, rows_list, value_kind))

    @staticmethod
    def _rows_frame(ref_uri: str, rows_list: list[tuple[datetime, Any]], value_kind: str) -> pl.DataFrame:
        n = len(rows_list)
        return pl.DataFrame(
            {
                "ref_uri": pl.Series("ref_uri", [ref_uri] * n, dtype=pl.Utf8),
                "ts": pl.Series("ts", [ts for ts, _ in rows_list], dtype=pl.Datetime("us", "UTC")),
                "value": typed_value_series([value for _, value in rows_list]),
                "value_kind": pl.Series("value_kind", [value_kind] * n, dtype=pl.Utf8),
            }
        )

    def replace_rows(
        self,
        ref_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "text",
    ) -> int:
        rows_list = list(rows)
        logger.debug("replace_rows ref_uri=%s new_rows=%d kind=%s", ref_uri, len(rows_list), value_kind)
        df = (
            self._prepare_frame(self._rows_frame(ref_uri, rows_list, value_kind))
            if rows_list
            else pl.DataFrame()
        )
        # One transaction for delete + insert: a failure leaves the old rows intact.
        with self._lock, timed_debug(logger, "replace_rows ref_uri=%s rows=%d", ref_uri, len(df)), self._write_conn() as conn:
            conn.execute(
                f"""
                DELETE FROM {TIMESERIES_TABLE}
                WHERE ref_id = (SELECT ref_id FROM {REF_IDS_TABLE} WHERE ref_uri = ?)
                """,
                [ref_uri],
            )
            if not df.is_empty():
                self._insert_frame(conn, df)
        return len(df)

    def bulk_insert_polars(self, df: pl.DataFrame) -> int:
        """Bulk-insert a Polars DataFrame with canonical or split value columns.

        Uses DuckDB's native Polars bridge — no ADBC required.
        """
        if df.is_empty():
            logger.debug("bulk_insert_polars: empty DataFrame, skipping")
            return 0
        in_rows = len(df)
        with timed_debug(logger, "bulk_insert_polars prepare/dedupe rows=%d", in_rows):
            df = self._prepare_frame(df)
        logger.debug("bulk_insert_polars: %d rows after dedupe (was %d)", len(df), in_rows)
        with self._lock, timed_debug(logger, "bulk_insert_polars DELETE+INSERT rows=%d", len(df)), self._write_conn() as conn:
            self._insert_frame(conn, df)
        return len(df)

    @staticmethod
    def _prepare_frame(df: pl.DataFrame) -> pl.DataFrame:
        """Split value columns, normalise ts to naive UTC, dedupe on (ref_uri, ts)."""
        df = prepare_value_columns(df).with_columns(
            pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        )
        return df.unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)

    @staticmethod
    def _insert_frame(conn, df: pl.DataFrame) -> None:
        """Upsert a prepared frame on *conn*: delete colliding (ref, ts) rows, insert.

        The frame carries ref_uri strings; ids are assigned for unseen uris and
        the rows keyed by ref_id, all inside the caller's transaction.
        """
        conn.register("_acquirium_incoming_timeseries", df)
        try:
            conn.execute(
                f"""
                INSERT INTO {REF_IDS_TABLE} (ref_uri)
                SELECT DISTINCT ref_uri FROM _acquirium_incoming_timeseries
                ON CONFLICT (ref_uri) DO NOTHING
                """
            )
            conn.execute(
                f"""
                DELETE FROM {TIMESERIES_TABLE}
                USING (
                    SELECT r.ref_id, i.ts
                    FROM _acquirium_incoming_timeseries AS i
                    JOIN {REF_IDS_TABLE} AS r USING (ref_uri)
                ) AS incoming
                WHERE {TIMESERIES_TABLE}.ref_id = incoming.ref_id
                  AND {TIMESERIES_TABLE}.ts = incoming.ts
                """
            )
            conn.execute(
                f"""
                INSERT INTO {TIMESERIES_TABLE} (ref_id, ts, numeric_value, text_value)
                SELECT r.ref_id, i.ts, i.numeric_value, i.text_value
                FROM _acquirium_incoming_timeseries AS i
                JOIN {REF_IDS_TABLE} AS r USING (ref_uri)
                """
            )
        finally:
            conn.unregister("_acquirium_incoming_timeseries")

    # ---- stream reference registry ----

    def ensure_stream_ref(
        self,
        point_uri: str | None,
        source_id: str,
        ref_name: str,
        ref_uri: str | None = None,
        value_kind: str = "text",
    ) -> str:
        """Register a stream reference and return its canonical ref URI."""
        if ref_uri is None:
            ref_uri = compute_ref_uri(source_id, ref_name)
        ref_uri_str = str(ref_uri)
        value_kind = normalize_value_kind(value_kind)
        # logger.debug(
        #     "ensure_stream_ref source=%s ref_name=%s point_uri=%s kind=%s",
        #     source_id, ref_name, point_uri, value_kind,
        # )
        with self._lock, self._write_conn() as conn:
            conn.execute(
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

    def ensure_stream_refs(
        self,
        refs: Iterable[tuple[str | None, str, str, str | None, str]],
    ) -> list[str]:
        """Batch form of :meth:`ensure_stream_ref`: one statement for the lot.

        Each entry is ``(point_uri, source_id, ref_name, ref_uri, value_kind)``.
        Upserting from a registered frame rather than one statement per row is
        what makes this worth having: 1000 refs take ~16ms this way versus ~3.6s
        row-by-row.
        """
        prepared: dict[str, list[Any]] = {}
        for point_uri, source_id, ref_name, ref_uri, value_kind in refs:
            key = str(ref_uri if ref_uri is not None else compute_ref_uri(source_id, ref_name))
            # Last occurrence wins, matching a sequence of individual upserts.
            # DuckDB's ON CONFLICT DO UPDATE rejects a source naming the same row
            # twice, and the caller's SPARQL can legitimately yield repeats.
            prepared[key] = [key, point_uri, source_id, ref_name, normalize_value_kind(value_kind)]
        if not prepared:
            return []
        df = pl.DataFrame(
            list(prepared.values()),
            schema=["ref_uri", "point_uri", "source_id", "ref_name", "value_kind"],
            orient="row",
        )
        with self._lock, timed_debug(logger, "ensure_stream_refs rows=%d", len(df)), self._write_conn() as conn:
            conn.register("_acquirium_incoming_refs", df)
            try:
                conn.execute(
                    f"""
                    INSERT INTO {STREAMS_TABLE} (ref_uri, point_uri, source_id, ref_name, value_kind)
                    SELECT ref_uri, point_uri, source_id, ref_name, value_kind
                    FROM _acquirium_incoming_refs
                    ON CONFLICT (ref_uri) DO UPDATE SET
                        source_id  = excluded.source_id,
                        ref_name   = excluded.ref_name,
                        point_uri  = COALESCE(excluded.point_uri, {STREAMS_TABLE}.point_uri),
                        value_kind = excluded.value_kind
                    """
                )
            finally:
                conn.unregister("_acquirium_incoming_refs")
        return list(prepared.keys())

    def resolve_storage_key(self, point_uri: str) -> str:
        """Return the storage ref URI for point_uri, or point_uri itself if unregistered."""
        with self._own_conn() as conn:
            row = conn.execute(
                f"SELECT ref_uri FROM {STREAMS_TABLE} WHERE point_uri = ?", [point_uri]
            ).fetchone()
        return point_uri if row is None else row[0]

    def resolve_storage_keys(self, point_uris: list[str]) -> dict[str, str]:
        """Batch-resolve point_uris to storage keys."""
        if not point_uris:
            return {}
        placeholders = ", ".join("?" * len(point_uris))
        with self._own_conn() as conn, timed_debug(logger, "resolve_storage_keys n=%d", len(point_uris)):
            d = conn.execute(
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
                logger, "timeseries ref_uri=%s start=%s end=%s limit=%s order=%s mode=%s",
                ref_uri, start, end, limit, order, mode,
            ):
                ref_id_row = conn.execute(
                    f"SELECT ref_id FROM {REF_IDS_TABLE} WHERE ref_uri = ?", [ref_uri]
                ).fetchone()
                if ref_id_row is None:
                    # Never written to: no id, no rows.
                    return

                # Normal reads hide tombstones -- see continuous_batch.md's
                # "Canonical values and version heads": a deleted row is kept
                # physically (its last_stream_version stays resolvable by a
                # batch reader) but is invisible to this API.
                clauses = ["ref_id = ?", "NOT deleted"]
                params: list[Any] = [ref_id_row[0]]
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

                value_kind = self._stream_value_kind(conn, ref_uri)
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
                        ).fetchone()
                        if has_numeric and has_text:
                            resolved = "coalesce"
                        else:
                            resolved = "numeric" if has_numeric else "text"
                # Streamed, not materialized: batches are pulled as the caller
                # consumes them, so an unbounded range need not fit in memory.
                reader = conn.execute(query, params).to_arrow_reader(batch_size)
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
                uri_col = pa.array([ref_uri] * len(batch), type=pa.string())
                yield pa.record_batch([ts_col, val_col, uri_col], names=["ts", "value", "uri"])
        finally:
            # Runs on early caller exit (GeneratorExit) as well as on exhaustion.
            conn.close()
            logger.debug("timeseries: ref_uri=%s rows=%d (batch_size=%d)", ref_uri, rows, batch_size)

    def timeseries_info(self, ref_uri: str) -> TimeseriesInfo:
        with self._own_conn() as conn, timed_debug(logger, "timeseries_info ref_uri=%s", ref_uri):
            row = conn.execute(
                f"""
                SELECT COUNT(*), MIN(ts), MAX(ts)
                FROM {TIMESERIES_TABLE} AS t
                JOIN {REF_IDS_TABLE} AS r USING (ref_id)
                WHERE r.ref_uri = ? AND NOT t.deleted
                """,
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
        with self._own_conn() as conn, timed_debug(logger, "timeseries_info_batch n=%d", len(ref_uris)):
            d = conn.execute(
                f"""
                SELECT r.ref_uri,
                       COUNT(*)   AS row_count,
                       MIN(ts)    AS earliest,
                       MAX(ts)    AS latest
                FROM {TIMESERIES_TABLE} AS t
                JOIN {REF_IDS_TABLE} AS r USING (ref_id)
                WHERE r.ref_uri IN ({placeholders}) AND NOT t.deleted
                GROUP BY r.ref_uri
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
        logger.debug("insert_log point_uri=%s ts=%s", log.point_uri, log.timestamp)
        with self._lock, self._write_conn() as conn:
            conn.execute(
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
            with self._own_conn() as conn, timed_debug(logger, "query_logs point_uri=%s clauses=%d", point_uri, len(clauses)):
                tbl = conn.execute(query, params).to_arrow_table()
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
        logger.debug("delete_logs point_uri=%s", point_uri)
        with self._lock, self._write_conn() as conn:
            conn.execute(
                f"DELETE FROM {LOGS_TABLE} WHERE point_uri = ?", [point_uri]
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
            tbl = conn.execute(query).to_arrow_table()
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

    def stream_value_kind(self, ref_uri: str) -> str | None:
        with self._own_conn() as conn:
            return self._stream_value_kind(conn, ref_uri)

    def _stream_value_kind(self, conn, ref_uri: str) -> str | None:
        """Look up a stream's value kind on *conn* — the caller's connection, so
        a streaming read can resolve it without opening a second connection."""
        row = conn.execute(
            f"SELECT value_kind FROM {STREAMS_TABLE} WHERE ref_uri = ?",
            [ref_uri],
        ).fetchone()
        return row[0] if row else None
