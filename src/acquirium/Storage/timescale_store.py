from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Iterator
import hashlib
import random
import string

import psycopg
from psycopg import sql
from psycopg.types.json import Json

from acquirium.internals.models import Order, TimeseriesInfo, TimeInterval, LogEntry, TimeIntervalModel, compute_ref_uri
from acquirium.Storage.base import TimeseriesStore
import logging
import pyarrow as pa
import polars as pl
from rdflib import URIRef
from acquirium.Storage.values import (
    normalize_value_kind,
    normalize_value_mode,
    prepare_value_columns,
    split_value,
)
from acquirium.internals._log import timed_debug

logger = logging.getLogger(__name__)

TIMESERIES_TABLE = "timeseries"
STREAMS_TABLE = "streams"
LOGS_TABLE = "logs"
TIMESERIES_STREAMS_VIEW = "timeseries_streams"

# Continuous-batch tables (continuous_batch.md; DDL details in
# continuous_batch_plan.md Phase 1a). Keyed directly by ``ref_uri`` text,
# unlike the DuckDB backend's integer ``ref_id`` -- see
# ``Storage/continuous/postgres.py``.
STREAM_HEADS_TABLE = "stream_heads"
STREAM_PUBLICATIONS_TABLE = "stream_publications"
STREAM_CHANGE_KEYS_TABLE = "stream_change_keys"
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
    f"""
    CREATE TABLE IF NOT EXISTS {STREAM_HEADS_TABLE} (
        ref_uri TEXT PRIMARY KEY,
        current_version BIGINT NOT NULL,
        retained_from_version BIGINT NOT NULL
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STREAM_PUBLICATIONS_TABLE} (
        publication_seq BIGSERIAL PRIMARY KEY,
        publication_id TEXT UNIQUE NOT NULL,
        payload_hash TEXT NOT NULL,
        row_count BIGINT NOT NULL,
        versions_json JSONB NOT NULL,
        committed_at TIMESTAMPTZ NOT NULL
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STREAM_CHANGE_KEYS_TABLE} (
        publication_seq BIGINT NOT NULL,
        publication_row INTEGER NOT NULL,
        ref_uri TEXT NOT NULL,
        stream_version BIGINT NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (publication_seq, publication_row)
    );
    """,
    f"CREATE INDEX IF NOT EXISTS idx_stream_change_keys_ref_version ON {STREAM_CHANGE_KEYS_TABLE} (ref_uri, stream_version);",
    f"""
    CREATE TABLE IF NOT EXISTS {APP_RUNTIME_TABLE} (
        app_id TEXT PRIMARY KEY,
        generation BIGINT NOT NULL,
        status TEXT NOT NULL,
        topology_version BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_SUBSCRIPTIONS_TABLE} (
        app_id TEXT NOT NULL,
        generation BIGINT NOT NULL,
        ref_uri TEXT NOT NULL,
        stream_version BIGINT NOT NULL,
        PRIMARY KEY (app_id, generation, ref_uri)
    );
    """,
    f"CREATE INDEX IF NOT EXISTS idx_app_subscriptions_ref ON {APP_SUBSCRIPTIONS_TABLE} (ref_uri);",
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BATCH_COMMITS_TABLE} (
        app_id TEXT NOT NULL,
        generation BIGINT NOT NULL,
        batch_id TEXT NOT NULL,
        batch_kind TEXT NOT NULL,
        rows_inserted BIGINT NOT NULL,
        output_versions_json JSONB NOT NULL,
        committed_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (app_id, generation, batch_id)
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BATCH_INPUTS_TABLE} (
        app_id TEXT NOT NULL,
        generation BIGINT NOT NULL,
        batch_id TEXT NOT NULL,
        ref_uri TEXT NOT NULL,
        from_version BIGINT NOT NULL,
        to_version BIGINT NOT NULL,
        PRIMARY KEY (app_id, generation, batch_id, ref_uri)
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAPS_TABLE} (
        bootstrap_id TEXT PRIMARY KEY,
        app_id TEXT NOT NULL,
        generation BIGINT NOT NULL,
        status TEXT NOT NULL,
        next_ordinal BIGINT NOT NULL
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAP_STREAMS_TABLE} (
        bootstrap_id TEXT NOT NULL,
        ref_uri TEXT NOT NULL,
        stream_version BIGINT NOT NULL,
        PRIMARY KEY (bootstrap_id, ref_uri)
    );
    """,
    # Declared output targets for this bootstrap, captured at begin_bootstrap
    # time. finalize_bootstrap anti-joins this list against what actually
    # landed in app_bootstrap_outputs to tombstone an output stream a
    # narrower/changed selector no longer produces. Not part of the design
    # doc's logical schema list; added because finalize's reconciliation step
    # needs it.
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE} (
        bootstrap_id TEXT NOT NULL,
        output_ref_uri TEXT NOT NULL,
        PRIMARY KEY (bootstrap_id, output_ref_uri)
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAP_ROWS_TABLE} (
        bootstrap_id TEXT NOT NULL,
        ordinal BIGINT NOT NULL,
        ref_uri TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        numeric_value DOUBLE PRECISION,
        text_value TEXT,
        PRIMARY KEY (bootstrap_id, ordinal)
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_BOOTSTRAP_OUTPUTS_TABLE} (
        bootstrap_id TEXT NOT NULL,
        ordinal BIGINT NOT NULL,
        output_ref_uri TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        operation TEXT NOT NULL,
        numeric_value DOUBLE PRECISION,
        text_value TEXT,
        PRIMARY KEY (bootstrap_id, ordinal)
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {APP_WEBHOOK_INTENTS_TABLE} (
        app_id TEXT NOT NULL,
        generation BIGINT NOT NULL,
        batch_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        url TEXT NOT NULL,
        payload_json JSONB NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMPTZ,
        PRIMARY KEY (app_id, generation, batch_id, seq)
    );
    """,
]


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
        # default autocommit so reads don't hold open transactions; explicit begin toggles off
        logger.debug("TimescaleStore.__init__: connecting (recreate=%s)", recreate)
        with timed_debug(logger, "psycopg.connect"):
            self.conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=connect_timeout)
        self._in_tx = False
        if recreate:
            logger.debug("TimescaleStore.__init__: dropping existing tables/views")
            with self.conn.cursor() as cur:
                cur.execute(sql.SQL("DROP VIEW IF EXISTS {} CASCADE").format(sql.Identifier(TIMESERIES_STREAMS_VIEW)))
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
                ):
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(tbl)))
        self.ensure_table()
        logger.debug("TimescaleStore.__init__: ready")

    # -------------------- table management --------------------
    def ensure_table(self) -> str:
        with timed_debug(logger, "ensure_table"), self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TIMESERIES_TABLE} (
                    ref_uri TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    numeric_value DOUBLE PRECISION,
                    text_value TEXT,
                    -- Continuous-batch columns (see continuous_batch.md and
                    -- Storage/continuous/postgres.py). ``deleted`` marks a
                    -- physical tombstone, kept rather than removed so its
                    -- last_stream_version stays resolvable by a batch reader.
                    -- ``last_stream_version`` is the stream_heads version at
                    -- which this row was last written; next_app_batch uses it
                    -- to defer a key already superseded within the snapshot.
                    deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    last_stream_version BIGINT NOT NULL DEFAULT 0,
                    CHECK (numeric_value IS NULL OR text_value IS NULL)
                );
                """
            )
            # Create hypertable before enabling Timescale features. We target
            # new Acquirium-managed stores here; older point_uri/handle schemas
            # should be recreated rather than migrated in-place.
            cur.execute(
                sql.SQL(
                    "SELECT create_hypertable(%s, %s, if_not_exists => TRUE);"
                ),
                (TIMESERIES_TABLE, "ts"),
            )
            # Unique index supports idempotent upserts and scans by stream/time.
            # Timescale unique indexes on hypertables must include the time
            # partition column; (ref_uri, ts) satisfies that requirement.
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_timeseries_ref_ts_unique ON {TIMESERIES_TABLE} (ref_uri, ts);"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_timeseries_numeric_ref_ts ON {TIMESERIES_TABLE} (ref_uri, ts) WHERE numeric_value IS NOT NULL;"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_timeseries_text_ref_ts ON {TIMESERIES_TABLE} (ref_uri, ts) WHERE text_value IS NOT NULL;"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_timeseries_numeric_value ON {TIMESERIES_TABLE} (ref_uri, numeric_value) WHERE numeric_value IS NOT NULL;"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_timeseries_text_value ON {TIMESERIES_TABLE} (ref_uri, text_value) WHERE text_value IS NOT NULL;"
            )
            # Compression is intentionally NOT enabled in v1 (see
            # continuous_batch_plan.md Decision 3 / Finding 1): the
            # continuous-batch protocol upserts corrections and updates
            # last_stream_version on rows of any age, which compressed
            # TimescaleDB chunks restrict. Revisit together with a retention
            # policy once corrections have their own aging story.
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                    ref_uri TEXT PRIMARY KEY,
                    point_uri TEXT,
                    source_id TEXT NOT NULL,
                    ref_name TEXT NOT NULL,
                    value_kind TEXT NOT NULL DEFAULT 'text'
                );
                """
            )
            cur.execute(
                f"ALTER TABLE {STREAMS_TABLE} ALTER COLUMN point_uri DROP NOT NULL;"
            )
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_source_ref_name ON {STREAMS_TABLE} (source_id, ref_name);"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_streams_point_uri ON {STREAMS_TABLE} (point_uri) WHERE point_uri IS NOT NULL;"
            )
            cur.execute(
                f"""
                CREATE OR REPLACE VIEW {TIMESERIES_STREAMS_VIEW} AS
                SELECT
                    t.ref_uri,
                    s.point_uri,
                    s.source_id,
                    s.ref_name,
                    COALESCE(s.value_kind, 'text') AS value_kind,
                    t.ts,
                    t.numeric_value AS value_numeric,
                    t.text_value AS value_text
                FROM {TIMESERIES_TABLE} AS t
                LEFT JOIN {STREAMS_TABLE} AS s
                    ON t.ref_uri = s.ref_uri
                WHERE NOT t.deleted;
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
            for stmt in CONTINUOUS_BATCH_DDL:
                cur.execute(stmt)
        if not self._in_tx:
            self.conn.commit()
        return TIMESERIES_TABLE

    # -------------------- mutations --------------------
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
        payload = [
            (ref_uri, self._to_utc(ts), *split_value(val, value_kind))
            for ts, val in rows_list
        ]
        with timed_debug(logger, "upsert_rows INSERT n=%d", len(payload)), self.conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TIMESERIES_TABLE} (ref_uri, ts, numeric_value, text_value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ref_uri, ts) DO UPDATE SET
                    numeric_value = EXCLUDED.numeric_value,
                    text_value = EXCLUDED.text_value,
                    deleted = FALSE
                """,
                payload,
            )
        logger.debug("acquirium: upserted %d rows into %s", len(rows_list), TIMESERIES_TABLE)
        return len(rows_list)

    def replace_rows(
        self,
        ref_uri: str,
        rows: Iterable[tuple[datetime, Any]],
        *,
        value_kind: str = "text",
    ) -> int:
        logger.debug("replace_rows ref_uri=%s kind=%s", ref_uri, value_kind)
        with timed_debug(logger, "replace_rows DELETE ref_uri=%s", ref_uri), self.conn.cursor() as cur:
            cur.execute(sql.SQL("DELETE FROM {} WHERE ref_uri=%s").format(sql.Identifier(TIMESERIES_TABLE)), [ref_uri])
        return self.upsert_rows(ref_uri, rows, value_kind=value_kind)

    def bulk_insert_polars(self, df: pl.DataFrame) -> int:
        # Using polars to write to database via ADBC
        # Input df format: columns ["ref_uri", "ts", "value"] or already split
        # columns ["ref_uri", "ts", "numeric_value", "text_value"].
        if df.is_empty():
            logger.debug("bulk_insert_polars: empty DataFrame, skipping")
            return 0
        in_rows = len(df)
        with timed_debug(logger, "bulk_insert_polars prepare/dedupe rows=%d", in_rows):
            df = prepare_value_columns(df)
            df = df.unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
        deduped = len(df)
        logger.debug("bulk_insert_polars: %d rows after dedupe (was %d)", deduped, in_rows)
        random_string = ''.join(random.choice(string.ascii_lowercase) for _ in range(15))
        with self.conn.cursor() as cur:
            cur.execute(
                f"""DROP TABLE IF EXISTS {random_string};"""
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {random_string} (
                ref_uri TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                numeric_value DOUBLE PRECISION,
                text_value TEXT
            );""")
        try:
            with timed_debug(logger, "bulk_insert_polars ADBC write rows=%d", deduped):
                rows_affected = df.write_database(
                    table_name=random_string,
                    connection=self.dsn,
                    engine="adbc",
                    if_table_exists="append" # Use 'replace' to drop/create the table
                )
            with timed_debug(logger, "bulk_insert_polars merge into %s", TIMESERIES_TABLE), self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {TIMESERIES_TABLE} (ref_uri, ts, numeric_value, text_value)
                    SELECT ref_uri, ts, numeric_value, text_value FROM {random_string}
                    ON CONFLICT (ref_uri, ts) DO UPDATE SET
                        numeric_value = EXCLUDED.numeric_value,
                        text_value = EXCLUDED.text_value,
                        deleted = FALSE;
                    """
                )
            logger.info(f"acquirium: bulk inserted {rows_affected} rows into {TIMESERIES_TABLE}")
            return rows_affected
        except Exception:
            logger.exception("acquirium: bulk insert into %s failed", TIMESERIES_TABLE)
            raise
        finally:
            with self.conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {random_string};")

    # -------------------- stream references --------------------
    def ensure_stream_ref(
        self,
        point_uri: str | None,
        source_id: str,
        ref_name: str,
        ref_uri: URIRef | None = None,
        value_kind: str = "text",
    ) -> URIRef:
        """Register a stream reference in the streams table.

        The ref URI is computed deterministically from (source_id, ref_name) via
        :func:`compute_ref_uri`, so two sources with the same ref_name never
        produce the same storage key. The ref URI is also used as the
        TimescaleDB row key for the stream's data. Pass a precomputed ref_uri
        to avoid recomputing it when already available.

        Returns the ref URI.
        """
        if ref_uri is None:
            ref_uri = compute_ref_uri(source_id, ref_name)
        value_kind = normalize_value_kind(value_kind)
        # logger.debug(
        #     "ensure_stream_ref source=%s ref_name=%s point_uri=%s kind=%s",
        #     source_id, ref_name, point_uri, value_kind,
        # )
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {STREAMS_TABLE} (ref_uri, point_uri, source_id, ref_name, value_kind)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ref_uri) DO UPDATE
                    SET
                        point_uri = COALESCE(EXCLUDED.point_uri, {STREAMS_TABLE}.point_uri),
                        source_id = EXCLUDED.source_id,
                        ref_name = EXCLUDED.ref_name,
                        value_kind = EXCLUDED.value_kind
                """,
                (str(ref_uri), point_uri, source_id, ref_name, value_kind),
            )
        return ref_uri

    def ensure_stream_refs(
        self,
        refs: Iterable[tuple[str | None, str, str, str | None, str]],
    ) -> list[str]:
        """Batch form of :meth:`ensure_stream_ref`.

        Each entry is ``(point_uri, source_id, ref_name, ref_uri, value_kind)``.

        COPY into a staging table and upsert from it in one statement, rather
        than a statement per row: COPY streams the whole batch over the wire in
        a single command, which beats even a pipelined executemany.
        """
        prepared: dict[str, tuple[Any, ...]] = {}
        for point_uri, source_id, ref_name, ref_uri, value_kind in refs:
            key = str(ref_uri if ref_uri is not None else compute_ref_uri(source_id, ref_name))
            # Last occurrence wins, matching a sequence of individual upserts.
            # ON CONFLICT DO UPDATE also refuses a source naming the same row
            # twice, and the caller's SPARQL can legitimately yield repeats.
            prepared[key] = (key, point_uri, source_id, ref_name, normalize_value_kind(value_kind))
        if not prepared:
            return []
        cols = "(ref_uri, point_uri, source_id, ref_name, value_kind)"
        # The connection runs autocommit, so an explicit transaction is what keeps
        # the staging table alive from COPY through to the upsert (and drops it).
        with timed_debug(logger, "ensure_stream_refs rows=%d", len(prepared)), self.conn.transaction():
            with self.conn.cursor() as cur:
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
                    SELECT ref_uri, point_uri, source_id, ref_name, value_kind
                    FROM _acquirium_incoming_refs
                    ON CONFLICT (ref_uri) DO UPDATE
                        SET
                            point_uri = COALESCE(EXCLUDED.point_uri, {STREAMS_TABLE}.point_uri),
                            source_id = EXCLUDED.source_id,
                            ref_name = EXCLUDED.ref_name,
                            value_kind = EXCLUDED.value_kind
                    """
                )
        return list(prepared.keys())

    def resolve_storage_key(self, point_uri: str) -> str:
        """Return the storage key (ref URI) for a point_uri, or point_uri itself if not registered.

        Streams inserted via insert_timeseries are stored under their ref URI.
        This resolves the semantic URI → ref URI so reads find the right rows.
        Falls back to the URI itself for data inserted directly (e.g. bulk CSV ingest).
        """
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT ref_uri FROM {STREAMS_TABLE} WHERE point_uri = %s",
                (point_uri,),
            )
            row = cur.fetchone()
            return row[0] if row else point_uri

    def resolve_storage_keys(self, point_uris: list[str]) -> dict[str, str]:
        """Batch-resolve point_uris to storage keys in a single query.

        Returns a mapping of point_uri → ref_uri (or point_uri itself for
        unregistered URIs, preserving the single-URI fallback behaviour).
        """
        if not point_uris:
            return {}
        with timed_debug(logger, "resolve_storage_keys n=%d", len(point_uris)), self.conn.cursor() as cur:
            cur.execute(
                f"SELECT point_uri, ref_uri FROM {STREAMS_TABLE} WHERE point_uri = ANY(%s)",
                (point_uris,),
            )
            rows = cur.fetchall()
        registered = {row[0]: row[1] for row in rows}
        return {uri: registered.get(uri, uri) for uri in point_uris}

    # -------------------- queries --------------------
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
        '''
        Returns an iterator over the time series data for the given ref URI.
        '''
        mode = normalize_value_mode(value_mode)
        # Normal reads hide tombstones -- see continuous_batch.md's "Canonical
        # values and version heads".
        clauses = ["ref_uri = %s", "NOT deleted"]
        params: list[Any] = [ref_uri]

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

        value_kind = self.stream_value_kind(str(ref_uri))
        logger.debug(
            "timeseries ref_uri=%s start=%s end=%s limit=%s order=%s mode=%s",
            ref_uri, start, end, limit, order, mode,
        )
        with self.conn.cursor() as cur:
            with timed_debug(logger, "timeseries cur.execute ref_uri=%s", ref_uri):
                cur.execute(query, params)

            total = 0
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    logger.debug("timeseries ref_uri=%s yielded total rows=%d", ref_uri, total)
                    break
                total += len(rows)

                ts_col = [r[0] for r in rows]
                numeric_col = [r[1] for r in rows]
                text_col = [r[2] for r in rows]
                ref_uri_col = [ref_uri] * len(ts_col)
                if not ts_col or not ref_uri_col:
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
                        pa.array(ref_uri_col, type=pa.string()),
                    ],
                    names=["ts", "value", "uri"],
                )
                yield batch

    def timeseries_info(self, ref_uri: str) -> TimeseriesInfo:
        with timed_debug(logger, "timeseries_info ref_uri=%s", ref_uri), self.conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*), MIN(ts), MAX(ts) FROM {TIMESERIES_TABLE} WHERE ref_uri=%s AND NOT deleted",
                (ref_uri,),
            )
            cnt, earliest, latest = cur.fetchone()
        return TimeseriesInfo(table=TIMESERIES_TABLE, row_count=cnt, earliest=earliest, latest=latest)

    def timeseries_info_batch(self, ref_uris: list[str]) -> dict[str, TimeseriesInfo]:
        """Return stats (row_count, earliest, latest) for multiple ref URIs in one query."""
        if not ref_uris:
            return {}
        with timed_debug(logger, "timeseries_info_batch n=%d", len(ref_uris)), self.conn.cursor() as cur:
            cur.execute(
                f"SELECT ref_uri, COUNT(*), MIN(ts), MAX(ts) FROM {TIMESERIES_TABLE} WHERE ref_uri = ANY(%s) AND NOT deleted GROUP BY ref_uri",
                (ref_uris,),
            )
            rows = cur.fetchall()
        result: dict[str, TimeseriesInfo] = {}
        for uri, cnt, earliest, latest in rows:
            result[uri] = TimeseriesInfo(table=TIMESERIES_TABLE, row_count=cnt, earliest=earliest, latest=latest)
        for uri in ref_uris:
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
        
        logger.debug("insert_log point_uri=%s ts=%s", point_uri, ts)
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
            with timed_debug(logger, "query_logs point_uri=%s clauses=%d", point_uri, len(clauses)), self.conn.cursor() as cur:
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
        logger.debug("delete_logs point_uri=%s", point_uri)
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {LOGS_TABLE} WHERE point_uri=%s", (point_uri,))
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

    def stream_value_kind(self, ref_uri: str) -> str | None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT value_kind FROM {STREAMS_TABLE} WHERE ref_uri = %s",
                (ref_uri,),
            )
            row = cur.fetchone()
        return row[0] if row else None
