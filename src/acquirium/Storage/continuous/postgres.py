"""PostgreSQL/TimescaleDB implementation of :class:`ContinuousStore`.

Unlike the DuckDB backend, this one owns a dedicated
``psycopg_pool.ConnectionPool`` (not :class:`~acquirium.Storage.timescale_store.TimescaleStore`'s
single shared connection) because multiple server workers, the router, and
the compactor may all issue continuous-batch transactions concurrently
against Postgres -- unlike DuckDB, which this codebase restricts to one
writer per process. It targets the same database a ``TimescaleStore``
instance uses and is keyed directly by ``ref_uri`` text (no id-resolution
layer, unlike the DuckDB backend's integer ``ref_id``).

Because multiple real connections can genuinely interleave, this backend
performs the explicit "sorted head locking" continuous_batch.md describes:
every publication (including an app's output commit and a bootstrap's
finalize replacement) takes its ``stream_heads`` row locks via
``SELECT ... ORDER BY ref_uri FOR UPDATE`` so two overlapping writers always
acquire shared rows in the same relative order and cannot deadlock.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import polars as pl
import pyarrow as pa
from psycopg_pool import ConnectionPool

from acquirium.Storage.continuous import ids
from acquirium.Storage.continuous.types import (
    AppBatch,
    AppRuntimeRow,
    BatchIdMismatch,
    BatchInputRange,
    BootstrapPage,
    BootstrapState,
    CommitRequest,
    CommitResult,
    CompactReport,
    GenerationMismatch,
    PublicationConflict,
    PublicationReceipt,
    PublicationRequest,
)
from acquirium.Storage.timescale_store import (
    APP_BATCH_COMMITS_TABLE,
    APP_BATCH_INPUTS_TABLE,
    APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE,
    APP_BOOTSTRAP_OUTPUTS_TABLE,
    APP_BOOTSTRAP_ROWS_TABLE,
    APP_BOOTSTRAP_STREAMS_TABLE,
    APP_BOOTSTRAPS_TABLE,
    APP_RUNTIME_TABLE,
    APP_SUBSCRIPTIONS_TABLE,
    APP_WEBHOOK_INTENTS_TABLE,
    STREAM_CHANGE_KEYS_TABLE,
    STREAM_HEADS_TABLE,
    STREAM_PUBLICATIONS_TABLE,
    TIMESERIES_TABLE,
)

logger = logging.getLogger("acquirium.storage.continuous.postgres")


class ContinuousPostgres:
    """``ContinuousStore`` for a PostgreSQL/TimescaleDB-backed server.

    See ``continuous_batch_plan.md`` Phase 1c for the transaction-by-
    transaction algorithm each public method follows; this docstring only
    notes Postgres-specific choices not already covered there.
    """

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10):
        self._pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)

    def close(self) -> None:
        self._pool.close()

    # ------------------------------------------------------------------
    # publish
    # ------------------------------------------------------------------

    def publish(self, req: PublicationRequest) -> PublicationReceipt:
        with self._pool.connection() as conn, conn.transaction():
            with conn.cursor() as cur:
                return self._apply_publication(cur, req.publication_id, req.mutations)

    def _apply_publication(self, cur, publication_id: str, mutations: pa.Table) -> PublicationReceipt:
        """Apply one publication on an already-open transaction's cursor.

        Factored out of :meth:`publish` so :meth:`commit_app_batch` and
        :meth:`finalize_bootstrap` can publish their own outputs inside their
        own transaction. Implements the publication protocol's ten steps
        (continuous_batch.md): normalize, hash, check-or-apply, lock+advance
        heads (sorted, ``FOR UPDATE``), upsert/tombstone canonical rows,
        record one changed-key row per normalized mutation, and store the
        receipt.
        """
        p_hash = ids.payload_hash(mutations)
        cur.execute(
            f"SELECT payload_hash, row_count, versions_json FROM {STREAM_PUBLICATIONS_TABLE} WHERE publication_id = %s",
            [publication_id],
        )
        existing = cur.fetchone()
        if existing is not None:
            existing_hash, row_count, versions_json = existing
            if existing_hash != p_hash:
                raise PublicationConflict(publication_id)
            return PublicationReceipt(
                publication_id=publication_id,
                payload_hash=p_hash,
                row_count=row_count,
                versions=versions_json if isinstance(versions_json, dict) else json.loads(versions_json),
                deduplicated=True,
            )

        normalized = ids.normalize_mutations(mutations)
        if normalized.num_rows == 0:
            raise ValueError(f"publication {publication_id!r} has no mutation rows")

        df = pl.from_arrow(normalized)
        ref_uris = sorted(df["ref_uri"].unique().to_list())

        cur.execute(
            f"""
            INSERT INTO {STREAM_HEADS_TABLE} (ref_uri, current_version, retained_from_version)
            SELECT unnest(%s::text[]), 0, 0
            ON CONFLICT (ref_uri) DO NOTHING
            """,
            [ref_uris],
        )
        # Sorted ORDER BY ... FOR UPDATE: two overlapping writers always
        # acquire shared rows in the same relative order, so they can only
        # block on each other, never deadlock (module docstring).
        cur.execute(
            f"SELECT ref_uri, current_version FROM {STREAM_HEADS_TABLE} WHERE ref_uri = ANY(%s::text[]) ORDER BY ref_uri FOR UPDATE",
            [ref_uris],
        )
        current_version = dict(cur.fetchall())
        new_version = {ref_uri: current_version[ref_uri] + 1 for ref_uri in ref_uris}

        cur.execute(
            f"""
            UPDATE {STREAM_HEADS_TABLE} AS h
            SET current_version = nv.new_version
            FROM (SELECT unnest(%s::text[]) AS ref_uri, unnest(%s::bigint[]) AS new_version) AS nv
            WHERE h.ref_uri = nv.ref_uri
            """,
            [list(new_version.keys()), list(new_version.values())],
        )

        df = df.with_columns(
            [
                pl.when(pl.col("operation") == "delete")
                .then(None)
                .otherwise(pl.col("numeric_value"))
                .alias("numeric_value"),
                pl.when(pl.col("operation") == "delete")
                .then(None)
                .otherwise(pl.col("text_value"))
                .alias("text_value"),
                (pl.col("operation") == "delete").alias("deleted"),
                pl.col("ref_uri").replace_strict(new_version).alias("last_stream_version"),
            ]
        )

        cur.execute(
            f"""
            INSERT INTO {TIMESERIES_TABLE} (ref_uri, ts, numeric_value, text_value, deleted, last_stream_version)
            SELECT * FROM unnest(%s::text[], %s::timestamptz[], %s::double precision[], %s::text[], %s::boolean[], %s::bigint[])
            ON CONFLICT (ref_uri, ts) DO UPDATE SET
                numeric_value = excluded.numeric_value,
                text_value = excluded.text_value,
                deleted = excluded.deleted,
                last_stream_version = excluded.last_stream_version
            """,
            [
                df["ref_uri"].to_list(),
                df["ts"].to_list(),
                df["numeric_value"].to_list(),
                df["text_value"].to_list(),
                df["deleted"].to_list(),
                df["last_stream_version"].to_list(),
            ],
        )

        row_count = df.height
        versions = {ref_uri: new_version[ref_uri] for ref_uri in ref_uris}
        cur.execute(
            f"""
            INSERT INTO {STREAM_PUBLICATIONS_TABLE} (publication_id, payload_hash, row_count, versions_json, committed_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING publication_seq
            """,
            [publication_id, p_hash, row_count, json.dumps(versions), datetime.now(timezone.utc)],
        )
        publication_seq = cur.fetchone()[0]

        cur.execute(
            f"""
            INSERT INTO {STREAM_CHANGE_KEYS_TABLE} (publication_seq, publication_row, ref_uri, stream_version, ts)
            SELECT %s, row_number() OVER (), ref_uri, stream_version, ts
            FROM unnest(%s::text[], %s::bigint[], %s::timestamptz[]) AS t(ref_uri, stream_version, ts)
            """,
            [publication_seq, df["ref_uri"].to_list(), df["last_stream_version"].to_list(), df["ts"].to_list()],
        )

        return PublicationReceipt(
            publication_id=publication_id,
            payload_hash=p_hash,
            row_count=row_count,
            versions=versions,
            deduplicated=False,
        )

    # ------------------------------------------------------------------
    # app runtime lifecycle
    # ------------------------------------------------------------------

    def register_app_runtime(self, app_id: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                f"""
                INSERT INTO {APP_RUNTIME_TABLE} (app_id, generation, status, topology_version, updated_at)
                VALUES (%s, 1, 'registered', 1, %s)
                ON CONFLICT (app_id) DO NOTHING
                """,
                [app_id, datetime.now(timezone.utc)],
            )

    def app_runtime(self, app_id: str) -> AppRuntimeRow | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT app_id, generation, status, topology_version, updated_at
                FROM {APP_RUNTIME_TABLE} WHERE app_id = %s
                """,
                [app_id],
            ).fetchone()
        if row is None:
            return None
        app_id_, generation, status, topology_version, updated_at = row
        return AppRuntimeRow(
            app_id=app_id_, generation=generation, status=status,
            topology_version=topology_version, updated_at=updated_at,
        )

    def set_app_status(self, app_id: str, status: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                f"UPDATE {APP_RUNTIME_TABLE} SET status = %s, updated_at = %s WHERE app_id = %s",
                [status, datetime.now(timezone.utc), app_id],
            )

    def reset_app(self, app_id: str) -> int:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute(
                f"SELECT generation FROM {APP_RUNTIME_TABLE} WHERE app_id = %s", [app_id]
            ).fetchone()
            if row is None:
                raise KeyError(f"app {app_id!r} has no runtime state; register it first")
            new_generation = row[0] + 1
            conn.execute(
                f"""
                UPDATE {APP_RUNTIME_TABLE} SET generation = %s, status = 'registered', updated_at = %s
                WHERE app_id = %s
                """,
                [new_generation, datetime.now(timezone.utc), app_id],
            )
            conn.execute(
                f"DELETE FROM {APP_SUBSCRIPTIONS_TABLE} WHERE app_id = %s AND generation < %s",
                [app_id, new_generation],
            )
        return new_generation

    def has_subscriptions(self, app_id: str, generation: int) -> bool:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {APP_SUBSCRIPTIONS_TABLE} WHERE app_id = %s AND generation = %s LIMIT 1",
                [app_id, generation],
            ).fetchone()
        return row is not None

    def resumable(self, app_id: str, generation: int) -> bool:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) FROM {APP_SUBSCRIPTIONS_TABLE} s
                JOIN {STREAM_HEADS_TABLE} h ON h.ref_uri = s.ref_uri
                WHERE s.app_id = %s AND s.generation = %s AND s.stream_version < h.retained_from_version
                """,
                [app_id, generation],
            ).fetchone()
        return (row[0] if row else 0) == 0

    def delete_app_runtime(self, app_id: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                f"""
                DELETE FROM {APP_BOOTSTRAP_OUTPUTS_TABLE}
                WHERE bootstrap_id IN (SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = %s)
                """,
                [app_id],
            )
            conn.execute(
                f"""
                DELETE FROM {APP_BOOTSTRAP_ROWS_TABLE}
                WHERE bootstrap_id IN (SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = %s)
                """,
                [app_id],
            )
            conn.execute(
                f"""
                DELETE FROM {APP_BOOTSTRAP_STREAMS_TABLE}
                WHERE bootstrap_id IN (SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = %s)
                """,
                [app_id],
            )
            conn.execute(
                f"""
                DELETE FROM {APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE}
                WHERE bootstrap_id IN (SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = %s)
                """,
                [app_id],
            )
            for tbl in (
                APP_BOOTSTRAPS_TABLE,
                APP_WEBHOOK_INTENTS_TABLE,
                APP_BATCH_INPUTS_TABLE,
                APP_BATCH_COMMITS_TABLE,
                APP_SUBSCRIPTIONS_TABLE,
                APP_RUNTIME_TABLE,
            ):
                conn.execute(f"DELETE FROM {tbl} WHERE app_id = %s", [app_id])

    # ------------------------------------------------------------------
    # batch read/commit
    # ------------------------------------------------------------------

    def next_app_batch(
        self, app_id: str, generation: int, target_keys: int = 50_000
    ) -> AppBatch | None:
        runtime = self.app_runtime(app_id)
        if runtime is None or runtime.generation != generation:
            raise GenerationMismatch(
                f"app {app_id!r} generation {generation} is stale "
                f"(current: {runtime.generation if runtime else 'unregistered'})"
            )
        if runtime.status == "bootstrapping":
            return self._next_bootstrap_appbatch(app_id, generation)
        return self._next_tail_batch(app_id, generation, target_keys)

    def _next_bootstrap_appbatch(self, app_id: str, generation: int) -> AppBatch | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = %s AND generation = %s",
                [app_id, generation],
            ).fetchone()
        if row is None:
            return None
        page = self.bootstrap_page(row[0], page_size=50_000)
        if page is None:
            return None
        rows = page.rows.append_column(
            "operation", pa.array(["upsert"] * page.rows.num_rows, type=pa.string())
        ).select(["operation", "ref_uri", "ts", "numeric_value", "text_value"])
        return AppBatch(
            batch_id=page.page_id, batch_kind="bootstrap", generation=generation,
            has_more=page.has_more, inputs=[], rows=rows,
            bootstrap_id=page.bootstrap_id, end_ordinal=page.end_ordinal,
        )

    def _next_tail_batch(self, app_id: str, generation: int, target_keys: int) -> AppBatch | None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            subs = conn.execute(
                f"SELECT ref_uri, stream_version FROM {APP_SUBSCRIPTIONS_TABLE} WHERE app_id = %s AND generation = %s",
                [app_id, generation],
            ).fetchall()
            if not subs:
                return None
            from_version = dict(subs)
            ref_uris = list(from_version.keys())

            # Step 2: pending publications touching a subscribed ref beyond
            # its from_version, oldest first.
            pending = conn.execute(
                f"""
                SELECT ck.publication_seq, MIN(p.row_count) AS row_count
                FROM {STREAM_CHANGE_KEYS_TABLE} ck
                JOIN {STREAM_PUBLICATIONS_TABLE} p ON p.publication_seq = ck.publication_seq
                JOIN unnest(%s::text[], %s::bigint[]) AS fv(ref_uri, from_version)
                    ON ck.ref_uri = fv.ref_uri AND ck.stream_version > fv.from_version
                GROUP BY ck.publication_seq
                ORDER BY ck.publication_seq
                """,
                [ref_uris, [from_version[r] for r in ref_uris]],
            ).fetchall()
            if not pending:
                return None

            selected_seqs: list[int] = []
            accumulated = 0
            for seq, row_count in pending:
                if selected_seqs and accumulated + row_count > target_keys:
                    break
                selected_seqs.append(seq)
                accumulated += row_count
            has_more = len(selected_seqs) < len(pending)

            # Step 3: to_version per subscribed ref = max stream_version
            # touched by a SELECTED publication; unselected refs keep their
            # from_version.
            to_rows = conn.execute(
                f"""
                SELECT ck.ref_uri, MAX(ck.stream_version)
                FROM {STREAM_CHANGE_KEYS_TABLE} ck
                JOIN unnest(%s::text[]) AS fv(ref_uri) ON ck.ref_uri = fv.ref_uri
                WHERE ck.publication_seq = ANY(%s::bigint[])
                GROUP BY ck.ref_uri
                """,
                [ref_uris, selected_seqs],
            ).fetchall()
            to_version = dict(from_version)
            to_version.update(dict(to_rows))

            # Steps 4-5: distinct keys touched in (from, to] per ref,
            # left-joined to canonical state; completeness comes from this
            # range scan, not from which publications were selected above
            # (Finding 4 / continuous_batch_plan.md 1c).
            rows = conn.execute(
                f"""
                WITH keys AS (
                    SELECT DISTINCT ck.ref_uri, ck.ts
                    FROM {STREAM_CHANGE_KEYS_TABLE} ck
                    JOIN unnest(%s::text[], %s::bigint[], %s::bigint[]) AS fv(ref_uri, from_version, to_version)
                        ON ck.ref_uri = fv.ref_uri
                    WHERE ck.stream_version > fv.from_version AND ck.stream_version <= fv.to_version
                )
                SELECT k.ref_uri, k.ts, t.numeric_value, t.text_value, t.deleted, t.last_stream_version
                FROM keys k
                JOIN unnest(%s::text[], %s::bigint[]) AS tv(ref_uri, to_version) ON tv.ref_uri = k.ref_uri
                LEFT JOIN {TIMESERIES_TABLE} t ON t.ref_uri = k.ref_uri AND t.ts = k.ts
                WHERE t.last_stream_version IS NULL OR t.last_stream_version <= tv.to_version
                """,
                [
                    ref_uris, [from_version[r] for r in ref_uris], [to_version[r] for r in ref_uris],
                    ref_uris, [to_version[r] for r in ref_uris],
                ],
            ).fetchall()

        pl_rows = pl.DataFrame(
            rows,
            schema=["ref_uri", "ts", "numeric_value", "text_value", "deleted", "last_stream_version"],
            orient="row",
        ).with_columns(
            pl.when(pl.col("deleted").fill_null(True))
            .then(pl.lit("delete"))
            .otherwise(pl.lit("upsert"))
            .alias("operation")
        ).select(["operation", "ref_uri", "ts", "numeric_value", "text_value"])
        rows_table = pl_rows.to_arrow()

        touched = [r for r in ref_uris if to_version[r] > from_version[r]]
        inputs = sorted(
            (BatchInputRange(r, from_version[r], to_version[r]) for r in touched),
            key=lambda r: r.ref_uri,
        )
        batch_id = ids.tail_batch_id(
            generation, [(r.ref_uri, r.from_version, r.to_version) for r in inputs]
        )
        return AppBatch(
            batch_id=batch_id, batch_kind="tail", generation=generation,
            has_more=has_more, inputs=inputs, rows=rows_table,
        )

    def commit_app_batch(self, req: CommitRequest) -> CommitResult:
        if req.batch_kind != "tail":
            raise ValueError(
                f"commit_app_batch handles batch_kind='tail' only; got {req.batch_kind!r} "
                f"(bootstrap pages commit via commit_bootstrap_page)"
            )
        expected_batch_id = ids.tail_batch_id(
            req.generation, [(r.ref_uri, r.from_version, r.to_version) for r in req.inputs]
        )
        if expected_batch_id != req.batch_id:
            raise BatchIdMismatch(
                f"batch id {req.batch_id!r} does not match the id derived from its "
                f"input ranges ({expected_batch_id!r})"
            )

        with self._pool.connection() as conn, conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT generation FROM {APP_RUNTIME_TABLE} WHERE app_id = %s", [req.app_id]
                )
                runtime_row = cur.fetchone()
                if runtime_row is None or runtime_row[0] != req.generation:
                    raise GenerationMismatch(f"app {req.app_id!r} generation {req.generation} is stale")

                cur.execute(
                    f"""
                    SELECT rows_inserted, output_versions_json FROM {APP_BATCH_COMMITS_TABLE}
                    WHERE app_id = %s AND generation = %s AND batch_id = %s
                    """,
                    [req.app_id, req.generation, req.batch_id],
                )
                existing = cur.fetchone()
                if existing is not None:
                    rows_inserted, output_versions_json = existing
                    versions = output_versions_json if isinstance(output_versions_json, dict) else json.loads(output_versions_json)
                    return CommitResult(rows_inserted=rows_inserted, already_committed=True, output_versions=versions)

                output_versions: dict[str, int] = {}
                rows_inserted = 0
                if req.outputs.num_rows > 0:
                    receipt = self._apply_publication(
                        cur, ids.app_output_publication_id(req.app_id, req.batch_id), req.outputs
                    )
                    output_versions = receipt.versions
                    rows_inserted = receipt.row_count

                for r in req.inputs:
                    cur.execute(
                        f"""
                        INSERT INTO {APP_BATCH_INPUTS_TABLE}
                            (app_id, generation, batch_id, ref_uri, from_version, to_version)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        [req.app_id, req.generation, req.batch_id, r.ref_uri, r.from_version, r.to_version],
                    )
                    cur.execute(
                        f"""
                        INSERT INTO {APP_SUBSCRIPTIONS_TABLE} (app_id, generation, ref_uri, stream_version)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (app_id, generation, ref_uri) DO UPDATE SET
                            stream_version = GREATEST(excluded.stream_version, {APP_SUBSCRIPTIONS_TABLE}.stream_version)
                        """,
                        [req.app_id, req.generation, r.ref_uri, r.to_version],
                    )

                for seq, intent in enumerate(req.webhook_intents, start=1):
                    cur.execute(
                        f"""
                        INSERT INTO {APP_WEBHOOK_INTENTS_TABLE}
                            (app_id, generation, batch_id, seq, url, payload_json, status, attempts, next_attempt_at)
                        VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0, NULL)
                        """,
                        [req.app_id, req.generation, req.batch_id, seq, intent.url, json.dumps(intent.payload)],
                    )

                cur.execute(
                    f"""
                    INSERT INTO {APP_BATCH_COMMITS_TABLE}
                        (app_id, generation, batch_id, batch_kind, rows_inserted, output_versions_json, committed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        req.app_id, req.generation, req.batch_id, req.batch_kind, rows_inserted,
                        json.dumps(output_versions), datetime.now(timezone.utc),
                    ],
                )

        return CommitResult(rows_inserted=rows_inserted, already_committed=False, output_versions=output_versions)

    # ------------------------------------------------------------------
    # bootstrap
    # ------------------------------------------------------------------

    def begin_bootstrap(
        self, app_id: str, input_ref_uris: list[str], output_ref_uris: list[str]
    ) -> BootstrapState:
        bootstrap_id = str(uuid.uuid4())
        input_ref_uris = sorted(set(input_ref_uris))
        output_ref_uris = sorted(set(output_ref_uris))
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            with conn.cursor() as cur:
                cur.execute(f"SELECT generation FROM {APP_RUNTIME_TABLE} WHERE app_id = %s", [app_id])
                runtime_row = cur.fetchone()
                if runtime_row is None:
                    raise KeyError(f"app {app_id!r} has no runtime state; register it first")
                generation = runtime_row[0]

                heads: dict[str, int] = {}
                if input_ref_uris:
                    cur.execute(
                        f"SELECT ref_uri, current_version FROM {STREAM_HEADS_TABLE} WHERE ref_uri = ANY(%s::text[])",
                        [input_ref_uris],
                    )
                    heads = dict(cur.fetchall())
                streams_by_uri = {r: heads.get(r, 0) for r in input_ref_uris}

                cur.execute(
                    f"""
                    INSERT INTO {APP_BOOTSTRAPS_TABLE} (bootstrap_id, app_id, generation, status, next_ordinal)
                    VALUES (%s, %s, %s, 'staging', 0)
                    """,
                    [bootstrap_id, app_id, generation],
                )

                if input_ref_uris:
                    cur.execute(
                        f"""
                        INSERT INTO {APP_BOOTSTRAP_STREAMS_TABLE} (bootstrap_id, ref_uri, stream_version)
                        SELECT %s, unnest(%s::text[]), unnest(%s::bigint[])
                        """,
                        [bootstrap_id, input_ref_uris, [streams_by_uri[r] for r in input_ref_uris]],
                    )

                    cur.execute(
                        f"""
                        INSERT INTO {APP_BOOTSTRAP_ROWS_TABLE} (bootstrap_id, ordinal, ref_uri, ts, numeric_value, text_value)
                        SELECT %s, row_number() OVER (ORDER BY ref_uri, ts) - 1, ref_uri, ts, numeric_value, text_value
                        FROM {TIMESERIES_TABLE}
                        WHERE ref_uri = ANY(%s::text[]) AND NOT deleted
                        """,
                        [bootstrap_id, input_ref_uris],
                    )

                    cur.execute(
                        f"""
                        INSERT INTO {APP_SUBSCRIPTIONS_TABLE} (app_id, generation, ref_uri, stream_version)
                        SELECT %s, %s, unnest(%s::text[]), unnest(%s::bigint[])
                        ON CONFLICT (app_id, generation, ref_uri) DO UPDATE SET stream_version = excluded.stream_version
                        """,
                        [app_id, generation, input_ref_uris, [streams_by_uri[r] for r in input_ref_uris]],
                    )

                if output_ref_uris:
                    cur.execute(
                        f"""
                        INSERT INTO {APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE} (bootstrap_id, output_ref_uri)
                        SELECT %s, unnest(%s::text[])
                        """,
                        [bootstrap_id, output_ref_uris],
                    )

                cur.execute(
                    f"UPDATE {APP_RUNTIME_TABLE} SET status = 'bootstrapping', updated_at = %s WHERE app_id = %s",
                    [datetime.now(timezone.utc), app_id],
                )

        return BootstrapState(
            bootstrap_id=bootstrap_id, app_id=app_id, generation=generation, streams=streams_by_uri
        )

    def bootstrap_page(self, bootstrap_id: str, page_size: int) -> BootstrapPage | None:
        """Peek at the next unprocessed page. Read-only: does not advance
        ``next_ordinal`` (that happens in :meth:`commit_bootstrap_page`), so a
        crashed actor re-fetches the same page on restart."""
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT next_ordinal FROM {APP_BOOTSTRAPS_TABLE} WHERE bootstrap_id = %s", [bootstrap_id]
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown bootstrap {bootstrap_id!r}")
            start_ordinal = row[0]
            total = conn.execute(
                f"SELECT COUNT(*) FROM {APP_BOOTSTRAP_ROWS_TABLE} WHERE bootstrap_id = %s", [bootstrap_id]
            ).fetchone()[0]
            if start_ordinal >= total:
                return None
            end_ordinal = min(start_ordinal + page_size, total)
            rows = conn.execute(
                f"""
                SELECT ref_uri, ts, numeric_value, text_value
                FROM {APP_BOOTSTRAP_ROWS_TABLE}
                WHERE bootstrap_id = %s AND ordinal >= %s AND ordinal < %s
                ORDER BY ordinal
                """,
                [bootstrap_id, start_ordinal, end_ordinal],
            ).fetchall()

        table = pl.DataFrame(
            rows, schema=["ref_uri", "ts", "numeric_value", "text_value"], orient="row"
        ).to_arrow()
        page_id = ids.bootstrap_page_id(bootstrap_id, start_ordinal, end_ordinal)
        return BootstrapPage(
            bootstrap_id=bootstrap_id, page_id=page_id, start_ordinal=start_ordinal,
            end_ordinal=end_ordinal, has_more=end_ordinal < total, rows=table,
        )

    def commit_bootstrap_page(
        self, bootstrap_id: str, page_id: str, end_ordinal: int, outputs: pa.Table
    ) -> None:
        with self._pool.connection() as conn, conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT next_ordinal FROM {APP_BOOTSTRAPS_TABLE} WHERE bootstrap_id = %s", [bootstrap_id]
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(f"unknown bootstrap {bootstrap_id!r}")
                next_ordinal = row[0]
                if end_ordinal <= next_ordinal:
                    return  # already committed -- idempotent replay
                expected_page_id = ids.bootstrap_page_id(bootstrap_id, next_ordinal, end_ordinal)
                if expected_page_id != page_id:
                    raise BatchIdMismatch(
                        f"bootstrap page id {page_id!r} does not match the id derived for "
                        f"ordinals [{next_ordinal}, {end_ordinal})"
                    )

                if outputs.num_rows > 0:
                    cur.execute(
                        f"SELECT COALESCE(MAX(ordinal), -1) + 1 FROM {APP_BOOTSTRAP_OUTPUTS_TABLE} WHERE bootstrap_id = %s",
                        [bootstrap_id],
                    )
                    next_output_ordinal = cur.fetchone()[0]
                    out_df = pl.from_arrow(outputs)
                    n = out_df.height
                    cur.execute(
                        f"""
                        INSERT INTO {APP_BOOTSTRAP_OUTPUTS_TABLE}
                            (bootstrap_id, ordinal, output_ref_uri, ts, operation, numeric_value, text_value)
                        SELECT %s, unnest(%s::bigint[]), unnest(%s::text[]), unnest(%s::timestamptz[]),
                               unnest(%s::text[]), unnest(%s::double precision[]), unnest(%s::text[])
                        """,
                        [
                            bootstrap_id,
                            list(range(next_output_ordinal, next_output_ordinal + n)),
                            out_df["ref_uri"].to_list(),
                            out_df["ts"].to_list(),
                            out_df["operation"].to_list(),
                            out_df["numeric_value"].to_list(),
                            out_df["text_value"].to_list(),
                        ],
                    )

                cur.execute(
                    f"UPDATE {APP_BOOTSTRAPS_TABLE} SET next_ordinal = %s WHERE bootstrap_id = %s",
                    [end_ordinal, bootstrap_id],
                )

    def finalize_bootstrap(self, bootstrap_id: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT app_id, generation FROM {APP_BOOTSTRAPS_TABLE} WHERE bootstrap_id = %s", [bootstrap_id]
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(f"unknown bootstrap {bootstrap_id!r}")
                app_id, generation = row

                cur.execute(
                    f"""
                    SELECT output_ref_uri, ts, operation, numeric_value, text_value
                    FROM {APP_BOOTSTRAP_OUTPUTS_TABLE} WHERE bootstrap_id = %s
                    """,
                    [bootstrap_id],
                )
                staged = cur.fetchall()

                # Reconciliation: every declared output ref's existing live
                # row not covered by a staged output becomes a tombstone, so
                # a narrower/changed selector's stale prior output converges.
                cur.execute(
                    f"""
                    SELECT t.ref_uri, t.ts
                    FROM {TIMESERIES_TABLE} t
                    JOIN {APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE} bot
                        ON bot.bootstrap_id = %s AND bot.output_ref_uri = t.ref_uri
                    WHERE NOT t.deleted
                      AND NOT EXISTS (
                        SELECT 1 FROM {APP_BOOTSTRAP_OUTPUTS_TABLE} bo
                        WHERE bo.bootstrap_id = %s AND bo.output_ref_uri = t.ref_uri AND bo.ts = t.ts
                      )
                    """,
                    [bootstrap_id, bootstrap_id],
                )
                tombstones = cur.fetchall()

                rows: list[tuple] = [(op, ref_uri, ts, num, txt) for ref_uri, ts, op, num, txt in staged]
                rows.extend((("delete", ref_uri, ts, None, None) for ref_uri, ts in tombstones))

                if rows:
                    table = pa.table(
                        {
                            "operation": [r[0] for r in rows],
                            "ref_uri": [r[1] for r in rows],
                            "ts": [r[2] for r in rows],
                            "numeric_value": [r[3] for r in rows],
                            "text_value": [r[4] for r in rows],
                        }
                    )
                    self._apply_publication(cur, ids.bootstrap_publication_id(bootstrap_id), table)

                for tbl in (
                    APP_BOOTSTRAP_OUTPUTS_TABLE,
                    APP_BOOTSTRAP_ROWS_TABLE,
                    APP_BOOTSTRAP_STREAMS_TABLE,
                    APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE,
                ):
                    cur.execute(f"DELETE FROM {tbl} WHERE bootstrap_id = %s", [bootstrap_id])
                cur.execute(f"DELETE FROM {APP_BOOTSTRAPS_TABLE} WHERE bootstrap_id = %s", [bootstrap_id])
                cur.execute(
                    f"UPDATE {APP_RUNTIME_TABLE} SET status = 'active', updated_at = %s WHERE app_id = %s",
                    [datetime.now(timezone.utc), app_id],
                )

    # ------------------------------------------------------------------
    # router / compactor support
    # ------------------------------------------------------------------

    def subscription_index(self) -> dict[str, list[str]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT s.ref_uri, s.app_id
                FROM {APP_SUBSCRIPTIONS_TABLE} s
                JOIN {APP_RUNTIME_TABLE} ar ON ar.app_id = s.app_id AND ar.generation = s.generation
                WHERE ar.status IN ('active', 'bootstrapping')
                """
            ).fetchall()
        index: dict[str, list[str]] = {}
        for ref_uri, app_id in rows:
            index.setdefault(ref_uri, []).append(app_id)
        return index

    def lagging_apps(self) -> list[str]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT s.app_id
                FROM {APP_SUBSCRIPTIONS_TABLE} s
                JOIN {STREAM_HEADS_TABLE} h ON h.ref_uri = s.ref_uri
                JOIN {APP_RUNTIME_TABLE} ar ON ar.app_id = s.app_id AND ar.generation = s.generation
                WHERE ar.status IN ('active', 'bootstrapping') AND s.stream_version < h.current_version
                """
            ).fetchall()
        return [r[0] for r in rows]

    def compact(self, chunk_rows: int = 100_000) -> CompactReport:
        """Delete manifest rows no longer needed by any active/bootstrapping
        subscriber and advance each stream's retained floor.

        ``chunk_rows`` is accepted for interface parity with the design
        doc's defaults table, but v1 deletes each ref's eligible rows in one
        statement; paginated deletion for very large manifests is a Phase 5
        performance tuning concern, not a correctness one (see
        continuous_batch_plan.md Phase 5).
        """
        with self._pool.connection() as conn, conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH valid_subs AS (
                        SELECT s.ref_uri, s.stream_version
                        FROM {APP_SUBSCRIPTIONS_TABLE} s
                        JOIN {APP_RUNTIME_TABLE} ar ON ar.app_id = s.app_id AND ar.generation = s.generation
                        WHERE ar.status IN ('active', 'bootstrapping')
                    )
                    SELECT h.ref_uri, COALESCE(MIN(vs.stream_version), h.current_version) AS safe_version
                    FROM {STREAM_HEADS_TABLE} h
                    LEFT JOIN valid_subs vs ON vs.ref_uri = h.ref_uri
                    GROUP BY h.ref_uri, h.current_version
                    """
                )
                floors = cur.fetchall()

                total_deleted = 0
                refs_advanced = 0
                for ref_uri, safe_version in floors:
                    cur.execute(
                        f"DELETE FROM {STREAM_CHANGE_KEYS_TABLE} WHERE ref_uri = %s AND stream_version <= %s",
                        [ref_uri, safe_version],
                    )
                    total_deleted += cur.rowcount
                    cur.execute(
                        f"""
                        UPDATE {STREAM_HEADS_TABLE} SET retained_from_version = %s
                        WHERE ref_uri = %s AND retained_from_version < %s
                        """,
                        [safe_version, ref_uri, safe_version],
                    )
                    refs_advanced += 1
        return CompactReport(manifest_rows_deleted=total_deleted, refs_advanced=refs_advanced)

    def metrics(self) -> dict[str, Any]:
        with self._pool.connection() as conn:
            stream_count, lag_total = conn.execute(
                f"SELECT COUNT(*), COALESCE(SUM(current_version - retained_from_version), 0) FROM {STREAM_HEADS_TABLE}"
            ).fetchone()
            manifest_rows = conn.execute(f"SELECT COUNT(*) FROM {STREAM_CHANGE_KEYS_TABLE}").fetchone()[0]
            apps = conn.execute(f"SELECT app_id, status, generation FROM {APP_RUNTIME_TABLE}").fetchall()
            lag_rows = conn.execute(
                f"""
                SELECT s.app_id, COALESCE(SUM(h.current_version - s.stream_version), 0)
                FROM {APP_SUBSCRIPTIONS_TABLE} s
                JOIN {STREAM_HEADS_TABLE} h ON h.ref_uri = s.ref_uri
                JOIN {APP_RUNTIME_TABLE} ar ON ar.app_id = s.app_id AND ar.generation = s.generation
                GROUP BY s.app_id
                """
            ).fetchall()
        return {
            "stream_count": stream_count,
            "version_lag_total": int(lag_total),
            "manifest_rows": manifest_rows,
            "apps": {app_id: {"status": status, "generation": generation} for app_id, status, generation in apps},
            "app_version_lag": {app_id: int(lag) for app_id, lag in lag_rows},
        }
