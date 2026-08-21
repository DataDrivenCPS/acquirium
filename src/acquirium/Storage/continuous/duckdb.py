"""DuckDB implementation of :class:`ContinuousStore`.

Wraps an existing :class:`~acquirium.Storage.duckdb_store.DuckDBStore`,
reusing its connection factory and write lock rather than opening a second
connection pool: DuckDB's single-writer-per-process model means every write
in this module (publications, batch commits, bootstrap staging) must go
through the same ``DuckDBStore._lock``/``_write_conn`` coordinator the raw
:class:`~acquirium.Storage.base.TimeseriesStore` API uses, or the two layers
could interleave transactions unsafely.

All internal tables are keyed by the integer ``ref_id`` DuckDBStore already
uses for the canonical ``timeseries`` table (see that module's docstring for
the zonemap-pruning rationale); this module resolves ``ref_uri`` strings to
``ref_id`` at its boundary and never leaks ``ref_id`` through the
:class:`ContinuousStore` protocol.

Because DuckDB writes are already serialized by ``DuckDBStore._lock`` (one
writer, one process -- see that module's docstring on multi-process access),
this backend does not need PostgreSQL-style explicit row locking to prevent
the deadlocks that concurrent writers could otherwise cause; sorting ref ids
before locking (continuous_batch.md's "sorted head locking") is preserved
here for determinism and parity with the Postgres backend, not because
DuckDB needs it to avoid deadlock.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from acquirium.internals._log import timed_debug
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
from acquirium.Storage.duckdb_store import (
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
    DuckDBStore,
    REF_IDS_TABLE,
    STREAM_CHANGE_KEYS_TABLE,
    STREAM_CHANGE_RANGES_TABLE,
    STREAM_HEADS_TABLE,
    STREAM_PUBLICATIONS_SEQ,
    STREAM_PUBLICATIONS_TABLE,
    TIMESERIES_TABLE,
)

logger = logging.getLogger("acquirium.storage.continuous.duckdb")


def _now_naive() -> datetime:
    """Current time as naive UTC, matching DuckDBStore's TIMESTAMP convention."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _attach_utc(table: pa.Table, column: str = "ts") -> pa.Table:
    """Reattach a UTC tzinfo to a timestamp column read back from DuckDB.

    DuckDB TIMESTAMP columns are naive; every ContinuousStore-facing schema
    (MUTATION_SCHEMA, BOOTSTRAP_ROW_SCHEMA) requires ``ts`` to be tz-aware.
    """
    idx = table.schema.get_field_index(column)
    col = pc.assume_timezone(table.column(idx), timezone="UTC")
    return table.set_column(idx, table.schema.field(idx).with_type(col.type), col)


class ContinuousDuckDB:
    """``ContinuousStore`` for a DuckDB-backed server.

    See ``continuous_batch_plan.md`` Phase 1c for the transaction-by-
    transaction algorithm each public method follows; this docstring only
    notes DuckDB-specific choices not already covered there.
    """

    def __init__(self, store: DuckDBStore):
        self._store = store

    # ------------------------------------------------------------------
    # id resolution
    # ------------------------------------------------------------------

    def _resolve_ref_ids(self, conn, ref_uris: list[str]) -> dict[str, int]:
        """Ensure ``ref_ids`` rows exist for *ref_uris* and return the mapping.

        Runs on the caller's already-open connection/transaction so id
        assignment is atomic with whatever write it supports (a publication,
        a bootstrap snapshot, ...).
        """
        if not ref_uris:
            return {}
        distinct = sorted(set(ref_uris))
        df = pl.DataFrame({"ref_uri": distinct})
        conn.register("_acq_cont_refuris", df)
        try:
            conn.execute(
                f"""
                INSERT INTO {REF_IDS_TABLE} (ref_uri)
                SELECT ref_uri FROM _acq_cont_refuris
                ON CONFLICT (ref_uri) DO NOTHING
                """
            )
            rows = conn.execute(
                f"""
                SELECT ref_uri, ref_id FROM {REF_IDS_TABLE}
                WHERE ref_uri IN (SELECT ref_uri FROM _acq_cont_refuris)
                """
            ).fetchall()
        finally:
            conn.unregister("_acq_cont_refuris")
        return {ref_uri: ref_id for ref_uri, ref_id in rows}

    # ------------------------------------------------------------------
    # publish
    # ------------------------------------------------------------------

    def publish(self, req: PublicationRequest) -> PublicationReceipt:
        with self._store._lock, self._store._write_conn() as conn:
            return self._apply_publication(conn, req.publication_id, req.mutations)

    def _apply_publication(
        self, conn, publication_id: str, mutations: pa.Table
    ) -> PublicationReceipt:
        """Apply one publication on an already-open, already-locked *conn*.

        Factored out of :meth:`publish` so :meth:`commit_app_batch` and
        :meth:`finalize_bootstrap` can publish their own outputs inside their
        own transaction without re-acquiring ``DuckDBStore._lock`` (which is
        a plain, non-reentrant ``threading.Lock``).

        Implements the publication protocol's ten steps (continuous_batch.md):
        normalize, hash, check-or-apply, lock+advance heads, upsert/tombstone
        canonical rows, record one changed-key row per normalized mutation,
        and store the receipt.
        """
        p_hash = ids.payload_hash(mutations)
        existing = conn.execute(
            f"""
            SELECT payload_hash, row_count, versions_json
            FROM {STREAM_PUBLICATIONS_TABLE} WHERE publication_id = ?
            """,
            [publication_id],
        ).fetchone()
        if existing is not None:
            existing_hash, row_count, versions_json = existing
            if existing_hash != p_hash:
                raise PublicationConflict(publication_id)
            return PublicationReceipt(
                publication_id=publication_id,
                payload_hash=p_hash,
                row_count=row_count,
                versions=json.loads(versions_json),
                deduplicated=True,
            )

        normalized = ids.normalize_mutations(mutations)
        if normalized.num_rows == 0:
            raise ValueError(f"publication {publication_id!r} has no mutation rows")

        df = pl.from_arrow(normalized).with_columns(
            pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        )
        ref_uris = df["ref_uri"].unique().sort().to_list()
        ref_id_map = self._resolve_ref_ids(conn, ref_uris)
        sorted_ref_ids = sorted(ref_id_map.values())

        # Ensure every touched stream has a head row, then bump each exactly
        # once. Sorted for determinism/parity with the Postgres backend's
        # lock ordering -- see the module docstring on why DuckDB itself
        # doesn't need row-level locks here.
        heads_df = pl.DataFrame({"ref_id": sorted_ref_ids})
        conn.register("_acq_cont_headkeys", heads_df)
        try:
            conn.execute(
                f"""
                INSERT INTO {STREAM_HEADS_TABLE} (ref_id, current_version, retained_from_version)
                SELECT ref_id, 0, 0 FROM _acq_cont_headkeys
                ON CONFLICT (ref_id) DO NOTHING
                """
            )
            current_rows = conn.execute(
                f"""
                SELECT ref_id, current_version FROM {STREAM_HEADS_TABLE}
                WHERE ref_id IN (SELECT ref_id FROM _acq_cont_headkeys)
                """
            ).fetchall()
        finally:
            conn.unregister("_acq_cont_headkeys")
        current_version = dict(current_rows)
        new_version = {ref_id: current_version[ref_id] + 1 for ref_id in sorted_ref_ids}

        version_df = pl.DataFrame(
            {"ref_id": list(new_version.keys()), "new_version": list(new_version.values())}
        )
        conn.register("_acq_cont_newversions", version_df)
        try:
            conn.execute(
                f"""
                UPDATE {STREAM_HEADS_TABLE}
                SET current_version = nv.new_version
                FROM _acq_cont_newversions nv
                WHERE {STREAM_HEADS_TABLE}.ref_id = nv.ref_id
                """
            )
        finally:
            conn.unregister("_acq_cont_newversions")

        ref_id_df = pl.DataFrame(
            {"ref_uri": list(ref_id_map.keys()), "ref_id": list(ref_id_map.values())}
        )
        df = df.join(ref_id_df, on="ref_uri", how="left")
        df = df.join(
            version_df.rename({"new_version": "last_stream_version"}), on="ref_id", how="left"
        )
        # A delete mutation always writes a tombstone with null values, even
        # if the writer sent stray numeric/text data on a delete row.
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
            ]
        )

        write_df = df.select(
            ["ref_id", "ts", "numeric_value", "text_value", "deleted", "last_stream_version"]
        )
        conn.register("_acq_cont_write", write_df)
        try:
            # Delete-then-insert emulates an upsert that also sets deleted/
            # last_stream_version, mirroring DuckDBStore._insert_frame's idiom.
            conn.execute(
                f"""
                DELETE FROM {TIMESERIES_TABLE}
                USING (SELECT ref_id, ts FROM _acq_cont_write) AS incoming
                WHERE {TIMESERIES_TABLE}.ref_id = incoming.ref_id
                  AND {TIMESERIES_TABLE}.ts = incoming.ts
                """
            )
            conn.execute(
                f"""
                INSERT INTO {TIMESERIES_TABLE} (ref_id, ts, numeric_value, text_value, deleted, last_stream_version)
                SELECT ref_id, ts, numeric_value, text_value, deleted, last_stream_version
                FROM _acq_cont_write
                """
            )
        finally:
            conn.unregister("_acq_cont_write")

        row_count = df.height
        publication_seq = conn.execute(f"SELECT nextval('{STREAM_PUBLICATIONS_SEQ}')").fetchone()[0]
        change_df = (
            df.select(["ref_id", "ts", "last_stream_version"])
            .rename({"last_stream_version": "stream_version"})
            .with_columns(
                [
                    pl.Series("publication_row", list(range(1, row_count + 1))),
                    pl.lit(publication_seq).alias("publication_seq"),
                ]
            )
            .select(["publication_seq", "publication_row", "ref_id", "stream_version", "ts"])
        )
        conn.register("_acq_cont_changekeys", change_df)
        try:
            conn.execute(f"INSERT INTO {STREAM_CHANGE_KEYS_TABLE} SELECT * FROM _acq_cont_changekeys")
        finally:
            conn.unregister("_acq_cont_changekeys")

        # The old exact-key manifest remains dual-written during migration,
        # while range manifests are the new durable invalidation contract.
        from acquirium.Storage.materialization.ids import normalize_change_ranges
        ranges = normalize_change_ranges(
            publication_id=publication_id,
            stream_versions={ref: new_version[ref_id_map[ref]] for ref in ref_uris},
            changes=zip(
                normalized.column("ref_uri").to_pylist(),
                normalized.column("ts").to_pylist(),
                normalized.column("operation").to_pylist(),
            ),
        )
        conn.executemany(
            f"""INSERT INTO {STREAM_CHANGE_RANGES_TABLE}
            (ref_uri, stream_version, publication_id, start_ts, end_ts, change_kind, row_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [(item.ref_uri, item.stream_version, item.publication_id,
              item.interval.start.replace(tzinfo=None), item.interval.end.replace(tzinfo=None),
              item.change_kind, item.row_count) for item in ranges],
        )

        versions = {ref_uri: new_version[ref_id_map[ref_uri]] for ref_uri in ref_id_map}
        conn.execute(
            f"""
            INSERT INTO {STREAM_PUBLICATIONS_TABLE}
                (publication_seq, publication_id, payload_hash, row_count, versions_json, committed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [publication_seq, publication_id, p_hash, row_count, json.dumps(versions), _now_naive()],
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
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute(
                f"""
                INSERT INTO {APP_RUNTIME_TABLE} (app_id, generation, status, topology_version, updated_at)
                VALUES (?, 1, 'registered', 1, ?)
                ON CONFLICT (app_id) DO NOTHING
                """,
                [app_id, _now_naive()],
            )

    def app_runtime(self, app_id: str) -> AppRuntimeRow | None:
        with self._store._own_conn() as conn:
            row = conn.execute(
                f"""
                SELECT app_id, generation, status, topology_version, updated_at
                FROM {APP_RUNTIME_TABLE} WHERE app_id = ?
                """,
                [app_id],
            ).fetchone()
        if row is None:
            return None
        app_id_, generation, status, topology_version, updated_at = row
        return AppRuntimeRow(
            app_id=app_id_,
            generation=generation,
            status=status,
            topology_version=topology_version,
            updated_at=self._store._add_utc(updated_at),
        )

    def set_app_status(self, app_id: str, status: str) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute(
                f"UPDATE {APP_RUNTIME_TABLE} SET status = ?, updated_at = ? WHERE app_id = ?",
                [status, _now_naive(), app_id],
            )

    def reset_app(self, app_id: str) -> int:
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute(
                f"SELECT generation FROM {APP_RUNTIME_TABLE} WHERE app_id = ?", [app_id]
            ).fetchone()
            if row is None:
                raise KeyError(f"app {app_id!r} has no runtime state; register it first")
            new_generation = row[0] + 1
            conn.execute(
                f"""
                UPDATE {APP_RUNTIME_TABLE}
                SET generation = ?, status = 'registered', updated_at = ?
                WHERE app_id = ?
                """,
                [new_generation, _now_naive(), app_id],
            )
            # The old generation's subscriptions are dead weight -- provenance
            # for prior work lives in app_batch_commits/app_batch_inputs, not
            # here, and a stale row would otherwise never be cleaned up.
            conn.execute(
                f"DELETE FROM {APP_SUBSCRIPTIONS_TABLE} WHERE app_id = ? AND generation < ?",
                [app_id, new_generation],
            )
        return new_generation

    def has_subscriptions(self, app_id: str, generation: int) -> bool:
        with self._store._own_conn() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {APP_SUBSCRIPTIONS_TABLE} WHERE app_id = ? AND generation = ? LIMIT 1",
                [app_id, generation],
            ).fetchone()
        return row is not None

    def resumable(self, app_id: str, generation: int) -> bool:
        with self._store._own_conn() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) FROM {APP_SUBSCRIPTIONS_TABLE} s
                JOIN {STREAM_HEADS_TABLE} h ON h.ref_id = s.ref_id
                WHERE s.app_id = ? AND s.generation = ? AND s.stream_version < h.retained_from_version
                """,
                [app_id, generation],
            ).fetchone()
        return (row[0] if row else 0) == 0

    def delete_app_runtime(self, app_id: str) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute(
                f"""
                DELETE FROM {APP_BOOTSTRAP_OUTPUTS_TABLE}
                WHERE bootstrap_id IN (SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = ?)
                """,
                [app_id],
            )
            conn.execute(
                f"""
                DELETE FROM {APP_BOOTSTRAP_ROWS_TABLE}
                WHERE bootstrap_id IN (SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = ?)
                """,
                [app_id],
            )
            conn.execute(
                f"""
                DELETE FROM {APP_BOOTSTRAP_STREAMS_TABLE}
                WHERE bootstrap_id IN (SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = ?)
                """,
                [app_id],
            )
            conn.execute(
                f"""
                DELETE FROM {APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE}
                WHERE bootstrap_id IN (SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = ?)
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
                conn.execute(f"DELETE FROM {tbl} WHERE app_id = ?", [app_id])

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
        with self._store._own_conn() as conn:
            row = conn.execute(
                f"SELECT bootstrap_id FROM {APP_BOOTSTRAPS_TABLE} WHERE app_id = ? AND generation = ?",
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
            batch_id=page.page_id,
            batch_kind="bootstrap",
            generation=generation,
            has_more=page.has_more,
            inputs=[],
            rows=rows,
            bootstrap_id=page.bootstrap_id,
            end_ordinal=page.end_ordinal,
        )

    def _snapshot(self):
        """Open a fresh connection with its own transaction, for a consistent
        multi-statement read. Committing a read-only transaction is harmless;
        this simply closes the snapshot cleanly."""
        return _Snapshot(self._store)

    def _next_tail_batch(self, app_id: str, generation: int, target_keys: int) -> AppBatch | None:
        with self._snapshot() as conn:
            subs = conn.execute(
                f"""
                SELECT s.ref_id, r.ref_uri, s.stream_version
                FROM {APP_SUBSCRIPTIONS_TABLE} s JOIN {REF_IDS_TABLE} r USING (ref_id)
                WHERE s.app_id = ? AND s.generation = ?
                """,
                [app_id, generation],
            ).fetchall()
            if not subs:
                return None
            from_version = {ref_id: v for ref_id, _, v in subs}
            ref_uri_by_id = {ref_id: ref_uri for ref_id, ref_uri, _ in subs}
            ref_ids = list(from_version.keys())

            fv_df = pl.DataFrame(
                {"ref_id": ref_ids, "from_version": [from_version[r] for r in ref_ids]}
            )
            conn.register("_acq_from_versions", fv_df)
            try:
                # Step 2: pending publications touching a subscribed ref
                # beyond its from_version, oldest first.
                pending = conn.execute(
                    f"""
                    SELECT ck.publication_seq, MIN(p.row_count) AS row_count
                    FROM {STREAM_CHANGE_KEYS_TABLE} ck
                    JOIN _acq_from_versions fv ON ck.ref_id = fv.ref_id AND ck.stream_version > fv.from_version
                    JOIN {STREAM_PUBLICATIONS_TABLE} p ON p.publication_seq = ck.publication_seq
                    GROUP BY ck.publication_seq
                    ORDER BY ck.publication_seq
                    """
                ).fetchall()
                if not pending:
                    return None

                # Always take at least one publication whole, even if it
                # exceeds target_keys (continuous_batch.md's oversized-
                # publication rule); otherwise accumulate until the target.
                selected_seqs: list[int] = []
                accumulated = 0
                for seq, row_count in pending:
                    if selected_seqs and accumulated + row_count > target_keys:
                        break
                    selected_seqs.append(seq)
                    accumulated += row_count
                has_more = len(selected_seqs) < len(pending)

                # Step 3: to_version per subscribed ref = max stream_version
                # touched by a SELECTED publication; unselected refs keep
                # their from_version (nothing in this batch advances them).
                seq_ph = ",".join("?" * len(selected_seqs))
                to_rows = conn.execute(
                    f"""
                    SELECT ck.ref_id, MAX(ck.stream_version)
                    FROM {STREAM_CHANGE_KEYS_TABLE} ck
                    JOIN _acq_from_versions fv ON ck.ref_id = fv.ref_id
                    WHERE ck.publication_seq IN ({seq_ph})
                    GROUP BY ck.ref_id
                    """,
                    selected_seqs,
                ).fetchall()
                to_version = dict(from_version)
                to_version.update(dict(to_rows))

                tv_df = pl.DataFrame(
                    {"ref_id": list(to_version.keys()), "to_version": list(to_version.values())}
                )
                conn.register("_acq_to_versions", tv_df)
                try:
                    # Steps 4-5: distinct keys touched in (from, to] per ref,
                    # left-joined to canonical state; a key whose canonical
                    # row was superseded beyond this batch's to_version (a
                    # later batch already covers it) or has no live row
                    # becomes a delete, otherwise an upsert of the live value.
                    # Completeness comes from this range scan, not from which
                    # publications were selected above (Finding 4 /
                    # continuous_batch_plan.md 1c) -- an unselected
                    # publication whose versions still fall in (from, to] is
                    # still picked up here.
                    fetched = conn.execute(
                        f"""
                        WITH keys AS (
                            SELECT DISTINCT ck.ref_id, ck.ts
                            FROM {STREAM_CHANGE_KEYS_TABLE} ck
                            JOIN _acq_from_versions fv ON ck.ref_id = fv.ref_id
                            JOIN _acq_to_versions tv ON ck.ref_id = tv.ref_id
                            WHERE ck.stream_version > fv.from_version AND ck.stream_version <= tv.to_version
                        )
                        SELECT
                            r.ref_uri,
                            k.ts,
                            t.numeric_value,
                            t.text_value,
                            t.deleted,
                            t.last_stream_version
                        FROM keys k
                        JOIN {REF_IDS_TABLE} r ON r.ref_id = k.ref_id
                        JOIN _acq_to_versions tv2 ON tv2.ref_id = k.ref_id
                        LEFT JOIN {TIMESERIES_TABLE} t ON t.ref_id = k.ref_id AND t.ts = k.ts
                        WHERE t.last_stream_version IS NULL OR t.last_stream_version <= tv2.to_version
                        """
                    ).to_arrow_table()
                finally:
                    conn.unregister("_acq_to_versions")
            finally:
                conn.unregister("_acq_from_versions")

        pl_rows = pl.from_arrow(fetched).with_columns(
            [
                pl.when(pl.col("deleted").fill_null(True))
                .then(pl.lit("delete"))
                .otherwise(pl.lit("upsert"))
                .alias("operation"),
                pl.col("ts").dt.replace_time_zone("UTC"),
            ]
        ).select(["operation", "ref_uri", "ts", "numeric_value", "text_value"])
        rows_table = pl_rows.to_arrow()

        touched = [rid for rid in ref_ids if to_version[rid] > from_version[rid]]
        inputs = sorted(
            (
                BatchInputRange(ref_uri_by_id[rid], from_version[rid], to_version[rid])
                for rid in touched
            ),
            key=lambda r: r.ref_uri,
        )
        batch_id = ids.tail_batch_id(
            generation, [(r.ref_uri, r.from_version, r.to_version) for r in inputs]
        )
        return AppBatch(
            batch_id=batch_id,
            batch_kind="tail",
            generation=generation,
            has_more=has_more,
            inputs=inputs,
            rows=rows_table,
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

        with self._store._lock, self._store._write_conn() as conn:
            runtime_row = conn.execute(
                f"SELECT generation FROM {APP_RUNTIME_TABLE} WHERE app_id = ?", [req.app_id]
            ).fetchone()
            if runtime_row is None or runtime_row[0] != req.generation:
                raise GenerationMismatch(
                    f"app {req.app_id!r} generation {req.generation} is stale"
                )

            existing = conn.execute(
                f"""
                SELECT rows_inserted, output_versions_json FROM {APP_BATCH_COMMITS_TABLE}
                WHERE app_id = ? AND generation = ? AND batch_id = ?
                """,
                [req.app_id, req.generation, req.batch_id],
            ).fetchone()
            if existing is not None:
                rows_inserted, output_versions_json = existing
                return CommitResult(
                    rows_inserted=rows_inserted,
                    already_committed=True,
                    output_versions=json.loads(output_versions_json),
                )

            output_versions: dict[str, int] = {}
            rows_inserted = 0
            if req.outputs.num_rows > 0:
                receipt = self._apply_publication(
                    conn, ids.app_output_publication_id(req.app_id, req.batch_id), req.outputs
                )
                output_versions = receipt.versions
                rows_inserted = receipt.row_count

            input_ref_ids = self._resolve_ref_ids(conn, [r.ref_uri for r in req.inputs])
            for r in req.inputs:
                ref_id = input_ref_ids[r.ref_uri]
                conn.execute(
                    f"""
                    INSERT INTO {APP_BATCH_INPUTS_TABLE}
                        (app_id, generation, batch_id, ref_id, from_version, to_version)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [req.app_id, req.generation, req.batch_id, ref_id, r.from_version, r.to_version],
                )
                conn.execute(
                    f"""
                    INSERT INTO {APP_SUBSCRIPTIONS_TABLE} (app_id, generation, ref_id, stream_version)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (app_id, generation, ref_id) DO UPDATE SET
                        stream_version = GREATEST(excluded.stream_version, {APP_SUBSCRIPTIONS_TABLE}.stream_version)
                    """,
                    [req.app_id, req.generation, ref_id, r.to_version],
                )

            for seq, intent in enumerate(req.webhook_intents, start=1):
                conn.execute(
                    f"""
                    INSERT INTO {APP_WEBHOOK_INTENTS_TABLE}
                        (app_id, generation, batch_id, seq, url, payload_json, status, attempts, next_attempt_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, NULL)
                    """,
                    [req.app_id, req.generation, req.batch_id, seq, intent.url, json.dumps(intent.payload)],
                )

            conn.execute(
                f"""
                INSERT INTO {APP_BATCH_COMMITS_TABLE}
                    (app_id, generation, batch_id, batch_kind, rows_inserted, output_versions_json, committed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    req.app_id,
                    req.generation,
                    req.batch_id,
                    req.batch_kind,
                    rows_inserted,
                    json.dumps(output_versions),
                    _now_naive(),
                ],
            )

        return CommitResult(
            rows_inserted=rows_inserted, already_committed=False, output_versions=output_versions
        )

    # ------------------------------------------------------------------
    # bootstrap
    # ------------------------------------------------------------------

    def begin_bootstrap(
        self, app_id: str, input_ref_uris: list[str], output_ref_uris: list[str]
    ) -> BootstrapState:
        bootstrap_id = str(uuid.uuid4())
        with self._store._lock, self._store._write_conn() as conn:
            runtime_row = conn.execute(
                f"SELECT generation FROM {APP_RUNTIME_TABLE} WHERE app_id = ?", [app_id]
            ).fetchone()
            if runtime_row is None:
                raise KeyError(f"app {app_id!r} has no runtime state; register it first")
            generation = runtime_row[0]

            input_ref_id_map = self._resolve_ref_ids(conn, input_ref_uris)
            output_ref_id_map = self._resolve_ref_ids(conn, output_ref_uris)
            input_ref_ids = sorted(input_ref_id_map.values())

            if input_ref_ids:
                ph = ",".join("?" * len(input_ref_ids))
                heads = dict(
                    conn.execute(
                        f"SELECT ref_id, current_version FROM {STREAM_HEADS_TABLE} WHERE ref_id IN ({ph})",
                        input_ref_ids,
                    ).fetchall()
                )
            else:
                heads = {}
            streams_by_id = {rid: heads.get(rid, 0) for rid in input_ref_ids}

            conn.execute(
                f"""
                INSERT INTO {APP_BOOTSTRAPS_TABLE} (bootstrap_id, app_id, generation, status, next_ordinal)
                VALUES (?, ?, ?, 'staging', 0)
                """,
                [bootstrap_id, app_id, generation],
            )

            if input_ref_ids:
                streams_df = pl.DataFrame(
                    {
                        "bootstrap_id": [bootstrap_id] * len(input_ref_ids),
                        "ref_id": input_ref_ids,
                        "stream_version": [streams_by_id[rid] for rid in input_ref_ids],
                    }
                )
                conn.register("_acq_cont_bsstreams", streams_df)
                try:
                    conn.execute(f"INSERT INTO {APP_BOOTSTRAP_STREAMS_TABLE} SELECT * FROM _acq_cont_bsstreams")
                finally:
                    conn.unregister("_acq_cont_bsstreams")

                # Stage every live canonical row for the input streams in one
                # repeatable-snapshot statement, ordered so pages replay
                # deterministically.
                ph = ",".join("?" * len(input_ref_ids))
                staged = conn.execute(
                    f"""
                    SELECT ref_id, ts, numeric_value, text_value
                    FROM {TIMESERIES_TABLE}
                    WHERE ref_id IN ({ph}) AND NOT deleted
                    ORDER BY ref_id, ts
                    """,
                    input_ref_ids,
                ).to_arrow_table()
                if staged.num_rows > 0:
                    staged_df = pl.from_arrow(staged).with_columns(
                        pl.Series("bootstrap_id", [bootstrap_id] * staged.num_rows),
                        pl.Series("ordinal", list(range(staged.num_rows))),
                    ).select(["bootstrap_id", "ordinal", "ref_id", "ts", "numeric_value", "text_value"])
                    conn.register("_acq_cont_bsrows", staged_df)
                    try:
                        conn.execute(f"INSERT INTO {APP_BOOTSTRAP_ROWS_TABLE} SELECT * FROM _acq_cont_bsrows")
                    finally:
                        conn.unregister("_acq_cont_bsrows")

                for rid in input_ref_ids:
                    conn.execute(
                        f"""
                        INSERT INTO {APP_SUBSCRIPTIONS_TABLE} (app_id, generation, ref_id, stream_version)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (app_id, generation, ref_id) DO UPDATE SET stream_version = excluded.stream_version
                        """,
                        [app_id, generation, rid, streams_by_id[rid]],
                    )

            if output_ref_id_map:
                targets_df = pl.DataFrame(
                    {
                        "bootstrap_id": [bootstrap_id] * len(output_ref_id_map),
                        "output_ref_id": list(output_ref_id_map.values()),
                    }
                )
                conn.register("_acq_cont_bstargets", targets_df)
                try:
                    conn.execute(
                        f"INSERT INTO {APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE} SELECT * FROM _acq_cont_bstargets"
                    )
                finally:
                    conn.unregister("_acq_cont_bstargets")

            conn.execute(
                f"UPDATE {APP_RUNTIME_TABLE} SET status = 'bootstrapping', updated_at = ? WHERE app_id = ?",
                [_now_naive(), app_id],
            )

        return BootstrapState(
            bootstrap_id=bootstrap_id,
            app_id=app_id,
            generation=generation,
            streams={ref_uri: streams_by_id[input_ref_id_map[ref_uri]] for ref_uri in input_ref_id_map},
        )

    def bootstrap_page(self, bootstrap_id: str, page_size: int) -> BootstrapPage | None:
        """Peek at the next unprocessed page. Read-only: does not advance
        ``next_ordinal`` (that happens in :meth:`commit_bootstrap_page`), so a
        crashed actor re-fetches the same page on restart."""
        with self._store._own_conn() as conn:
            row = conn.execute(
                f"SELECT next_ordinal FROM {APP_BOOTSTRAPS_TABLE} WHERE bootstrap_id = ?", [bootstrap_id]
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown bootstrap {bootstrap_id!r}")
            start_ordinal = row[0]
            total = conn.execute(
                f"SELECT COUNT(*) FROM {APP_BOOTSTRAP_ROWS_TABLE} WHERE bootstrap_id = ?", [bootstrap_id]
            ).fetchone()[0]
            if start_ordinal >= total:
                return None
            end_ordinal = min(start_ordinal + page_size, total)
            fetched = conn.execute(
                f"""
                SELECT r.ref_uri, br.ts, br.numeric_value, br.text_value
                FROM {APP_BOOTSTRAP_ROWS_TABLE} br
                JOIN {REF_IDS_TABLE} r USING (ref_id)
                WHERE br.bootstrap_id = ? AND br.ordinal >= ? AND br.ordinal < ?
                ORDER BY br.ordinal
                """,
                [bootstrap_id, start_ordinal, end_ordinal],
            ).to_arrow_table()

        rows = _attach_utc(fetched)
        page_id = ids.bootstrap_page_id(bootstrap_id, start_ordinal, end_ordinal)
        return BootstrapPage(
            bootstrap_id=bootstrap_id,
            page_id=page_id,
            start_ordinal=start_ordinal,
            end_ordinal=end_ordinal,
            has_more=end_ordinal < total,
            rows=rows,
        )

    def commit_bootstrap_page(
        self, bootstrap_id: str, page_id: str, end_ordinal: int, outputs: pa.Table
    ) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute(
                f"SELECT next_ordinal FROM {APP_BOOTSTRAPS_TABLE} WHERE bootstrap_id = ?", [bootstrap_id]
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown bootstrap {bootstrap_id!r}")
            next_ordinal = row[0]
            if end_ordinal <= next_ordinal:
                # Already committed -- idempotent replay of a retried commit.
                return
            expected_page_id = ids.bootstrap_page_id(bootstrap_id, next_ordinal, end_ordinal)
            if expected_page_id != page_id:
                raise BatchIdMismatch(
                    f"bootstrap page id {page_id!r} does not match the id derived for "
                    f"ordinals [{next_ordinal}, {end_ordinal})"
                )

            if outputs.num_rows > 0:
                next_output_ordinal = conn.execute(
                    f"SELECT COALESCE(MAX(ordinal), -1) + 1 FROM {APP_BOOTSTRAP_OUTPUTS_TABLE} WHERE bootstrap_id = ?",
                    [bootstrap_id],
                ).fetchone()[0]
                out_df = pl.from_arrow(outputs).with_columns(
                    pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
                )
                n = out_df.height
                out_df = out_df.rename({"ref_uri": "output_ref_uri"}).with_columns(
                    [
                        pl.Series("ordinal", list(range(next_output_ordinal, next_output_ordinal + n))),
                        pl.lit(bootstrap_id).alias("bootstrap_id"),
                    ]
                ).select(
                    ["bootstrap_id", "ordinal", "output_ref_uri", "ts", "operation", "numeric_value", "text_value"]
                )
                conn.register("_acq_cont_bsoutputs", out_df)
                try:
                    conn.execute(f"INSERT INTO {APP_BOOTSTRAP_OUTPUTS_TABLE} SELECT * FROM _acq_cont_bsoutputs")
                finally:
                    conn.unregister("_acq_cont_bsoutputs")

            conn.execute(
                f"UPDATE {APP_BOOTSTRAPS_TABLE} SET next_ordinal = ? WHERE bootstrap_id = ?",
                [end_ordinal, bootstrap_id],
            )

    def finalize_bootstrap(self, bootstrap_id: str) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute(
                f"SELECT app_id, generation FROM {APP_BOOTSTRAPS_TABLE} WHERE bootstrap_id = ?", [bootstrap_id]
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown bootstrap {bootstrap_id!r}")
            app_id, generation = row

            staged = conn.execute(
                f"""
                SELECT output_ref_uri AS ref_uri, ts, operation, numeric_value, text_value
                FROM {APP_BOOTSTRAP_OUTPUTS_TABLE} WHERE bootstrap_id = ?
                """,
                [bootstrap_id],
            ).to_arrow_table()

            # Reconciliation: every declared output ref's existing live row
            # not covered by a staged output becomes a tombstone, so a
            # narrower/changed selector's stale prior output converges.
            tombstones = conn.execute(
                f"""
                SELECT r.ref_uri, t.ts
                FROM {TIMESERIES_TABLE} t
                JOIN {REF_IDS_TABLE} r USING (ref_id)
                JOIN {APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE} bot
                    ON bot.bootstrap_id = ? AND bot.output_ref_id = t.ref_id
                WHERE NOT t.deleted
                  AND NOT EXISTS (
                    SELECT 1 FROM {APP_BOOTSTRAP_OUTPUTS_TABLE} bo
                    WHERE bo.bootstrap_id = ? AND bo.output_ref_uri = r.ref_uri AND bo.ts = t.ts
                  )
                """,
                [bootstrap_id, bootstrap_id],
            ).fetchall()

            parts = []
            if staged.num_rows > 0:
                staged_pl = pl.from_arrow(staged).with_columns(
                    pl.col("ts").dt.replace_time_zone("UTC")
                )
                parts.append(staged_pl)
            if tombstones:
                t_refs, t_ts = zip(*tombstones)
                parts.append(
                    pl.DataFrame(
                        {
                            "ref_uri": list(t_refs),
                            "ts": [ts.replace(tzinfo=timezone.utc) for ts in t_ts],
                            "operation": ["delete"] * len(t_refs),
                            "numeric_value": [None] * len(t_refs),
                            "text_value": [None] * len(t_refs),
                        }
                    )
                )

            if parts:
                combined = pl.concat(parts, how="vertical_relaxed")
                mutation_table = combined.select(
                    ["operation", "ref_uri", "ts", "numeric_value", "text_value"]
                ).to_arrow()
                self._apply_publication(conn, ids.bootstrap_publication_id(bootstrap_id), mutation_table)

            for tbl in (
                APP_BOOTSTRAP_OUTPUTS_TABLE,
                APP_BOOTSTRAP_ROWS_TABLE,
                APP_BOOTSTRAP_STREAMS_TABLE,
                APP_BOOTSTRAP_OUTPUT_TARGETS_TABLE,
            ):
                conn.execute(f"DELETE FROM {tbl} WHERE bootstrap_id = ?", [bootstrap_id])
            conn.execute(f"DELETE FROM {APP_BOOTSTRAPS_TABLE} WHERE bootstrap_id = ?", [bootstrap_id])
            conn.execute(
                f"UPDATE {APP_RUNTIME_TABLE} SET status = 'active', updated_at = ? WHERE app_id = ?",
                [_now_naive(), app_id],
            )

    # ------------------------------------------------------------------
    # router / compactor support
    # ------------------------------------------------------------------

    def subscription_index(self) -> dict[str, list[str]]:
        with self._store._own_conn() as conn:
            rows = conn.execute(
                f"""
                SELECT r.ref_uri, s.app_id
                FROM {APP_SUBSCRIPTIONS_TABLE} s
                JOIN {REF_IDS_TABLE} r USING (ref_id)
                JOIN {APP_RUNTIME_TABLE} ar ON ar.app_id = s.app_id AND ar.generation = s.generation
                WHERE ar.status IN ('active', 'bootstrapping')
                """
            ).fetchall()
        index: dict[str, list[str]] = {}
        for ref_uri, app_id in rows:
            index.setdefault(ref_uri, []).append(app_id)
        return index

    def lagging_apps(self) -> list[str]:
        with self._store._own_conn() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT s.app_id
                FROM {APP_SUBSCRIPTIONS_TABLE} s
                JOIN {STREAM_HEADS_TABLE} h ON h.ref_id = s.ref_id
                JOIN {APP_RUNTIME_TABLE} ar ON ar.app_id = s.app_id AND ar.generation = s.generation
                WHERE ar.status IN ('active', 'bootstrapping') AND s.stream_version < h.current_version
                """
            ).fetchall()
        return [r[0] for r in rows]

    def compact(self, chunk_rows: int = 100_000) -> CompactReport:
        """Delete manifest rows no longer needed by any active/bootstrapping
        subscriber and advance each stream's retained floor.

        ``chunk_rows`` is accepted for interface parity with the Postgres
        backend and the design doc's defaults table, but v1 deletes each
        ref's eligible rows in one statement; paginated deletion for very
        large manifests is a Phase 5 performance tuning concern, not a
        correctness one (see continuous_batch_plan.md Phase 5).
        """
        with self._store._lock, self._store._write_conn() as conn:
            floors = conn.execute(
                f"""
                WITH valid_subs AS (
                    SELECT s.ref_id, s.stream_version
                    FROM {APP_SUBSCRIPTIONS_TABLE} s
                    JOIN {APP_RUNTIME_TABLE} ar ON ar.app_id = s.app_id AND ar.generation = s.generation
                    WHERE ar.status IN ('active', 'bootstrapping')
                )
                SELECT h.ref_id, COALESCE(MIN(vs.stream_version), h.current_version) AS safe_version
                FROM {STREAM_HEADS_TABLE} h
                LEFT JOIN valid_subs vs ON vs.ref_id = h.ref_id
                GROUP BY h.ref_id, h.current_version
                """
            ).fetchall()

            total_deleted = 0
            refs_advanced = 0
            for ref_id, safe_version in floors:
                deleted_here = conn.execute(
                    f"DELETE FROM {STREAM_CHANGE_KEYS_TABLE} WHERE ref_id = ? AND stream_version <= ?",
                    [ref_id, safe_version],
                ).fetchone()
                total_deleted += deleted_here[0] if deleted_here else 0
                conn.execute(
                    f"""
                    UPDATE {STREAM_HEADS_TABLE} SET retained_from_version = ?
                    WHERE ref_id = ? AND retained_from_version < ?
                    """,
                    [safe_version, ref_id, safe_version],
                )
                refs_advanced += 1
        return CompactReport(manifest_rows_deleted=total_deleted, refs_advanced=refs_advanced)

    def metrics(self) -> dict[str, Any]:
        with self._store._own_conn() as conn:
            stream_count, lag_total = conn.execute(
                f"SELECT COUNT(*), COALESCE(SUM(current_version - retained_from_version), 0) FROM {STREAM_HEADS_TABLE}"
            ).fetchone()
            manifest_rows = conn.execute(f"SELECT COUNT(*) FROM {STREAM_CHANGE_KEYS_TABLE}").fetchone()[0]
            apps = conn.execute(f"SELECT app_id, status, generation FROM {APP_RUNTIME_TABLE}").fetchall()
            lag_rows = conn.execute(
                f"""
                SELECT s.app_id, COALESCE(SUM(h.current_version - s.stream_version), 0)
                FROM {APP_SUBSCRIPTIONS_TABLE} s
                JOIN {STREAM_HEADS_TABLE} h ON h.ref_id = s.ref_id
                JOIN {APP_RUNTIME_TABLE} ar ON ar.app_id = s.app_id AND ar.generation = s.generation
                GROUP BY s.app_id
                """
            ).fetchall()
        return {
            "stream_count": stream_count,
            "version_lag_total": lag_total,
            "manifest_rows": manifest_rows,
            "apps": {app_id: {"status": status, "generation": generation} for app_id, status, generation in apps},
            "app_version_lag": {app_id: lag for app_id, lag in lag_rows},
        }


class _Snapshot:
    """Context manager yielding a connection with its own open transaction,
    for a multi-statement read that must see one consistent state."""

    def __init__(self, store: DuckDBStore):
        self._store = store
        self._conn = None

    def __enter__(self):
        self._conn = self._store._connect()
        self._conn.begin()
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._conn is not None:
                self._conn.commit()
        finally:
            if self._conn is not None:
                self._conn.close()
