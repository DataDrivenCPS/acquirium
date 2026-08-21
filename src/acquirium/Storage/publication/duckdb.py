"""DuckDB implementation of the canonical versioned publication protocol."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import polars as pl

from acquirium.Storage.duckdb_store import DuckDBStore, REF_IDS_TABLE, STREAM_HEADS_TABLE, STREAM_PUBLICATIONS_SEQ, STREAM_PUBLICATIONS_TABLE, TIMESERIES_TABLE
from acquirium.Storage.materialization.ids import normalize_change_ranges
from acquirium.Storage.publication import ids
from acquirium.Storage.publication.types import PublicationConflict, PublicationReceipt, PublicationRequest


class PublicationDuckDB:
    """Canonical writes with idempotent receipts and range manifests."""
    def __init__(self, store: DuckDBStore) -> None:
        self._store = store

    def publish(self, request: PublicationRequest) -> PublicationReceipt:
        with self._store._lock, self._store._write_conn() as conn:
            return self._apply_publication(conn, request.publication_id, request.mutations)

    def _apply_publication(self, conn, publication_id: str, mutations) -> PublicationReceipt:
            payload_hash = ids.payload_hash(mutations)
            existing = conn.execute(f"SELECT payload_hash, row_count, versions_json FROM {STREAM_PUBLICATIONS_TABLE} WHERE publication_id = ?", [publication_id]).fetchone()
            if existing is not None:
                if existing[0] != payload_hash:
                    raise PublicationConflict(publication_id)
                return PublicationReceipt(publication_id, payload_hash, existing[1], json.loads(existing[2]), True)
            normalized = ids.normalize_mutations(mutations)
            if not normalized.num_rows:
                raise ValueError("a publication requires at least one mutation")
            frame = pl.from_arrow(normalized).with_columns(pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None))
            refs = sorted(frame["ref_uri"].unique().to_list())
            conn.register("_acq_publication_refs", pl.DataFrame({"ref_uri": refs}))
            try:
                conn.execute(f"INSERT INTO {REF_IDS_TABLE} (ref_uri) SELECT ref_uri FROM _acq_publication_refs ON CONFLICT (ref_uri) DO NOTHING")
                ref_ids = dict(conn.execute(f"SELECT ref_uri, ref_id FROM {REF_IDS_TABLE} WHERE ref_uri IN (SELECT ref_uri FROM _acq_publication_refs)").fetchall())
            finally:
                conn.unregister("_acq_publication_refs")
            ids_frame = pl.DataFrame({"ref_uri": list(ref_ids), "ref_id": list(ref_ids.values())})
            ordered_ids = sorted(ref_ids.values())
            conn.register("_acq_publication_heads", pl.DataFrame({"ref_id": ordered_ids}))
            try:
                conn.execute(f"INSERT INTO {STREAM_HEADS_TABLE} (ref_id, current_version, retained_from_version) SELECT ref_id, 0, 0 FROM _acq_publication_heads ON CONFLICT (ref_id) DO NOTHING")
                previous = dict(conn.execute(f"SELECT ref_id, current_version FROM {STREAM_HEADS_TABLE} WHERE ref_id IN (SELECT ref_id FROM _acq_publication_heads)").fetchall())
            finally:
                conn.unregister("_acq_publication_heads")
            versions_by_id = {ref_id: previous[ref_id] + 1 for ref_id in ordered_ids}
            conn.register("_acq_publication_versions", pl.DataFrame({"ref_id": list(versions_by_id), "version": list(versions_by_id.values())}))
            try:
                conn.execute(f"UPDATE {STREAM_HEADS_TABLE} SET current_version = update_values.version FROM _acq_publication_versions update_values WHERE {STREAM_HEADS_TABLE}.ref_id = update_values.ref_id")
            finally:
                conn.unregister("_acq_publication_versions")
            frame = frame.join(ids_frame, on="ref_uri").with_columns(pl.col("ref_id").replace_strict(versions_by_id).alias("last_stream_version"))
            frame = frame.with_columns([
                pl.when(pl.col("operation") == "delete").then(None).otherwise(pl.col("numeric_value")).alias("numeric_value"),
                pl.when(pl.col("operation") == "delete").then(None).otherwise(pl.col("text_value")).alias("text_value"),
                (pl.col("operation") == "delete").alias("deleted"),
            ])
            writes = frame.select(["ref_id", "ts", "numeric_value", "text_value", "deleted", "last_stream_version"])
            conn.register("_acq_publication_writes", writes)
            try:
                conn.execute(f"DELETE FROM {TIMESERIES_TABLE} USING _acq_publication_writes writes WHERE {TIMESERIES_TABLE}.ref_id = writes.ref_id AND {TIMESERIES_TABLE}.ts = writes.ts")
                conn.execute(f"INSERT INTO {TIMESERIES_TABLE} (ref_id, ts, numeric_value, text_value, deleted, last_stream_version) SELECT ref_id, ts, numeric_value, text_value, deleted, last_stream_version FROM _acq_publication_writes")
            finally:
                conn.unregister("_acq_publication_writes")
            versions = {ref: versions_by_id[ref_id] for ref, ref_id in ref_ids.items()}
            ranges = normalize_change_ranges(publication_id=publication_id, stream_versions=versions, changes=zip(
                normalized.column("ref_uri").to_pylist(), normalized.column("ts").to_pylist(), normalized.column("operation").to_pylist()))
            conn.executemany("""INSERT INTO stream_change_ranges
                (ref_uri, stream_version, publication_id, start_ts, end_ts, change_kind, row_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)""", [(item.ref_uri, item.stream_version, item.publication_id,
                item.interval.start.replace(tzinfo=None), item.interval.end.replace(tzinfo=None), item.change_kind, item.row_count) for item in ranges])
            sequence = conn.execute(f"SELECT nextval('{STREAM_PUBLICATIONS_SEQ}')").fetchone()[0]
            conn.execute(f"INSERT INTO {STREAM_PUBLICATIONS_TABLE} (publication_seq, publication_id, payload_hash, row_count, versions_json, committed_at) VALUES (?, ?, ?, ?, ?, ?)", [sequence, publication_id, payload_hash, frame.height, json.dumps(versions), datetime.now(timezone.utc).replace(tzinfo=None)])
            return PublicationReceipt(publication_id, payload_hash, frame.height, versions)
