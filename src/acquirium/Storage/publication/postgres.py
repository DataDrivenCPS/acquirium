"""PostgreSQL implementation of canonical versioned publication."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import polars as pl
from psycopg_pool import ConnectionPool

from acquirium.Storage.materialization.ids import normalize_change_ranges
from acquirium.Storage.publication import ids
from acquirium.Storage.publication.types import PublicationConflict, PublicationReceipt, PublicationRequest
from acquirium.Storage.timescale_store import STREAM_HEADS_TABLE, STREAM_PUBLICATIONS_TABLE, TIMESERIES_TABLE


class PublicationPostgres:
    """Atomic, idempotent publications using sorted stream-head row locks."""
    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        self._pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)

    def close(self) -> None:
        self._pool.close()

    def publish(self, request: PublicationRequest) -> PublicationReceipt:
        with self._pool.connection() as conn, conn.transaction():
            return self._apply_publication(conn.cursor(), request.publication_id, request.mutations)

    def _apply_publication(self, conn, publication_id: str, mutations) -> PublicationReceipt:
        payload_hash = ids.payload_hash(mutations)
        normalized = ids.normalize_mutations(mutations)
        if not normalized.num_rows:
            raise ValueError("a publication requires at least one mutation")
        frame = pl.from_arrow(normalized)
        refs = sorted(frame["ref_uri"].unique().to_list())
        existing = conn.execute(f"SELECT payload_hash, row_count, versions_json FROM {STREAM_PUBLICATIONS_TABLE} WHERE publication_id = %s", [publication_id]).fetchone()
        if existing is not None:
            if existing[0] != payload_hash:
                raise PublicationConflict(publication_id)
            versions = existing[2] if isinstance(existing[2], dict) else json.loads(existing[2])
            return PublicationReceipt(publication_id, payload_hash, existing[1], versions, True)
        conn.execute(f"INSERT INTO {STREAM_HEADS_TABLE} (ref_uri, current_version, retained_from_version) SELECT unnest(%s::text[]), 0, 0 ON CONFLICT (ref_uri) DO NOTHING", [refs])
        current = dict(conn.execute(f"SELECT ref_uri, current_version FROM {STREAM_HEADS_TABLE} WHERE ref_uri = ANY(%s::text[]) ORDER BY ref_uri FOR UPDATE", [refs]).fetchall())
        versions = {ref: current[ref] + 1 for ref in refs}
        conn.execute(f"""UPDATE {STREAM_HEADS_TABLE} AS head SET current_version = next.version
            FROM (SELECT unnest(%s::text[]) AS ref_uri, unnest(%s::bigint[]) AS version) next
            WHERE head.ref_uri = next.ref_uri""", [refs, [versions[ref] for ref in refs]])
        frame = frame.with_columns([pl.col("ref_uri").replace_strict(versions).alias("last_stream_version"), pl.when(pl.col("operation") == "delete").then(None).otherwise(pl.col("numeric_value")).alias("numeric_value"), pl.when(pl.col("operation") == "delete").then(None).otherwise(pl.col("text_value")).alias("text_value"), (pl.col("operation") == "delete").alias("deleted")])
        conn.execute(f"""INSERT INTO {TIMESERIES_TABLE} (ref_uri, ts, numeric_value, text_value, deleted, last_stream_version)
            SELECT * FROM unnest(%s::text[], %s::timestamptz[], %s::double precision[], %s::text[], %s::boolean[], %s::bigint[])
            ON CONFLICT (ref_uri, ts) DO UPDATE SET numeric_value = excluded.numeric_value, text_value = excluded.text_value, deleted = excluded.deleted, last_stream_version = excluded.last_stream_version""", [frame["ref_uri"].to_list(), frame["ts"].to_list(), frame["numeric_value"].to_list(), frame["text_value"].to_list(), frame["deleted"].to_list(), frame["last_stream_version"].to_list()])
        ranges = normalize_change_ranges(publication_id=publication_id, stream_versions=versions, changes=zip(normalized.column("ref_uri").to_pylist(), normalized.column("ts").to_pylist(), normalized.column("operation").to_pylist()))
        conn.executemany("""INSERT INTO stream_change_ranges (ref_uri, stream_version, publication_id, start_ts, end_ts, change_kind, row_count) VALUES (%s, %s, %s, %s, %s, %s, %s)""", [(item.ref_uri, item.stream_version, item.publication_id, item.interval.start, item.interval.end, item.change_kind, item.row_count) for item in ranges])
        conn.execute(f"INSERT INTO {STREAM_PUBLICATIONS_TABLE} (publication_id, payload_hash, row_count, versions_json, committed_at) VALUES (%s, %s, %s, %s, %s)", [publication_id, payload_hash, frame.height, json.dumps(versions), datetime.now(timezone.utc)])
        return PublicationReceipt(publication_id, payload_hash, frame.height, versions)
