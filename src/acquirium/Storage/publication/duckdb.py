"""Canonical revisioned writes for the materialization runtime.

Despite the historical module name, this is deliberately backend-neutral.  Both
stores expose the same tiny write seam: allocate a revision and insert a frame
in one transaction.  Keeping that logic here prevents the manager from
maintaining a second DuckDB/Postgres write path.
"""
from __future__ import annotations

from datetime import timezone

import polars as pl

from acquirium.Storage.publication import ids
from acquirium.Storage.publication.types import PublicationReceipt, PublicationRequest


class PublicationDuckDB:
    def __init__(self, store: object) -> None: self._store = store

    def publish(self, request: PublicationRequest) -> PublicationReceipt:
        mutations = ids.normalize_mutations(request.mutations)
        if not mutations.num_rows:
            return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), 0, {})
        frame = pl.from_arrow(mutations)
        if (frame["operation"] != "upsert").any():
            raise ValueError("deletion is not supported by incremental materialization")
        # Collapse repeated keys before allocating a revision: within one
        # publication, the final mutation is the only observable value.
        writes = frame.select(["ref_uri", "ts", "numeric_value", "text_value"]).with_columns(
            pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        ).unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
        with self._store._lock, self._store._write_conn() as conn:
            # Revision allocation and data mutation must commit together, or a
            # reader could advance past a write it was never able to observe.
            revision = self._store._next_revision(conn)
            self._store._insert_frame(conn, writes, revision)
        return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), writes.height, {})

    def replace(self, request: PublicationRequest, ref_uri: str) -> PublicationReceipt:
        # Replace is deletion plus upsert and deliberately absent from v1.
        raise ValueError("replace is not supported by incremental materialization")
