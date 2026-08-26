"""Canonical revisioned writes for the materialization runtime."""
from __future__ import annotations

from datetime import timezone

import polars as pl

from acquirium.Storage.publication import ids
from acquirium.Storage.publication.types import PublicationReceipt, PublicationRequest


class RevisionPublisher:
    """Publish one frame through the shared revision-writing store seam."""

    def __init__(self, store: object) -> None:
        self._store = store

    def publish(self, request: PublicationRequest) -> PublicationReceipt:
        mutations = ids.normalize_mutations(request.mutations)
        if not mutations.num_rows:
            return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), 0, {})
        frame = pl.from_arrow(mutations)
        if (frame["operation"] != "upsert").any():
            raise ValueError("deletion is not supported by incremental materialization")
        writes = frame.select(["ref_uri", "ts", "numeric_value", "text_value"]).with_columns(
            pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        ).unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
        with self._store._lock, self._store._write_conn() as conn:
            revision = self._store._next_revision(conn)
            self._store._insert_frame(conn, writes, revision)
        return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), writes.height, {})

    def replace(self, request: PublicationRequest, ref_uri: str) -> PublicationReceipt:
        raise ValueError("replace is not supported by incremental materialization")
