"""Canonical revisioned writes for the materialization runtime."""
from __future__ import annotations

import polars as pl

from acquirium.Storage.publication import ids
from acquirium.Storage.publication.types import PublicationReceipt, PublicationRequest


class RevisionPublisher:
    """Publish one normalized upsert frame through the revision-writing seam.

    This adapter does not keep a receipt ledger, so ``publication_id`` labels
    the returned receipt but does not deduplicate retries.
    """

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
        """Replace one stream, including empty requests, at one atomic revision."""
        frame = pl.from_arrow(request.mutations)
        if (
            frame["operation"].is_null() | (frame["operation"] != "upsert")
            | frame["ref_uri"].is_null() | (frame["ref_uri"] != ref_uri)
        ).any():
            raise ValueError("replacement requires only upserts for the specified stream")
        if frame["ts"].null_count():
            raise ValueError("replacement timestamps must not be null")
        if (frame["numeric_value"].is_not_null() & frame["text_value"].is_not_null()).any():
            raise ValueError("replacement rows cannot contain both numeric and text values")
        mutations = ids.normalize_mutations(request.mutations)
        writes = self._store._prepare_frame(pl.from_arrow(mutations))
        row_count = self._store._replace_frame(ref_uri, writes)
        return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), row_count, {})
