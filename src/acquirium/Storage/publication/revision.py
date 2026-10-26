"""Canonical revisioned writes for the materialization runtime."""
from __future__ import annotations

import polars as pl

from acquirium.Storage.metadata_outbox import execute
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
        writes = self._store._prepare_frame(frame)
        with self._store._lock, self._store._write_conn() as conn:
            self.publish_frame(conn, writes)
        return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), writes.height, {})

    def publish_frame(self, conn, writes: pl.DataFrame) -> int | None:
        """Publish prepared samples in an existing transaction without commit."""
        if writes.is_empty():
            return None
        revision = self._store._next_revision(conn)
        self._store._insert_frame(conn, writes, revision)
        return revision

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

    def reset(self, conn, refs, *, revision=None) -> int | None:
        """Physically clear streams at one revision in the caller's transaction.

        The caller owns the writer lock and connection, commits or rolls back,
        and retains responsibility for registrations and other domain records.
        Empty batches do nothing; empty streams still receive reset markers.
        Pass an existing revision to join other writes at the same frontier.
        """
        refs = list(dict.fromkeys(refs))
        if not refs:
            return None
        revision = self._store._next_revision(conn) if revision is None else revision
        marks = ",".join("?" for _ in refs)
        execute(conn, f"DELETE FROM timeseries WHERE ref_id IN (SELECT ref_id FROM ref_ids WHERE ref_uri IN ({marks}))", refs)
        for ref in refs:
            execute(conn, "INSERT INTO stream_resets (ref_uri, last_revision) VALUES (?, ?) "
                    "ON CONFLICT (ref_uri) DO UPDATE SET last_revision=EXCLUDED.last_revision", [ref, revision])
        return revision
