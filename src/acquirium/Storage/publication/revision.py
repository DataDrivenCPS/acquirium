"""Canonical revisioned writes for the materialization runtime."""
from __future__ import annotations

import polars as pl

from acquirium.Storage.publication import ids
from acquirium.Storage.publication.types import PublicationReceipt, PublicationRequest


class RevisionPublisher:
    """Hand one normalized upsert frame to the engine's inbox.

    The receipt's ``versions`` carries the inbox sequence number the request
    was recorded under. Rows reach ``timeseries`` when a step applies that
    entry; with a non-deferred engine that has happened by the time this
    returns. This adapter keeps no receipt ledger, so ``publication_id``
    labels the receipt but does not deduplicate retries.
    """

    def __init__(self, engine: object) -> None:
        self._engine = engine

    def publish(self, request: PublicationRequest) -> PublicationReceipt:
        mutations = ids.normalize_mutations(request.mutations)
        if not mutations.num_rows:
            return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), 0, {})
        frame = pl.from_arrow(mutations)
        if (frame["operation"] != "upsert").any():
            raise ValueError("deletion is not supported by incremental materialization")
        seq = self._engine.submit("readings", mutations, publication_id=request.publication_id)
        return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), mutations.num_rows, {"seq": seq})

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
        seq = self._engine.submit("replace", mutations, ref_uri=ref_uri, publication_id=request.publication_id)
        return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), mutations.num_rows, {"seq": seq})
