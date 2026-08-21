"""Backend-neutral canonical publication contract."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa


MUTATION_SCHEMA = pa.schema([
    pa.field("operation", pa.string()),
    pa.field("ref_uri", pa.string()),
    pa.field("ts", pa.timestamp("us", tz="UTC")),
    pa.field("numeric_value", pa.float64()),
    pa.field("text_value", pa.string()),
])


class PublicationConflict(ValueError):
    """A stable publication id was retried with different mutations."""
    def __init__(self, publication_id: str):
        super().__init__(f"publication {publication_id!r} was already committed with a different payload")
        self.publication_id = publication_id


@dataclass(frozen=True)
class PublicationRequest:
    publication_id: str
    mutations: pa.Table


@dataclass(frozen=True)
class PublicationReceipt:
    publication_id: str
    payload_hash: str
    row_count: int
    versions: dict[str, int]
    deduplicated: bool = False


class PublicationStore(Protocol):
    def publish(self, request: PublicationRequest) -> PublicationReceipt: ...
