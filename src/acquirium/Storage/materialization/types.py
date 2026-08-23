"""Backend-neutral durable models for revision-aware materialization."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal
from acquirium.Materialization.impact import TimeRange

ChangeKind = Literal["upsert", "delete", "mixed"]

# A partition is retried at most this many times before it is dead-lettered,
# so a deterministically failing transform cannot be re-leased forever.
MAX_PARTITION_ATTEMPTS = 8

@dataclass(frozen=True)
class StreamChangeRange:
    ref_uri: str
    stream_version: int
    publication_id: str
    interval: TimeRange
    change_kind: ChangeKind
    row_count: int
    def __post_init__(self) -> None:
        if not self.ref_uri or not self.publication_id or self.stream_version < 1 or self.row_count < 1:
            raise ValueError("invalid stream change range")
