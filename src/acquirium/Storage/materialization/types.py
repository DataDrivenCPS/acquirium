"""Backend-neutral durable models for revision-aware materialization."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, Sequence
import pyarrow as pa
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

@dataclass(frozen=True)
class GraphRevision:
    graph_revision: int
    source_version: int
    content_digest: str
    published_at: datetime

@dataclass(frozen=True)
class PlanPartition:
    """An independently replaceable, half-open materialization work range."""
    partition_id: str
    plan_id: str
    interval: TimeRange
    status: Literal["pending", "leased", "committed", "failed"] = "pending"

@dataclass(frozen=True)
class WorkLease:
    """An operational lease, deliberately separate from semantic work IDs."""
    partition: PlanPartition
    owner: str
    attempt: int
    expires_at: datetime

@dataclass(frozen=True)
class InputSnapshot:
    """Pinned Arrow input and the stream-head vector observed for an attempt."""
    lease: WorkLease
    inputs: pa.Table
    input_versions: dict[str, int]

class RangeManifestStore(Protocol):
    def record_change_ranges(self, ranges: Sequence[StreamChangeRange]) -> None: ...
    def change_ranges(self, ref_uri: str, *, after_version: int, through_version: int) -> Sequence[StreamChangeRange]: ...
