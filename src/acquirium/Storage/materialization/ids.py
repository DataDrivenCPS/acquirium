"""Deterministic IDs and bounded range-manifest normalisation."""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Iterable
from acquirium.Materialization.impact import MICROSECOND, TimeRange, coalesce_ranges
from acquirium.Storage.materialization.types import ChangeKind, StreamChangeRange

def materialization_id(*parts: object) -> str:
    return sha256("".join(f"{len(str(p))}:{p}" for p in parts).encode()).hexdigest()

def bucket_range(timestamp: datetime, bucket: timedelta) -> TimeRange:
    if bucket < MICROSECOND:
        raise ValueError("range bucket must be at least one microsecond")
    timestamp = timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp.astimezone(timezone.utc)
    bucket_us = int(bucket.total_seconds() * 1_000_000)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = timestamp - epoch
    epoch_us = delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    start_us = epoch_us - epoch_us % bucket_us
    start = datetime.fromtimestamp(start_us / 1_000_000, tz=timezone.utc)
    return TimeRange(start, start + bucket)

def normalize_change_ranges(*, publication_id: str, stream_versions: dict[str, int], changes: Iterable[tuple[str, datetime, str]], bucket: timedelta = timedelta(minutes=1)) -> tuple[StreamChangeRange, ...]:
    per_stream: dict[str, list[TimeRange]] = defaultdict(list)
    bucket_counts: dict[str, dict[datetime, int]] = defaultdict(lambda: defaultdict(int))
    operations: dict[str, set[str]] = defaultdict(set)
    for ref_uri, timestamp, operation in changes:
        if operation not in {"upsert", "delete"}:
            raise ValueError(f"unknown mutation operation {operation!r}")
        interval = bucket_range(timestamp, bucket)
        per_stream[ref_uri].append(interval)
        bucket_counts[ref_uri][interval.start] += 1
        operations[ref_uri].add(operation)
    result: list[StreamChangeRange] = []
    for ref_uri, intervals in per_stream.items():
        if ref_uri not in stream_versions:
            raise ValueError(f"missing stream version for {ref_uri!r}")
        kind: ChangeKind = "mixed" if len(operations[ref_uri]) > 1 else next(iter(operations[ref_uri]))  # type: ignore[assignment]
        for interval in coalesce_ranges(intervals):
            count = sum(
                count for start, count in bucket_counts[ref_uri].items()
                if interval.start <= start < interval.end
            )
            result.append(StreamChangeRange(ref_uri, stream_versions[ref_uri], publication_id, interval, kind, count))
    return tuple(sorted(result, key=lambda item: (item.ref_uri, item.interval.start)))

def partition_ranges(ranges: Iterable[TimeRange], *, maximum_duration: timedelta) -> tuple[TimeRange, ...]:
    """Split dirty intervals without gaps or overlap, after normalizing them."""
    if maximum_duration < MICROSECOND:
        raise ValueError("partition duration must be at least one microsecond")
    result: list[TimeRange] = []
    for interval in coalesce_ranges(ranges):
        start = interval.start
        while start < interval.end:
            end = min(start + maximum_duration, interval.end)
            result.append(TimeRange(start, end))
            start = end
    return tuple(result)
