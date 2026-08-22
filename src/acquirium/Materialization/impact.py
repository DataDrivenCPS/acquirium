"""Serializable event-time impact policies and half-open range algebra."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Literal


UTC = timezone.utc
MICROSECOND = timedelta(microseconds=1)


def _utc(value: datetime) -> datetime:
    """Normalise a timestamp to UTC without silently losing precision."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(frozen=True, order=True)
class TimeRange:
    """An event-time interval ``[start, end)`` in UTC microseconds."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        start, end = _utc(self.start), _utc(self.end)
        if end <= start:
            raise ValueError("a time range must have end after start")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)

    @classmethod
    def point(cls, timestamp: datetime) -> "TimeRange":
        timestamp = _utc(timestamp)
        return cls(timestamp, timestamp + MICROSECOND)

    def intersects(self, other: "TimeRange") -> bool:
        return self.start < other.end and other.start < self.end

    def touches(self, other: "TimeRange") -> bool:
        return self.start <= other.end and other.start <= self.end


def coalesce_ranges(ranges: Iterable[TimeRange]) -> tuple[TimeRange, ...]:
    """Return sorted, non-overlapping ranges, merging adjacent intervals."""
    ordered = sorted(ranges)
    if not ordered:
        return ()
    result = [ordered[0]]
    for current in ordered[1:]:
        previous = result[-1]
        if previous.end >= current.start:
            result[-1] = TimeRange(previous.start, max(previous.end, current.end))
        else:
            result.append(current)
    return tuple(result)


@dataclass(frozen=True)
class ImpactPolicy:
    """A storage-evaluable mapping from changed input ranges to dirty output.

    ``before`` and ``after`` name the direction of the *input* an output reads,
    not the direction the dirty range grows:

    - ``before`` is how far back from an output timestamp the transform reads
      input (a lookback). An input change therefore dirties output up to
      ``before`` *later*, extending the dirty range's END forward.
    - ``after`` is how far ahead of an output timestamp the transform reads
      input (a look-ahead). An input change therefore dirties output up to
      ``after`` *earlier*, extending the dirty range's START backward.

    Concretely, ``affected()`` returns
    ``[changed.start - after, changed.end + before]``.
    """

    kind: Literal["pointwise", "lookback", "window", "full_history"]
    before: timedelta = timedelta()
    after: timedelta = timedelta()

    def __post_init__(self) -> None:
        if self.before < timedelta() or self.after < timedelta():
            raise ValueError("impact durations must not be negative")
        if self.kind == "pointwise" and (self.before or self.after):
            raise ValueError("pointwise impact has no expansion")
        if self.kind == "full_history" and (self.before or self.after):
            raise ValueError("full-history impact has no expansion")

    def affected(self, changed: TimeRange, *, retained: TimeRange | None = None) -> TimeRange:
        """Map an input change to its dirty output range.

        Returns ``[changed.start - after, changed.end + before]`` (see the class
        docstring for the field naming). ``full_history`` requires the retained
        boundary, because the scheduler deliberately owns retention knowledge
        rather than user code.
        """
        if self.kind == "full_history":
            if retained is None:
                raise ValueError("full-history impact requires a retained range")
            return retained
        return TimeRange(changed.start - self.after, changed.end + self.before)

    def to_json(self) -> dict[str, object]:
        return {"kind": self.kind, "before_us": int(self.before.total_seconds() * 1_000_000), "after_us": int(self.after.total_seconds() * 1_000_000)}

    @classmethod
    def from_json(cls, value: dict[str, object]) -> "ImpactPolicy":
        return cls(
            kind=value["kind"],  # type: ignore[arg-type]
            before=timedelta(microseconds=int(value.get("before_us", 0))),
            after=timedelta(microseconds=int(value.get("after_us", 0))),
        )


def pointwise() -> ImpactPolicy:
    return ImpactPolicy("pointwise")


def lookback(duration: timedelta) -> ImpactPolicy:
    """A transform that reads ``duration`` of input before each output timestamp.

    An input change dirties output up to ``duration`` later, so the policy is
    stored as ``before=duration`` (the dirty range's END extends forward).
    """
    return ImpactPolicy("lookback", before=duration)


def window(*, before: timedelta, after: timedelta) -> ImpactPolicy:
    """A transform reading input ``before`` prior to and ``after`` past each output.

    The dirty range becomes ``[changed.start - after, changed.end + before]``.
    """
    return ImpactPolicy("window", before=before, after=after)


def full_history() -> ImpactPolicy:
    return ImpactPolicy("full_history")
