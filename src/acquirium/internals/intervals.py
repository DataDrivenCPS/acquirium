"""Times and interval sets on the event-time axis.

Times are integer microseconds since the epoch in UTC. ``NEG_INF`` and
``POS_INF`` stand for the unbounded ends. Intervals are half-open
``(start, end)`` pairs; an interval set is a sorted list of disjoint,
non-touching intervals, the form ``merge`` returns. The functions here accept
any iterable of intervals and return interval sets.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Hashable, Iterable, Mapping
from datetime import datetime, timedelta, timezone

NEG_INF = -(2**63)
POS_INF = 2**63 - 1

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

Interval = tuple[int, int]


def to_micros(value: datetime) -> int:
    """Microseconds since the epoch; a naive datetime is taken as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def from_micros(value: int) -> datetime | str:
    """The inverse of ``to_micros``; the unbounded ends become strings."""
    if value == NEG_INF:
        return "-infinity"
    if value == POS_INF:
        return "infinity"
    return _EPOCH + timedelta(microseconds=value)


def now_micros() -> int:
    return to_micros(datetime.now(timezone.utc))


def merge(intervals: Iterable[Interval]) -> list[Interval]:
    """Sorted disjoint union; touching intervals join."""
    out: list[Interval] = []
    for start, end in sorted(intervals):
        if start >= end:
            continue
        if out and start <= out[-1][1]:
            if end > out[-1][1]:
                out[-1] = (out[-1][0], end)
        else:
            out.append((start, end))
    return out


def _sweep(sets: Iterable[Iterable[Interval]], keep) -> list[Interval]:
    """The points whose number of covering sets satisfies ``keep``."""
    edges: dict[int, int] = defaultdict(int)
    for intervals in sets:
        for start, end in merge(intervals):
            edges[start] += 1
            edges[end] -= 1
    out: list[Interval] = []
    depth, left = 0, None
    for point in sorted(edges):
        depth += edges[point]
        if keep(depth):
            if left is None:
                left = point
        elif left is not None:
            out.append((left, point))
            left = None
    return merge(out)


def symmetric_difference(a: Iterable[Interval], b: Iterable[Interval]) -> list[Interval]:
    """The points covered by exactly one of *a* and *b*."""
    return _sweep((a, b), lambda depth: depth == 1)


def intersect(a: Iterable[Interval], b: Iterable[Interval]) -> list[Interval]:
    return _sweep((a, b), lambda depth: depth == 2)


def subtract(a: Iterable[Interval], b: Iterable[Interval]) -> list[Interval]:
    """The points of *a* that are not in *b*."""
    a = merge(a)
    return intersect(a, symmetric_difference(a, b))


def run_at(intervals: Iterable[Interval], at: int) -> Interval | None:
    """The maximal interval of the set that contains *at*, if any."""
    for start, end in merge(intervals):
        if start <= at < end:
            return (start, end)
        if start > at:
            break
    return None


def _shift(value: int, by: int) -> int:
    if value in (NEG_INF, POS_INF):
        return value
    return max(NEG_INF, min(POS_INF, value + by))


def expand(intervals: Iterable[Interval], *, lookback: int, lookahead: int) -> list[Interval]:
    """The output times whose window touches the set.

    An output at ``t`` reads ``[t - lookback, t + lookahead]``. It touches
    ``[start, end)`` if and only if ``t`` lies in
    ``[start - lookahead, end + lookback)``. ``lookahead`` may be negative (a
    delay); ``POS_INF`` as ``lookback`` reads all history. The window must not
    be empty: ``lookback + lookahead >= 0``.
    """
    if lookback + lookahead < 0:
        raise ValueError("a window must not be empty")
    after = POS_INF if lookback == POS_INF else lookback
    out = []
    for start, end in merge(intervals):
        out.append((_shift(start, -lookahead), POS_INF if after == POS_INF else _shift(end, after)))
    return merge(out)


def lives(pieces: Iterable[tuple[int, int, Iterable[Hashable]]]) -> dict[Hashable, list[Interval]]:
    """Where each key is present, given disjoint pieces ``(start, end, keys)``.

    Adjacent pieces that both hold a key join, so a key's intervals are its
    maximal runs.
    """
    found: dict[Hashable, list[Interval]] = defaultdict(list)
    for start, end, keys in pieces:
        for key in keys:
            found[key].append((start, end))
    return {key: merge(intervals) for key, intervals in found.items()}


def pieces_of(values: Mapping[Hashable, Iterable[Interval]]) -> list[tuple[int, int, frozenset]]:
    """The inverse of ``lives`` over the covered points: maximal intervals on
    which the set of present keys is constant, with that set."""
    edges: dict[int, list[tuple[Hashable, int]]] = defaultdict(list)
    for key, intervals in values.items():
        for start, end in merge(intervals):
            edges[start].append((key, 1))
            edges[end].append((key, -1))
    out: list[tuple[int, int, frozenset]] = []
    present: set[Hashable] = set()
    left = None
    for point in sorted(edges):
        if left is not None and present:
            if out and out[-1][1] == left and out[-1][2] == frozenset(present):
                out[-1] = (out[-1][0], point, out[-1][2])
            else:
                out.append((left, point, frozenset(present)))
        for key, step in edges[point]:
            if step > 0:
                present.add(key)
            else:
                present.discard(key)
        left = point
    return out
