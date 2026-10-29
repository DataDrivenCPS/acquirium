"""The view set over time.

The view set changes like the model graphs: a deployment change gives a view
name a definition over an effective interval ``[start, end)``, or no
definition (``None``) to remove it there, and Acquirium keeps every change.
The definition a name has at a time is the one of the latest-recorded change
whose interval contains that time. Deploying from ``d`` is a change over
``[d, +inf)``; removing at ``e`` is ``None`` over ``[e, +inf)``, which keeps
the outputs before ``e``; purging is ``None`` over all time.

Definitions are compared as text, so a redeployment of the same text is no
change. Times are integer microseconds; recording times ``r`` increase.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from acquirium.internals.intervals import Interval, merge


@dataclass(frozen=True)
class DeploymentChange:
    r: int
    name: str
    definition: str | None
    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError("a deployment change needs start < end")


def _pieces(changes: Iterable[DeploymentChange]) -> list[tuple[int, int, str | None]]:
    """Elementary intervals between the endpoints of one name's changes, each
    with the definition of the latest-recorded change covering it."""
    changes = sorted(changes, key=lambda change: change.r)
    points = sorted({p for change in changes for p in (change.start, change.end)})
    out: list[tuple[int, int, str | None]] = []
    for left, right in zip(points, points[1:]):
        definition = None
        for change in changes:
            if change.start <= left < change.end:
                definition = change.definition
        if out and out[-1][1] == left and out[-1][2] == definition:
            out[-1] = (out[-1][0], right, definition)
        else:
            out.append((left, right, definition))
    return out


def deployment(changes: Iterable[DeploymentChange], name: str, *, as_of: int | None = None) -> list[tuple[int, int, str]]:
    """The maximal intervals over which ``name`` has one definition, with that
    definition, as known at recording time ``as_of`` (the latest if omitted)."""
    own = [c for c in changes if c.name == name and (as_of is None or c.r <= as_of)]
    return [(left, right, definition) for left, right, definition in _pieces(own) if definition is not None]


def definition_at(changes: Iterable[DeploymentChange], name: str, at: int, *, as_of: int | None = None) -> str | None:
    for left, right, definition in deployment(changes, name, as_of=as_of):
        if left <= at < right:
            return definition
    return None


def names(changes: Iterable[DeploymentChange]) -> list[str]:
    return sorted({change.name for change in changes})


def changed(changes: Iterable[DeploymentChange], r: int) -> dict[str, list[Interval]]:
    """Where the changes recorded at ``r`` changed the view set: per name, the
    intervals over which its definition differs before and after them."""
    changes = list(changes)
    regions: dict[str, list[Interval]] = {}
    for name in sorted({c.name for c in changes if c.r == r}):
        before = deployment(changes, name, as_of=r - 1)
        after = deployment(changes, name, as_of=r)
        points = sorted({p for left, right, _ in before + after for p in (left, right)})
        region = [
            (left, right)
            for left, right in zip(points, points[1:])
            if _at(before, left) != _at(after, left)
        ]
        if region:
            regions[name] = merge(region)
    return regions


def _at(pieces: list[tuple[int, int, str]], at: int) -> str | None:
    for left, right, definition in pieces:
        if left <= at < right:
            return definition
    return None
