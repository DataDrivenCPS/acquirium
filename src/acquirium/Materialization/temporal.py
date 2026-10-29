"""Instances over time, the view graph they imply, and the one-writer check.

A view's query has rows at each time; a row's life is where it is a row, over
all of time. Every input and output of a view is a column of those rows: the
view reads some stream columns (each with a window) and writes others. This
module turns rows with lives into instances and checks them, with no I/O:

- an instance is a row (``per_match``) or all rows (``all_matches``) under
  one definition; it is present where that definition is deployed and its
  rows exist, and it writes only where present;
- the view graph has an edge ``W -> V`` labelled with the lookahead with
  which ``V`` reads a stream that ``W`` writes, at any times;
- at most one present instance writes a stream at a time.

Times are integer microseconds; intervals are half-open.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from acquirium.internals.intervals import Interval, intersect, merge


@dataclass(frozen=True)
class Row:
    """One row of a view's query: its key, the streams each column binds,
    and its life."""
    key: str
    streams: Mapping[str, tuple[str, ...]]
    life: tuple[Interval, ...]


@dataclass(frozen=True)
class ViewDefinition:
    """What the temporal plan needs of one definition of a view.

    ``deployed`` are the intervals over which this definition holds.
    ``writes`` names the written columns; ``lookahead`` gives each read
    column's lookahead (negative: a delay). A column may be in both.
    """
    view: str
    text: str
    grouping: str
    deployed: tuple[Interval, ...]
    rows: tuple[Row, ...]
    writes: frozenset[str]
    lookahead: Mapping[str, int]


@dataclass(frozen=True)
class Instance:
    view: str
    definition: str
    rows: tuple[Row, ...]
    present: tuple[Interval, ...]
    writes: frozenset[str] = frozenset()

    def written(self) -> dict[str, list[Interval]]:
        """Each written stream with the times this instance writes it: where
        it is present and the row that binds the stream exists."""
        out: dict[str, list[Interval]] = defaultdict(list)
        for row in self.rows:
            for column, streams in row.streams.items():
                if column in self.writes:
                    for stream in streams:
                        out[stream].extend(intersect(row.life, self.present))
        return {stream: merge(times) for stream, times in out.items() if merge(times)}


def instances(definition: ViewDefinition) -> list[Instance]:
    """The instances of one definition, each with where it is present."""
    out = []
    if definition.grouping == "per_match":
        for row in definition.rows:
            present = intersect(row.life, definition.deployed)
            if present:
                out.append(Instance(definition.view, definition.text, (row,), tuple(present), definition.writes))
    else:
        rows = tuple(definition.rows)
        present = intersect([span for row in rows for span in row.life], definition.deployed)
        if present:
            out.append(Instance(definition.view, definition.text, rows, tuple(present), definition.writes))
    return out


def reads(definitions: Iterable[ViewDefinition]) -> list[tuple[str, str, int]]:
    """The edges of the view graph as ``(writer, reader, lookahead)``.

    Reading and writing are compared stream by stream over all time: an
    edge exists when some row of the reader reads a stream that some
    present instance of the writer writes.
    """
    definitions = list(definitions)
    writers: dict[str, set[str]] = defaultdict(set)
    for definition in definitions:
        for instance in instances(definition):
            for stream in instance.written():
                writers[stream].add(definition.view)
    out = set()
    for definition in definitions:
        for row in definition.rows:
            for column, lookahead in definition.lookahead.items():
                for stream in row.streams.get(column, ()):
                    for writer in writers.get(stream, ()):
                        out.add((writer, definition.view, lookahead))
    return sorted(out)


@dataclass(frozen=True)
class WriterConflict:
    stream: str
    views: tuple[str, ...]
    times: tuple[Interval, ...]

    def __str__(self) -> str:
        return (f"stream {self.stream} would be written by more than one instance "
                f"({', '.join(self.views)}) over {list(self.times)}")


def writer_conflicts(definitions: Iterable[ViewDefinition]) -> list[WriterConflict]:
    """Streams that two present instances write at the same time."""
    by_stream: dict[str, list[tuple[str, list[Interval]]]] = defaultdict(list)
    for definition in definitions:
        for instance in instances(definition):
            for stream, times in instance.written().items():
                by_stream[stream].append((definition.view, times))
    out = []
    for stream, writers in sorted(by_stream.items()):
        overlaps: list[Interval] = []
        views: set[str] = set()
        for i, (left_view, left) in enumerate(writers):
            for right_view, right in writers[i + 1:]:
                common = intersect(left, right)
                if common:
                    overlaps.extend(common)
                    views.update((left_view, right_view))
        if overlaps:
            out.append(WriterConflict(stream, tuple(sorted(views)), tuple(merge(overlaps))))
    return out
