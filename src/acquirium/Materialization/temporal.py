"""Instances over time, the view graph they imply, and the one-writer check.

A view's inputs are an optional query and a set of streams named directly.
The query has rows at each time; a row's life is where it is a row, over all
of time. This module turns rows with lives into instances and checks them,
with no I/O:

- with a query, ``per_match`` gives one instance per row and ``all_matches``
  one over all rows; without a query there is one instance. An instance is
  present where its definition is deployed (and, per match, where its row
  exists), and it writes only where present. Named inputs are shared by all
  instances and never decide which instances exist;
- the view graph has an edge ``W -> V`` labelled with the lookahead with
  which ``V`` reads a stream that ``W`` writes, at any times; ``V`` selects
  ``W`` when its query binds a generated output of ``W``;
- at most one present instance writes a stream at a time.

Streams are identified by their reference URIs. Times are integer
microseconds; intervals are half-open.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from acquirium.internals.intervals import NEG_INF, POS_INF, Interval, intersect, merge

ALL_TIME: tuple[Interval, ...] = ((NEG_INF, POS_INF),)


@dataclass(frozen=True)
class Row:
    """One row of a view's query: its key, the streams each column binds,
    its life, and, for a per-match instance, the streams of its generated
    ports."""
    key: str
    streams: Mapping[str, tuple[str, ...]]
    life: tuple[Interval, ...]
    outputs: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ViewDefinition:
    """What the temporal plan needs of one definition of a view.

    ``grouping`` is ``per_match``, ``all_matches``, or ``None`` for a view
    without a query. ``deployed`` are the intervals over which this
    definition holds. ``named_inputs`` maps input names to the streams read
    directly. ``lookahead`` gives every input's lookahead (negative: a
    delay). ``outputs`` maps the named ports, and the generated ports of the
    single instance of an ``all_matches`` or query-less view, to their
    streams; ``generated`` names the generated ports.
    """
    view: str
    text: str
    grouping: str | None
    deployed: tuple[Interval, ...]
    rows: tuple[Row, ...] = ()
    named_inputs: Mapping[str, str] = field(default_factory=dict)
    lookahead: Mapping[str, int] = field(default_factory=dict)
    outputs: Mapping[str, str] = field(default_factory=dict)
    generated: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Instance:
    view: str
    definition: str
    rows: tuple[Row, ...]
    present: tuple[Interval, ...]
    outputs: Mapping[str, str]
    match: Row | None = None

    def written(self) -> dict[str, list[Interval]]:
        """Each written stream with the times this instance writes it."""
        return {stream: list(self.present) for stream in self.outputs.values()}

    def reads(self, named_inputs: Mapping[str, str]) -> list[tuple[str, str, tuple[Interval, ...]]]:
        """``(input, stream, life)`` for every stream this instance reads; the
        life is where a read of it is not clipped away."""
        out = []
        for row in self.rows:
            for column, streams in row.streams.items():
                out.extend((column, stream, row.life) for stream in streams)
        named_life = self.match.life if self.match is not None else ALL_TIME
        out.extend((name, stream, named_life) for name, stream in named_inputs.items())
        return out


def instances(definition: ViewDefinition) -> list[Instance]:
    """The instances of one definition, each with where it is present."""
    named = {port: stream for port, stream in definition.outputs.items() if port not in definition.generated}
    if definition.grouping == "per_match":
        out = []
        for row in definition.rows:
            present = intersect(row.life, definition.deployed)
            if present:
                out.append(Instance(definition.view, definition.text, (row,), tuple(present),
                                    {**named, **row.outputs}, match=row))
        return out
    present = merge(definition.deployed)
    if not present:
        return []
    return [Instance(definition.view, definition.text, tuple(definition.rows), tuple(present),
                     dict(definition.outputs))]


@dataclass(frozen=True)
class ViewGraph:
    """``reads`` are ``(writer, reader, lookahead)`` edges for admission;
    ``selects`` are ``(reader, writer)`` pairs where the reader's query binds a
    generated output of the writer."""
    reads: tuple[tuple[str, str, int], ...]
    selects: tuple[tuple[str, str], ...]


def view_graph(definitions: Iterable[ViewDefinition]) -> ViewGraph:
    """The view graph over every time and definition."""
    definitions = list(definitions)
    writers: dict[str, set[str]] = defaultdict(set)
    generated: dict[str, set[str]] = defaultdict(set)
    for definition in definitions:
        for instance in instances(definition):
            for port, stream in instance.outputs.items():
                writers[stream].add(definition.view)
                if port in definition.generated:
                    generated[stream].add(definition.view)
    reads, selects = set(), set()
    for definition in definitions:
        for row in definition.rows:
            for column, streams in row.streams.items():
                for stream in streams:
                    for writer in writers.get(stream, ()):
                        reads.add((writer, definition.view, definition.lookahead.get(column, 0)))
                    for writer in generated.get(stream, ()):
                        selects.add((definition.view, writer))
        for name, stream in definition.named_inputs.items():
            for writer in writers.get(stream, ()):
                reads.add((writer, definition.view, definition.lookahead.get(name, 0)))
    return ViewGraph(tuple(sorted(reads)), tuple(sorted(selects)))


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
