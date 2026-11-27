"""Declarations, messages, and input snapshots for soft sensor actors."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import re
from typing import Generic, Literal, TypeVar

from escapement import NotFound, Snapshot
import polars as pl
import pyarrow as pa

from acquirium.Client.explore.core import Query
from acquirium.Client.streams import Stream
from acquirium.app_spec import duration_ns
from acquirium.core import time_ns

State = TypeVar("State")
Timestamp = datetime | int | str
Value = int | float | str

_READING_SCHEMA = {"time": pl.Datetime("ns", "UTC"), "value": pl.Float64, "text": pl.String}
_STREAM_SCHEMA = {"stream_uri": pl.String, **_READING_SCHEMA}
_QUERY_SCHEMA = {"input": pl.String, **_STREAM_SCHEMA}


@dataclass(frozen=True)
class _BoundInput:
    streams: tuple[str, ...]
    metadata: pl.DataFrame | None = None
    match_key: tuple | None = None


@dataclass(frozen=True)
class InputSnapshot:
    """Read the actor's resolved inputs at one published epoch."""

    _snapshot: Snapshot
    _inputs: Mapping[str, _BoundInput]
    graph_revision: int | None = None

    @property
    def epoch(self) -> int:
        return self._snapshot.epoch

    def read(self, name: str, *, start: Timestamp | None = None,
             end: Timestamp | None = None) -> pl.DataFrame:
        """Read a named input over [start, end), with stream URIs for query inputs."""
        binding = self._inputs[name]
        frame = self._read_streams(binding.streams, time_ns(start), time_ns(end))
        return frame.drop("stream_uri") if binding.metadata is None else frame

    def metadata(self, name: str) -> pl.DataFrame:
        """Return the captured query metadata or the URI of an explicit stream."""
        binding = self._inputs[name]
        if binding.metadata is None:
            return pl.DataFrame({"stream_uri": binding.streams})
        return binding.metadata.clone()

    def query(self, *names: str) -> SnapshotQuery:
        """Build a deferred read over the named inputs, or all inputs by default."""
        selected = tuple(dict.fromkeys(names)) if names else tuple(self._inputs)
        for name in selected:
            if name not in self._inputs:
                raise KeyError(name)
        return SnapshotQuery(self, selected)

    def _read_streams(self, streams: tuple[str, ...], start: int | None,
                      end: int | None) -> pl.DataFrame:
        frames = []
        for stream in streams:
            try:
                batches = self._snapshot.read([stream], start=start, end=end)
            except NotFound:
                # A graph binding can exist before its first reading.
                continue
            with pa.RecordBatchReader.from_stream(batches) as reader:
                frames.append(pl.from_arrow(reader.read_all()).select(
                    pl.lit(stream).alias("stream_uri"), "time", pl.col("number").alias("value"), "text",
                ))
        return pl.concat(frames) if frames else pl.DataFrame(schema=_STREAM_SCHEMA)


@dataclass(frozen=True)
class SnapshotQuery:
    """A read-only dataframe query over inputs already bound for one tick."""

    _snapshot: InputSnapshot
    _names: tuple[str, ...]

    def dataframe(self, *, start: Timestamp | None = None,
                  end: Timestamp | None = None) -> pl.DataFrame:
        """Collect rows over [start, end), labeled by input name and stream URI."""
        start_ns, end_ns = time_ns(start), time_ns(end)
        frames = [
            self._snapshot._read_streams(self._snapshot._inputs[name].streams, start_ns, end_ns)
            .select(pl.lit(name).alias("input"), *_STREAM_SCHEMA)
            for name in self._names
        ]
        return pl.concat(frames) if frames else pl.DataFrame(schema=_QUERY_SCHEMA)


@dataclass(frozen=True)
class TimerFired:
    """One scheduled check and the published inputs available to it."""

    name: str
    scheduled_at: datetime
    interval: timedelta
    snapshot: InputSnapshot


@dataclass(frozen=True)
class InputChange:
    """Changes to one declared input's streams and captured metadata."""

    added_streams: tuple[str, ...]
    removed_streams: tuple[str, ...]
    old_metadata: pl.DataFrame
    new_metadata: pl.DataFrame


@dataclass(frozen=True)
class GraphChange:
    """One actor's affected inputs and its snapshot at the new graph revision."""

    old_revision: int
    new_revision: int
    inputs: Mapping[str, InputChange]
    snapshot: InputSnapshot


@dataclass(frozen=True)
class Transition(Generic[State]):
    """State to save and time/value pairs to publish under named outputs."""

    state: State
    writes: Mapping[str, Sequence[tuple[Timestamp, Value]]] = field(default_factory=dict)


@dataclass(frozen=True)
class Actor:
    """One configured actor instance and its bound streams and timers.

    A ``[[actors]]`` config entry can point directly to an Actor declaration
    using a spec such as ``turbidity_alarm.py:alarm``.

    The handler class defines ``initial_state()`` and ``on_timer(tick)``. It
    can also define ``on_graph_change(change)`` for changes to query inputs.
    Acquirium sets ``handler.state`` to the last committed JSON state before
    each message. The class uses a no-argument constructor.
    """

    name: str
    handler: type
    inputs: Mapping[str, Stream | Query]
    outputs: Mapping[str, Literal["numeric", "text"]]
    timers: Mapping[str, timedelta]

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", self.name):
            raise ValueError("actor name must contain only letters, digits, _, ., or -")
        if not isinstance(self.handler, type) or any(
            not callable(getattr(self.handler, name, None)) for name in ("initial_state", "on_timer")
        ):
            raise ValueError("actor handler must be a class with initial_state and on_timer methods")
        on_graph_change = getattr(self.handler, "on_graph_change", None)
        if on_graph_change is not None and not callable(on_graph_change):
            raise ValueError("actor on_graph_change must be callable")
        for name, declaration in self.inputs.items():
            if not isinstance(declaration, (Stream, Query)):
                raise ValueError(f"actor input {name!r} must be a Stream handle or a Query")
            if isinstance(declaration, Query):
                graph = declaration.query_graph
                if not any(not graph.nodes[node].constraints.get("dropped") for node in graph.data_nodes):
                    raise ValueError(f"actor input {name!r} must select a measurement")
        if any(kind not in ("numeric", "text") for kind in self.outputs.values()):
            raise ValueError("actor output kinds must be numeric or text")
        if not self.timers or any(
            not isinstance(interval, timedelta) or interval <= timedelta()
            for interval in self.timers.values()
        ):
            raise ValueError("an actor needs at least one positive timer interval")
        if any(duration_ns(interval, "timer interval") % 1000 for interval in self.timers.values()):
            raise ValueError("actor timer intervals must have microsecond precision")
        for field_name, mapping in (("inputs", self.inputs), ("outputs", self.outputs), ("timers", self.timers)):
            if any(not isinstance(name, str) or not name for name in mapping):
                raise ValueError(f"actor {field_name} must have nonempty names")

    @property
    def source_id(self) -> str:
        """Source under which Acquirium registers and publishes the outputs."""
        return f"actor:{self.name}"
