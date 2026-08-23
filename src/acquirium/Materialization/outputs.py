"""Runtime input and output handles for query-driven transformations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import polars as pl
import pyarrow as pa

from acquirium.Materialization.api import OutputSpec


UTC = timezone.utc
_OUTPUT_SCHEMA = pa.schema([
    ("ref_uri", pa.string()),
    ("ts", pa.timestamp("us", tz="UTC")),
    ("numeric_value", pa.float64()),
    ("text_value", pa.string()),
])


def _as_frame(value: Any) -> pl.DataFrame:
    if isinstance(value, pl.DataFrame):
        return value
    if isinstance(value, pa.Table):
        return pl.from_arrow(value)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return pl.DataFrame(list(value))
    raise TypeError("output.write() expects a Polars frame, Arrow table, or row iterable")


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("output timestamps must be datetime values")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _value_columns(values: list[Any]) -> tuple[list[float | None], list[str | None]]:
    numeric: list[float | None] = []
    text: list[str | None] = []
    for value in values:
        if value is None:
            numeric.append(None)
            text.append(None)
        elif isinstance(value, bool):
            raise TypeError("output values must be numeric, text, or None; bool is not supported")
        elif isinstance(value, (int, float)):
            numeric.append(float(value))
            text.append(None)
        elif isinstance(value, str):
            numeric.append(None)
            text.append(value)
        else:
            raise TypeError(f"unsupported output value type: {type(value).__name__}")
    return numeric, text


@dataclass
class OutputStream:
    """A declared output stream that buffers rows for one invocation."""

    name: str
    ref_uri: str
    spec: OutputSpec
    _rows: list[tuple[datetime, Any]] = field(default_factory=list)

    def add(self, timestamp: datetime, value: Any) -> None:
        if self.spec.value_kind == "numeric" and value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float))
        ):
            raise TypeError(f"output {self.name!r} accepts numeric values")
        if self.spec.value_kind == "text" and value is not None and not isinstance(value, str):
            raise TypeError(f"output {self.name!r} accepts text values")
        self._rows.append((_timestamp(timestamp), value))

    def write(self, frame: Any) -> None:
        """Append rows from a frame with exactly ``time`` and ``value`` columns."""
        frame = _as_frame(frame)
        required = {"time", "value"}
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"output.write() is missing columns: {sorted(missing)}")
        for timestamp, value in frame.select(["time", "value"]).iter_rows():
            self.add(timestamp, value)

    def to_arrow(self) -> pa.Table:
        numeric, text = _value_columns([value for _, value in self._rows])
        return pa.Table.from_arrays([
            pa.array([self.ref_uri] * len(self._rows), type=pa.string()),
            pa.array([timestamp for timestamp, _ in self._rows], type=pa.timestamp("us", tz="UTC")),
            pa.array(numeric, type=pa.float64()),
            pa.array(text, type=pa.string()),
        ], schema=_OUTPUT_SCHEMA)


class OutputSet:
    """The only output-writing surface exposed to transformation code."""

    def __init__(
        self,
        allowed: Mapping[str, tuple[str, ...]],
        specs: Mapping[str, OutputSpec],
    ) -> None:
        self._allowed = {str(name): tuple(refs) for name, refs in allowed.items()}
        self._specs = dict(specs)
        self._streams: dict[tuple[str, str], OutputStream] = {}

    def declare(
        self,
        name: str,
        *,
        for_input: Any | None = None,
        ref_uri: str | None = None,
    ) -> OutputStream:
        """Return a stable handle for one planned output.

        ``for_input`` is intentionally a handle-only convenience: the
        planner has already assigned the output URI to this invocation.  A
        fixed URI can be supplied as a trapdoor, but it must be the URI
        planned by the output spec so that topology ownership stays explicit.
        """
        if name not in self._allowed:
            raise KeyError(f"unknown output {name!r}; declare it in the class outputs mapping")
        refs = self._allowed[name]
        if len(refs) != 1:
            raise ValueError(f"output {name!r} has {len(refs)} planned streams; use one output per binding")
        planned = refs[0]
        if ref_uri is not None and ref_uri != planned:
            raise ValueError(
                f"output {name!r} URI {ref_uri!r} is not the planned URI {planned!r}; "
                "set ref_uri on outputs.stream(...) to use an explicit URI"
            )
        # A binding owns the planned URI.  ``for_input`` documents which
        # matched row the caller is handling, but must not create multiple
        # buffers for the same output stream.
        key = (name, planned)
        stream = self._streams.get(key)
        if stream is None:
            try:
                spec = self._specs[name]
            except KeyError as error:
                raise KeyError(f"missing output metadata for {name!r}") from error
            stream = OutputStream(name, planned, spec)
            self._streams[key] = stream
        return stream

    def to_arrow(self) -> pa.Table:
        tables = [stream.to_arrow() for stream in self._streams.values() if stream._rows]
        if not tables:
            return pa.Table.from_arrays([
                pa.array([], type=pa.string()),
                pa.array([], type=pa.timestamp("us", tz="UTC")),
                pa.array([], type=pa.float64()),
                pa.array([], type=pa.string()),
            ], schema=_OUTPUT_SCHEMA)
        return pa.concat_tables(tables, promote_options="default")
