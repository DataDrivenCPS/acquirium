"""Handles for one stored stream and for a time range of it."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from acquirium.ExperimentValues import timestamp as _utc
import polars as pl
from acquirium.internals._names import accepts_old_names


def samples_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Return time-series samples as ``time`` and ``value`` columns."""
    if frame.is_empty() and "ts" not in frame.columns:
        return pl.DataFrame(schema={"time": pl.Datetime("us", "UTC"), "value": pl.Float64})
    return frame.select(pl.col("ts").alias("time"), "value")


@accepts_old_names
@dataclass(frozen=True)
class Stream:
    """One stored stream, identified by its reference URI.

    The handle reads the stream's current samples; it holds no data itself.
    Handles compare equal when their reference URIs do, and ``str()`` returns
    the URI.
    """
    stream_uri: str
    _client: Any = field(default=None, repr=False, compare=False)

    def between(self, start: datetime | str, end: datetime | str) -> "StreamInterval":
        """Select the samples from ``start`` to ``end``, inclusive."""
        return StreamInterval(self, _utc(start), _utc(end))

    def dataframe(self, *, start: datetime | str | None = None, end: datetime | str | None = None,
                  **options: Any) -> pl.DataFrame:
        """Read the stream's samples as ``time`` and ``value`` columns.

        Other options (``limit``, ``order``, ``timeout``, ``value_mode``) pass
        through to the time-series read.
        """
        if self._client is None:
            raise RuntimeError("this stream handle is not bound to a client")
        frame = self._client.timeseries_df(
            self.stream_uri,
            start=_utc(start).isoformat() if start is not None else None,
            end=_utc(end).isoformat() if end is not None else None,
            **options,
        )
        return samples_frame(frame)

    def insert_metadata(self, values: Any) -> dict[str, Any]:
        """Attach metadata to the stream: ``{key: value}``.

        Written on the property the stream is linked to, with the rules of
        ``Acquirium.insert_metadata``.
        """
        if self._client is None:
            raise RuntimeError("this stream handle is not bound to a client")
        return self._client.insert_metadata(
            {self._client.stream_property(self.stream_uri): values}
        )

    def __str__(self) -> str:
        return self.stream_uri

    def __repr__(self) -> str:
        return f"<Stream {self.stream_uri}>"


@dataclass(frozen=True)
class StreamInterval:
    """A stream between two times. Reads see the samples present when read."""
    stream: Stream
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError("a stream interval must end at or after its start")

    @property
    def stream_uri(self) -> str:
        return self.stream.stream_uri

    def dataframe(self, **options: Any) -> pl.DataFrame:
        """Read the samples in this interval as ``time`` and ``value`` columns."""
        return self.stream.dataframe(start=self.start, end=self.end, **options)

    def __repr__(self) -> str:
        return f"<StreamInterval {self.stream_uri} [{self.start.isoformat()}, {self.end.isoformat()}]>"
