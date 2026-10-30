"""Handles for one stored stream and for a time range of it."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from acquirium.core import time_ns
from acquirium.ExperimentValues import timestamp as _utc
import polars as pl


def _after_ns(value: datetime | str) -> int:
    """UTC nanoseconds just after the microsecond ``value`` names: a half-open
    read up to it includes every sample within that microsecond."""
    return time_ns(_utc(value)) + 1000


def samples_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Return time-series samples as ``time`` and ``value`` columns."""
    if frame.is_empty() and "ts" not in frame.columns:
        return pl.DataFrame(schema={"time": pl.Datetime("us", "UTC"), "value": pl.Float64})
    return frame.select(pl.col("ts").alias("time"), "value")


@dataclass(frozen=True)
class Stream:
    """One stored stream, identified by its reference URI.

    The handle reads the stream's current samples; it holds no data itself.
    Handles compare equal when their reference URIs do, and ``str()`` returns
    the URI. A handle returned for samples just written reads them even
    before they publish.
    """
    ref_uri: str
    _client: Any = field(default=None, repr=False, compare=False)
    # The request sequence of samples written through this handle.
    _after: int | None = field(default=None, repr=False, compare=False)

    def between(self, start: datetime | str, end: datetime | str) -> "StreamInterval":
        """Select the samples from ``start`` to ``end``, inclusive."""
        return StreamInterval(self, _utc(start), _utc(end))

    def dataframe(self, *, start: datetime | str | None = None, end: datetime | str | None = None,
                  **options: Any) -> pl.DataFrame:
        """Read the stream's samples from ``start`` to ``end``, inclusive, as
        ``time`` and ``value`` columns.

        Other options (``limit``, ``order``, ``timeout``, ``value_mode``) pass
        through to the time-series read.
        """
        if self._client is None:
            raise RuntimeError("this stream handle is not bound to a client")
        frame = self._client.timeseries_df(
            self.ref_uri,
            start=_utc(start).isoformat() if start is not None else None,
            # Reads are half-open, so read up to just after the end.
            end_ns=_after_ns(end) if end is not None else None,
            **({"after": self._after} if self._after is not None else {}),
            **options,
        )
        return samples_frame(frame)

    def __str__(self) -> str:
        return self.ref_uri

    def __repr__(self) -> str:
        return f"<Stream {self.ref_uri}>"


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
    def ref_uri(self) -> str:
        return self.stream.ref_uri

    def dataframe(self, **options: Any) -> pl.DataFrame:
        """Read the samples in this interval as ``time`` and ``value`` columns."""
        return self.stream.dataframe(start=self.start, end=self.end, **options)

    def __repr__(self) -> str:
        return f"<StreamInterval {self.ref_uri} [{self.start.isoformat()}, {self.end.isoformat()}]>"
