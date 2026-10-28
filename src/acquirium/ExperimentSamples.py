"""Experiment time-series samples, stored in Escapement.

The experiment ledger records which streams a run owns; the samples are
ordinary Escapement readings, so they read like any other stream.
"""
from __future__ import annotations

import uuid

import polars as pl

from acquirium.core import Core

# Escapement accepts at most this many mutations per request.
MAX_REQUEST_ROWS = 10_000


def sample_frame(ref_uri: str, rows) -> pl.DataFrame:
    """``(timestamp, number)`` rows as a frame ``Core.ingest`` accepts."""
    return pl.DataFrame({
        "ref_uri": pl.Series([ref_uri] * len(rows), dtype=pl.String),
        "ts": pl.Series([ts for ts, _ in rows], dtype=pl.Datetime("us", "UTC")),
        "value": pl.Series([value for _, value in rows], dtype=pl.Float64),
        "value_kind": pl.Series(["numeric"] * len(rows), dtype=pl.String),
    })


class EscapementSamples:
    def __init__(self, core: Core, *, timeout: float = 30.0) -> None:
        self.core = core
        self.timeout = timeout

    def write(self, source: str, frame: pl.DataFrame | None) -> int:
        """Ingest ``frame`` and return the epoch that publishes it and every
        request accepted before it. Without a frame, only wait for those."""
        if frame is None or frame.is_empty():
            return self.core.settle(timeout=self.timeout).epoch
        frame = frame.unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
        receipt = None
        for offset in range(0, frame.height, MAX_REQUEST_ROWS):
            receipt = self.core.ingest(source, str(uuid.uuid4()), frame.slice(offset, MAX_REQUEST_ROWS))
        return self.core.wait(receipt, timeout=self.timeout).epoch

    def count(self, refs) -> int:
        """How many samples ``refs`` hold, without reading them."""
        snapshot = self.core.engine.snapshot()
        return sum((snapshot.stats(ref) or {"count": 0})["count"] for ref in refs)

    def clear(self, refs) -> None:
        """Delete every sample of ``refs``, one request-sized page at a time,
        and wait until the deletions are published."""
        snapshot = self.core.engine.snapshot()
        receipt = None
        for ref in refs:
            start = None
            while page := snapshot.query(ref, start=start, limit=MAX_REQUEST_ROWS):
                times = [reading.event_time_ns for reading in page]
                receipt = self.core.ingest("experiments", str(uuid.uuid4()), pl.DataFrame({
                    "ref_uri": pl.Series([ref] * len(times), dtype=pl.String),
                    "ts": pl.Series(times, dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
                    "value": pl.Series([None] * len(times), dtype=pl.Float64),
                    "operation": ["delete"] * len(times),
                }))
                start = times[-1] + 1
        if receipt is not None:
            self.core.wait(receipt, timeout=self.timeout)

    def usage(self, refs) -> tuple[int, int]:
        """Sample count and their bytes: 8 for each time and 8 for each number."""
        count = self.count(refs)
        return count, 16 * count
