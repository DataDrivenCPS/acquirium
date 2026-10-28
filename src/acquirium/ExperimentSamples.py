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

    def write(self, source: str, frame: pl.DataFrame) -> int:
        """Ingest ``frame``, wait until it is published, and return the epoch
        that includes it."""
        frame = frame.unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
        receipt = None
        for offset in range(0, frame.height, MAX_REQUEST_ROWS):
            receipt = self.core.ingest(source, str(uuid.uuid4()), frame.slice(offset, MAX_REQUEST_ROWS))
        if receipt is None:
            return self.epoch()
        return self.core.wait(receipt, timeout=self.timeout).epoch

    def epoch(self) -> int:
        """The latest published epoch."""
        return self.core.engine.snapshot().epoch

    def _readings(self, refs):
        snapshot = self.core.engine.snapshot()
        return {ref: snapshot.readings(ref) for ref in refs}

    def clear(self, refs) -> int:
        """Delete every sample of ``refs``; returns how many there were."""
        deletes = [(ref, reading.event_time_ns)
                   for ref, readings in self._readings(refs).items() for reading in readings]
        if deletes:
            self.write("experiments", pl.DataFrame({
                "ref_uri": pl.Series([ref for ref, _ in deletes], dtype=pl.String),
                "ts": pl.Series([t for _, t in deletes], dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
                "value": pl.Series([None] * len(deletes), dtype=pl.Float64),
                "operation": ["delete"] * len(deletes),
            }))
        return len(deletes)

    def usage(self, refs) -> tuple[int, int]:
        """Sample count and an estimate of their bytes."""
        count = size = 0
        for readings in self._readings(refs).values():
            for reading in readings:
                count += 1
                size += 8 + (len(reading.value) if isinstance(reading.value, str) else 8)
        return count, size
