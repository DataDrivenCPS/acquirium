"""Experiment time-series samples, stored in Escapement.

The experiment ledger records which streams a run owns; the samples are
ordinary Escapement readings, so they read like any other stream.
"""
from __future__ import annotations

import uuid

import polars as pl
from escapement import Receipt

from acquirium.core import Core
from acquirium.internals.models import MAX_REQUEST_ROWS


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
        """Ingest ``frame`` without waiting for it to publish, and return the
        sequence of the last request Escapement has accepted, which covers
        the frame and everything written before it."""
        if frame is None or frame.is_empty():
            return self.core.engine.status()["accepted"]
        frame = frame.unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
        for offset in range(0, frame.height, MAX_REQUEST_ROWS):
            receipt = self.core.ingest(source, str(uuid.uuid4()), frame.slice(offset, MAX_REQUEST_ROWS))
        return receipt.sequence

    def wait(self, sequence: int) -> None:
        """Wait until the request at ``sequence`` is published."""
        self.core.wait(Receipt(sequence), timeout=self.timeout)

    def epochs(self, sequences) -> dict[int, int]:
        """The epoch that published each sequence, waiting for any not yet."""
        sequences = sorted(set(sequences))
        if not sequences:
            return {}
        self.wait(sequences[-1])
        return dict(zip(sequences, self.core.engine.epochs_of(sequences)))

    def count(self, refs) -> int:
        """How many samples ``refs`` hold, without reading them."""
        snapshot = self.core.engine.snapshot()
        return sum((snapshot.stats(ref) or {"count": 0})["count"] for ref in refs)

    def clear(self, refs) -> None:
        """Delete every sample of ``refs`` and wait until that is published."""
        refs = list(refs)
        for offset in range(0, len(refs), MAX_REQUEST_ROWS):
            receipt = self.core.clear("experiments", str(uuid.uuid4()), refs[offset:offset + MAX_REQUEST_ROWS])
        if refs:
            self.wait(receipt.sequence)

    def usage(self, refs) -> tuple[int, int]:
        """Sample count and their bytes: 8 for each time and 8 for each number."""
        count = self.count(refs)
        return count, 16 * count
