"""Replay generated data into a server with late arrivals and corrections.

The replay keeps the canonical current value of every (stream, timestamp)
it has sent, so the oracle can recompute views from exactly the data the
server should hold. Every insert is logged with its wall-clock time and
the simulated timestamps it carried.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import polars as pl

from experiments import benicia


@dataclass
class Replay:
    client: Any
    replica: benicia.Replica
    wide: pl.DataFrame
    rate: float = 60.0                       # simulated seconds per wall-clock second
    late_fraction: float = 0.0               # share of readings held back
    late_max: timedelta = timedelta(hours=2) # maximum simulated lateness
    correction_fraction: float = 0.0         # corrections per reading sent
    seed: int = 0
    truth: dict[str, dict[datetime, float]] = field(default_factory=dict)
    sent: list[dict[str, Any]] = field(default_factory=list)
    _rng: random.Random = field(init=False)
    _held: list[tuple[datetime, datetime, str, float]] = field(default_factory=list)  # (release, ts, ref_name, value)
    cursor: int = 0                          # next row of ``wide`` to send

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)
        self.columns = [c for c in self.wide.columns if c != "timestamp"]

    def _remember(self, ref_name: str, ts: datetime, value: float) -> None:
        self.truth.setdefault(self.replica.ref_uri(ref_name), {})[ts] = value

    def _send(self, rows: list[tuple[datetime, str, float]], kind: str, sim_now: datetime) -> None:
        if not rows:
            return
        long = pl.DataFrame({"ts": [r[0] for r in rows], "ref_name": [r[1] for r in rows],
                             "value": [r[2] for r in rows]}).with_columns(
            pl.col("ts").cast(pl.Datetime("us", "UTC")))
        for ts, name, value in rows:
            self._remember(name, ts, value)
        started = time.time()
        # The publication id reaches the server's ingest event, so cost can
        # be attributed to fresh, late and corrected readings separately.
        benicia.insert(self.client, self.replica, long, publication_id=f"{kind}:{len(self.sent)}")
        self.sent.append({"kind": kind, "t": started, "t_done": time.time(), "sim_now": sim_now,
                          "rows": len(rows), "min_ts": min(r[0] for r in rows), "max_ts": max(r[0] for r in rows)})

    def _release_due(self, sim_now: datetime) -> list[tuple[datetime, str, float]]:
        due = [(ts, name, value) for release, ts, name, value in self._held if release <= sim_now]
        self._held = [item for item in self._held if item[0] > sim_now]
        return due

    def _corrections(self, count: int) -> list[tuple[datetime, str, float]]:
        rows = []
        streams = [s for s in self.truth if self.truth[s]]
        for _ in range(count):
            if not streams:
                break
            ref = self._rng.choice(streams)
            ts = self._rng.choice(list(self.truth[ref]))
            name = self._name_of(ref)
            rows.append((ts, name, self.truth[ref][ts] * self._rng.uniform(0.8, 1.2)))
        return rows

    def _name_of(self, ref_uri: str) -> str:
        if not hasattr(self, "_names"):
            self._names = {self.replica.ref_uri(c): c for c in self.columns}
        return self._names[ref_uri]

    def register_name(self, ref_uri: str, name: str) -> None:
        """Make a stream added after construction correctable and deletable."""
        self._name_of(next(iter(self._names)) if hasattr(self, "_names") else self.replica.ref_uri(self.columns[0]))
        self._names[ref_uri] = name

    def run(self, *, rows: int | None = None, pace: bool = True) -> None:
        """Send rows from the cursor up to row ``rows`` at ``rate``, holding back late readings."""
        interval = None
        end = self.wide.height if rows is None else min(rows, self.wide.height)
        frames = self.wide.slice(self.cursor, max(end - self.cursor, 0))
        self.cursor = max(self.cursor, end)
        timestamps = frames["timestamp"].to_list()
        if len(timestamps) > 1:
            interval = timestamps[1] - timestamps[0]
        wall_step = (interval.total_seconds() / self.rate) if (interval and pace) else 0.0
        next_wall = time.monotonic()
        for row in frames.iter_rows(named=True):
            sim_now = row["timestamp"]
            fresh, corrections_due = [], 0
            for name in self.columns:
                value = row[name]
                if value is None:
                    continue
                if self.late_fraction and self._rng.random() < self.late_fraction:
                    delay = timedelta(seconds=self._rng.expovariate(1.0 / (self.late_max.total_seconds() / 3)))
                    self._held.append((sim_now + min(delay, self.late_max), sim_now, name, float(value)))
                else:
                    fresh.append((sim_now, name, float(value)))
                if self.correction_fraction and self._rng.random() < self.correction_fraction:
                    corrections_due += 1
            self._send(fresh, "fresh", sim_now)
            self._send(self._release_due(sim_now), "late", sim_now)
            self._send(self._corrections(corrections_due), "correction", sim_now)
            if wall_step:
                next_wall += wall_step
                delay = next_wall - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

    def flush_late(self, sim_now: datetime) -> None:
        """Release everything still held back, as if the clock jumped to ``sim_now``."""
        self._send(self._release_due(sim_now + self.late_max), "late", sim_now)

    def release_late(self, count: int) -> int:
        """Release up to ``count`` held-back readings now, whatever their release time."""
        self._held.sort()
        due, self._held = self._held[:count], self._held[count:]
        rows = [(ts, name, value) for _, ts, name, value in due]
        self._send(rows, "late", self.sim_now())
        return len(rows)

    def correct(self, count: int) -> int:
        """Rewrite ``count`` already-sent readings with new values."""
        rows = self._corrections(count)
        self._send(rows, "correction", self.sim_now())
        return len(rows)

    def sim_now(self) -> datetime:
        index = min(max(self.cursor - 1, 0), self.wide.height - 1)
        return self.wide["timestamp"][index]

    def truth_frames(self) -> dict[str, pl.DataFrame]:
        """Canonical current data per raw stream, the oracle's input."""
        frames = {}
        for ref, values in self.truth.items():
            if not values:
                continue
            frame = pl.DataFrame({"time": list(values), "value": list(values.values())})
            frames[ref] = frame.with_columns(pl.col("time").cast(pl.Datetime("us", "UTC"))).sort("time")
        return frames

    def sent_frame(self) -> pl.DataFrame:
        return pl.from_dicts(self.sent) if self.sent else pl.DataFrame()
