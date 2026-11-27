"""Window and calculation declarations used by the Acquirium app API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from acquirium.core import time_ns


def duration_ns(value: timedelta, name: str) -> int:
    """Convert a duration such as ``pd.Timedelta("5m")`` to nanoseconds."""
    if not isinstance(value, timedelta):
        raise TypeError(f"{name} must be a timedelta, such as pd.Timedelta(\"5m\")")
    # pd.Timedelta subclasses timedelta and adds a nanoseconds component.
    seconds = value.days * 86_400 + value.seconds
    return seconds * 1_000_000_000 + value.microseconds * 1000 + getattr(value, "nanoseconds", 0)


def duration(ns: int) -> pd.Timedelta:
    """Rebuild a stored nanosecond duration without losing precision."""
    return pd.Timedelta(ns, unit="ns")


@dataclass(frozen=True)
class Pointwise:
    def to_dict(self):
        return {"kind": "pointwise"}


@dataclass(frozen=True)
class FixedBucket:
    """Buckets of ``width``, aligned to ``origin`` (the Unix epoch by default)."""

    width: timedelta
    origin: datetime | int | str = 0

    def __post_init__(self):
        self.to_dict()

    def to_dict(self):
        return {"kind": "fixed", "width_ns": duration_ns(self.width, "width"), "origin_ns": time_ns(self.origin)}


@dataclass(frozen=True)
class Rolling:
    """A result every ``step``, reading ``lookback`` before and ``lookahead`` after it."""

    lookback: timedelta
    step: timedelta
    lookahead: timedelta = timedelta()
    origin: datetime | int | str = 0

    def __post_init__(self):
        self.to_dict()

    def to_dict(self):
        return {
            "kind": "rolling", "step_ns": duration_ns(self.step, "step"), "origin_ns": time_ns(self.origin),
            "lookback_ns": duration_ns(self.lookback, "lookback"),
            "lookahead_ns": duration_ns(self.lookahead, "lookahead"),
        }


@dataclass(frozen=True)
class PythonFunction:
    path: str
    sha256: str
    entrypoint: str
    batch: bool = False
    sequential: bool = False

    @classmethod
    def from_file(cls, path, entrypoint, *, batch=False, sequential=False):
        path = Path(path).resolve()
        return cls(str(path), sha256(path.read_bytes()).hexdigest(), entrypoint, batch, sequential)

    def to_dict(self):
        spec = {"kind": "python", "path": self.path, "sha256": self.sha256, "entrypoint": self.entrypoint}
        if self.batch:
            spec["batch"] = True
        if self.sequential:
            spec["sequential"] = True
        return spec


@dataclass(frozen=True)
class Native:
    """Built-in Acquirium calculations executed by the Python workers."""

    kind: str
    port: str
    above: Any = None
    per_stream: bool = False
    per: timedelta | None = None
    reset: FixedBucket | None = None
    max_gap: timedelta | None = None

    @classmethod
    def mean(cls, port, *, per_stream=False):
        return cls("mean", port, per_stream=per_stream)

    @classmethod
    def sum(cls, port):
        return cls("sum", port)

    @classmethod
    def max(cls, port):
        return cls("max", port)

    @classmethod
    def min(cls, port):
        return cls("min", port)

    @classmethod
    def count(cls, port):
        return cls("count", port)

    @classmethod
    def threshold(cls, port, above):
        return cls("threshold", port, above=above)

    @classmethod
    def cumulative_sum(cls, port, *, reset=None):
        return cls("cumulative_sum", port, reset=reset)

    @classmethod
    def integral(cls, port, *, per, reset=None, max_gap=None):
        return cls("integral", port, per=per, reset=reset, max_gap=max_gap)

    def to_dict(self):
        spec = {"kind": self.kind, "port": self.port}
        if self.kind == "mean":
            spec["per_stream"] = self.per_stream
        if self.kind == "threshold":
            spec["above"] = self.above
        if self.kind == "integral":
            spec["per_ns"] = duration_ns(self.per, "per")
        if self.reset is not None:
            reset = self.reset.to_dict()
            spec["reset"] = {"width_ns": reset["width_ns"], "origin_ns": reset["origin_ns"]}
        if self.max_gap is not None:
            spec["max_gap_ns"] = duration_ns(self.max_gap, "max_gap")
        return spec
