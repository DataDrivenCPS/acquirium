"""Window and calculation declarations used by the Acquirium app API."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Pointwise:
    def to_dict(self):
        return {"kind": "pointwise"}


@dataclass(frozen=True)
class FixedBucket:
    width_ns: int
    origin_ns: int = 0

    def to_dict(self):
        return {"kind": "fixed", "width_ns": self.width_ns, "origin_ns": self.origin_ns}


@dataclass(frozen=True)
class Rolling:
    lookback_ns: int
    step_ns: int
    lookahead_ns: int = 0
    origin_ns: int = 0

    def to_dict(self):
        return {
            "kind": "rolling", "step_ns": self.step_ns, "origin_ns": self.origin_ns,
            "lookback_ns": self.lookback_ns, "lookahead_ns": self.lookahead_ns,
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
    per_ns: int | None = None
    reset: FixedBucket | None = None
    max_gap_ns: int | None = None

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
    def integral(cls, port, *, per_ns, reset=None, max_gap_ns=None):
        return cls("integral", port, per_ns=per_ns, reset=reset, max_gap_ns=max_gap_ns)

    def to_dict(self):
        spec = {"kind": self.kind, "port": self.port}
        if self.kind == "mean":
            spec["per_stream"] = self.per_stream
        if self.kind == "threshold":
            spec["above"] = self.above
        if self.kind == "integral":
            spec["per_ns"] = self.per_ns
        if self.reset is not None:
            spec["reset"] = {"width_ns": self.reset.width_ns, "origin_ns": self.reset.origin_ns}
        if self.max_gap_ns is not None:
            spec["max_gap_ns"] = self.max_gap_ns
        return spec
