from __future__ import annotations

import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

log = logging.getLogger(__name__)


class DriverState:
    """Persistent key-value store for a single driver instance.

    Backed by a JSON file in the driver's state directory.  All writes are
    atomic (write-then-rename) so a crash mid-write cannot corrupt the file.
    Values must be JSON-serialisable.
    """

    def __init__(self, state_dir: Path) -> None:
        self.path = state_dir
        self.path.mkdir(parents=True, exist_ok=True)
        self._kv_path = self.path / "state.json"
        self._kv: dict[str, Any] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for *key*, or *default* if not present."""
        return self._kv.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Persist *key* → *value* immediately."""
        self._kv[key] = value
        self._save()

    def delete(self, key: str) -> None:
        """Remove *key* from the store (no-op if absent)."""
        self._kv.pop(key, None)
        self._save()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if self._kv_path.exists():
            try:
                with open(self._kv_path) as f:
                    self._kv = json.load(f)
            except Exception:
                log.warning("DriverState: could not read %s; starting empty", self._kv_path)
                self._kv = {}

    def _save(self) -> None:
        tmp = self._kv_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(self._kv, f, indent=2, default=str)
        os.replace(tmp, self._kv_path)


class WriteAheadLog:
    """File-backed buffer for observation DataFrames.

    Each buffered batch is written as a separate Parquet file so that a crash
    mid-write corrupts at most one entry (the file being written), and all
    previously-committed entries remain intact.

    The DataFrame stored in each file always contains the columns
    ``(source_id, ts, ref_name, value)``; the caller must materialise
    ``source_id`` before calling :meth:`append`.

    Entries are replayed in sequence-number order.  Once an entry has been
    successfully transmitted, call :meth:`ack` to remove its file.
    """

    def __init__(self, wal_dir: Path) -> None:
        self.path = wal_dir
        self.path.mkdir(parents=True, exist_ok=True)
        self._next_seq = self._scan_max_seq() + 1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, df: "pl.DataFrame") -> int:
        """Write *df* to the WAL and return its sequence number."""
        seq = self._next_seq
        self._next_seq += 1
        out_path = self._seq_path(seq)
        df.write_parquet(out_path)
        return seq

    def pending(self) -> list[tuple[int, "pl.DataFrame"]]:
        """Return all pending entries as ``(seq, df)`` ordered by seq."""
        import polars as pl

        result: list[tuple[int, "pl.DataFrame"]] = []
        for p in sorted(self._parquet_files(), key=lambda p: int(p.stem)):
            try:
                result.append((int(p.stem), pl.read_parquet(p)))
            except Exception:
                log.warning("WriteAheadLog: could not read %s; skipping", p)
        return result

    def ack(self, seq: int) -> None:
        """Mark entry *seq* as delivered by deleting its file."""
        self._seq_path(seq).unlink(missing_ok=True)

    def is_empty(self) -> bool:
        """Return True if there are no pending WAL entries."""
        return not any(self._parquet_files())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _seq_path(self, seq: int) -> Path:
        return self.path / f"{seq:08d}.parquet"

    def _parquet_files(self):
        return (p for p in self.path.glob("*.parquet") if p.stem.isdigit())

    def _scan_max_seq(self) -> int:
        seqs = [int(p.stem) for p in self._parquet_files()]
        return max(seqs, default=-1)


class ExponentialBackoff:
    """Retry-timing helper using exponential backoff with optional jitter.

    Delay formula after *n* consecutive failures::

        delay = min(base ** n, max_delay) * (1 ± jitter)

    Call :meth:`record_failure` after each failed attempt and
    :meth:`record_success` when a call succeeds (this resets the counter).
    Use :meth:`ready` to check whether enough time has elapsed before the
    next attempt.
    """

    def __init__(
        self,
        base: float = 2.0,
        max_delay: float = 300.0,
        jitter: float = 0.1,
    ) -> None:
        self._base = base
        self._max = max_delay
        self._jitter = jitter
        self._failures = 0
        self._next_attempt: float = 0.0

    def record_success(self) -> None:
        """Reset the backoff state after a successful call."""
        self._failures = 0
        self._next_attempt = 0.0

    def record_failure(self) -> None:
        """Advance the backoff timer after a failed call."""
        self._failures += 1
        delay = min(self._base ** self._failures, self._max)
        jitter = delay * self._jitter * random.uniform(-1.0, 1.0)
        self._next_attempt = time.monotonic() + delay + jitter

    def ready(self) -> bool:
        """Return True if enough time has passed to attempt another call."""
        return time.monotonic() >= self._next_attempt

    def next_delay(self) -> float:
        """Seconds remaining until the next attempt is allowed (0 if ready)."""
        return max(0.0, self._next_attempt - time.monotonic())
