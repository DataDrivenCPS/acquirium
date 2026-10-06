"""Opt-in event log for measuring the materialization runtime.

When a server is configured with ``[server] materialization_event_log``,
the timeseries store carries an :class:`EventLog` and the runtime appends
one JSON line per ingestion commit, transform invocation, publication and
replan. Each line has a wall-clock ``t`` and a monotonic ``mono`` timestamp.
Experiments join ``ingest`` and ``commit`` lines on revision numbers to get
arrival-to-visibility latency, and sum ``invocation`` lines for cost.

Without the setting the store has no ``events`` attribute and every hook is
one attribute lookup, so the production path is unchanged.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any


class EventLog:
    """Append-only JSON-lines writer, safe to call from several threads."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", buffering=1)
        self._lock = threading.Lock()

    def emit(self, kind: str, **fields: Any) -> None:
        record = {"kind": kind, "t": time.time(), "mono": time.perf_counter(), **fields}
        line = json.dumps(record, default=str, separators=(",", ":"))
        with self._lock:
            self._file.write(line + "\n")

    def close(self) -> None:
        with self._lock:
            self._file.close()


def emit(store: Any, kind: str, **fields: Any) -> None:
    """Record ``kind`` on the store's log, if it has one."""
    log = getattr(store, "events", None)
    if log is not None:
        log.emit(kind, **fields)
