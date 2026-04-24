from __future__ import annotations

import logging
import threading


log = logging.getLogger("acquirium.api")


class InsertStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._buckets: dict[str, dict[str, int | set[str]]] = {}

    def record(self, origin: str, rows: int, streams: list[str]) -> None:
        with self._lock:
            bucket = self._buckets.setdefault(origin, {"rows": 0, "streams": set()})
            bucket["rows"] += rows
            bucket["streams"].update(streams)

    def snapshot_and_reset(self) -> list[tuple[str, int, int]]:
        with self._lock:
            snapshot = [
                (origin, int(bucket["rows"]), len(bucket["streams"]))
                for origin, bucket in self._buckets.items()
            ]
            self._buckets = {}
        return snapshot


insert_stats = InsertStats()


def start_insert_summary_thread(
    stop_event: threading.Event,
    interval: float = 30.0,
) -> threading.Thread:
    def _run() -> None:
        while not stop_event.wait(timeout=interval):
            for origin, rows, streams in insert_stats.snapshot_and_reset():
                if rows > 0:
                    log.info(
                        "insert_timeseries[%s]: %d rows across %d stream(s) in the last %.0fs",
                        origin,
                        rows,
                        streams,
                        interval,
                    )

    thread = threading.Thread(target=_run, daemon=True, name="acquirium-insert-summary")
    thread.start()
    return thread
