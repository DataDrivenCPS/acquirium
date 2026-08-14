"""Interval scheduling with overrun protection for app/task run loops.

:class:`IntervalScheduler` owns the loop shape used by the app runner (and
the task host): dispatch on a drift-free deadline grid, track how many runs
are in flight, and when a run outlasts the interval **skip the tick and
count it** instead of piling up unbounded concurrent runs.

Deliberately Ray-free: the same scheduler drives Ray actors on the server
and plain processes elsewhere. Everything here runs on one asyncio event
loop; ``stop()`` must be called on that loop (callers invoked from other
threads wrap it in ``call_soon_threadsafe``, as the app runner already does
for its stop event).

The driver runner keeps its own loop: its tick is a blocking call inside
the loop, so slow ticks drift but cannot pile up — overrun protection would
be a no-op there.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

logger = logging.getLogger("acquirium.scheduling")


class IntervalScheduler:
    """Dispatch an async callable every ``interval`` seconds, at most
    ``max_in_flight`` concurrently.

    ``dispatch`` is awaited for each run and its await must span the run's
    full duration — that is what makes the in-flight count meaningful.
    Exceptions from ``dispatch`` are logged and never kill the loop; recording
    a failed run's outcome is the dispatcher's job.

    Deadlines form a drift-free grid: each tick is scheduled from the
    previous deadline, not from "now", and after a stall the grid skips past
    missed ticks rather than bursting to catch up. Out-of-band callers (e.g.
    a data-change trigger) enter through :meth:`trigger`, so every dispatch
    path shares one capacity check.
    """

    def __init__(
        self,
        interval: float,
        dispatch: Callable[[], Awaitable[Any]],
        *,
        max_in_flight: int = 1,
        name: str = "",
        stop_event: asyncio.Event | None = None,
    ):
        if interval <= 0:
            raise ValueError("interval must be greater than zero")
        if max_in_flight < 1:
            raise ValueError("max_in_flight must be at least 1")
        self.interval = float(interval)
        self.max_in_flight = int(max_in_flight)
        self.name = name
        self._dispatch = dispatch
        # An injected event lets a host share its existing stop signal (the
        # app runner's sync stop() already flips one via the loop).
        self._stop_event = stop_event if stop_event is not None else asyncio.Event()
        self._in_flight = 0
        self._dispatched = 0
        self._skipped = 0
        self._last_duration: float | None = None
        self._running = False
        self._run_tasks: set[asyncio.Task] = set()

    # ─────────────── loop ───────────────

    async def run(self) -> None:
        """Dispatch immediately, then on the deadline grid until stop()."""
        self._running = True
        self._stop_event.clear()
        try:
            next_tick = time.monotonic()
            while not self._stop_event.is_set():
                now = time.monotonic()
                if now >= next_tick:
                    self._attempt("interval")
                    missed = int((now - next_tick) // self.interval)
                    next_tick += (missed + 1) * self.interval
                timeout = max(0.0, next_tick - time.monotonic())
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)
                    break
                except asyncio.TimeoutError:
                    continue
        finally:
            self._running = False

    def stop(self) -> None:
        """Break the loop after the current wait; in-flight runs finish.

        Must be called on the scheduler's event loop.
        """
        self._stop_event.set()

    async def drain(self) -> None:
        """Wait for every in-flight run to finish (call after stop())."""
        while self._run_tasks:
            await asyncio.gather(*tuple(self._run_tasks), return_exceptions=True)

    # ─────────────── dispatch paths ───────────────

    def trigger(self, reason: str = "trigger") -> bool:
        """Out-of-band dispatch through the same capacity check as the loop.

        Returns False (and counts a skip) when max_in_flight is reached.
        """
        return self._attempt(reason)

    def _attempt(self, reason: str) -> bool:
        if self._in_flight >= self.max_in_flight:
            self._skipped += 1
            logger.debug(
                "%s: skipping %s dispatch (%d in flight, max %d, %d skipped)",
                self.name or "scheduler", reason,
                self._in_flight, self.max_in_flight, self._skipped,
            )
            return False
        self._in_flight += 1
        self._dispatched += 1
        task = asyncio.create_task(self._run_once())
        self._run_tasks.add(task)
        task.add_done_callback(self._run_tasks.discard)
        return True

    async def _run_once(self) -> None:
        started = time.monotonic()
        try:
            await self._dispatch()
        except Exception:
            logger.exception("%s: dispatch failed", self.name or "scheduler")
        finally:
            self._in_flight -= 1
            self._last_duration = time.monotonic() - started

    # ─────────────── reporting ───────────────

    def status(self) -> dict[str, Any]:
        return {
            "interval": self.interval,
            "max_in_flight": self.max_in_flight,
            "running": self._running,
            "in_flight": self._in_flight,
            "dispatched": self._dispatched,
            "skipped": self._skipped,
            "last_duration": self._last_duration,
        }
