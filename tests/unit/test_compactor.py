"""Unit tests for the Compactor lifespan loop (no live storage needed --
a fake ContinuousStore.compact stands in)."""

from __future__ import annotations

import asyncio

from acquirium.Server.compactor import Compactor
from acquirium.Storage.continuous.types import CompactReport


def run(coro, timeout: float = 2.0):
    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


class FakeContinuous:
    def __init__(self):
        self.calls: list[int] = []

    def compact(self, chunk_rows: int = 100_000) -> CompactReport:
        self.calls.append(chunk_rows)
        return CompactReport(manifest_rows_deleted=len(self.calls), refs_advanced=1)


def test_compactor_runs_on_interval_and_records_last_report():
    async def body():
        continuous = FakeContinuous()
        compactor = Compactor(continuous, interval_seconds=0.05, chunk_rows=42)
        await compactor.start()
        await asyncio.sleep(0.17)
        await compactor.stop()

        assert len(continuous.calls) >= 2
        assert all(c == 42 for c in continuous.calls)
        assert compactor.last_report is not None
        assert compactor.last_report.refs_advanced == 1

    run(body())


def test_compactor_survives_a_failing_pass():
    async def body():
        class FailingContinuous:
            def __init__(self):
                self.calls = 0

            def compact(self, chunk_rows: int = 100_000):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("db down")
                return CompactReport(manifest_rows_deleted=1, refs_advanced=1)

        continuous = FailingContinuous()
        compactor = Compactor(continuous, interval_seconds=0.03)
        await compactor.start()
        await asyncio.sleep(0.15)
        await compactor.stop()

        assert continuous.calls >= 2
        assert compactor.last_report is not None  # the later successful pass recorded

    run(body())


def test_compactor_stop_cancels_the_loop():
    async def body():
        continuous = FakeContinuous()
        compactor = Compactor(continuous, interval_seconds=0.02)
        await compactor.start()
        await asyncio.sleep(0.05)
        await compactor.stop()
        calls_at_stop = len(continuous.calls)
        await asyncio.sleep(0.1)
        assert len(continuous.calls) == calls_at_stop

    run(body())
