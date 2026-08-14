"""Tests for internals.scheduling.IntervalScheduler."""

import asyncio

import pytest

from acquirium.internals.scheduling import IntervalScheduler


async def run_for(scheduler: IntervalScheduler, seconds: float) -> None:
    loop_task = asyncio.create_task(scheduler.run())
    await asyncio.sleep(seconds)
    scheduler.stop()
    await loop_task
    await scheduler.drain()


def test_overrun_skips_and_counts():
    events = []

    async def slow_dispatch():
        events.append("start")
        await asyncio.sleep(0.09)
        events.append("end")

    async def main():
        s = IntervalScheduler(0.02, slow_dispatch, name="t")
        await run_for(s, 0.08)
        return s

    s = asyncio.run(main())
    status = s.status()
    # One dispatch immediately; every tick during the 90ms run is skipped.
    assert status["dispatched"] == 1
    assert status["skipped"] >= 2
    assert status["in_flight"] == 0
    assert status["last_duration"] == pytest.approx(0.09, abs=0.05)
    # The overlapping starts never happened: strict start/end pairing.
    assert events == ["start", "end"]


def test_no_burst_after_stall():
    starts = []

    async def dispatch():
        starts.append(asyncio.get_running_loop().time())
        # First run stalls several intervals; later runs are instant.
        if len(starts) == 1:
            await asyncio.sleep(0.07)

    async def main():
        s = IntervalScheduler(0.02, dispatch, name="t")
        await run_for(s, 0.13)
        return s

    s = asyncio.run(main())
    # 130ms at 20ms interval = 7 grid points. The 70ms stall covers ~3 of
    # them (skipped, not queued): dispatches stay well under the grid count
    # and there is no catch-up burst afterwards.
    assert 2 <= s.status()["dispatched"] <= 5
    gaps = [b - a for a, b in zip(starts[1:], starts[2:])]
    assert all(g >= 0.015 for g in gaps)


def test_max_in_flight_allows_bounded_concurrency():
    concurrent = 0
    peak = 0

    async def dispatch():
        nonlocal concurrent, peak
        concurrent += 1
        peak = max(peak, concurrent)
        await asyncio.sleep(0.05)
        concurrent -= 1

    async def main():
        s = IntervalScheduler(0.02, dispatch, max_in_flight=2, name="t")
        await run_for(s, 0.06)
        return s

    s = asyncio.run(main())
    assert peak == 2
    assert s.status()["skipped"] >= 1


def test_trigger_shares_the_capacity_check():
    async def slow_dispatch():
        await asyncio.sleep(0.1)

    async def main():
        s = IntervalScheduler(10.0, slow_dispatch, name="t")
        assert s.trigger("change") is True       # capacity free
        await asyncio.sleep(0)                    # let the run task start
        assert s.trigger("change") is False       # in flight -> skip
        assert s.status()["skipped"] == 1
        assert s.status()["in_flight"] == 1
        s.stop()
        await s.drain()
        return s

    s = asyncio.run(main())
    assert s.status()["in_flight"] == 0


def test_dispatch_exception_does_not_kill_the_loop():
    calls = []

    async def failing_dispatch():
        calls.append(1)
        raise RuntimeError("boom")

    async def main():
        s = IntervalScheduler(0.02, failing_dispatch, name="t")
        await run_for(s, 0.05)
        return s

    s = asyncio.run(main())
    assert len(calls) >= 2                       # kept dispatching after failure
    assert s.status()["in_flight"] == 0


def test_stop_is_prompt():
    async def dispatch():
        pass

    async def main():
        s = IntervalScheduler(60.0, dispatch, name="t")
        loop_task = asyncio.create_task(s.run())
        await asyncio.sleep(0.01)
        s.stop()
        await asyncio.wait_for(loop_task, timeout=1.0)
        return s

    s = asyncio.run(main())
    assert s.status()["running"] is False
    assert s.status()["dispatched"] == 1         # the immediate first run


def test_validation():
    async def dispatch():
        pass

    with pytest.raises(ValueError):
        IntervalScheduler(0, dispatch)
    with pytest.raises(ValueError):
        IntervalScheduler(1.0, dispatch, max_in_flight=0)
