"""Unit tests for ChangeRouter: coalescing, busy-app pending bit, has_more
re-dispatch, and safety-scan recovery -- all with a fake dispatch callback,
no Ray and no HTTP (continuous_batch_plan.md Phase 2f)."""

from __future__ import annotations

import asyncio

from acquirium.Server.router import ChangeRouter


def run(coro, timeout: float = 2.0):
    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


class FakeDispatcher:
    """Records calls and lets a test control each call's outcome/timing."""

    def __init__(self):
        self.calls: list[str] = []
        self.results: dict[str, list[dict]] = {}
        self.gates: dict[str, asyncio.Event] = {}

    async def __call__(self, app_id: str) -> dict:
        self.calls.append(app_id)
        gate = self.gates.get(app_id)
        if gate is not None:
            await gate.wait()
        queue = self.results.get(app_id)
        if queue:
            return queue.pop(0)
        return {"has_more": False}


def make_router(dispatcher, subs: dict[str, list[str]], lagging: list[str] | None = None, **kwargs):
    return ChangeRouter(
        subscription_index=lambda: subs,
        lagging_apps=lambda: lagging or [],
        dispatch=dispatcher,
        coalesce_seconds=kwargs.pop("coalesce_seconds", 0.02),
        safety_scan_seconds=kwargs.pop("safety_scan_seconds", 100.0),
        **kwargs,
    )


def test_wake_before_start_is_a_noop():
    router = make_router(FakeDispatcher(), {"s1": ["app1"]})
    router.wake(["s1"])  # must not raise even though start() was never called


def test_wake_dispatches_subscribed_app_after_coalescing():
    async def body():
        dispatcher = FakeDispatcher()
        router = make_router(dispatcher, {"s1": ["app1"]})
        await router.start()
        router.wake(["s1"])
        await asyncio.sleep(0.1)
        assert dispatcher.calls == ["app1"]
        await router.stop()

    run(body())


def test_wake_ignores_unsubscribed_ref():
    async def body():
        dispatcher = FakeDispatcher()
        router = make_router(dispatcher, {"s1": ["app1"]})
        await router.start()
        router.wake(["s2"])  # nobody subscribes to s2
        await asyncio.sleep(0.1)
        assert dispatcher.calls == []
        await router.stop()

    run(body())


def test_two_wakes_within_coalescing_window_dispatch_once():
    async def body():
        dispatcher = FakeDispatcher()
        router = make_router(dispatcher, {"s1": ["app1"]}, coalesce_seconds=0.1)
        await router.start()
        router.wake(["s1"])
        await asyncio.sleep(0.01)
        router.wake(["s1"])  # arrives inside the still-open coalescing window
        await asyncio.sleep(0.2)
        assert dispatcher.calls == ["app1"]
        await router.stop()

    run(body())


def test_busy_app_gets_one_pending_bit_not_a_queue():
    """A wake arriving while the app's dispatch is in flight must not fire a
    second concurrent dispatch; it's picked up once the first completes."""

    async def body():
        dispatcher = FakeDispatcher()
        gate = asyncio.Event()
        dispatcher.gates["app1"] = gate
        router = make_router(dispatcher, {"s1": ["app1"]})
        await router.start()

        router.wake(["s1"])
        await asyncio.sleep(0.05)  # first dispatch is now in flight, blocked on gate
        assert dispatcher.calls == ["app1"]

        router.wake(["s1"])  # a second wake while busy
        await asyncio.sleep(0.1)
        assert dispatcher.calls == ["app1"], "must not double-dispatch a busy app"

        gate.set()  # release the first call
        await asyncio.sleep(0.1)
        assert dispatcher.calls == ["app1", "app1"], "the pending wake fires once free"
        await router.stop()

    run(body())


def test_has_more_true_triggers_redispatch_until_false():
    async def body():
        dispatcher = FakeDispatcher()
        dispatcher.results["app1"] = [{"has_more": True}, {"has_more": True}, {"has_more": False}]
        router = make_router(dispatcher, {"s1": ["app1"]})
        await router.start()
        router.wake(["s1"])
        await asyncio.sleep(0.3)
        assert dispatcher.calls == ["app1", "app1", "app1"]
        await router.stop()

    run(body())


def test_dispatch_exception_does_not_wedge_the_app():
    async def body():
        calls = []

        async def flaky(app_id):
            calls.append(app_id)
            if len(calls) == 1:
                raise RuntimeError("boom")
            return {"has_more": False}

        router = make_router(flaky, {"s1": ["app1"]})
        await router.start()
        router.wake(["s1"])
        await asyncio.sleep(0.1)
        assert calls == ["app1"]
        # The app is not stuck in_flight after the exception -- a fresh wake
        # dispatches it again.
        router.wake(["s1"])
        await asyncio.sleep(0.1)
        assert calls == ["app1", "app1"]
        await router.stop()

    run(body())


def test_trigger_dispatches_without_a_wake():
    async def body():
        dispatcher = FakeDispatcher()
        router = make_router(dispatcher, {})  # no subscriptions at all
        await router.start()
        router.trigger("app1")
        await asyncio.sleep(0.1)
        assert dispatcher.calls == ["app1"]
        await router.stop()

    run(body())


def test_safety_scan_recovers_a_lost_wakeup():
    async def body():
        dispatcher = FakeDispatcher()
        router = make_router(dispatcher, {}, lagging=["app1"], safety_scan_seconds=0.05)
        await router.start()
        # No wake() call at all -- only the safety scan should catch this.
        await asyncio.sleep(0.2)
        assert "app1" in dispatcher.calls
        await router.stop()

    run(body())
