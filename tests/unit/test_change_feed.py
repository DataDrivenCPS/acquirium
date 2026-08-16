"""Tests for Apps.change_feed.ChangeFeed: enqueue-only hook, subscription
index, cycle guard, and debounced dispatch."""

from __future__ import annotations

import threading
import time

from acquirium.Apps.change_feed import ChangeFeed


class Recorder:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def __call__(self, app, reason):
        self.calls.append((app, reason))
        return True


def make_feed(**kw):
    rec = Recorder()
    feed = ChangeFeed(rec, debounce_seconds=kw.pop("debounce", 0.0),
                      min_interval=kw.pop("min_interval", 0.0), **kw)
    return feed, rec


class TestSubscriptions:
    def test_subscribe_replace_unsubscribe(self):
        feed, _ = make_feed()
        feed.subscribe("a", ["urn:r1", "urn:r2"])
        assert feed.subscribers("urn:r1") == {"a"}
        feed.subscribe("a", ["urn:r2"])                     # replaced, r1 dropped
        assert feed.subscribers("urn:r1") == set()
        assert feed.subscribers("urn:r2") == {"a"}
        feed.unsubscribe("a")
        assert feed.subscribers("urn:r2") == set()
        assert feed.status()["subscribed_apps"] == 0

    def test_many_apps_one_stream(self):
        feed, _ = make_feed()
        feed.subscribe("a", ["urn:r"]); feed.subscribe("b", ["urn:r"])
        assert feed.subscribers("urn:r") == {"a", "b"}


class TestDispatch:
    def test_notify_then_dispatch(self):
        feed, rec = make_feed()
        feed.subscribe("a", ["urn:r"])
        feed.notify("driver-x", ["urn:r"])
        assert feed.flush_now() == ["a"]
        assert rec.calls == [("a", "change")]

    def test_unsubscribed_stream_is_ignored(self):
        feed, rec = make_feed()
        feed.subscribe("a", ["urn:r"])
        feed.notify("driver-x", ["urn:other"])
        assert feed.flush_now() == [] and rec.calls == []
        assert feed.status()["notified"] == 0

    def test_bulk_insert_coalesces_to_one_run(self):
        feed, rec = make_feed()
        feed.subscribe("a", ["urn:r"])
        for _ in range(50_000 // 1000):                     # 50 notifications of a bulk insert
            feed.notify("driver-x", ["urn:r"] * 1000)
        assert feed.flush_now() == ["a"]
        assert rec.calls == [("a", "change")]
        assert feed.status()["coalesced"] == 49

    def test_debounce_window_holds_dispatch(self):
        feed, rec = make_feed(debounce=0.2)
        feed.subscribe("a", ["urn:r"])
        feed.notify("driver-x", ["urn:r"])
        assert feed.flush_now() == []                       # inside the window
        time.sleep(0.25)
        assert feed.flush_now() == ["a"]

    def test_min_interval_floors_rate(self):
        feed, rec = make_feed(min_interval=0.3)
        feed.subscribe("a", ["urn:r"])
        feed.notify("d", ["urn:r"]); assert feed.flush_now() == ["a"]
        feed.notify("d", ["urn:r"]); assert feed.flush_now() == []   # floored, stays pending
        assert feed.status()["pending"] == ["a"]
        time.sleep(0.35)
        assert feed.flush_now() == ["a"]

    def test_worker_thread_dispatches(self):
        feed, rec = make_feed(debounce=0.02)
        feed.subscribe("a", ["urn:r"])
        feed.start()
        try:
            feed.notify("d", ["urn:r"])
            deadline = time.time() + 2
            while not rec.calls and time.time() < deadline:
                time.sleep(0.01)
        finally:
            feed.stop()
        assert rec.calls == [("a", "change")]

    def test_dispatch_failure_does_not_kill_the_worker(self):
        calls = []
        def flaky(app, reason):
            calls.append(app)
            if len(calls) == 1:
                raise RuntimeError("actor died")
            return True
        feed = ChangeFeed(flaky, debounce_seconds=0.0, min_interval=0.0)
        feed.subscribe("a", ["urn:r"])
        feed.start()
        try:
            feed.notify("d", ["urn:r"])
            time.sleep(0.1)
            feed.notify("d", ["urn:r"])
            deadline = time.time() + 2
            while len(calls) < 2 and time.time() < deadline:
                time.sleep(0.01)
        finally:
            feed.stop()
        assert len(calls) == 2


class TestCycleGuard:
    def test_app_never_triggers_itself(self):
        # measurement(frm="*") matches an app's own virtual points: its own
        # output insert must not re-run it.
        feed, rec = make_feed()
        feed.subscribe("a", ["urn:a-out"])
        feed.notify("app:a", ["urn:a-out"])
        assert feed.flush_now() == [] and rec.calls == []
        assert feed.status()["suppressed_cycles"] == 1

    def test_cascade_depth_one(self):
        # driver -> A (change) ; A's outputs -> B (cascade) ; B's outputs -> nothing.
        feed, rec = make_feed()
        feed.subscribe("A", ["urn:drv"])
        feed.subscribe("B", ["urn:A-out"])
        feed.subscribe("A", ["urn:drv", "urn:B-out"])       # A also reads B: a real cycle
        feed.notify("driver", ["urn:drv"])
        assert feed.flush_now() == ["A"]
        # A ran because a driver wrote: its outputs are ordinary app inserts.
        feed.notify("app:A", ["urn:A-out"])
        assert feed.flush_now() == ["B"]
        assert rec.calls[-1] == ("B", "cascade")
        # B ran as a cascade: its outputs are marked and trigger nothing.
        feed.notify("app:B", ["urn:B-out"], cascade=True)
        assert feed.flush_now() == []
        assert feed.status()["suppressed_cycles"] == 1

    def test_mutual_subscription_terminates(self):
        # A reads B's output and B reads A's: with depth 1 the loop dies
        # after one hop no matter who starts it.
        feed, rec = make_feed()
        feed.subscribe("A", ["urn:B-out"]); feed.subscribe("B", ["urn:A-out"])
        feed.notify("app:A", ["urn:A-out"])                 # A wrote (e.g. manual run)
        assert feed.flush_now() == ["B"]                    # B runs as cascade
        feed.notify("app:B", ["urn:B-out"], cascade=True)   # B's outputs are marked
        assert feed.flush_now() == []                       # A does not run
        assert rec.calls == [("B", "cascade")]


class TestEnqueueOnly:
    def test_notify_never_blocks_on_dispatch(self):
        # The hook runs on insert threads: it must return even while a
        # dispatch is blocking the worker (a slow Ray call).
        gate = threading.Event()
        def slow(app, reason):
            gate.wait(5); return True
        feed = ChangeFeed(slow, debounce_seconds=0.0, min_interval=0.0)
        feed.subscribe("a", ["urn:r"]); feed.subscribe("b", ["urn:s"])
        feed.start()
        try:
            feed.notify("d", ["urn:r"])
            time.sleep(0.05)                                # worker now blocked in slow()
            t0 = time.monotonic()
            feed.notify("d", ["urn:s"])                     # must not wait for it
            assert time.monotonic() - t0 < 0.05
        finally:
            gate.set(); feed.stop()
