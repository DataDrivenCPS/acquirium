"""Tests for AppRunner query caching and graph-change refresh.

The runner is exercised against a stub Manager so the tests stay in the
unit-test tier (no Docker / no Postgres).
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

import pytest

from acquirium.Apps.base import App, Output
from acquirium.Server.acq_app_runner import AppRunner


# ─────────────────────── stubs ───────────────────────


class StubManager:
    """Minimal Manager surface used by AppRunner."""

    def __init__(self) -> None:
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="stub-mgr")
        self._listeners: list[Callable[[], None]] = []
        self._listeners_lock = threading.Lock()
        self.timeseries_inserts: list[dict[str, Any]] = []

    def add_graph_change_listener(self, cb: Callable[[], None]) -> None:
        with self._listeners_lock:
            if cb not in self._listeners:
                self._listeners.append(cb)

    def remove_graph_change_listener(self, cb: Callable[[], None]) -> None:
        with self._listeners_lock:
            if cb in self._listeners:
                self._listeners.remove(cb)

    def fire_graph_change(self) -> None:
        with self._listeners_lock:
            listeners = list(self._listeners)
        for cb in listeners:
            cb()

    def insert_timeseries(
        self,
        *,
        source_id: str,
        ref_name: str,
        rows: Any,
        point_uri: str | None = None,
        replace: bool = False,
    ) -> int:
        self.timeseries_inserts.append(
            {
                "source_id": source_id,
                "ref_name": ref_name,
                "rows": list(rows),
                "point_uri": point_uri,
            }
        )
        return len(rows)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)


class CountingApp(App):
    """App that counts build_query() invocations and lets tests inject delays."""

    name = "counting_app"
    version = "0.1"
    app_type = "soft_sensor"

    def __init__(self) -> None:
        self.build_count = 0
        self.run_count = 0
        self.build_started = threading.Event()
        self.build_release = threading.Event()
        self.build_release.set()  # default: don't block

    def build_query(self, aq):  # type: ignore[override]
        self.build_count += 1
        self.build_started.set()
        # Allow tests to hold the build inside this method.
        self.build_release.wait()
        # Return a sentinel that AppRunner stores opaquely as ctx.query.
        return object()

    def run(self, ctx):  # type: ignore[override]
        self.run_count += 1
        return []


# ─────────────────────── fixtures ───────────────────────


@pytest.fixture
def stub_manager():
    mgr = StubManager()
    yield mgr
    mgr.shutdown()


@pytest.fixture
def runner(stub_manager):
    r = AppRunner(manager=stub_manager, aq=object())
    yield r
    r.close()


# ─────────────────────── tests ───────────────────────


def test_build_query_called_once_on_register(runner):
    app = CountingApp()
    runner.register(app)
    assert app.build_count == 1


def test_run_app_does_not_rebuild_query(runner):
    app = CountingApp()
    runner.register(app)
    runner.run_app(app.name).result()
    runner.run_app(app.name).result()
    runner.run_app(app.name).result()
    assert app.build_count == 1
    assert app.run_count == 3


def test_run_app_uses_cached_query(runner):
    app = CountingApp()
    runner.register(app)
    cached_q = runner._cached_queries[app.name].query  # noqa: SLF001

    captured: dict[str, Any] = {}

    def capture(ctx):
        captured["query"] = ctx.query
        captured["queries"] = ctx.queries
        return []

    app.run = capture  # type: ignore[assignment]
    runner.run_app(app.name).result()
    assert captured["query"] is cached_q
    assert captured["queries"]["default"] is cached_q


def test_graph_change_refreshes_all_apps(runner, stub_manager):
    a = CountingApp()
    b = CountingApp()
    b.name = "counting_app_b"
    runner.register(a)
    runner.register(b)
    assert a.build_count == 1
    assert b.build_count == 1

    stub_manager.fire_graph_change()
    runner._refresh_event.wait(timeout=2)  # noqa: SLF001

    assert a.build_count == 2
    assert b.build_count == 2


def test_run_app_blocks_during_refresh(runner, stub_manager):
    app = CountingApp()
    runner.register(app)

    # Hold the next build inside build_query so the refresh stays in flight.
    app.build_started.clear()
    app.build_release.clear()

    stub_manager.fire_graph_change()
    # Wait until the refresh worker is actually inside build_query.
    assert app.build_started.wait(timeout=2)

    run_started = threading.Event()
    run_finished = threading.Event()

    def call_run():
        run_started.set()
        runner.run_app(app.name).result()
        run_finished.set()

    t = threading.Thread(target=call_run)
    t.start()
    run_started.wait(timeout=1)

    # Give the run thread a chance to attempt to proceed; it must remain
    # blocked because the refresh is in flight.
    time.sleep(0.1)
    assert not run_finished.is_set(), "run_app should block while refresh is in progress"
    assert app.run_count == 0

    # Release the refresh; the run should then complete.
    app.build_release.set()
    t.join(timeout=2)
    assert run_finished.is_set()
    assert app.run_count == 1


def test_overlapping_graph_changes_coalesce(runner, stub_manager):
    """Multiple changes during a single refresh should fold into one extra pass."""
    app = CountingApp()
    runner.register(app)
    assert app.build_count == 1

    app.build_started.clear()
    app.build_release.clear()

    # First change starts a refresh and blocks inside build_query.
    stub_manager.fire_graph_change()
    assert app.build_started.wait(timeout=2)

    # Fire several more changes while the refresh is in flight.
    for _ in range(5):
        stub_manager.fire_graph_change()

    # Reset the latch so we can detect the second build entering.
    app.build_started.clear()
    # Release the in-flight build; the loop should now run exactly once more.
    app.build_release.set()
    runner._refresh_event.wait(timeout=2)  # noqa: SLF001

    # Initial register (1) + first refresh (1) + coalesced second pass (1) = 3.
    assert app.build_count == 3


def test_listener_unregistered_on_close(stub_manager):
    runner = AppRunner(manager=stub_manager, aq=object())
    assert len(stub_manager._listeners) == 1  # noqa: SLF001
    runner.close()
    assert len(stub_manager._listeners) == 0  # noqa: SLF001


def test_manager_graph_version_bumps_on_change():
    """Sanity-check the real Manager's _notify_graph_change increments version.

    Constructs no DB — just exercises the in-memory counter and listener
    bookkeeping by calling _notify_graph_change directly on a bare instance.
    """
    from acquirium.Server.manager import Manager

    # Bypass __init__ entirely (it requires PG_DSN, Docker, etc.) and only
    # initialize the few attributes _notify_graph_change touches.
    mgr = Manager.__new__(Manager)
    mgr._graph_version = 0
    mgr._graph_version_lock = threading.Lock()
    mgr._graph_change_listeners = []
    mgr._graph_change_listeners_lock = threading.Lock()

    assert mgr.graph_version() == 0
    mgr._notify_graph_change()
    assert mgr.graph_version() == 1
    mgr._notify_graph_change()
    mgr._notify_graph_change()
    assert mgr.graph_version() == 3

    # Listeners still fire alongside the bump.
    calls: list[int] = []
    mgr.add_graph_change_listener(lambda: calls.append(1))
    mgr._notify_graph_change()
    assert mgr.graph_version() == 4
    assert calls == [1]


def test_persist_routes_timeseries_to_manager(runner, stub_manager):
    app = CountingApp()
    runner.register(app)

    from datetime import datetime, timezone

    ts_rows = [(datetime(2026, 1, 1, tzinfo=timezone.utc), 1.0)]

    def run_with_output(ctx):
        return [Output.timeseries(point_uri="urn:test:p1", rows=ts_rows)]

    app.run = run_with_output  # type: ignore[assignment]
    runner.run_app(app.name)

    assert len(stub_manager.timeseries_inserts) == 1
    assert stub_manager.timeseries_inserts[0]["point_uri"] == "urn:test:p1"
    assert stub_manager.timeseries_inserts[0]["rows"] == ts_rows
