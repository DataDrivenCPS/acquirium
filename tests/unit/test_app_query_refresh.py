"""Tests for AppRunner-side query rebuild on graph_version change.

AppRunner is a ``@ray.remote`` actor; these tests exercise the underlying
plain class (``__ray_actor_class__``) so no Ray cluster is needed.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from acquirium.Apps.base import App
from acquirium.Server.ray_backend import AppRunner
from acquirium.internals.models import AppSpec

AppRunnerCls = AppRunner.__ray_actor_class__


# ─────────────────────── stubs ───────────────────────


class CountingApp(App):
    """App that records build_query() calls and returns sentinel objects."""

    name = "runner_test_app"
    version = "0.1"
    app_type = "soft_sensor"

    def __init__(self) -> None:
        self.build_count = 0
        self.return_dict = False
        self.raise_on_build = False

    def build_query(self, aq):  # type: ignore[override]
        self.build_count += 1
        if self.raise_on_build:
            raise RuntimeError("simulated build failure")
        if self.return_dict:
            return {"default": SimpleNamespace(tag=f"q{self.build_count}-default"),
                    "alt": SimpleNamespace(tag=f"q{self.build_count}-alt")}
        return SimpleNamespace(tag=f"q{self.build_count}")

    def run(self, ctx):  # type: ignore[override]
        return []


def make_runner(tmp_path, version_value: int | Exception = 0):
    """Build a plain AppRunner with a loaded CountingApp and a stub client
    whose graph_version() returns/raises the given value."""
    aq = MagicMock()
    if isinstance(version_value, Exception):
        aq.graph_version.side_effect = version_value
    else:
        aq.graph_version.return_value = version_value
    runner = AppRunnerCls(AppSpec(name="runner_test_app"), tmp_path, aq)
    runner.app = CountingApp()
    return runner


def run_loop_ticks(runner, ticks: int = 2, interval: float = 0.01):
    """Drive _run_loop, stopping after `ticks` dispatches; return the calls."""
    calls: list[tuple] = []

    def fake_dispatch(start, end, params):
        calls.append((start, end, params))
        if len(calls) >= ticks:
            runner._stop_event.set()
        return f"run-{len(calls)}"

    runner._dispatch_run = fake_dispatch
    asyncio.run(runner._run_loop(interval, None, None, {}))
    return calls


# ─────────────────────── build_query ───────────────────────


def test_build_query_single_query(tmp_path):
    runner = make_runner(tmp_path)
    runner.build_query()
    assert runner.query.tag == "q1"
    assert runner.queries == {"default": runner.query}


def test_build_query_dict_uses_default_key(tmp_path):
    runner = make_runner(tmp_path)
    runner.app.return_dict = True
    runner.build_query()
    assert runner.query.tag == "q1-default"
    assert set(runner.queries) == {"default", "alt"}
    assert runner.queries["default"] is runner.query


def test_build_query_before_load_raises(tmp_path):
    runner = make_runner(tmp_path)
    runner.app = None
    with pytest.raises(RuntimeError):
        runner.build_query()


# ─────────────────────── keep-alive query refresh ───────────────────────


def test_no_rebuild_when_version_unchanged(tmp_path):
    runner = make_runner(tmp_path, version_value=5)
    runner.build_query()
    runner.graph_version = 5
    initial_query = runner.query

    run_loop_ticks(runner)
    assert runner.app.build_count == 1  # only the initial build
    assert runner.query is initial_query


def test_rebuild_when_version_advances(tmp_path):
    runner = make_runner(tmp_path, version_value=7)
    runner.build_query()
    runner.graph_version = 5
    initial_query = runner.query

    run_loop_ticks(runner)
    assert runner.graph_version == 7
    assert runner.app.build_count == 2  # initial + rebuild
    assert runner.query is not initial_query
    assert runner.query.tag == "q2"
    assert runner.queries["default"] is runner.query


def test_keeps_query_when_graph_version_call_fails(tmp_path):
    runner = make_runner(tmp_path, version_value=ConnectionError("server unreachable"))
    runner.build_query()
    runner.graph_version = 3
    initial_query = runner.query

    calls = run_loop_ticks(runner)
    assert runner.graph_version == 3  # unchanged
    assert runner.app.build_count == 1  # no rebuild attempted
    assert runner.query is initial_query
    assert len(calls) == 2  # the poll failure did not skip the run


def test_keeps_query_when_rebuild_fails(tmp_path):
    runner = make_runner(tmp_path, version_value=10)
    runner.build_query()
    runner.graph_version = 5
    initial_query = runner.query
    runner.app.raise_on_build = True

    calls = run_loop_ticks(runner)
    # build_query was called again (and failed)
    assert runner.app.build_count == 2
    # runner.query is still the original (rebuild failure keeps previous query)
    assert runner.query is initial_query
    # the failure did not skip the run
    assert len(calls) == 2
