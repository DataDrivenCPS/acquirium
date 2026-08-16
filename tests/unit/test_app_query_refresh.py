"""Tests for AppRunner-side query rebuild on source-generation change.

AppRunner is a ``@ray.remote`` actor; these tests exercise the underlying
plain class (``__ray_actor_class__``) so no Ray cluster is needed.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from acquirium.Apps.base import App
from acquirium.Apps.runner import AppRunner
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
    whose graph_status() returns/raises the given source generation."""
    aq = MagicMock()
    if isinstance(version_value, Exception):
        aq.graph_status.side_effect = version_value
    else:
        aq.graph_status.return_value = {"source_version": version_value}
    runner = AppRunnerCls(AppSpec(name="runner_test_app"), tmp_path, aq)
    runner.app = CountingApp()
    return runner


def run_loop_ticks(runner, ticks: int = 2, interval: float = 0.01):
    """Drive _run_loop, stopping after `ticks` dispatches; return the calls."""
    calls: list[tuple] = []

    # Graph polling has its own cadence with a 10s floor; test intervals are
    # milliseconds, so force a poll before every dispatch.
    runner.graph_poll_interval = 0.0

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


def test_deregister_updates_the_app_owned_graph(tmp_path):
    runner = make_runner(tmp_path)

    runner.deregister()

    runner.acquirium_cli.sparql_update.assert_called_once()
    _, kwargs = runner.acquirium_cli.sparql_update.call_args
    assert kwargs["source_id"] == runner.source_id


def test_loaded_app_receives_source_scoped_graph_helpers(tmp_path):
    runner = make_runner(tmp_path)
    app = CountingApp()
    app._bind_graph_api(runner.acquirium_cli, runner.source_id)

    app.insert_graph("@prefix ex: <urn:ex:> . ex:x ex:p ex:y .")
    app.sparql_update("DELETE WHERE { ?s ?p ?o }")

    assert app.source_id == "app:runner_test_app"
    assert runner.acquirium_cli.insert_graph.call_args.kwargs["source_id"] == app.source_id
    assert runner.acquirium_cli.sparql_update.call_args.kwargs["source_id"] == app.source_id


# ─────────────────────── keep-alive query refresh ───────────────────────


def test_no_rebuild_when_source_version_unchanged(tmp_path):
    runner = make_runner(tmp_path, version_value=5)
    runner.build_query()
    runner.source_version = 5
    initial_query = runner.query

    run_loop_ticks(runner)
    assert runner.app.build_count == 1  # only the initial build
    assert runner.query is initial_query


def test_rebuild_when_source_version_advances(tmp_path):
    runner = make_runner(tmp_path, version_value=7)
    runner.build_query()
    runner.source_version = 5
    initial_query = runner.query

    run_loop_ticks(runner)
    assert runner.source_version == 7
    assert runner.app.build_count == 2  # initial + rebuild
    assert runner.query is not initial_query
    assert runner.query.tag == "q2"
    assert runner.queries["default"] is runner.query


def test_keeps_query_when_graph_status_call_fails(tmp_path):
    runner = make_runner(tmp_path, version_value=ConnectionError("server unreachable"))
    runner.build_query()
    runner.source_version = 3
    initial_query = runner.query

    calls = run_loop_ticks(runner)
    assert runner.source_version == 3  # unchanged
    assert runner.app.build_count == 1  # no rebuild attempted
    assert runner.query is initial_query
    assert len(calls) == 2  # the poll failure did not skip the run


def test_keeps_query_when_rebuild_fails(tmp_path):
    runner = make_runner(tmp_path, version_value=10)
    runner.build_query()
    runner.source_version = 5
    initial_query = runner.query
    runner.app.raise_on_build = True

    calls = run_loop_ticks(runner)
    # build_query was called again (and failed)
    assert runner.app.build_count == 2
    # runner.query is still the original (rebuild failure keeps previous query)
    assert runner.query is initial_query
    # the failure did not skip the run
    assert len(calls) == 2


def test_graph_poll_floor_limits_polling(tmp_path):
    # At a fast run cadence the version poll must not fire every tick: with
    # the poll interval left at its floor, ~ms ticks poll at most once.
    runner = make_runner(tmp_path, version_value=5)
    runner.build_query()
    runner.source_version = 5

    calls: list[tuple] = []

    def fake_dispatch(start, end, params):
        calls.append((start, end, params))
        if len(calls) >= 3:
            runner._stop_event.set()
        return f"run-{len(calls)}"

    runner._dispatch_run = fake_dispatch
    asyncio.run(runner._run_loop(0.01, None, None, {}))
    assert len(calls) == 3
    # graph_poll_interval derived as max(interval, 10s) -> only the first
    # dispatch polled.
    assert runner.acquirium_cli.graph_status.call_count == 1


# ─────────────────────── overrun policy ───────────────────────


def test_overrun_skips_ticks_and_reports(tmp_path):
    runner = make_runner(tmp_path, version_value=0)
    runner.build_query()
    runner.source_version = 0
    runner.graph_poll_interval = 1e9  # no polls in this test

    calls: list[tuple] = []

    def fake_dispatch(start, end, params):
        calls.append((start, end, params))

        async def slow_monitor():
            await asyncio.sleep(0.08)

        run_id = f"run-{len(calls)}"
        runner._runs[run_id] = {"_monitor": asyncio.create_task(slow_monitor())}
        return run_id

    runner._dispatch_run = fake_dispatch

    async def drive():
        loop_task = asyncio.create_task(runner._run_loop(0.02, None, None, {}))
        await asyncio.sleep(0.07)
        runner._stop_event.set()
        await loop_task
        await runner._scheduler.drain()

    asyncio.run(drive())
    # One run in flight the whole time; every tick during it was skipped.
    assert len(calls) == 1
    status = runner.status()
    assert status["dispatched"] == 1
    assert status["skipped"] >= 2
    assert status["in_flight"] == 0
    assert status["last_duration"] is not None


def test_status_before_any_loop_has_zero_counters(tmp_path):
    runner = make_runner(tmp_path)
    status = runner.status()
    assert (status["in_flight"], status["dispatched"], status["skipped"]) == (0, 0, 0)
    assert status["last_duration"] is None


# ─────────────────────── build status ───────────────────────


def test_setup_failure_marks_build_failed(tmp_path):
    # No source was persisted under the app dir, so _load_app raises.
    runner = make_runner(tmp_path)
    assert runner._build_status == "pending"
    with pytest.raises(Exception):
        runner.setup()
    assert runner._build_status == "failed"


# ─────────────────────── registration ───────────────────────


def test_register_registers_the_datasource_before_the_graph(tmp_path):
    runner = make_runner(tmp_path)
    runner.spec.source_code = "class X: pass"
    runner.register()
    aq = runner.acquirium_cli
    aq.register_datasource.assert_called_once_with("app:runner_test_app")
    # Datasource first, then the registration graph under the same owner.
    names = [c[0] for c in aq.method_calls]
    assert names.index("register_datasource") < names.index("insert_graph")
    assert aq.insert_graph.call_args.kwargs["source_id"] == "app:runner_test_app"


# ─────────────────────── provenance ───────────────────────


def test_watches_data_version_not_source_version(tmp_path):
    runner = make_runner(tmp_path)
    runner.build_query()
    runner.source_version = 1
    runner.graph_poll_interval = 0.0
    # Provenance-only churn: source_version moves, data_version doesn't.
    runner.acquirium_cli.graph_status.return_value = {"source_version": 9, "data_version": 1}
    runner._maybe_refresh_query()
    assert runner.app.build_count == 1                 # no rebuild
    runner.acquirium_cli.graph_status.return_value = {"source_version": 10, "data_version": 2}
    runner._maybe_refresh_query()
    assert runner.app.build_count == 2                 # real data write rebuilds


def test_build_query_records_declared_provenance(tmp_path):
    runner = make_runner(tmp_path)
    runner.provenance.min_write_interval = 0
    fake_query = SimpleNamespace(
        tag="q", provenance=lambda: {"points": [{"ref_uri": "urn:ref1"}, {"ref_uri": "urn:ref2"}]},
    )
    runner.app.build_query = lambda aq: fake_query
    runner.build_query()
    assert runner.provenance.may_use == {"urn:ref1", "urn:ref2"}
    # Written to the app's own provenance graph, replace=True, never sparql_update.
    aq = runner.acquirium_cli
    kw = aq.insert_graph.call_args.kwargs
    assert kw["source_id"] == "app:runner_test_app:prov" and kw["replace"] is True


def test_run_task_returns_outputs_and_reads():
    from acquirium.Apps.runner import _app_run_task
    from acquirium.internals.read_recorder import record_reads

    class Reader:
        def run(self, ctx):
            record_reads(["urn:refA"])
            return ["out"]

    fn = _app_run_task.__wrapped__ if hasattr(_app_run_task, "__wrapped__") else _app_run_task._function
    outputs, reads = fn(Reader(), None)
    assert outputs == ["out"] and reads == ["urn:refA"]


def test_monitor_records_observed_reads_and_flushes(tmp_path):
    runner = make_runner(tmp_path)
    runner.provenance.min_write_interval = 0
    runner.provenance.set_declared(["urn:refA", "urn:refB"])
    runner.acquirium_cli.insert_graph.reset_mock()

    async def fake_ref():
        return (["o1", "o2"], ["urn:refA"])

    async def drive():
        runner._runs["r1"] = {"status": "running"}
        await runner._monitor_run("r1", fake_ref())

    with __import__("unittest.mock").mock.patch("acquirium.Apps.output_emission.emit_outputs"):
        asyncio.run(drive())
    assert runner._runs["r1"]["status"] == "done"
    assert runner.provenance.used == {"urn:refA"}
    assert runner.acquirium_cli.insert_graph.call_args.kwargs["source_id"] == "app:runner_test_app:prov"
    assert runner.status()["provenance"] == {"may_use": 2, "used": 1, "outputs": 0, "pending": False}
