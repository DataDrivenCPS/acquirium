"""Tests for the shared TaskHost actor (undecorated class, no cluster).

The host runs task bodies inline on its event loop and shares one graph
poll across every task; these tests exercise registration/persistence,
running (one-shot + keep-alive with overrun), status parity with the app
runner, and restart self-heal from disk.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from acquirium.Apps.task_fn import ship_function
from acquirium.Apps.task_host import TaskHost, load_persisted_task, persisted_task_names
from acquirium.internals.models import AppOutputSpec, TaskSpec

HostCls = TaskHost.__ray_actor_class__


def emit_count(ctx):
    from acquirium import Output
    return [Output.event(point_uri="urn:t:out", severity="info", message="tick")]


def boom(ctx):
    raise RuntimeError("body exploded")


def make_client():
    aq = MagicMock()
    aq.client = MagicMock()
    aq.graph_status.return_value = {"source_version": 1, "data_version": 1}
    return aq


def make_spec(name="t", fn=emit_count, **kw) -> TaskSpec:
    shipped = ship_function(fn)
    return TaskSpec(
        name=name, query={}, outputs=[AppOutputSpec(kind="event", point_uri="urn:t:out")],
        **shipped, **kw,
    )


def make_host(tmp_path, aq=None):
    return HostCls(tmp_path, aq or make_client())


# ─────────────────────── registration ───────────────────────


class TestRegistration:
    def test_register_persists_writes_graph_and_loads(self, tmp_path):
        aq = make_client()
        host = make_host(tmp_path, aq)
        info = host.register(make_spec())
        assert info["load_error"] is None
        assert info["outputs"] == ["urn:t:out"]
        # Datasource + graph under the task's own source id.
        aq.register_datasource.assert_called_once_with("app:t")
        assert aq.insert_graph.call_args.kwargs["source_id"] == "app:t"
        # Persisted: meta without the blob, blob beside it.
        assert persisted_task_names(tmp_path) == ["t"]
        reloaded = load_persisted_task(tmp_path, "t")
        assert reloaded.fn_name == "emit_count" and reloaded.fn_blob is not None

    def test_deregister_strips_graph_and_disk(self, tmp_path):
        aq = make_client()
        host = make_host(tmp_path, aq)
        host.register(make_spec())
        host.deregister("t")
        assert not host.has("t")
        assert aq.sparql_update.call_args.kwargs["source_id"] == "app:t"
        assert persisted_task_names(tmp_path) == []

    def test_bad_source_is_recorded_not_fatal(self, tmp_path):
        host = make_host(tmp_path)
        spec = make_spec()
        spec.fn_source = "x = 1\n"      # defines no function
        spec.fn_blob = None
        info = host.register(spec)
        assert "did not define" in info["load_error"]
        assert host.status("t")["build"] == "failed"

    def test_restart_reloads_from_disk(self, tmp_path):
        host = make_host(tmp_path)
        host.register(make_spec("a"))
        host.register(make_spec("b"))
        # A fresh host (restart) heals from disk without any server call.
        aq2 = make_client()
        host2 = make_host(tmp_path, aq2)
        assert {t["name"] for t in host2.list_tasks()} == {"a", "b"}
        aq2.insert_graph.assert_not_called()

    def test_corrupt_persisted_task_does_not_sink_the_host(self, tmp_path):
        host = make_host(tmp_path)
        host.register(make_spec("good"))
        (tmp_path / "bad").mkdir()
        (tmp_path / "bad" / "task.json").write_text("{not json")
        host2 = make_host(tmp_path)
        names = {t["name"]: t for t in host2.list_tasks()}
        assert names["good"]["load_error"] is None
        assert "reload failed" in names["bad"]["load_error"]


# ─────────────────────── running ───────────────────────


class TestRunning:
    def test_one_shot_emits_outputs(self, tmp_path):
        aq = make_client()
        host = make_host(tmp_path, aq)
        host.register(make_spec())
        result = asyncio.run(host.run("t"))
        assert result["run_id"] == "t-1"
        (run,) = host.status("t")["runs"]
        assert run["status"] == "done" and run["outputs"] == 1
        # emit_outputs went through the client's insert_timeseries.
        aq.client.insert_timeseries.assert_called_once()
        assert aq.client.insert_timeseries.call_args.kwargs["source_id"] == "app:t"

    def test_failed_body_is_recorded(self, tmp_path):
        host = make_host(tmp_path)
        host.register(make_spec(fn=boom))
        asyncio.run(host.run("t"))
        (run,) = host.status("t")["runs"]
        assert run["status"] == "failed" and "body exploded" in run["error"]

    def test_unknown_task(self, tmp_path):
        host = make_host(tmp_path)
        with pytest.raises(KeyError):
            asyncio.run(host.run("nope"))

    def test_keep_alive_dispatches_and_stops(self, tmp_path):
        host = make_host(tmp_path)
        host.register(make_spec())

        async def drive():
            info = await host.run("t", keep_alive=True, interval=0.02)
            assert info["keep_alive"] is True
            await asyncio.sleep(0.07)
            host.stop("t")
            await host._tasks["t"].loop_task
            return host.status("t")

        status = asyncio.run(drive())
        assert status["dispatched"] >= 2
        assert status["keep_alive"] is False
        assert all(r["status"] == "done" for r in status["runs"])

    def test_slow_body_skips_ticks(self, tmp_path):
        host = make_host(tmp_path)
        spec = make_spec()
        host.register(spec)
        # Replace the loaded body with a slow one (bodies run inline; the
        # scheduler's in-flight window spans the awaited tick).
        original_run_once = host._run_once

        async def slow_run_once(t, params, start=None, end=None, reason="interval"):
            await asyncio.sleep(0.08)
            return await original_run_once(t, params, start, end, reason=reason)

        host._run_once = slow_run_once

        async def drive():
            await host.run("t", keep_alive=True, interval=0.02)
            await asyncio.sleep(0.07)
            host.stop("t")
            await host._tasks["t"].loop_task
            await host._tasks["t"].scheduler.drain()
            return host.status("t")

        status = asyncio.run(drive())
        assert status["dispatched"] == 1
        assert status["skipped"] >= 2

    def test_graph_poll_is_shared_and_floored(self, tmp_path):
        aq = make_client()
        host = make_host(tmp_path, aq)
        host.register(make_spec("a"))
        host.register(make_spec("b"))

        async def drive():
            await host.run("a", keep_alive=True, interval=0.01)
            await host.run("b", keep_alive=True, interval=0.01)
            await asyncio.sleep(0.06)
            host.stop("a"); host.stop("b")
            await asyncio.gather(host._tasks["a"].loop_task, host._tasks["b"].loop_task)

        asyncio.run(drive())
        # Many ticks across two tasks, but the 10s floor means one poll total.
        assert aq.graph_status.call_count == 1

    def test_query_refresh_on_data_version_change(self, tmp_path):
        aq = make_client()
        aq.graph_status.return_value = {"source_version": 5, "data_version": 5}
        host = make_host(tmp_path, aq)
        host.graph_poll_interval = 0.0
        spec = make_spec()
        spec.query = {"nodes": [{"id": 0, "alias": "a", "constraints": {"rdf_class": "urn:c"}}],
                      "edges": [], "aliases": {"a": 0}, "aliases_reverse": {0: "a"},
                      "current_pointer": 0, "selects": [], "data_nodes": []}
        host.register(spec)
        q1 = host._tasks["t"].query
        assert q1 is not None
        host._maybe_refresh_queries()      # version 0 -> 5: rebuilt
        q2 = host._tasks["t"].query
        assert q2 is not q1 and q2.query_graph == q1.query_graph
        host._maybe_refresh_queries()      # unchanged: kept
        assert host._tasks["t"].query is q2

    def test_status_parity_fields(self, tmp_path):
        host = make_host(tmp_path)
        host.register(make_spec())
        s = host.status("t")
        for key in ("build", "keep_alive", "in_flight", "dispatched", "skipped",
                    "last_duration", "runs"):
            assert key in s
        assert s["kind"] == "task"


# ─────────────────────── provenance ───────────────────────


def read_and_emit(ctx):
    from acquirium import Output
    from acquirium.internals.read_recorder import record_reads
    record_reads(["urn:refA"])          # stands in for ctx.query.data().latest()
    return [Output.event(point_uri="urn:t:out", severity="info", message="x")]


class TestProvenance:
    def test_observed_reads_are_recorded_and_flushed(self, tmp_path):
        aq = make_client()
        aq.client.sparql_query.return_value = {"rows": []}
        host = make_host(tmp_path, aq)
        host.register(make_spec(fn=read_and_emit))
        host._tasks["t"].provenance.min_write_interval = 0
        asyncio.run(host.run("t"))
        prov = host.status("t")["provenance"]
        assert prov["used"] == 1 and prov["outputs"] == 1 and prov["pending"] is False
        # Written to the task's own provenance graph, never via sparql_update.
        prov_writes = [c for c in aq.insert_graph.call_args_list
                       if c.kwargs.get("source_id") == "app:t:prov"]
        assert prov_writes and prov_writes[-1].kwargs["replace"] is True

    def test_scopes_do_not_leak_between_tasks(self, tmp_path):
        aq = make_client()
        aq.client.sparql_query.return_value = {"rows": []}
        host = make_host(tmp_path, aq)
        host.register(make_spec("reader", fn=read_and_emit))
        host.register(make_spec("quiet", fn=emit_count))
        for t in host._tasks.values():
            t.provenance.min_write_interval = 0
        asyncio.run(host.run("reader"))
        asyncio.run(host.run("quiet"))
        assert host._tasks["reader"].provenance.used == {"urn:refA"}
        assert host._tasks["quiet"].provenance.used == set()

    def test_deregister_drops_the_prov_graph(self, tmp_path):
        aq = make_client()
        host = make_host(tmp_path, aq)
        host.register(make_spec())
        host.deregister("t")
        wipes = [c for c in aq.insert_graph.call_args_list
                 if c.kwargs.get("source_id") == "app:t:prov" and c.kwargs.get("replace")]
        assert wipes and wipes[-1].args[0] == ""
