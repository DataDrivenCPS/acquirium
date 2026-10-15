"""A committed step's derived-stream changes reach the feed and any trigger URL."""
from __future__ import annotations

import json
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import pyarrow as pa
import pytest

from acquirium.Materialization import ChangeFeed, StepChanges, StreamChange, output
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import NOW
from tests.unit.test_materialization_readiness import CopyEach, FleetGraph

RECEIVED: list[dict] = []


class _Hook(BaseHTTPRequestHandler):
    def do_POST(self):
        RECEIVED.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
        self.send_response(204); self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def hook():
    RECEIVED.clear()
    server = HTTPServer(("127.0.0.1", 0), _Hook)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}/hook"
    server.shutdown()


class Alarmed(CopyEach):
    name = "alarmed"
    outputs = {"out": output.trigger("http://placeholder.invalid/", value_kind="numeric")}


def _runtime(tmp_path, app=CopyEach, **params):
    store = DuckDBStore(tmp_path / "changes.duckdb")
    runtime = Materializer(store, FleetGraph(["urn:a", "urn:b"]))
    for ref in ("urn:a", "urn:b"):
        store.upsert_rows(ref, [(NOW, 1.0)], value_kind="numeric")
    runtime.deploy(Deployment.from_class(app, parameters=params) if params else Deployment.from_class(app))
    while runtime.run_once():
        pass
    return store, runtime


def test_a_committed_step_publishes_what_it_wrote(tmp_path):
    store, runtime = _runtime(tmp_path)
    try:
        last = runtime.changes.since(0)["last_step"]
        store.upsert_rows("urn:a", [(NOW + timedelta(minutes=1), 4.0)], value_kind="numeric")
        assert runtime.run_once()
        feed = runtime.changes.since(last)
        assert len(feed["steps"]) == 1 and not feed["gap"]
        (change,) = feed["steps"][0]["changes"]
        assert change["app"] == "copy-each" and change["port"] == "out"
        assert [row["value"] for row in change["rows"]] == [4.0]
    finally:
        runtime.close(); store.close()


def test_a_step_that_rolls_back_publishes_nothing(tmp_path, monkeypatch):
    store, runtime = _runtime(tmp_path)
    try:
        last = runtime.changes.since(0)["last_step"]
        store.upsert_rows("urn:a", [(NOW + timedelta(minutes=1), 4.0)], value_kind="numeric")
        monkeypatch.setattr(store, "_insert_frame", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("disk full")))
        with pytest.raises(RuntimeError):
            runtime.run_once()
        monkeypatch.undo()
        assert runtime.changes.since(last)["steps"] == []
        assert runtime.run_once()
        assert len(runtime.changes.since(last)["steps"]) == 1
    finally:
        runtime.close(); store.close()


def test_a_trigger_output_is_posted_after_the_commit(tmp_path, hook, monkeypatch):
    monkeypatch.setattr(Alarmed, "outputs", {"out": output.trigger(hook, value_kind="numeric")})
    store, runtime = _runtime(tmp_path, Alarmed)
    try:
        runtime.changes.close()   # wait for the posts already submitted
        assert {body["app"] for body in RECEIVED} == {"alarmed"} and len(RECEIVED) == 2
        assert all(body["rows"][0]["value"] == 1.0 and body["step_no"] >= 1 for body in RECEIVED)
    finally:
        runtime.close(); store.close()


def test_a_failed_trigger_is_reported_not_retried(tmp_path, monkeypatch):
    monkeypatch.setattr(Alarmed, "outputs", {"out": output.trigger("http://127.0.0.1:9/none", value_kind="numeric")})
    store, runtime = _runtime(tmp_path, Alarmed)
    try:
        runtime.changes.close()
        assert any(key.startswith("trigger[") for key in runtime.failures())
    finally:
        runtime.close(); store.close()


def test_a_reader_waits_for_the_next_step_and_learns_of_gaps():
    feed = ChangeFeed(capacity=2)
    empty = pa.table({"time": pa.array([], pa.timestamp("us", tz="UTC")), "value": pa.array([], pa.float64())})
    step = lambda n: StepChanges(n, n, (StreamChange("urn:x", "app", "out", ("a", "b"), empty),))
    threading.Timer(0.1, feed.publish, args=(step(1),)).start()
    assert [s["step_no"] for s in feed.since(0, wait=2.0)["steps"]] == [1]
    feed.publish(step(2)); feed.publish(step(3))
    assert feed.since(0)["gap"] and not feed.since(1)["gap"]
    feed.close()


def test_a_trigger_must_be_a_url():
    with pytest.raises(ValueError):
        output.trigger("not-a-url", value_kind="numeric")
