"""Native signed feed pagination, publication cuts, and admission pressure."""

from types import SimpleNamespace

import polars as pl
import pytest
from fastapi.testclient import TestClient

from acquirium.core import Core
from acquirium.Server.app import app as api


def submit(core, request, values):
    frame = pl.DataFrame(
        {
            "stream_uri": ["urn:probe"] * len(values),
            "ts": pl.Series([time for time, _ in values], dtype=pl.Int64).cast(
                pl.Datetime("ns", "UTC")
            ),
            "value": [value for _, value in values],
        }
    )
    return core.ingest("probe", request, frame)


def test_signed_feed_pages_split_updates_and_keep_complete_epoch_frontiers(tmp_path):
    core = Core(tmp_path / "core.duckdb")
    try:
        core.wait(submit(core, "initial", [(1, 20.0), (2, 30.0)]))
        core.wait(submit(core, "correct", [(1, 40.0)]))
        core.wait(
            core.ingest(
                "probe",
                "delete",
                pl.DataFrame(
                    {
                        "stream_uri": ["urn:probe"],
                        "ts": pl.Series([2], dtype=pl.Int64).cast(
                            pl.Datetime("ns", "UTC")
                        ),
                        "value": [None],
                        "operation": ["delete"],
                    }
                ),
            )
        )
        cursor, rows, complete = {"epoch": 0, "ordinal": 0}, [], 0
        while True:
            page = core.changes(**cursor, limit=1)
            assert page["complete_through"] >= complete
            complete = page["complete_through"]
            rows.extend(page["changes"])
            cursor = page["cursor"]
            if not page["has_more"]:
                break
        assert [row["weight"] for row in rows] == [1, 1, -1, 1, -1]
        assert [row["row"]["data"]["value"] for row in rows] == [20, 30, 20, 40, 30]
        assert all(set(row["row"]["data"]) == {"value"} for row in rows)
        assert all(set(row["row"]["key"]) == {"stream", "time"} for row in rows)
        assert len({(row["epoch"], row["ordinal"]) for row in rows}) == 5
        assert complete == core.engine.snapshot().epoch
        empty = core.changes(**cursor)
        assert empty["changes"] == [] and empty["cursor"] == cursor
    finally:
        core.close()


def test_page_excludes_publications_after_its_captured_frontier(tmp_path, monkeypatch):
    core = Core(tmp_path / "core.duckdb")
    try:
        core.wait(submit(core, "initial", [(1, 20.0)]))
        native = core.engine
        frontier = native.snapshot().epoch

        snapshot = native.snapshot

        def growing_feed(*args, **kwargs):
            monkeypatch.setattr(native, "snapshot", snapshot)
            core.wait(submit(core, "later", [(2, 30.0)]))
            return snapshot(*args, **kwargs)

        monkeypatch.setattr(native, "snapshot", growing_feed)
        page = core.changes(limit=1)
        assert page["published_epoch"] == frontier
        assert page["complete_through"] == frontier
        assert not page["has_more"]
        assert [change["row"]["key"]["time"] for change in page["changes"]] == [1]
        assert core.changes(**page["cursor"])["changes"][0]["row"]["key"]["time"] == 2
    finally:
        core.close()


def test_consumer_resumes_checkpoints_with_old_native_fields(tmp_path):
    import json

    from examples.streaming.consumer import ChangeConsumer

    checkpoint = tmp_path / "sink.json"
    old = {
        "key": {"stream": "urn:probe", "time": 1000, "sample": 0},
        "data": {"value": 20., "quality": 0}, "revision": 1,
    }
    checkpoint.write_text(json.dumps({
        "cursor": {"epoch": 1, "ordinal": 0}, "complete_through": 1,
        "rows": [old], "changes": 1,
    }))
    public = {
        "key": {"stream": "urn:probe", "time": 1000},
        "data": {"value": 20.}, "revision": 1,
    }
    page = {
        "changes": [{"epoch": 2, "ordinal": 0, "weight": -1, "row": public}],
        "cursor": {"epoch": 2, "ordinal": 0}, "complete_through": 2, "has_more": False,
    }
    consumer = ChangeConsumer(SimpleNamespace(changes=lambda **kwargs: page), checkpoint)
    assert consumer.values() == {("urn:probe", 1000): 20.}
    consumer.poll()
    assert consumer.values() == {}
    assert json.loads(checkpoint.read_text())["cursor"] == page["cursor"]


def test_feed_rejects_invalid_bounds_and_future_cursors(tmp_path):
    core = Core(tmp_path / "core.duckdb")
    try:
        for kwargs in (
            {"epoch": 1},
            {"ordinal": 1},
            {"epoch": -1},
            {"ordinal": 2**64},
            {"limit": 0},
            {"limit": 10_001},
            {"limit": True},
        ):
            with pytest.raises(ValueError):
                core.changes(**kwargs)
    finally:
        core.close()


def test_http_capacity_is_retryable_and_unpublished_admission_is_not_in_feed(
    tmp_path, monkeypatch
):
    from acquirium.internals.models import compute_stream_uri
    from tests.epoch_helpers import pause_publication
    from acquirium.Server.manager import Manager

    manager = Manager(data_dir=tmp_path, exact_only=True, max_pending_requests=1)
    ref = str(compute_stream_uri("pressure", "probe"))
    try:
        manager.insert_graph(
            f'<{ref}> <urn:acquirium#sourceId> "pressure" ; '
            '<urn:acquirium#refName> "probe" .',
            source_id="pressure",
        )
        with pause_publication(manager.core, tmp_path / "app-functions", ref):
            monkeypatch.setattr(api.state, "manager", manager, raising=False)
            http = TestClient(api)
            try:
                payload = [
                    {
                        "source_id": "pressure",
                        "stream_name": "probe",
                        "values": [[1, 20]],
                        "request_id": "first",
                    }
                ]
                response = http.post("/insert_timeseries", params={"wait": 0}, json=payload)
                assert response.status_code == 200, response.text
                receipt = response.json()["receipts"][0]
                assert receipt["accepted"] and not receipt["published"]
                response = http.get("/engine/changes")
                assert response.json()["changes"] == []
                payload[0]["request_id"] = "second"
                response = http.post("/insert_timeseries", params={"wait": 0}, json=payload)
                assert response.status_code == 503
                assert response.headers["Retry-After"] == "1"
                payload[0]["request_id"] = "first"
                response = http.post("/insert_timeseries", params={"wait": 0}, json=payload)
                assert response.status_code == 200
                assert response.json()["receipts"][0]["sequence"] == receipt["sequence"]
                assert (
                    http.get("/engine/changes", params={"limit": 10_001}).status_code == 422
                )
                assert http.get("/engine/changes", params={"epoch": 999}).status_code == 400
            finally:
                http.close()
    finally:
        manager.close()


def test_a_recording_refused_at_capacity_is_retryable_and_left_uncommitted(tmp_path, monkeypatch):
    from acquirium.Server.manager import Manager
    from escapement import Capacity

    manager = Manager(data_dir=tmp_path, exact_only=True, max_pending_requests=1)
    try:
        experiments = manager.experiments
        study = experiments.define("capacity")
        level = experiments.declare(study["template_id"], "level", "output", "timeseries", {})
        run = experiments.start(study["template_id"], {})["run_id"]
        monkeypatch.setattr(api.state, "manager", manager, raising=False)
        http = TestClient(api)
        try:
            entries = [{"form": "samples", "variable_id": level["variable_id"],
                        "samples": [["2026-01-01T00:00:00+00:00", 1.0]]}]
            # The first fills the inbox; it commits without waiting to publish.
            assert http.post(f"/experiments/runs/{run}/record", json=entries).status_code == 200
            def over_capacity(*args, **kwargs):
                raise Capacity("request capacity reached")

            with monkeypatch.context() as patch:
                patch.setattr(manager.core.engine, "write", over_capacity)
                response = http.post(f"/experiments/runs/{run}/record", json=entries)
            assert response.status_code == 503 and response.headers["Retry-After"] == "1"
            # The route's own error statuses are not remapped as engine errors.
            assert http.post("/experiments/runs/missing/record", json=entries).status_code == 404
        finally:
            http.close()
        assert len(experiments.observations(study["template_id"])) == 1
    finally:
        manager.close()
