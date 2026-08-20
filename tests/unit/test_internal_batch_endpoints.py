"""Unit tests for the internal continuous-batch HTTP endpoints
(continuous_batch_plan.md Phase 2a). Exercises the real FastAPI app via
TestClient with app.state.manager replaced by a fake -- no lifespan, no
Ray, no live storage. TestClient used outside a ``with`` block does not
trigger the app's (heavy: ontology/embedding) lifespan startup.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from fastapi.testclient import TestClient

import acquirium.Server.app as server_app
from acquirium.Storage.continuous.types import (
    AppBatch,
    AppRuntimeRow,
    BatchIdMismatch,
    BatchInputRange,
    CommitResult,
    GenerationMismatch,
    MUTATION_SCHEMA,
    PublicationReceipt,
)


@pytest.fixture
def client():
    return TestClient(server_app.app)


@pytest.fixture
def fake_manager():
    manager = MagicMock()
    server_app.app.state.manager = manager
    yield manager


def _mutation_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "operation": [r[0] for r in rows],
            "ref_uri": [r[1] for r in rows],
            "ts": [r[2] for r in rows],
            "numeric_value": [r[3] for r in rows],
            "text_value": [r[4] for r in rows],
        },
        schema=MUTATION_SCHEMA,
    )


def _read_arrow_response(content: bytes) -> tuple[pa.Table, dict]:
    reader = ipc.RecordBatchStreamReader(pa.BufferReader(content))
    table = reader.read_all()
    meta = json.loads(table.schema.metadata[b"acquirium_batch"])
    return table, meta


def _arrow_body(table: pa.Table, key: bytes, metadata: dict) -> bytes:
    tagged = table.replace_schema_metadata({key: json.dumps(metadata).encode()})
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, tagged.schema) as writer:
        writer.write_table(tagged)
    return sink.getvalue().to_pybytes()


# ---------------------------------------------------------------------------
# next_app_batch
# ---------------------------------------------------------------------------


def test_next_batch_returns_204_when_nothing_pending(client, fake_manager):
    fake_manager.continuous.next_app_batch.return_value = None
    resp = client.post("/internal/apps/app1/batches/next", json={"generation": 1, "target_keys": 50000})
    assert resp.status_code == 204
    fake_manager.continuous.next_app_batch.assert_called_once_with("app1", 1, 50000)


def test_next_batch_returns_arrow_with_metadata(client, fake_manager):
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = _mutation_table([("upsert", "s1", ts, 1.0, None)])
    fake_manager.continuous.next_app_batch.return_value = AppBatch(
        batch_id="b1", batch_kind="tail", generation=3, has_more=True,
        inputs=[BatchInputRange("s1", 0, 1)], rows=rows,
    )
    resp = client.post("/internal/apps/app1/batches/next", json={"generation": 3, "target_keys": 100})
    assert resp.status_code == 200
    table, meta = _read_arrow_response(resp.content)
    assert table.num_rows == 1
    assert meta["batch_id"] == "b1"
    assert meta["batch_kind"] == "tail"
    assert meta["generation"] == 3
    assert meta["has_more"] is True
    assert meta["inputs"] == [{"ref_uri": "s1", "from_version": 0, "to_version": 1}]
    assert meta["bootstrap_id"] is None


def test_next_batch_bootstrap_carries_bootstrap_id_and_end_ordinal(client, fake_manager):
    rows = _mutation_table([("upsert", "s1", datetime(2026, 1, 1, tzinfo=timezone.utc), 1.0, None)])
    fake_manager.continuous.next_app_batch.return_value = AppBatch(
        batch_id="page-1", batch_kind="bootstrap", generation=1, has_more=False,
        inputs=[], rows=rows, bootstrap_id="boot-123", end_ordinal=50,
    )
    resp = client.post("/internal/apps/app1/batches/next", json={"generation": 1})
    _, meta = _read_arrow_response(resp.content)
    assert meta["bootstrap_id"] == "boot-123"
    assert meta["end_ordinal"] == 50


def test_next_batch_generation_mismatch_is_409(client, fake_manager):
    fake_manager.continuous.next_app_batch.side_effect = GenerationMismatch("stale")
    resp = client.post("/internal/apps/app1/batches/next", json={"generation": 1})
    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# commit_app_batch (tail)
# ---------------------------------------------------------------------------


def test_commit_tail_batch_roundtrip(client, fake_manager):
    fake_manager.continuous.commit_app_batch.return_value = CommitResult(
        rows_inserted=2, already_committed=False, output_versions={"out1": 5},
    )
    outputs = _mutation_table([
        ("upsert", "out1", datetime(2026, 1, 1, tzinfo=timezone.utc), 2.0, None),
    ])
    body = _arrow_body(
        outputs,
        b"acquirium_commit",
        {
            "generation": 2,
            "batch_kind": "tail",
            "inputs": [{"ref_uri": "s1", "from_version": 0, "to_version": 1}],
            "webhook_intents": [],
        },
    )
    resp = client.post(
        "/internal/apps/app1/batches/b1/commit",
        content=body,
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload == {"rows_inserted": 2, "already_committed": False, "output_versions": {"out1": 5}}

    call_req = fake_manager.continuous.commit_app_batch.call_args.args[0]
    assert call_req.app_id == "app1"
    assert call_req.batch_id == "b1"
    assert call_req.generation == 2
    assert call_req.batch_kind == "tail"
    assert call_req.inputs == [BatchInputRange("s1", 0, 1)]

    fake_manager.wake_router.assert_called_once()
    assert set(fake_manager.wake_router.call_args.args[0]) == {"out1"}


def test_commit_tail_batch_generation_mismatch_is_409(client, fake_manager):
    fake_manager.continuous.commit_app_batch.side_effect = GenerationMismatch("stale")
    body = _arrow_body(
        _mutation_table([]), b"acquirium_commit",
        {"generation": 1, "batch_kind": "tail", "inputs": [], "webhook_intents": []},
    )
    resp = client.post(
        "/internal/apps/app1/batches/b1/commit", content=body,
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert resp.status_code == 409


def test_commit_tail_batch_id_mismatch_is_400(client, fake_manager):
    fake_manager.continuous.commit_app_batch.side_effect = BatchIdMismatch("bad id")
    body = _arrow_body(
        _mutation_table([]), b"acquirium_commit",
        {"generation": 1, "batch_kind": "tail", "inputs": [], "webhook_intents": []},
    )
    resp = client.post(
        "/internal/apps/app1/batches/b1/commit", content=body,
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert resp.status_code == 400


def test_commit_missing_metadata_is_400(client, fake_manager):
    sink = pa.BufferOutputStream()
    table = _mutation_table([])
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    resp = client.post(
        "/internal/apps/app1/batches/b1/commit", content=sink.getvalue().to_pybytes(),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert resp.status_code == 400


def test_commit_empty_output_does_not_wake_router(client, fake_manager):
    fake_manager.continuous.commit_app_batch.return_value = CommitResult(
        rows_inserted=0, already_committed=False, output_versions={},
    )
    body = _arrow_body(
        _mutation_table([]), b"acquirium_commit",
        {"generation": 1, "batch_kind": "tail", "inputs": [], "webhook_intents": []},
    )
    client.post(
        "/internal/apps/app1/batches/b1/commit", content=body,
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    fake_manager.wake_router.assert_not_called()


# ---------------------------------------------------------------------------
# commit_bootstrap_page
# ---------------------------------------------------------------------------


def test_commit_bootstrap_page_routes_to_commit_bootstrap_page(client, fake_manager):
    outputs = _mutation_table([
        ("upsert", "out1", datetime(2026, 1, 1, tzinfo=timezone.utc), 3.0, None),
    ])
    body = _arrow_body(
        outputs, b"acquirium_commit",
        {"generation": 1, "batch_kind": "bootstrap", "bootstrap_id": "boot-1", "end_ordinal": 200},
    )
    resp = client.post(
        "/internal/apps/app1/batches/page-1/commit", content=body,
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert resp.status_code == 200
    assert resp.json()["rows_inserted"] == 1

    fake_manager.continuous.commit_bootstrap_page.assert_called_once()
    args = fake_manager.continuous.commit_bootstrap_page.call_args.args
    assert args[0] == "boot-1"
    assert args[1] == "page-1"
    assert args[2] == 200
    # Bootstrap page commits don't publish immediately, so they never wake
    # the router directly (finalize_bootstrap's own publish does that).
    fake_manager.wake_router.assert_not_called()


# ---------------------------------------------------------------------------
# runtime / status
# ---------------------------------------------------------------------------


def test_app_runtime_404_when_unregistered(client, fake_manager):
    fake_manager.continuous.app_runtime.return_value = None
    resp = client.get("/internal/apps/app1/runtime")
    assert resp.status_code == 404


def test_app_runtime_returns_fields(client, fake_manager):
    fake_manager.continuous.app_runtime.return_value = AppRuntimeRow(
        app_id="app1", generation=2, status="active", topology_version=1,
        updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    resp = client.get("/internal/apps/app1/runtime")
    assert resp.status_code == 200
    assert resp.json() == {
        "app_id": "app1", "generation": 2, "status": "active", "topology_version": 1,
    }


def test_set_app_status_calls_through(client, fake_manager):
    resp = client.post("/internal/apps/app1/status", json={"status": "failed"})
    assert resp.status_code == 200
    fake_manager.continuous.set_app_status.assert_called_once_with("app1", "failed")
