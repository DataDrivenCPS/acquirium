"""Unit tests for the internal continuous-batch HTTP endpoints
(continuous_batch_plan.md Phase 2a). Exercises the real FastAPI app via
TestClient with app.state.manager replaced by a fake -- no lifespan, no
Ray, no live storage. TestClient used outside a ``with`` block does not
trigger the app's (heavy: ontology/embedding) lifespan startup.
"""

from __future__ import annotations

import json
import base64
from datetime import datetime, timezone
from unittest.mock import MagicMock
from types import SimpleNamespace

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
from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.materialization.types import InputSnapshot, PlanPartition, WorkLease
from acquirium.Materialization.state import ArtifactLease, ArtifactRequest, StateRevision
from acquirium.Storage.artifacts import ArtifactRecord


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


def test_materialization_lease_and_snapshot_arrow_transport(client, fake_manager):
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    lease = WorkLease(PlanPartition("partition", "plan", TimeRange(timestamp, timestamp.replace(minute=1)), "leased"), "worker", 1, timestamp.replace(minute=5))
    table = _mutation_table([("upsert", "urn:in", timestamp, 1.0, None)])
    fake_manager.materialization.lease_partition.return_value = lease
    fake_manager.materialization.leased_partition.return_value = lease
    fake_manager.materialization.partition_refs.return_value = (("urn:in",), ("urn:out",))
    fake_manager.materialization.snapshot_partition.return_value = InputSnapshot(lease, table, {"urn:in": 1})
    response = client.post("/internal/materializations/lease", json={"owner": "worker"})
    assert response.status_code == 200 and response.json()["lease"]["partition_id"] == "partition"
    response = client.post("/internal/materializations/partition/snapshot", json={"owner": "worker", "attempt": 1})
    assert response.status_code == 200
    reader = ipc.RecordBatchStreamReader(pa.BufferReader(response.content)); result = reader.read_all()
    metadata = json.loads(result.schema.metadata[b"acquirium-materialization-snapshot"])
    assert result.num_rows == 1 and metadata["output_refs"] == ["urn:out"]


def test_materialization_commit_and_fail_transport(client, fake_manager):
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    lease = WorkLease(PlanPartition("partition", "plan", TimeRange(timestamp, timestamp.replace(minute=1)), "leased"), "worker", 2, timestamp.replace(minute=5))
    snapshot = InputSnapshot(lease, _mutation_table([("upsert", "urn:in", timestamp, 1.0, None)]), {"urn:in": 1})
    fake_manager.materialization.leased_partition.return_value = lease
    fake_manager.materialization.partition_refs.return_value = (("urn:in",), ("urn:out",))
    fake_manager.materialization.snapshot_partition.return_value = snapshot
    fake_manager.materialization.commit_replacement.return_value = "publication"
    output = pa.table({"ref_uri": ["urn:out"], "ts": [timestamp], "numeric_value": [2.0], "text_value": [None]})
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, output.schema) as writer:
        writer.write_table(output)
    response = client.post("/internal/materializations/partition/commit?owner=worker&attempt=2", content=sink.getvalue().to_pybytes())
    assert response.status_code == 200 and response.json()["output_publication_id"] == "publication"
    fake_manager.materialization.commit_replacement.assert_called_once()
    response = client.post("/internal/materializations/partition/fail", json={"owner": "worker", "attempt": 2, "error": {"message": "bad"}})
    assert response.status_code == 200
    fake_manager.materialization.fail_partition.assert_called_once_with(lease, {"message": "bad"})


def test_generic_artifact_request_transport(client, fake_manager):
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    request = ArtifactRequest("artifact-request", "calibration", "demo", "binding", {"urn:in": 3},
                              TimeRange(timestamp, timestamp.replace(minute=1)))
    lease = ArtifactLease(request, "producer", 2, timestamp.replace(minute=5))
    fake_manager.materialization.create_artifact_request.return_value = request.request_id
    fake_manager.materialization.lease_artifact_request.return_value = lease
    record = ArtifactRecord("a" * 64, "file:///artifact", 3, "application/octet-stream", {})
    revision = StateRevision("revision", "demo", "binding", record, "candidate")
    fake_manager.materialization.leased_artifact_request.return_value = lease
    fake_manager.materialization.complete_artifact_request.return_value = revision

    create = client.post("/artifact-requests", json={"request_id": request.request_id, "kind": request.kind,
        "deployment_name": request.deployment_name, "binding_id": request.binding_id,
        "input_versions": dict(request.input_versions), "start": request.interval.start.isoformat(),
        "end": request.interval.end.isoformat()})
    assert create.status_code == 200 and create.json()["request_id"] == request.request_id
    leased = client.post("/artifact-requests/lease", json={"owner": "producer"})
    assert leased.status_code == 200 and leased.json()["lease"]["attempt"] == 2
    fake_manager.materialization_artifacts.put.return_value = record
    complete = client.post(f"/artifact-requests/{request.request_id}/complete", json={"owner": "producer",
        "attempt": 2, "data_base64": base64.b64encode(b"abc").decode()})
    assert complete.status_code == 200 and complete.json()["revision_id"] == "revision"


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


# ---------------------------------------------------------------------------
# resume_status / reset / bootstrap begin+finalize
# ---------------------------------------------------------------------------


def test_resume_status(client, fake_manager):
    fake_manager.continuous.has_subscriptions.return_value = True
    fake_manager.continuous.resumable.return_value = False
    resp = client.get("/internal/apps/app1/resume_status", params={"generation": 2})
    assert resp.status_code == 200
    assert resp.json() == {"has_subscriptions": True, "resumable": False}
    fake_manager.continuous.has_subscriptions.assert_called_once_with("app1", 2)
    fake_manager.continuous.resumable.assert_called_once_with("app1", 2)


def test_reset_app_returns_new_generation(client, fake_manager):
    fake_manager.continuous.reset_app.return_value = 4
    resp = client.post("/internal/apps/app1/reset")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "app_id": "app1", "generation": 4}


def test_reset_app_404_when_unregistered(client, fake_manager):
    fake_manager.continuous.reset_app.side_effect = KeyError("no runtime state")
    resp = client.post("/internal/apps/app1/reset")
    assert resp.status_code == 404


def test_begin_bootstrap_roundtrip(client, fake_manager):
    from acquirium.Storage.continuous.types import BootstrapState

    fake_manager.continuous.begin_bootstrap.return_value = BootstrapState(
        bootstrap_id="boot-1", app_id="app1", generation=1, streams={"s1": 3},
    )
    resp = client.post(
        "/internal/apps/app1/bootstrap/begin",
        json={"input_ref_uris": ["s1"], "output_ref_uris": ["out1"]},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "bootstrap_id": "boot-1", "app_id": "app1", "generation": 1, "streams": {"s1": 3},
    }
    fake_manager.continuous.begin_bootstrap.assert_called_once_with("app1", ["s1"], ["out1"])


def test_finalize_bootstrap_roundtrip(client, fake_manager):
    resp = client.post("/internal/bootstrap/boot-1/finalize")
    assert resp.status_code == 200
    fake_manager.continuous.finalize_bootstrap.assert_called_once_with("boot-1")


def test_finalize_bootstrap_404_for_unknown_id(client, fake_manager):
    fake_manager.continuous.finalize_bootstrap.side_effect = KeyError("unknown bootstrap")
    resp = client.post("/internal/bootstrap/nope/finalize")
    assert resp.status_code == 404


def test_promote_state_revision_forwards_explicit_policy(client, fake_manager):
    fake_manager.materialization.promote_state_revision.return_value = SimpleNamespace(
        revision_id="revision", status="active", policy="prospective", effective_from=None
    )
    response = client.post("/state-revisions/revision/promote", json={"policy": "prospective"})
    assert response.status_code == 200
    assert response.json()["revision_id"] == "revision"
    fake_manager.materialization.promote_state_revision.assert_called_once_with(
        "revision", policy="prospective", effective_from=None
    )
