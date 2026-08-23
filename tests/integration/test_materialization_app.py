"""End-to-end contracts for the live materialization application runtime."""

from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import os
import time
from uuid import uuid4

import pyarrow.ipc as ipc
import pytest
import requests

from acquirium.Materialization.definitions import definition_spec

from tests.integration.materialization_targets import (
    AddOne,
    FailingService,
    INPUT_POINT_URI,
    INPUT_REF_URI,
    INPUT_REF_NAME,
    OUTPUT_REF_URI,
    SOURCE_ID,
)


BASE_URL = (
    f"http://{os.getenv('ACQUIRIUM_TEST_SERVER_HOST', 'localhost')}:"
    f"{os.getenv('ACQUIRIUM_TEST_SERVER_PORT', '8000')}"
)

UTC = timezone.utc
START = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module", autouse=True)
def _isolate_seeded_graph():
    """Remove this module's deployment and graph-visible streams afterwards.

    Later suite modules (for example ``test_query_data``) assert over every
    stream the shared server's graph exposes, so the seeded input stream must
    not outlive this module.
    """
    yield
    _delete_transformation(AddOne.name)
    response = requests.post(
        f"{BASE_URL}/insert_graph",
        json={"rdf_graph": "# cleared by test_materialization_app teardown",
              "format": "turtle", "replace": True, "source_id": SOURCE_ID},
        timeout=30,
    )
    assert response.status_code == 200, response.text


def _definition_payload(target: type) -> dict[str, object]:
    definition = target.__acquirium_definition__
    return {
        "name": definition.name,
        "source_digest": definition.source_digest,
        "entrypoint": definition.entrypoint,
        **definition_spec(definition),
    }


def _delete_transformation(name: str) -> None:
    response = requests.delete(f"{BASE_URL}/transformations/{name}", timeout=10)
    assert response.status_code in {200, 404}, response.text


def _delete_service(name: str) -> None:
    response = requests.post(f"{BASE_URL}/services/{name}/stop", timeout=10)
    assert response.status_code in {200, 404}, response.text


def _seed_input(values: list[tuple[datetime, float]]) -> None:
    graph = f"""\
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .

<{INPUT_POINT_URI}> ref:hasExternalReference <{INPUT_REF_URI}> .
<{INPUT_POINT_URI}> acq:dataSource "{SOURCE_ID}" .
<{INPUT_REF_URI}> a acq:Stream ;
    acq:sourceId "{SOURCE_ID}" ;
    acq:refName "{INPUT_REF_NAME}" ;
    acq:valueKind "numeric" .
"""
    response = requests.post(
        f"{BASE_URL}/insert_graph",
        json={"rdf_graph": graph, "format": "turtle", "replace": True, "source_id": SOURCE_ID},
        timeout=30,
    )
    assert response.status_code == 200, response.text

    response = requests.post(
        f"{BASE_URL}/insert_timeseries",
        json=[{
            "source_id": SOURCE_ID,
            "ref_name": INPUT_REF_NAME,
            "point_uri": INPUT_POINT_URI,
            "replace": True,
            "values": [[timestamp.isoformat(), value] for timestamp, value in values],
        }],
        timeout=30,
    )
    assert response.status_code == 200, response.text


def _timeseries(uri: str) -> list[dict]:
    response = requests.get(f"{BASE_URL}/timeseries", params={"uri": uri}, timeout=10)
    assert response.status_code == 200, response.text
    return ipc.open_stream(response.content).read_all().to_pylist()


def _status() -> dict:
    response = requests.get(f"{BASE_URL}/materialization/status", timeout=10)
    assert response.status_code == 200, response.text
    return response.json()


def _wait_for(predicate, *, timeout: float = 30.0, description: str):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {description}; last value: {last!r}")


def test_transformation_is_executed_and_published_through_live_app():
    """Deployment, retained history, incremental work, and activation are one contract."""

    _delete_transformation(AddOne.name)
    _seed_input([(START, 10.0), (START.replace(hour=1), 20.0)])

    response = requests.put(
        f"{BASE_URL}/transformations/{AddOne.name}",
        json=_definition_payload(AddOne),
        timeout=30,
    )
    assert response.status_code == 200, response.text
    deployment = response.json()
    assert deployment["status"] == "deploying"
    assert deployment["epoch_id"]

    def initial_output_ready():
        rows = _timeseries(OUTPUT_REF_URI)
        values = {row["ts"]: row["value"] for row in rows}
        return values if values.get(START) == 11.0 and values.get(START.replace(hour=1)) == 21.0 else None

    initial_values = _wait_for(initial_output_ready, description="retained-history transformation output")
    assert initial_values[START] == 11.0
    assert initial_values[START.replace(hour=1)] == 21.0

    def active_status():
        status = _status()
        work = status.get("work", {})
        return status if status["active_epoch_id"] and not any(
            work.get(state, 0) for state in ("pending", "claimed")
        ) else None

    status = _wait_for(active_status, description="active materialization epoch")
    assert any(item["name"] == AddOne.name for item in status["deployments"])

    response = requests.get(f"{BASE_URL}/materialization/epochs", timeout=10)
    assert response.status_code == 200, response.text
    epoch = response.json()
    assert epoch["epoch"]["status"] == "active"
    assert len(epoch["bindings"]) == 1
    binding = epoch["bindings"][0]
    assert binding["inputs"] == {"input": [INPUT_REF_URI]}
    assert binding["outputs"] == {"output": [OUTPUT_REF_URI]}

    later = START.replace(hour=2)
    _seed_input([(START, 10.0), (START.replace(hour=1), 20.0), (later, 30.0)])

    def incremental_output_ready():
        rows = _timeseries(OUTPUT_REF_URI)
        values = {row["ts"]: row["value"] for row in rows}
        return values if values.get(later) == 31.0 else None

    values = _wait_for(incremental_output_ready, description="incremental transformation output")
    assert values[later] == 31.0


def test_artifact_request_lifecycle_is_persisted_through_http():
    request_id = f"integration-artifact-{uuid4().hex}"
    start = START
    end = START.replace(hour=1)
    response = requests.post(
        f"{BASE_URL}/artifact-requests",
        json={
            "request_id": request_id,
            "kind": "state",
            "deployment_name": "integration-stateful",
            "binding_id": "integration-binding",
            "input_versions": {INPUT_REF_URI: 1},
            "start": start.isoformat(),
            "end": end.isoformat(),
            "metadata": {"scenario": "live-api", "run_id": request_id},
        },
        timeout=10,
    )
    assert response.status_code == 200, response.text
    assert response.json()["request_id"] == request_id

    lease = None
    for _ in range(20):
        response = requests.post(
            f"{BASE_URL}/artifact-requests/lease",
            json={"owner": "integration-worker"},
            timeout=10,
        )
        assert response.status_code == 200, response.text
        candidate = response.json()["lease"]
        if candidate is None:
            continue
        if candidate["request_id"] == request_id:
            lease = candidate
            break
        # Do not let an abandoned request from an earlier local run prevent
        # this test from reaching its own durable request.
        requests.post(
            f"{BASE_URL}/artifact-requests/{candidate['request_id']}/fail",
            json={
                "owner": candidate["owner"],
                "attempt": candidate["attempt"],
                "error": {"message": "requeued by integration test"},
            },
            timeout=10,
        )
    assert lease is not None
    assert lease["attempt"] == 1

    payload = b"integration-state-v1"
    response = requests.post(
        f"{BASE_URL}/artifact-requests/{request_id}/complete",
        json={
            "owner": lease["owner"],
            "attempt": lease["attempt"],
            "data_base64": base64.b64encode(payload).decode(),
            "media_type": "application/octet-stream",
            "metadata": {"version": 1},
            "metrics": {"loss": 0.25},
        },
        timeout=10,
    )
    assert response.status_code == 200, response.text
    completion = response.json()
    assert completion["status"] == "candidate"
    revision_id = completion["revision_id"]

    response = requests.post(
        f"{BASE_URL}/state-revisions/{revision_id}/promote",
        json={"policy": "prospective"},
        timeout=10,
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "active"


def test_service_worker_failure_is_visible_through_live_app():
    _delete_service(FailingService.name)
    response = requests.post(
        f"{BASE_URL}/services/register",
        json={
            "name": FailingService.name,
            "source_digest": FailingService.__acquirium_definition__.source_digest,
            "entrypoint": FailingService.__acquirium_definition__.entrypoint,
        },
        timeout=30,
    )
    assert response.status_code == 200, response.text

    response = requests.post(f"{BASE_URL}/services/{FailingService.name}/start", timeout=10)
    assert response.status_code == 200, response.text

    def service_failed():
        response = requests.get(f"{BASE_URL}/services/{FailingService.name}", timeout=10)
        assert response.status_code == 200, response.text
        record = response.json()
        return record if record["status"] == "failed" else None

    record = _wait_for(service_failed, description="failed service status")
    assert "RuntimeError" in record["health"]
    _delete_service(FailingService.name)


def test_transformation_registration_rejects_digest_mismatch():
    payload = _definition_payload(AddOne)
    payload["source_digest"] = hashlib.sha256(b"not-the-deployed-code").hexdigest()
    response = requests.put(
        f"{BASE_URL}/transformations/materialization-integration-invalid",
        json={**payload, "name": "materialization-integration-invalid"},
        timeout=10,
    )
    assert response.status_code == 400
    assert "digest mismatch" in response.json()["detail"]
