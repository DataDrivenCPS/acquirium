"""A local server restarted on the same data keeps its experiment ledger."""
from datetime import datetime, timezone

import pytest

import acquirium as aq
from tests.local_server import local_server_storage

WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _declare(client, template_id, label, role, kind, metadata=None):
    return client.declare_experiment_variable(template_id, label=label, role=role, kind=kind, metadata=metadata or {})


@pytest.fixture(params=["duckdb", "postgres"])
def storage(request, tmp_path, separate_server):
    with local_server_storage(tmp_path, request.param) as storage:
        yield storage


def test_restarted_server_keeps_the_experiment_ledger(storage):
    options, _ = storage
    client = aq.init(**options).client
    try:
        study = client.define_experiment("experiment restart contract")
        variable = _declare(client, study["template_id"], "input", "input", "file")
        run = client.start_experiment(study["template_id"], {})
        client.attach_experiment_bytes(run["run_id"], variable["variable_id"], b"input", filename="input.txt", media_type="text/plain")
        client.annotate_experiment_stream(run["run_id"], "urn:contract:stream", start="2026-01-01T00:00:00Z")
    finally:
        aq.shutdown()
    client = aq.init(**options).client
    try:
        assert client.get_experiment_study(study["template_id"])["name"] == "experiment restart contract"
        assert len(client.list_experiment_variables(study["template_id"])) == 1
        observation = client.list_experiment_observations(study["template_id"])[0]
        assert (observation["filename"], observation["byte_length"]) == ("input.txt", 5)
        assert client.experiment_artifact(observation["artifact_digest"]) == b"input"
        assert client.get_experiment(run["run_id"])["status"] == "running"
        assert len(client.list_experiment_stream_annotations()) == 1
    finally:
        aq.shutdown()
