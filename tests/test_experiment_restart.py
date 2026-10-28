"""A local server restarted on the same data keeps, or upgrades, its experiment ledger."""
from datetime import datetime, timezone

import pytest

import acquirium as aq
from acquirium.Storage.metadata_outbox import execute
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


def test_key_column_migrates_existing_ledger(storage):
    options, open_store = storage
    client = aq.init(**options).client
    try:
        study = client.define_experiment("old ledger")
        variable = _declare(client, study["template_id"], "cost", "output", "scalar")
        run = client.start_experiment(study["template_id"], {})
        client.observe_experiment(run["run_id"], variable["variable_id"], value=2)
    finally:
        aq.shutdown()
    # Return the stopped server's ledger to the schema before observation keys.
    store = open_store()
    try:
        with store._lock, store._write_conn() as conn:
            # DuckDB requires dropping secondary indexes before dropping columns.
            execute(conn, "DROP INDEX experiment_observations_variable_idx")
            execute(conn, "DROP INDEX experiment_observations_ref_idx")
            execute(conn, "ALTER TABLE experiment_observations DROP COLUMN key_json")
            execute(conn, "DELETE FROM schema_migrations WHERE component='experiments' AND version=1")
            execute(conn, "CREATE INDEX experiment_observations_variable_idx ON experiment_observations(variable_id, run_id, sequence)")
            execute(conn, "CREATE INDEX experiment_observations_ref_idx ON experiment_observations(ref_uri, range_start, range_end)")
    finally:
        store.close()
    ac = aq.init(**options)
    client = ac.client
    try:
        observations = client.list_experiment_observations(study["template_id"])
        assert observations[0]["value"] == 2
        assert observations[0]["key"] is None
        flow = _declare(client, study["template_id"], "flow", "output", "timeseries", {"keys": ["pump"]})
        refs = [client.record_experiment_batch(run["run_id"], [{"form": "samples", "variable_id": flow["variable_id"],
                                                                "key": [pump], "samples": [[WHEN.isoformat(), 1.0]]}])[0]["ref_uri"]
                for pump in ("p1", "p2")]
        assert [item["key"] for item in client.list_experiment_observations(study["template_id"])][1:] == [["p1"], ["p2"]]
        client.finish_experiment(run["run_id"])
        client.delete_experiments([run["run_id"]])
        assert [client.timeseries_df(ref).height for ref in refs] == [0, 0]
    finally:
        aq.shutdown()
