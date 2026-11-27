"""A local server restarted on the same data keeps its experiment ledger."""
from datetime import datetime, timezone

import pytest

import acquirium as aq
from acquirium.Client.explore.attributes import ATTR_NS
from acquirium.ExperimentMetadata import EXPERIMENT, annotation_uri, run_uri
from tests.local_server import local_server_storage

WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(params=["duckdb", "postgres"])
def storage(request, tmp_path, separate_server):
    with local_server_storage(tmp_path, request.param) as storage:
        yield storage


def test_restarted_server_keeps_the_experiment_ledger(storage):
    options, _ = storage
    ac = aq.init(**options)
    client = ac.client
    try:
        # Declare a file input, start a run, and attach both a file and a stream annotation.
        study = client.define_experiment("experiment restart contract")
        variable = client.declare_experiment_variable(
            study["template_id"], label="input", role="input", kind="file", metadata={},
        )
        run = client.start_experiment(study["template_id"], {"scenario": "baseline"})
        ac.insert_metadata(run_uri(run["run_id"]), {"scenario": "edited"})
        client.attach_experiment_bytes(run["run_id"], variable["variable_id"], b"input", filename="input.txt", media_type="text/plain")
        annotation = client.annotate_experiment_stream(
            run["run_id"], "urn:contract:stream", start=WHEN.isoformat(), metadata={"treatment": "A"},
        )
        client.close_experiment_stream_annotation(annotation["annotation_id"], end=WHEN.isoformat())
    finally:
        aq.shutdown()
    client = aq.init(**options).client
    try:
        # Reopen the same storage and check the declaration, run status, bytes, and annotation.
        assert client.get_experiment_study(study["template_id"])["name"] == "experiment restart contract"
        assert len(client.list_experiment_variables(study["template_id"])) == 1
        observation = client.list_experiment_observations(study["template_id"])[0]
        assert (observation["filename"], observation["byte_length"]) == ("input.txt", 5)
        assert client.experiment_artifact(observation["artifact_digest"]) == b"input"
        restored = client.get_experiment(run["run_id"])
        assert restored["status"] == "running"
        assert restored["metadata"] == {"scenario": "edited"}
        [restored_annotation] = client.list_experiment_stream_annotations()
        assert restored_annotation["metadata"] == {"treatment": "A"}
        assert datetime.fromisoformat(restored_annotation["range_end"]) == WHEN
        result = client.sparql_query(f"""
            SELECT ?treatment WHERE {{
                <{run_uri(run['run_id'])}> a <{EXPERIMENT.Run}> .
                <{annotation_uri(annotation['annotation_id'])}> <{ATTR_NS}treatment> ?treatment .
            }}
        """, wait_for_fresh=True)
        assert result["rows"] == [["A"]]
    finally:
        aq.shutdown()
