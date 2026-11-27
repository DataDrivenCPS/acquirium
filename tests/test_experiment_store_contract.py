"""Experiment ledger contract for local servers on DuckDB and PostgreSQL."""
import base64
import io
from datetime import datetime, timezone
from hashlib import sha256

import polars as pl
import pytest
from requests import HTTPError

import acquirium as aq
from tests.local_server import acquirium_without_experiments, local_server_storage

WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module", params=["duckdb", "postgres"])
def backend(request, local_acquirium, tmp_path_factory):
    """Options for the shared DuckDB server, or for one on a PostgreSQL schema."""
    if request.param == "duckdb":
        yield local_acquirium
        return
    with local_server_storage(tmp_path_factory.mktemp("postgres"), "postgres") as (options, _):
        aq.shutdown()
        try:
            yield options
        finally:
            aq.shutdown()


@pytest.fixture
def ac(backend):
    return acquirium_without_experiments(backend)


def _declare(client, template_id, label, role, kind, metadata=None):
    return client.declare_experiment_variable(template_id, label=label, role=role, kind=kind, metadata=metadata or {})


def _post(ac, path, body):
    return ac.client._http.post(f"{ac.client.base_url}{path}", json=body)


def test_backend_deletes_runs_studies_and_clears_experiments(ac):
    client = ac.client
    # A plant stream is outside the ledger, even when a run annotates it.
    ac.register_streams([{"source_id": "contract", "stream_name": "site level"}])
    ac.insert_timeseries("contract", "site level", [(WHEN, 1000.0)])
    site = str(ac.reference_uri("contract", "site level"))
    first = client.define_experiment("tank")["template_id"]
    second = client.define_experiment("pump")["template_id"]
    tank_config = _declare(client, first, "config", "input", "file")["variable_id"]
    tank_level = _declare(client, first, "level", "output", "timeseries")["variable_id"]
    pump_config = _declare(client, second, "config", "input", "file")["variable_id"]
    pump_level = _declare(client, second, "level", "output", "timeseries")["variable_id"]

    def record(study, scenario, config, level, readings):
        """Start a run, attach the shared bytes, and return its run and stream IDs."""
        run = client.start_experiment(study, {"scenario": scenario})["run_id"]
        client.attach_experiment_bytes(run, config, b"shared", filename="shared.txt")
        [receipt] = client.record_experiment_batch(run, [
            {"form": "samples", "variable_id": level, "samples": readings},
        ])
        return run, receipt["stream_uri"]

    baseline, baseline_stream = record(first, "baseline", tank_config, tank_level, [(WHEN.isoformat(), 1)])
    readings = [(WHEN.isoformat(), 2), (WHEN.replace(minute=1).isoformat(), 3)]
    trial, trial_stream = record(first, "trial", tank_config, tank_level, readings)
    stress, stress_stream = record(second, "stress", pump_config, pump_level, readings)
    for run in (baseline, trial):
        client.annotate_experiment_stream(run, site, start=WHEN.isoformat())
    own = client.attach_experiment_bytes(baseline, tank_config, b"own", filename="own.txt")["digest"]
    shared = sha256(b"shared").hexdigest()

    # Usage counts all five samples but stores the shared attachment once.
    usage = client.experiment_usage()
    expected = {"studies": 2, "variables": 4, "runs": 3, "observations": 7,
                "annotations": 2, "samples": 5, "files": 2, "file_bytes": 9}
    assert {key: usage[key] for key in expected} == expected
    assert usage["sample_bytes"] == 5 * 16
    assert usage["total_bytes"] == usage["sample_bytes"] + usage["file_bytes"] + usage["ledger_bytes"]
    scope = client.experiment_usage(template_id=first)
    assert (scope["studies"], scope["runs"], scope["observations"], scope["files"], scope["file_bytes"]) == (1, 2, 5, 2, 9)
    scope = client.experiment_usage(run_ids=[baseline])
    assert (scope["studies"], scope["variables"], scope["runs"], scope["samples"]) == (0, 0, 1, 1)
    assert client.experiment_usage(run_ids=[])["runs"] == 0
    with pytest.raises(HTTPError) as missing:
        client.experiment_usage(template_id="missing")
    assert missing.value.response.status_code == 404

    # The run and its Study cannot be deleted while it is active.
    with pytest.raises(HTTPError, match="finish or fail"):
        client.delete_experiments([baseline])
    with pytest.raises(HTTPError, match=baseline):
        client.delete_experiment_study(first)
    client.finish_experiment(baseline)
    client.finish_experiment(trial)
    assert client.delete_experiments([baseline]) == {
        "runs": 1, "observations": 3, "annotations": 1, "samples": 1, "files": 1,
    }
    assert [item["run_id"] for item in client.list_experiments(first)] == [trial]
    assert client.list_experiment_stream_annotations(run_id=baseline) == []
    assert len(client.list_experiment_stream_annotations(run_id=trial)) == 1
    assert client.timeseries_df(baseline_stream).is_empty()
    assert client.timeseries_df(trial_stream).height == 2
    with pytest.raises(HTTPError) as removed_file:
        client.experiment_artifact(own)
    assert removed_file.value.response.status_code == 404
    assert client.experiment_artifact(shared) == b"shared"
    with pytest.raises(HTTPError) as removed_run:
        client.delete_experiments([baseline])
    assert removed_run.value.response.status_code == 404

    # The same attachment stays while the second Study uses it, then leaves on clear.
    assert client.delete_experiment_study(first) == {
        "studies": 1, "variables": 2, "runs": 1, "observations": 2,
        "annotations": 1, "samples": 2, "files": 0,
    }
    with pytest.raises(HTTPError) as removed_study:
        client.get_experiment_study(first)
    assert removed_study.value.response.status_code == 404
    assert client.experiment_usage()["files"] == 1
    with pytest.raises(HTTPError, match="finish or fail"):
        client.clear_experiments(confirm=True)
    client.finish_experiment(stress, failed=True, error={"message": "abandoned"})
    assert client.clear_experiments(confirm=True) == {
        "studies": 1, "variables": 2, "runs": 1, "observations": 2,
        "annotations": 0, "samples": 2, "files": 1,
    }
    assert client.list_experiment_studies() == []
    assert client.experiment_usage()["total_bytes"] == 0
    with pytest.raises(HTTPError) as removed_file:
        client.experiment_artifact(shared)
    assert removed_file.value.response.status_code == 404
    assert client.timeseries_df(stress_stream).is_empty()
    assert client.timeseries_df(site)["value"].to_list() == [1000.0]
    fresh = client.define_experiment("tank")["template_id"]
    assert client.list_experiment_variables(fresh) == []


def test_batch_declarations_and_observations_are_ordered_and_atomic(ac):
    """Both HTTP batches use the same single-item checks on both backends."""
    client = ac.client
    study = client.define_experiment("batches")
    template_id = study["template_id"]
    declarations_url = f"/experiments/templates/{template_id}/variables/batch"
    declarations = [dict(label=label, role="output", kind=kind, metadata=metadata) for label, kind, metadata in (
        ("cost", "scalar", {}), ("flow", "timeseries", {"keys": ["pump"]}), ("config", "json", {}))]
    invalid = [*declarations, dict(label="bad", role="output", kind="unknown")]
    assert _post(ac, declarations_url, invalid).status_code == 400
    assert client.list_experiment_variables(template_id) == []
    response = _post(ac, declarations_url, declarations)
    assert response.status_code == 200, response.text
    variables = response.json()
    assert [item["label"] for item in variables] == ["cost", "flow", "config"]
    assert [item["created"] for item in _post(ac, declarations_url, declarations).json()] == [False] * 3
    conflict = [dict(label="fresh", role="input", kind="text"), {**declarations[0], "kind": "json"}]
    assert _post(ac, declarations_url, conflict).status_code == 400
    assert len(client.list_experiment_variables(template_id)) == 3
    run = client.start_experiment(template_id, {})
    url = f"/experiments/runs/{run['run_id']}/observations/batch"
    observations = [dict(variable_id=variables[0]["variable_id"], value=4),
                    dict(variable_id=variables[1]["variable_id"], stream_uri="urn:test:flow", key=["p1"]),
                    dict(variable_id=variables[2]["variable_id"], value={"case": 1})]
    # The last value is invalid: no prefix of the batch may be committed.
    assert _post(ac, url, [*observations, {**observations[0], "value": "bad"}]).status_code == 400
    assert client.list_experiment_observations(template_id) == []
    assert _post(ac, url, [{**observations[1], "key": None}]).status_code == 400
    assert _post(ac, url, [{**observations[0], "key": ["p1"]}]).status_code == 400
    assert _post(ac, url, [{**observations[1], "key": ["p1", "p2"]}]).status_code == 400
    assert _post(ac, url, [{**observations[1], "start": "2026-01-01T00:00:00Z"}]).status_code == 400
    assert _post(ac, url, [{**observations[0], "variable_id": "missing"}]).status_code == 404
    other = client.define_experiment("other batches")
    foreign = _declare(client, other["template_id"], "foreign", "output", "scalar")
    assert _post(ac, url, [{**observations[0], "variable_id": foreign["variable_id"]}]).status_code == 400
    response = _post(ac, url, observations)
    assert response.status_code == 200, response.text
    assert [item["sequence"] for item in response.json()] == [1, 2, 3]
    assert [item["key"] for item in client.list_experiment_observations(template_id)] == [None, ["p1"], None]
    client.finish_experiment(run["run_id"])
    assert _post(ac, url, observations).status_code == 400
    assert len(client.list_experiment_observations(template_id)) == 3


def test_table_artifact_batch_validation_and_atomicity(ac):
    client = ac.client
    study = client.define_experiment("table batch")
    scalar = _declare(client, study["template_id"], "scalar", "output", "scalar")
    table = _declare(client, study["template_id"], "table", "output", "table")
    run = client.start_experiment(study["template_id"], {})
    buffer = io.BytesIO()
    pl.DataFrame({"x": [1, 2]}).write_parquet(buffer)
    attachment = dict(variable_id=table["variable_id"], artifact=dict(filename="table.parquet",
        media_type="application/vnd.apache.parquet", data=base64.b64encode(buffer.getvalue()).decode()))
    with pytest.raises(HTTPError):
        client.observe_experiment_batch(run["run_id"], [attachment, dict(variable_id=scalar["variable_id"], value="bad")])
    assert client.list_experiment_observations(study["template_id"]) == []
    assert client.experiment_usage()["files"] == 0
    malformed = {**attachment, "artifact": {**attachment["artifact"], "data": base64.b64encode(b"not parquet").decode()}}
    with pytest.raises(HTTPError, match="Parquet"):
        client.observe_experiment_batch(run["run_id"], [malformed])
    client.observe_experiment_batch(run["run_id"], [attachment, dict(variable_id=scalar["variable_id"], value=3)])
    rows = client.list_experiment_observations(study["template_id"])
    assert [row["sequence"] for row in rows] == [1, 2]
    saved = client.experiment_artifact(rows[0]["artifact_digest"])
    assert pl.read_parquet(io.BytesIO(saved))["x"].to_list() == [1, 2]
    with pytest.raises(HTTPError, match="unknown artifact"):
        client.observe_experiment_batch(run["run_id"], [dict(variable_id=table["variable_id"], artifact_digest="missing")])


@pytest.mark.parametrize("sweep", [False, True])
def test_deleted_artifact_bytes_can_be_attached_again(ac, sweep):
    client = ac.client
    study = client.define_experiment("artifact reattach")
    variable = _declare(client, study["template_id"], "file", "input", "file")
    old = client.start_experiment(study["template_id"], {})
    data = b"shared bytes" * 1000
    saved = client.attach_experiment_bytes(old["run_id"], variable["variable_id"], data, filename="old.txt", media_type="text/plain")
    client.finish_experiment(old["run_id"])
    # Bytes leave in the deleting transaction, so nothing is left to race.
    removed = client.clear_experiments(confirm=True) if sweep else client.delete_experiments([old["run_id"]])
    assert removed["files"] == 1
    with pytest.raises(HTTPError) as caught:
        client.experiment_artifact(saved["digest"])
    assert caught.value.response.status_code == 404
    if sweep:
        study = client.define_experiment("artifact reattach")
        variable = _declare(client, study["template_id"], "file", "input", "file")
    active = client.start_experiment(study["template_id"], {})
    client.attach_experiment_bytes(active["run_id"], variable["variable_id"], data, filename="new.txt", media_type="text/plain")
    assert client.experiment_artifact(saved["digest"]) == data
    assert client.list_experiment_observations(study["template_id"], run_id=active["run_id"])[0]["filename"] == "new.txt"
