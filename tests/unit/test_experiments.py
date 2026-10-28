from datetime import datetime, timezone
from hashlib import sha256
from types import SimpleNamespace
from urllib.parse import quote
from unittest.mock import Mock
import warnings

import polars as pl
import pytest
from requests import HTTPError

from acquirium.Client.streams import Stream, _after_ns
from acquirium.Experiments import Study, StudyService, timestamp, _stream_name
from acquirium.internals.models import compute_ref_uri


def _declare(client, template_id, label, role, kind, metadata=None):
    return client.declare_experiment_variable(template_id, label=label, role=role, kind=kind, metadata=metadata or {})


def _status(error):
    return error.value.response.status_code


def _recorded_stream(ac, run, variable, rows):
    """Record samples with a run, as a time-series record() does."""
    receipt = ac.client.record_experiment_batch(run["run_id"], [{
        "form": "samples", "variable_id": variable["variable_id"],
        "samples": [[when.isoformat(), value] for when, value in rows]}])
    return receipt[0]["ref_uri"]


def _site_stream(ac, name, rows):
    """A stream a driver wrote, outside any experiment."""
    ac.register_streams([{"source_id": "site", "ref_name": name}])
    ac.insert_timeseries("site", name, rows)
    return str(ac.reference_uri("site", name))


def _samples(ac, ref):
    return ac.client.timeseries_df(ref).height


def _stored(ac, digest):
    """Whether the server still has an artifact's bytes."""
    try:
        ac.client.experiment_artifact(digest)
    except HTTPError as error:
        if error.response.status_code == 404:
            return False
        raise
    return True


def _post(ac, path, **kwargs):
    return ac.client._http.post(f"{ac.client.base_url}{path}", **kwargs)


def _get(ac, path, **kwargs):
    return ac.client._http.get(f"{ac.client.base_url}{path}", **kwargs)


@pytest.mark.parametrize("status", [404, 400])
def test_metadata_sync_reports_old_servers_without_hiding_other_errors(status):
    from requests import HTTPError, Response
    from acquirium.Client.client import AcquiriumClient

    client = AcquiriumClient()
    response = Response()
    response.status_code = status
    error = HTTPError("metadata sync failed", response=response)
    client._http.post = Mock(side_effect=error)
    expected = RuntimeError if status == 404 else HTTPError
    with pytest.raises(expected) as caught:
        client.sync_experiment_metadata()
    if status == 404:
        assert "Restart the Acquirium server" in str(caught.value)
    else:
        assert caught.value is error


def test_reusable_template_variables_are_isolated_and_timestamped(ac):
    client = ac.client
    template = client.define_experiment("load-shift")
    variable = _declare(client, template["template_id"], "configuration", "input", "json")
    first = client.start_experiment(template["template_id"], {"case": 1})
    first_observation = client.observe_experiment(first["run_id"], variable["variable_id"], value={"flow": 10})
    assert first_observation["sequence"] == 1
    assert timestamp(first_observation["recorded_at"]).utcoffset() is not None
    client.finish_experiment(first["run_id"])
    with pytest.raises(HTTPError, match="terminal"):
        client.observe_experiment(first["run_id"], variable["variable_id"], value={"flow": 20})
    second = client.start_experiment(template["template_id"], {"case": 2})
    second_observation = client.observe_experiment(second["run_id"], variable["variable_id"], value={"flow": 20})
    assert second_observation["sequence"] == 1


def test_file_attachment_is_content_addressed(ac):
    client = ac.client
    template = client.define_experiment("files")
    variable = _declare(client, template["template_id"], "config", "input", "file")
    run = client.start_experiment(template["template_id"], {})
    first = client.attach_experiment_bytes(run["run_id"], variable["variable_id"], b"{}", filename="a.json", media_type="application/json")
    second = client.attach_experiment_bytes(run["run_id"], variable["variable_id"], b"{}", filename="b.json", media_type="application/json")
    assert first["digest"] == second["digest"]
    assert client.experiment_artifact(first["digest"]) == b"{}"
    assert client.experiment_usage()["files"] == 1


@pytest.mark.parametrize("data", [b"", bytes(range(256)) * (2 ** 14 + 3)], ids=["empty", "chunked"])
def test_file_attachment_round_trips_across_chunks(ac, data):
    # The second payload is just over 4 MiB, the size of one stored chunk.
    client = ac.client
    study = client.define_experiment("chunks")
    variable = _declare(client, study["template_id"], "dump", "output", "file")
    run = client.start_experiment(study["template_id"], {})
    saved = client.attach_experiment_bytes(run["run_id"], variable["variable_id"], data, filename="dump.bin")
    assert saved["byte_length"] == len(data)
    assert client.experiment_artifact(saved["digest"]) == data


def test_batch_update_names_each_subject_and_its_graph():
    from acquirium.Metadata import batch_update
    text = batch_update([
        {"operation": "replace", "subject": "urn:x#a", "triples": [["urn:x#a", "urn:p#v", '"1"']]},
        {"operation": "patch", "subject": "urn:x#b", "predicates": ["urn:p#v"], "triples": []},
    ], "urn:g#experiments")
    # A VALUES list joined against ?s ?p ?o scans the whole graph per update.
    assert "VALUES" not in text
    assert text.split(";\n") == [
        "DELETE WHERE { GRAPH <urn:g#experiments> { <urn:x#a> ?p ?o } }",
        "DELETE WHERE { GRAPH <urn:g#experiments> { <urn:x#b> <urn:p#v> ?o } }",
        'INSERT DATA { GRAPH <urn:g#experiments> { <urn:x#a> <urn:p#v> "1" . } }',
    ]


def test_recording_rejects_empty_samples_so_receipts_match_entries(ac):
    client = ac.client
    study = client.define_experiment("samples")
    empty, full = (_declare(client, study["template_id"], label, "output", "timeseries") for label in ("empty", "full"))
    run = client.start_experiment(study["template_id"], {})
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(HTTPError, match="at least one sample"):
        client.record_experiment_batch(run["run_id"], [
            {"form": "samples", "variable_id": empty["variable_id"], "samples": []},
            {"form": "samples", "variable_id": full["variable_id"], "samples": [[when.isoformat(), 1.0]]}])
    assert client.list_experiment_observations(study["template_id"], run_id=run["run_id"]) == []


def test_concurrent_declaration_of_same_label_is_idempotent(ac):
    from concurrent.futures import ThreadPoolExecutor

    client = ac.client
    study = client.define_experiment("race")
    with ThreadPoolExecutor(8) as pool:
        declared = list(pool.map(lambda _: _declare(client, study["template_id"], "flow", "input", "scalar"), range(8)))
    assert sum(item["created"] for item in declared) == 1
    assert len({item["variable_id"] for item in declared}) == 1
    assert [item["label"] for item in client.list_experiment_variables(study["template_id"])] == ["flow"]
    with pytest.raises(HTTPError, match="declared differently"):
        _declare(client, study["template_id"], "flow", "output", "scalar")


@pytest.mark.parametrize("attachment", [False, True])
@pytest.mark.parametrize("foreign", [False, True])
def test_observation_rejects_unknown_or_foreign_variable(ac, attachment, foreign):
    client = ac.client
    study = client.define_experiment("owner")
    other = client.define_experiment("other")
    variable = _declare(client, other["template_id"], "result", "output", "file" if attachment else "scalar")
    run = client.start_experiment(study["template_id"], {})
    variable_id = variable["variable_id"] if foreign else "missing"
    with pytest.raises(HTTPError) as caught:
        if attachment:
            client.attach_experiment_bytes(run["run_id"], variable_id, b"result", filename="result.txt", media_type="text/plain")
        else:
            client.observe_experiment(run["run_id"], variable_id, value=12.5)
    assert _status(caught) == (400 if foreign else 404)
    assert client.experiment_usage()["observations"] == 0
    assert client.experiment_usage()["files"] == 0
    owned = _declare(client, study["template_id"], "valid", "output", "scalar")
    assert client.observe_experiment(run["run_id"], owned["variable_id"], value=42)["sequence"] == 1


T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _stream_frame(ref, rows):
    """What the time-series client returns for one stream."""
    return pl.DataFrame({"ts": [when for when, _ in rows], "value": [value for _, value in rows], "uri": [ref] * len(rows)})


def test_delete_runs_removes_what_they_recorded_and_keeps_shared_files(ac):
    client = ac.client
    study = client.define_experiment("tank")
    config = _declare(client, study["template_id"], "config", "input", "file")
    level = _declare(client, study["template_id"], "level", "output", "timeseries")
    site = _site_stream(ac, "level", [(T0, 1000.0)])
    runs, refs = [], []
    for index in range(2):
        run = client.start_experiment(study["template_id"], {"case": index})
        client.attach_experiment_bytes(run["run_id"], config["variable_id"], b"{}", filename="shared.json")
        refs.append(_recorded_stream(ac, run, level, [(T0, float(index))]))
        client.annotate_experiment_stream(run["run_id"], site, start=T0.isoformat())
        runs.append(run)
    own = client.attach_experiment_bytes(runs[0]["run_id"], config["variable_id"], b"[]", filename="own.json")
    with pytest.raises(HTTPError, match="finish or fail"):
        client.delete_experiments([runs[0]["run_id"]])
    assert client.experiment_usage(template_id=study["template_id"])["observations"] == 5
    for run in runs:
        client.finish_experiment(run["run_id"])

    removed = client.delete_experiments([runs[0]["run_id"]])
    assert removed == {"runs": 1, "observations": 3, "annotations": 1, "samples": 1, "files": 1}
    assert [run["run_id"] for run in client.list_experiments(study["template_id"])] == [runs[1]["run_id"]]
    assert client.list_experiment_stream_annotations(run_id=runs[0]["run_id"]) == []
    assert (_samples(ac, refs[0]), _samples(ac, refs[1]), _samples(ac, site)) == (0, 1, 1)
    assert not _stored(ac, own["digest"]) and _stored(ac, sha256(b"{}").hexdigest())
    client.delete_experiments([runs[1]["run_id"]])
    assert not _stored(ac, sha256(b"{}").hexdigest())
    with pytest.raises(HTTPError) as caught:
        client.delete_experiments([runs[1]["run_id"]])
    assert _status(caught) == 404


def test_delete_keeps_streams_that_other_runs_reference(ac):
    client = ac.client
    simulation, analysis = client.define_experiment("simulation"), client.define_experiment("analysis")
    level = _declare(client, simulation["template_id"], "level", "output", "timeseries")
    used = _declare(client, analysis["template_id"], "level", "input", "timeseries")
    source = client.start_experiment(simulation["template_id"], {})
    ref = _recorded_stream(ac, source, level, [(T0, 1.0)])
    reader = client.start_experiment(analysis["template_id"], {})
    client.observe_experiment(reader["run_id"], used["variable_id"], ref_uri=ref)
    for run in (source, reader):
        client.finish_experiment(run["run_id"])
    for delete in (lambda: client.delete_experiments([source["run_id"]]),
                   lambda: client.delete_experiment_study(simulation["template_id"])):
        with pytest.raises(HTTPError, match=reader["run_id"]):
            delete()
    assert _samples(ac, ref) == 1
    # Deleting the reader with the run it read from is allowed.
    assert client.delete_experiments([source["run_id"], reader["run_id"]])["runs"] == 2
    assert _samples(ac, ref) == 0


def test_delete_study_clear_and_usage_cover_experiments_only(ac):
    client = ac.client
    site = _site_stream(ac, "level", [(T0, 1000.0)])
    studies, runs = [], []
    for name in ("first", "second"):
        study = client.define_experiment(name)
        config = _declare(client, study["template_id"], "config", "input", "file")
        level = _declare(client, study["template_id"], "level", "output", "timeseries")
        run = client.start_experiment(study["template_id"], {})
        client.attach_experiment_bytes(run["run_id"], config["variable_id"], b"shared", filename="shared.json")
        _recorded_stream(ac, run, level, [(T0, 1.0), (T0.replace(minute=1), 2.0)])
        studies.append(study); runs.append(run)

    everything = client.experiment_usage()
    assert {key: everything[key] for key in ("studies", "variables", "runs", "observations", "samples", "files", "file_bytes")} == {
        "studies": 2, "variables": 4, "runs": 2, "observations": 4, "samples": 4, "files": 1, "file_bytes": 6,
    }
    assert everything["sample_bytes"] == 4 * 16
    assert everything["total_bytes"] == everything["sample_bytes"] + everything["file_bytes"] + everything["ledger_bytes"]
    first = client.experiment_usage(template_id=studies[0]["template_id"])
    assert (first["studies"], first["runs"], first["files"], first["file_bytes"]) == (1, 1, 1, 6)
    selected = client.experiment_usage(run_ids=[runs[0]["run_id"]])
    assert (selected["studies"], selected["variables"], selected["runs"], selected["samples"]) == (0, 0, 1, 2)
    assert client.experiment_usage(run_ids=[])["runs"] == 0
    with pytest.raises(HTTPError) as caught:
        client.experiment_usage(template_id="missing")
    assert _status(caught) == 404

    with pytest.raises(HTTPError, match=runs[0]["run_id"]):
        client.delete_experiment_study(studies[0]["template_id"])
    client.finish_experiment(runs[0]["run_id"])
    removed = client.delete_experiment_study(studies[0]["template_id"])
    assert removed == {"studies": 1, "variables": 2, "runs": 1, "observations": 2, "annotations": 0, "samples": 2, "files": 0}
    with pytest.raises(HTTPError) as caught:
        client.get_experiment_study(studies[0]["template_id"])
    assert _status(caught) == 404
    assert client.experiment_usage()["files"] == 1  # the second study still uses it

    with pytest.raises(HTTPError, match="finish or fail"):
        client.clear_experiments(confirm=True)
    client.finish_experiment(runs[1]["run_id"], failed=True, error={"message": "abandoned"})
    assert client.clear_experiments(confirm=True) == {"studies": 1, "variables": 2, "runs": 1, "observations": 2, "annotations": 0, "samples": 2, "files": 1}
    assert client.list_experiment_studies() == []
    assert client.experiment_usage()["total_bytes"] == 0
    assert not _stored(ac, sha256(b"shared").hexdigest())
    assert _samples(ac, site) == 1
    fresh = client.define_experiment("first")
    assert client.list_experiment_variables(fresh["template_id"]) == []


def test_http_delete_routes_report_conflicts_and_require_confirmation(ac):
    client = ac.client
    study = client.define_experiment("http")
    run = client.start_experiment(study["template_id"], {})
    assert _post(ac, "/experiments/runs/delete", json={"run_ids": ["missing"]}).status_code == 404
    assert _post(ac, "/experiments/runs/delete", json={"run_ids": [run["run_id"]]}).status_code == 409
    assert _post(ac, f"/experiments/templates/{study['template_id']}/delete").status_code == 409
    assert _post(ac, "/experiments/clear", json={}).status_code == 400
    assert _post(ac, "/experiments/clear", json={"confirm": True}).status_code == 409
    assert _post(ac, "/experiments/usage", json={}).json()["runs"] == 1
    client.finish_experiment(run["run_id"])
    assert _post(ac, "/experiments/runs/delete", json={"run_ids": [run["run_id"]]}).json()["runs"] == 1
    assert _post(ac, f"/experiments/templates/{study['template_id']}/delete").json()["studies"] == 1
    assert _post(ac, "/experiments/templates/missing/delete").status_code == 404
    assert _post(ac, "/experiments/clear", json={"confirm": True}).json()["studies"] == 0


def test_ledger_enforces_what_each_kind_stores(ac):
    client = ac.client
    study = client.define_experiment("kinds")
    declare = lambda label, kind: _declare(client, study["template_id"], label, "output", kind)["variable_id"]
    with pytest.raises(HTTPError, match="kind must be one of"):
        declare("bogus", "blob")
    scalar, text, data, file, table, series = (declare(kind, kind) for kind in ("scalar", "text", "json", "file", "table", "timeseries"))
    run = client.start_experiment(study["template_id"], {})["run_id"]
    # NaN and infinity are not JSON, so the client cannot send them at all.
    rejected = [
        (scalar, {"value": "abc"}), (scalar, {"value": None}), (scalar, {"value": [1]}), (text, {"value": 5}),
        (scalar, {"value": 1, "ref_uri": "urn:ref"}), (series, {"value": 1}), (file, {"value": "x"}),
    ]
    for variable, body in rejected:
        with pytest.raises(HTTPError):
            client.observe_experiment(run, variable, **body)
    for variable, value in ((scalar, 2), (scalar, 2.5), (scalar, True), (text, "ok"), (data, None), (data, [1, {"a": "b"}])):
        client.observe_experiment(run, variable, value=value)
    with pytest.raises(HTTPError, match="only file, table, and object"):
        client.attach_experiment_bytes(run, scalar, b"x", filename="x.txt")
    with pytest.raises(HTTPError, match="Parquet"):
        client.attach_experiment_bytes(run, table, b"not parquet", filename="x.parquet")
    assert len(client.list_experiment_observations(study["template_id"])) == 6
    assert client.experiment_usage()["files"] == 0


def test_http_artifacts_download_with_their_saved_names(ac):
    client = ac.client
    study = client.define_experiment("http")
    report = _declare(client, study["template_id"], "report", "output", "file")
    cost = _declare(client, study["template_id"], "cost", "output", "scalar")
    run = client.start_experiment(study["template_id"], {})
    path = f"/experiments/runs/{run['run_id']}/variables/{quote(report['variable_id'], safe='')}/file"
    saved = _post(ac, path, data=b"report", headers={"X-Filename": quote("débit.txt", safe=""), "Content-Type": "text/plain"}).json()
    download = _get(ac, f"/experiments/artifacts/{saved['digest']}")
    assert (download.status_code, download.content) == (200, b"report")
    assert download.headers["content-type"].startswith("text/plain")
    assert client.list_experiment_observations(study["template_id"])[0]["filename"] == "débit.txt"
    for digest in ("0" * 64, "..%2F..%2Fdata.duckdb", "not-a-digest"):
        assert _get(ac, f"/experiments/artifacts/{digest}").status_code == 404
    scalar_path = f"/experiments/runs/{run['run_id']}/variables/{quote(cost['variable_id'], safe='')}"
    assert _post(ac, f"{scalar_path}/file", data=b"x").status_code == 400
    assert _post(ac, f"{scalar_path}/observations", json={"value": "abc"}).status_code == 400


@pytest.fixture
def study_api(ac):
    # The real client and Acquirium handle from aq.init(), wrapped so tests
    # can count calls and stub stream reads; every other call reaches the server.
    client = Mock(wraps=ac.client)
    spy = Mock(wraps=ac, client=client)
    item = ac.client.define_experiment("study")
    return SimpleNamespace(study=Study(spy, item), ac=spy, client=client, item=item)


def test_collections_preserve_handles_and_roles(study_api):
    study = study_api.study
    cost = study.output("cost").scalar(unit="USD")
    config = study.input("config").json()
    log = study.log("events")
    assert study.output("cost").scalar(unit="USD") is cost
    assert study.output["cost"] is cost
    assert cost.variable_id == str(compute_ref_uri(f"study/{study.study_id}", "cost"))
    assert list(study.output) == [cost]
    assert list(study.output.items()) == [("cost", cost)]
    assert list(study.output.keys()) == ["cost"]
    assert list(study.output.values()) == [cost]
    assert len(study.output) == 1
    assert "cost" in study.output
    assert "config" not in study.output
    assert list(study.input) == [config]
    assert list(study.log) == [log]
    with pytest.raises(KeyError):
        study.output["missing"]
    with pytest.raises(HTTPError, match="declared differently"):
        study.output("cost").scalar(unit="EUR")
    with pytest.raises(HTTPError, match="declared differently"):
        study.input("cost").scalar(unit="USD")
    assert study.output["cost"] is cost


def test_handles_print_what_they_hold_without_reading(study_api):
    study = study_api.study
    cost = study.output("cost").scalar(unit="USD")
    source = study.input("source").timeseries()
    log = study.log("events")
    run = study.start({"scenario": "baseline"})
    assert repr(study) == f"<Study 'study': 1 input, 1 output, 1 log; running {run.run_id}>"
    cost.record(500)
    source.record(Stream("urn:example:ref").between("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"))
    log.record("done")
    run.finish()
    observations = study.experiments.observations().all()

    calls = len(study_api.client.mock_calls)
    assert repr(study) == "<Study 'study': 1 input, 1 output, 1 log>"
    assert repr(run) == f"<Experiment {run.run_id} succeeded {{'scenario': 'baseline'}}>"
    assert repr(cost) == "<ExperimentVariable output 'cost' scalar>"
    assert repr(log) == "<ExperimentVariable log 'events'>"
    assert repr(study.input) == "<VariableCollection input ['source']>"
    assert repr(study.experiments.where(status="succeeded")) == "<ExperimentCollection study='study' status='succeeded'>"
    assert repr(cost.observations()) == "<ObservationCollection study='study' variable='cost'>"
    assert [repr(item) for item in observations] == [
        f"<Observation 'cost' = 500 (run {run.run_id}, #1)>",
        f"<Observation 'source' = stream urn:example:ref [2026-01-01T00:00:00+00:00, 2026-01-02T00:00:00+00:00] (run {run.run_id}, #2)>",
        f"<Observation 'events' = 'done' (run {run.run_id}, #3)>",
    ]
    assert len(study_api.client.mock_calls) == calls


def test_stream_annotations_support_historical_and_live_intervals(study_api):
    study, ac = study_api.study, study_api.ac
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, tzinfo=timezone.utc)
    run = study.start({"digester": "A"})
    historical = run.annotate_stream("urn:plant:gas-flow", start=start, end=end,
                                     metadata={"treatment": "new membrane"})
    live = run.annotate_stream("urn:plant:gas-flow", start=end)
    run.finish()

    reopened = StudyService(ac).get("study")
    matches = reopened.stream_annotations(ref_uri="urn:plant:gas-flow", start=end)
    assert [annotation.annotation_id for annotation in matches] == [historical.annotation_id, live.annotation_id]
    assert reopened.experiments.get(run.run_id).observations().all() == []
    assert reopened.experiments.get(run.run_id).stream_annotations()[0].metadata == {"treatment": "new membrane"}
    assert StudyService(ac).stream_annotations(ref_uri="urn:plant:gas-flow", end=start)[0].run_id == run.run_id
    assert not reopened.stream_annotations(ref_uri="urn:another-stream")

    study_api.client.timeseries_df.return_value = _stream_frame("urn:plant:gas-flow", [(start, 1.0)])
    assert historical.dataframe().rows() == [(start, 1.0)]
    assert historical.stream == Stream("urn:plant:gas-flow")
    study_api.client.timeseries_df.assert_called_once_with(
        "urn:plant:gas-flow", start=start.isoformat(), end_ns=_after_ns(end))

    live.close(datetime(2026, 1, 3, tzinfo=timezone.utc))
    assert live.range_end == datetime(2026, 1, 3, tzinfo=timezone.utc)
    with pytest.raises(HTTPError, match="already closed"):
        live.close(datetime(2026, 1, 4, tzinfo=timezone.utc))
    assert len(reopened.stream_annotations(start=datetime(2026, 1, 4, tzinfo=timezone.utc))) == 0
    retrospective = reopened.experiments.get(run.run_id).annotate_stream(
        "urn:plant:gas-flow", start=start, end=end,
        metadata={"identified_later": True},
    )
    assert retrospective.created_at > retrospective.range_end
    assert retrospective.metadata == {"identified_later": True}


def test_stream_annotation_rejects_invalid_range_and_keeps_parallel_runs(study_api):
    study = study_api.study
    start = datetime(2026, 1, 2, tzinfo=timezone.utc)
    before = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = study.start()
    with pytest.raises(HTTPError, match="at or after start"):
        first.annotate_stream("urn:plant:flow", start=start, end=before)
    with pytest.raises(HTTPError, match="reference URI"):
        first.annotate_stream(" ", start=start)
    annotation = first.annotate_stream("urn:plant:flow", start=start)
    with pytest.raises(HTTPError, match="at or after start"):
        annotation.close(before)
    second_study = StudyService(study_api.ac).get("study")
    second = second_study.start()
    other = second.annotate_stream("urn:plant:flow", start=start)
    assert other.run_id != annotation.run_id
    assert len(StudyService(study_api.ac).stream_annotations(ref_uri="urn:plant:flow")) == 2


def test_long_running_experiment_resumes_in_another_session(study_api):
    original = study_api.study
    result = original.output("efficiency").scalar()
    started = original.start(metadata={"train": "A"})
    opened = started.annotate_stream("urn:plant:gas-flow", start="2026-01-01T00:00:00Z")

    resumed_study = StudyService(study_api.ac).get("study")
    resumed = resumed_study.resume(started.run_id)
    assert resumed.run_id == opened.run_id
    resumed_study.output["efficiency"].record(0.82)
    assert resumed.observations("efficiency").latest().value == 0.82
    assert resumed.stream_annotations()[0].range_end is None
    resumed.stream_annotations()[0].close("2026-01-31T00:00:00Z")
    resumed.finish()
    with pytest.raises(ValueError, match="running"):
        resumed_study.resume(started.run_id)
    assert result.variable_id == resumed_study.output["efficiency"].variable_id


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("complete_active", [False, True])
def test_completing_retrieved_run_only_clears_matching_active_binding(study_api, failed, complete_active):
    study = study_api.study
    output = study.output("result").scalar()
    active = study.start()
    other_study = StudyService(study_api.ac).get("study")
    other = other_study.start()
    selected = study.experiments.get(active.run_id if complete_active else other.run_id)

    result = selected.fail(RuntimeError("failed")) if failed else selected.finish()

    assert result["status"] == ("failed" if failed else "succeeded")
    assert selected.finished_at is not None
    if complete_active:
        assert study._experiment is None
        with pytest.raises(RuntimeError, match="start an experiment"):
            output.record(42)
        assert study.start().run_id != active.run_id
    else:
        assert study._experiment is active
        output.record(42)
        assert active.observations(output).latest().value == 42
        with pytest.raises(RuntimeError, match="finish the active experiment"):
            study.start()
        active.finish()
        assert study._experiment is None


def test_reopened_study_hydrates_variables_and_reads_experiments(study_api):
    study, ac, client = study_api.study, study_api.ac, study_api.client
    cost = study.output("operating cost").scalar(unit="USD", objective="minimize")
    power = study.output("electrical power").timeseries(
        observed="urn:plant:tank",
        unit="kW",
    )
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)

    first = study.start({"scenario": "baseline", "batch": "notebook-1"})
    cost.record(12.5)
    power.record(Stream("urn:acquirium:power-1").between(start, end))
    first.finish()

    reopened = StudyService(ac).get("study")

    assert reopened.study_id == study.study_id
    assert reopened.variables["operating cost"].variable_id == cost.variable_id
    assert reopened.variables[power.variable_id].label == "electrical power"
    assert reopened.variables["operating cost"].metadata == {"objective": "minimize"}
    assert reopened.variables["operating cost"].unit == "USD"
    assert reopened.output["electrical power"].metadata == {}
    assert reopened.output["electrical power"].observed == "urn:plant:tank"
    assert reopened.output["electrical power"].unit == "kW"
    assert reopened.variables.where(role="output").frame().select(
        "label", "role", "kind", "metadata"
    ).to_dicts() == [
        {
            "label": "operating cost",
            "role": "output",
            "kind": "scalar",
            "metadata": {"objective": "minimize"},
        },
        {
            "label": "electrical power",
            "role": "output",
            "kind": "timeseries",
            "metadata": {},
        },
    ]

    selected = reopened.experiments.where(
        status="succeeded",
        metadata={"batch": "notebook-1"},
    )
    experiments = selected.all()
    assert [experiment.experiment_id for experiment in experiments] == [first.run_id]
    assert selected.frame()["metadata"].to_list() == [
        {"batch": "notebook-1", "scenario": "baseline"}
    ]
    assert reopened.experiments.get(first.run_id).metadata["scenario"] == "baseline"

    costs = selected.observations("operating cost")
    assert costs.frame().select("experiment_id", "value").to_dicts() == [
        {"experiment_id": first.run_id, "value": 12.5}
    ]
    assert reopened.variables["operating cost"].observations(selected).latest().value == 12.5

    power_observation = reopened.experiments.get(first.run_id).observations(power.variable_id).latest()
    client.timeseries_df.return_value = _stream_frame("urn:acquirium:power-1", [(start, 3.0), (end, 4.0)])
    assert power_observation.ref_uri == "urn:acquirium:power-1"
    assert power_observation.dataframe().columns == ["time", "value"]
    assert power_observation.dataframe()["value"].to_list() == [3.0, 4.0]
    client.timeseries_df.assert_called_with(
        power_observation.ref_uri,
        start=start.isoformat(),
        end_ns=_after_ns(end),
    )
    # The recorded range can be recorded again, as another run's input.
    assert power_observation.interval == Stream("urn:acquirium:power-1").between(start, end)


def test_variable_catalog_expands_metadata_and_omits_deleted_keys(study_api):
    study = study_api.study
    study.input("configuration").json(
        category="simulation",
        objective=None,
        only_null=None,
    )
    study.output("cost").scalar(unit="USD", objective="minimize")

    frame = StudyService(study_api.ac).get("study").variables.frame()

    assert frame.columns == ["variable_id", "label", "role", "kind", "metadata", "created_at", "unit"]
    assert frame["metadata"].to_list() == [
        {"category": "simulation", "objective": None, "only_null": None}, {"objective": "minimize"}]
    assert frame["unit"].to_list() == [None, "USD"]
    assert frame["created_at"].dtype.is_temporal()


@pytest.mark.parametrize("values", [
    [12.5, {"event": "done"}],
    [{"event": "done"}, 12.5],
    [{"x": 1}, {"y": [2, 3]}],
    [[1, 2], ["three", {"x": 4}]],
    [None, True, 42, "note", {"nested": [1, None]}],
    [*range(101), {"after_inference_window": True}],
])
def test_observation_frame_preserves_heterogeneous_values(study_api, values):
    result = study_api.study.output("result").json()
    run = study_api.study.start()
    for value in values:
        result.record(value)
    observations = run.observations()
    assert [item.value for item in observations.all()] == values
    assert observations.frame()["value"].to_list() == values


def test_scalar_observation_frame_supports_numeric_analysis(study_api):
    result = study_api.study.output("cost").scalar()
    run = study_api.study.start()
    result.record(12.5)
    result.record(7.5)
    assert run.observations().frame().select(pl.col("value").sum()).item() == 20.0


@pytest.mark.parametrize("accessor", ["all", "frame", "latest"])
def test_observations_refresh_variables_declared_in_another_session(study_api, accessor):
    study = study_api.study
    known = study.output("known").scalar()
    reader = StudyService(study_api.ac).get("study")
    known_handle = reader.output["known"]
    added = study.output("added later").scalar(unit="kW")
    run = study.start()
    known.record(1)
    added.record(2)
    study_api.client.list_experiment_variables.reset_mock()

    result = getattr(reader.experiments.get(run.run_id).observations(), accessor)()

    if accessor == "all":
        assert [item.value for item in result] == [1, 2]
        assert result[-1].variable is reader.output["added later"]
    elif accessor == "frame":
        assert result["value"].to_list() == [1, 2]
    else:
        assert result.value == 2
    assert reader.output["known"] is known_handle
    assert reader.output["added later"].unit == "kW"
    assert reader.output["added later"].created_at is not None
    reader.experiments.get(run.run_id).observations().all()
    study_api.client.list_experiment_variables.assert_called_once_with(reader.study_id)


def test_only_new_active_run_declarations_warn(study_api):
    study = study_api.study
    study.output("cost").scalar(unit="USD")
    run = study.start()
    with pytest.warns(UserWarning, match="New variable 'peak'.*active experiment"):
        peak = study.output("peak").scalar()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert study.output("peak").scalar() is peak
        study.output("cost").scalar(unit="USD")
        # A fresh client handle can reuse a persisted declaration without warning.
        reopened = Study(study_api.ac, study_api.item)
        reopened_run = reopened.start()
        reopened.output("cost").scalar(unit="USD")
        reopened_run.finish()
    run.finish()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        study.output("after run").text()


@pytest.mark.parametrize("kind,value", [("scalar", 12.5), ("json", {"x": 1}), ("text", "note"), ("log", {"event": "done"})])
def test_record_values_and_events_across_runs(study_api, kind, value):
    study, client = study_api.study, study_api.client
    variable = study.log("value") if kind == "log" else getattr(study.output("value"), kind)()
    with pytest.raises(RuntimeError, match="start an experiment"):
        variable.record(value)
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = study.start()
    variable.record(value, occurred_at=when)
    variable.record(value)
    assert client.record_experiment_batch.call_count == 2
    assert client.record_experiment_batch.call_args_list[0].args == (first.run_id, [{"form": "value", "variable_id": variable.variable_id, "value": value, "occurred_at": when.isoformat()}])
    first.finish()
    with pytest.raises(RuntimeError):
        variable.record(value)
    second = study.start()
    variable.record(value)
    assert client.record_experiment_batch.call_args.args[0] == second.run_id
    assert first.run_id != second.run_id


def test_record_files_and_streams(study_api, tmp_path):
    study, client, ac = study_api.study, study_api.client, study_api.ac
    file = study.input("config").file(media_type="application/json")
    stream = study.output("volume").timeseries(observed="urn:tank", unit="M3")
    with pytest.raises(RuntimeError):
        stream.record([])
    run = study.start()
    path = tmp_path / "config.json"
    path.write_text("{}")
    file.record(path)
    client.attach_experiment_file.assert_called_once_with(run.run_id, file.variable_id, path, media_type="application/json")
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ac.reference_uri.return_value = "urn:acquirium#recorded-volume"
    recorded = stream.record([(when.isoformat(), 10)])
    client.record_experiment_batch.assert_called_once_with(run.run_id, [{
        "form": "samples", "variable_id": stream.variable_id, "key": None,
        "samples": [[when.isoformat(), 10]],
    }])
    ac.register_streams.assert_not_called()
    assert run.observations(stream).latest().ref_uri == recorded.ref_uri
    client.timeseries_df.return_value = _stream_frame(recorded.ref_uri, [(when, 10.0)])
    assert recorded.dataframe(limit=5).rows() == [(when, 10.0)]
    client.timeseries_df.assert_called_once_with(recorded.ref_uri, start=None, end_ns=None, limit=5)
    assert str(recorded) == recorded.ref_uri == str(compute_ref_uri(f"experiment/{run.run_id}", _stream_name(stream.variable_id, ())))
    for variable in (file, stream):
        with pytest.raises(TypeError, match="occurred_at"):
            variable.record(None, occurred_at=when)
    run.finish()
    with pytest.raises(RuntimeError):
        file.record(path)


def test_timeseries_result_does_not_require_plant_model(study_api):
    study, ac = study_api.study, study_api.ac
    simulated_power = study.output("simulated power").timeseries(unit="kW")
    run = study.start()
    simulated_power.record([("2026-01-01T00:00:00Z", 42.0)])
    ac.register_streams.assert_not_called()
    assert run.observations("simulated power").latest().ref_uri is not None


def test_timeseries_registration_is_reused_per_run_and_variable(study_api):
    study, ac = study_api.study, study_api.ac
    power = study.output("power").timeseries(unit="kW")
    volume = study.output("volume").timeseries(unit="M3")
    ac.reference_uri.side_effect = compute_ref_uri
    first = study.start()
    assert power.record([]) is None
    ac.register_streams.assert_not_called()

    recorded = power.record([("2026-01-01T00:00:00Z", 10)])
    appended = power.add([("2026-01-01T01:00:00Z", 20)])

    ac.register_streams.assert_not_called()
    assert study_api.client.record_experiment_batch.call_count == 2
    assert recorded.ref_uri == appended.ref_uri
    observations = first.observations(power).all()
    assert [item.sequence for item in observations] == [1, 2]
    assert [item.range_start.hour for item in observations] == [0, 1]
    volume.record([("2026-01-01T00:00:00Z", 30)])
    volume.record([("2026-01-01T01:00:00Z", 40)])
    assert study_api.client.record_experiment_batch.call_count == 4

    first.finish()
    second = study.start()
    new_recording = power.record([("2026-01-01T00:00:00Z", 50)])
    power.record([("2026-01-01T01:00:00Z", 60)])
    assert study_api.client.record_experiment_batch.call_count == 6
    assert new_recording.ref_uri != recorded.ref_uri
    assert study_api.client.record_experiment_batch.call_args.args[0] == second.run_id
    assert len(second.observations(power).all()) == 2


def test_timeseries_atomic_request_can_be_retried(study_api):
    study, client = study_api.study, study_api.client
    power = study.output("power").timeseries(unit="kW")
    run = study.start()
    operation = client.record_experiment_batch.side_effect
    client.record_experiment_batch.side_effect = RuntimeError("temporary failure")
    rows = [("2026-01-01T00:00:00Z", 10)]
    with pytest.raises(RuntimeError, match="temporary failure"):
        power.record(rows)
    assert run.observations(power).all() == []
    client.record_experiment_batch.side_effect = operation
    power.record(rows)
    power.record(rows)
    assert client.record_experiment_batch.call_count == 3
    assert len(run.observations(power).all()) == 2


def test_resumed_run_registers_stream_once_in_new_session(study_api):
    study, ac = study_api.study, study_api.ac
    power = study.output("power").timeseries(unit="kW")
    ac.reference_uri.side_effect = compute_ref_uri
    run = study.start()
    original = power.record([("2026-01-01T00:00:00Z", 10)])
    ac.register_streams.reset_mock()

    reopened = StudyService(ac).get("study")
    resumed = reopened.resume(run.run_id)
    recorded = reopened.output["power"].record([("2026-01-01T01:00:00Z", 20)])
    reopened.output["power"].record([("2026-01-01T02:00:00Z", 30)])

    ac.register_streams.assert_not_called()
    assert recorded.ref_uri == original.ref_uri
    assert [item.range_start.hour for item in resumed.observations("power").all()] == [0, 1, 2]


def test_http_routes_accept_reference_uri_variable_ids_and_stream_annotations(ac):
    client = ac.client
    study = _post(ac, "/experiments/templates", json={"name": "http"}).json()
    variable = _post(
        ac, f"/experiments/templates/{study['template_id']}/variables",
        json={"label": "power (kW)", "role": "output", "kind": "scalar", "metadata": {}},
    ).json()
    assert variable["variable_id"].startswith("urn:acquirium#")
    run = _post(ac, f"/experiments/templates/{study['template_id']}/runs", json={"metadata": {}}).json()
    observed = _post(
        ac, f"/experiments/runs/{run['run_id']}/variables/{quote(variable['variable_id'], safe='')}/observations",
        json={"value": 42.0},
    )
    assert observed.status_code == 200
    selected_runs = _post(ac, f"/experiments/templates/{study['template_id']}/runs/query", json={"run_ids": [run["run_id"]]})
    assert selected_runs.status_code == 200
    assert [item["run_id"] for item in selected_runs.json()] == [run["run_id"]]
    selected_observations = _post(
        ac, f"/experiments/templates/{study['template_id']}/observations/query",
        json={"run_ids": [run["run_id"]], "variable_id": variable["variable_id"]},
    )
    assert selected_observations.status_code == 200
    assert selected_observations.json()[0]["value"] == 42.0
    assert _post(ac, f"/experiments/templates/{study['template_id']}/runs/query", json={"run_ids": []}).json() == []
    assert _post(ac, f"/experiments/templates/{study['template_id']}/observations/query", json={"run_ids": ["missing"]}).json() == []
    other = client.define_experiment("other")
    foreign = _declare(client, other["template_id"], "foreign", "output", "scalar")
    for invalid_id, expected_status in (("missing", 404), (foreign["variable_id"], 400)):
        path = f"/experiments/runs/{run['run_id']}/variables/{quote(invalid_id, safe='')}"
        assert _post(ac, f"{path}/observations", json={"value": 99}).status_code == expected_status
        assert _post(ac, f"{path}/file", data=b"invalid").status_code == expected_status
    assert len(client.list_experiment_observations(study["template_id"])) == 1
    assert client.experiment_usage()["files"] == 0
    tagged = _post(
        ac, f"/experiments/runs/{run['run_id']}/stream-annotations",
        json={"ref_uri": "urn:plant:flow", "start": "2026-01-01T00:00:00Z", "metadata": {"phase": "trial"}},
    )
    assert tagged.status_code == 200
    listed = _get(ac, "/experiments/stream-annotations", params={"ref_uri": "urn:plant:flow"})
    assert listed.status_code == 200
    assert listed.json()[0]["metadata"] == {"phase": "trial"}
    assert _post(
        ac, f"/experiments/stream-annotations/{tagged.json()['annotation_id']}/close",
        json={"end": "2026-01-02T00:00:00Z"},
    ).status_code == 200


def test_handles_delete_their_records_and_clear_needs_confirmation(study_api):
    study, client, ac = study_api.study, study_api.client, study_api.ac
    cost = study.output("cost").scalar()
    runs = []
    for case in ("baseline", "trial", "stress"):
        run = study.start({"case": case})
        cost.record(1.0)
        run.finish()
        runs.append(run)
    assert study.usage()["runs"] == 3
    assert study.experiments.where(metadata={"case": "trial"}).usage()["runs"] == 1
    assert runs[0].delete()["runs"] == 1
    assert study.experiments.where(metadata={"case": "trial"}).delete()["runs"] == 1
    assert [run.run_id for run in study.experiments.all()] == [runs[2].run_id]
    service = StudyService(ac)
    with pytest.raises(ValueError, match="confirm=True"):
        service.clear()
    client.clear_experiments.assert_not_called()
    assert service.usage()["studies"] == 1
    assert study.delete()["runs"] == 1
    assert service.clear(confirm=True)["studies"] == 0


@pytest.mark.parametrize("batch", [False, True])
def test_http_observation_ranges_require_both_bounds(study_api, ac, batch):
    study = study_api.study
    variable = study.input("flow").timeseries()
    run = study.start()
    path = (f"/experiments/runs/{run.run_id}/observations/batch" if batch else
            f"/experiments/runs/{run.run_id}/variables/{quote(variable.variable_id, safe='')}/observations")
    start, end = T0.isoformat(), T0.replace(hour=1).isoformat()
    saved = lambda: ac.client.list_experiment_observations(study.study_id)
    def record(bounds):
        body = {"ref_uri": "urn:site:flow", **bounds}
        return _post(ac, path, json=[{"variable_id": variable.variable_id, **body}] if batch else body)
    for bounds in ({"start": start}, {"end": end},
                   {"start": start, "end": None}, {"start": None, "end": end}):
        response = record(bounds)
        assert response.status_code == 400, response.text
        assert response.json()["detail"] == "stream ranges require both start and end"
    assert saved() == []
    for bounds in ({}, {"start": None, "end": None}, {"start": start, "end": end}):
        response = record(bounds)
        assert response.status_code == 200, response.text
        receipt = response.json()[0] if batch else response.json()
        assert receipt["sequence"] == len(saved())
    ranges = [(item["range_start"], item["range_end"]) for item in saved()]
    assert [tuple(timestamp(value) if value else None for value in pair) for pair in ranges] == [
        (None, None), (None, None), (T0, T0.replace(hour=1))]


def test_record_converts_values_and_checks_kinds_before_sending(study_api):
    from datetime import date
    np = pytest.importorskip("numpy")
    study, client = study_api.study, study_api.client
    flow = study.input("flow").scalar(unit="M3-PER-HR", design=np.float64(12.5))
    note = study.output("note").text()
    settings = study.input("settings").json()
    events = study.log("events")
    assert flow.metadata["design"] == 12.5
    run = study.start({"pumps": np.int64(2), "day": date(2026, 1, 1)})
    assert run.metadata == {"pumps": 2, "day": "2026-01-01"}
    flow.record(np.float32(0.5))
    flow.record(np.bool_(True))
    settings.record({"levels": np.array([1.0, 2.0]), "window": (1, 2)})
    events.record({"at": datetime(2026, 1, 1, 6), "step": np.int64(3)})
    values = [item.value for item in run.observations().all()]
    assert values == [0.5, True, {"levels": [1.0, 2.0], "window": [1, 2]}, {"at": "2026-01-01T06:00:00+00:00", "step": 3}]
    calls = client.record_experiment_batch.call_count
    for variable, value, error in ((flow, "fast", TypeError), (flow, np.nan, ValueError), (note, 5, TypeError),
                                   (settings, {"x": float("nan")}, ValueError), (settings, {1: "a"}, TypeError)):
        with pytest.raises(error):
            variable.record(value)
    assert client.record_experiment_batch.call_count == calls  # rejected before sending


def test_value_handlers_extend_recording():
    from acquirium import register_value_handler
    from acquirium.ExperimentValues import _HANDLERS, to_json

    class Pump:
        def __init__(self, speed): self.speed = speed
    class FastPump(Pump):
        pass
    with pytest.raises(TypeError, match="register_value_handler"):
        to_json({"pump": Pump(1)})
    register_value_handler(Pump, lambda pump: {"speed": pump.speed})
    try:
        assert to_json({"pumps": [Pump(1), FastPump(2)]}) == {"pumps": [{"speed": 1}, {"speed": 2}]}
        with pytest.raises(ValueError, match=r"value\.pumps\[0\]\.speed"):
            to_json({"pumps": [Pump(float("nan"))]})
    finally:
        del _HANDLERS[Pump]


def test_table_outputs_round_trip_as_parquet(study_api):
    study = study_api.study
    profile = study.output("profile").table(unit="M3")
    run = study.start()
    frame = pl.DataFrame({"hour": [0, 1, 2], "flow": [1.5, 2.0, 2.5], "pump": ["a", "b", "a"]})
    profile.record(frame)
    profile.record(frame.to_arrow())
    pandas = pytest.importorskip("pandas")
    profile.record(pandas.DataFrame({"hour": [3], "flow": [3.0], "pump": ["b"]}))
    with pytest.raises(TypeError, match="Polars, pandas, or Arrow"):
        profile.record({"hour": [0]})
    with pytest.raises(TypeError, match="occurred_at"):
        profile.record(frame, occurred_at=T0)
    run.finish()
    observations = run.observations(profile).all()
    assert len(observations) == 3
    assert observations[0].dataframe().equals(frame)
    assert observations[1].artifact_digest == observations[0].artifact_digest  # identical bytes
    assert observations[2].dataframe()["flow"].to_list() == [3.0]
    assert observations[0].read_bytes()[:4] == b"PAR1"
    assert observations[0].media_type == "application/vnd.apache.parquet"
    assert repr(observations[0]).startswith("<Observation 'profile' = table 'profile.parquet' (")
    with pytest.raises(TypeError, match="no options"):
        observations[0].dataframe(limit=1)


def test_scalars_frame_has_one_row_per_run_and_a_column_per_scalar(study_api):
    study = study_api.study
    pumps = study.input("pumps").scalar()
    cost = study.output("cost").scalar(unit="USD")
    feasible = study.output("feasible").scalar()
    study.output("peak power").scalar(unit="kW")
    status = study.output("status").text()
    runs = []
    for case, (count, value, ok) in enumerate([(1, 480.0, True), (2, 455.5, True), (3, None, False)]):
        run = study.start({"scenario": f"case {case}", "settings": {"flow": 10 * (case + 1)}})
        pumps.record(count)
        if value is not None:
            cost.record(value + 10)
            cost.record(value)  # the last recording is the run's value
        feasible.record(ok)
        status.record("optimal")
        run.finish()
        runs.append(run)

    frame = study.experiments.scalars(metadata=["scenario", "settings.flow", "missing"])
    assert frame.columns == ["experiment_id", "scenario", "settings.flow", "missing", "pumps", "cost", "feasible", "peak power"]
    assert frame["experiment_id"].to_list() == [run.run_id for run in runs]
    assert frame["cost"].to_list() == [480.0, 455.5, None]
    assert frame["settings.flow"].to_list() == [10, 20, 30]
    assert frame["missing"].null_count() == 3
    assert (frame.schema["feasible"], frame.schema["peak power"]) == (pl.Boolean, pl.Float64)
    selected = study.experiments.where(metadata={"scenario": "case 1"}).scalars("cost", pumps)
    assert selected.rows() == [(runs[1].run_id, 455.5, 2)]
    empty = study.experiments.where(metadata={"scenario": "none"}).scalars()
    assert (empty.height, empty.schema["cost"]) == (0, pl.Float64)
    with pytest.raises(TypeError, match="text variable"):
        study.experiments.scalars("status")
    with pytest.raises(ValueError, match="repeat: pumps"):
        study.experiments.scalars(metadata=["pumps"])


def test_stream_handles_name_a_stream_and_a_range():
    from acquirium.Client.acquirium import Acquirium
    from acquirium.Client.streams import StreamInterval

    client = Mock()
    by_name = Acquirium.stream(SimpleNamespace(client=client), "site-driver", "biogas-flow")
    assert by_name == Stream(str(compute_ref_uri("site-driver", "biogas-flow")))
    assert Acquirium.stream(SimpleNamespace(client=client), by_name.ref_uri) == by_name
    assert str(by_name) == by_name.ref_uri and repr(by_name) == f"<Stream {by_name.ref_uri}>"
    interval = by_name.between("2026-01-01T00:00:00", "2026-01-02T00:00:00Z")
    assert (interval.start, interval.end) == (T0, T0.replace(day=2))  # naive times are UTC
    assert isinstance(interval, StreamInterval) and interval.ref_uri == by_name.ref_uri
    with pytest.raises(ValueError, match="end at or after"):
        by_name.between(T0.replace(day=2), T0)
    client.timeseries_df.return_value = pl.DataFrame()
    empty = interval.dataframe()
    assert (empty.height, empty.schema["time"], empty.schema["value"]) == (0, pl.Datetime("us", "UTC"), pl.Float64)
    client.timeseries_df.assert_called_once_with(by_name.ref_uri, start=T0.isoformat(), end_ns=_after_ns(T0.replace(day=2)))
    with pytest.raises(RuntimeError, match="not bound"):
        Stream("urn:x").dataframe()


def test_time_series_record_a_reference_or_a_copy(study_api):
    study, client, ac = study_api.study, study_api.client, study_api.ac
    site = Stream("urn:site:biogas", client)
    biogas = study.input("biogas flow").timeseries(unit="M3-PER-HR")
    trajectory = study.output("trajectory").timeseries()
    cost = study.output("cost").scalar()
    window = site.between(T0, T0.replace(hour=2))
    run = study.start()

    # A range records a reference; no samples are written.
    biogas.record(window)
    client.record_experiment_batch.assert_called_with(run.run_id, [{
        "form": "reference", "variable_id": biogas.variable_id, "ref_uri": "urn:site:biogas",
        "interval": [T0.isoformat(), T0.replace(hour=2).isoformat()]}])
    ac.insert_timeseries.assert_not_called()

    # Rows read from the range are stored with the run, under its own source.
    client.timeseries_df.return_value = _stream_frame("urn:site:biogas", [(T0, 1.0), (T0.replace(hour=1), 2.0)])
    ac.reference_uri.return_value = "urn:acquirium#copied"
    copied = biogas.record(window.dataframe())
    assert client.record_experiment_batch.call_args.args[1][0]["samples"] == [[T0.isoformat(), 1.0], [T0.replace(hour=1).isoformat(), 2.0]]
    assert copied == Stream(str(compute_ref_uri(f"experiment/{run.run_id}", _stream_name(biogas.variable_id, ()))))

    # Stream query frames use ts or time, and value or value_numeric.
    narrow = pl.DataFrame({"data_alias": ["flow"] * 2, "point_uri": ["urn:p"] * 2, "ref_uri": ["urn:ref"] * 2,
                           "time": [T0, T0.replace(hour=1)], "value_numeric": [3.0, 4.0], "value_text": [None, None]})
    for rows in (narrow, narrow.to_arrow()):
        trajectory.record(rows)
        assert client.record_experiment_batch.call_args.args[1][0]["samples"] == [[T0.isoformat(), 3.0], [T0.replace(hour=1).isoformat(), 4.0]]
    # So do wide query frames with one stream column.
    trajectory.record(pl.DataFrame({"time": [T0], "biogas": [5.0]}))
    assert client.record_experiment_batch.call_args.args[1][0]["samples"] == [[T0.isoformat(), 5.0]]
    for rows, message in ((narrow.with_columns(point_uri=pl.Series(["urn:p", "urn:q"])), "several streams"),
                          (pl.DataFrame({"time": [T0], "value_numeric": [None]}, schema_overrides={"value_numeric": pl.Float64}), "numeric value"),
                          (pl.DataFrame({"time": [T0], "value": ["high"]}), "numeric value"),
                          (pl.DataFrame({"time": [T0], "a": [1.0], "b": [2.0]}), "time \\(or ts\\)"),
                          (pl.DataFrame({"when": [T0], "value": [1.0]}), "time \\(or ts\\)")):
        with pytest.raises(ValueError, match=message):
            trajectory.record(rows)
    with pytest.raises(TypeError, match="records rows"):
        biogas.add(window)
    with pytest.raises(TypeError, match="between"):
        biogas.record(site)
    with pytest.raises(TypeError, match="only time-series"):
        cost.record(window)
    run.finish()

    # One run's recorded range can be another run's input.
    observations = run.observations(biogas).all()
    reference, copy = observations
    assert reference.interval == window and reference.stream == site
    assert copy.stream == copied
    second = study.start()
    biogas.record(copy.interval)
    assert client.record_experiment_batch.call_args.args[1][0]["ref_uri"] == copied.ref_uri
    second.finish()


@pytest.mark.parametrize("arrow", [False, True])
@pytest.mark.parametrize("identifier", ["point_uri", "ref_uri"])
def test_time_series_record_rejects_multiple_query_streams_before_sending(study_api, arrow, identifier):
    study, client = study_api.study, study_api.client
    variable = study.output("flow").timeseries()
    study.start()
    # Query frames can use one alias for several streams at the same timestamp.
    frame = pl.DataFrame({"data_alias": ["data"] * 2, "point_uri": ["urn:point"] * 2,
                          "ref_uri": ["urn:ref"] * 2, "time": [T0] * 2,
                          "value_numeric": [1.0, 2.0], "value_text": [None, None]})
    frame = frame.with_columns(pl.Series(identifier, ["urn:first", "urn:second"]))
    with pytest.raises(ValueError, match="several streams"):
        variable.record(frame.to_arrow() if arrow else frame)
    client.record_experiment_batch.assert_not_called()


def test_output_assignment_reuses_handles_and_preserves_run_boundary(study_api):
    study, client = study_api.study, study_api.client
    cost = study.output("cost").scalar(unit="USD")
    first = study.start()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        first.output["cost"] = 12
        first.output["cost"] = 13
    assert first.output["cost"] is cost
    assert dict(first.output) == {"cost": cost}
    assert client.record_experiment_batch.call_count == 2
    assert client.record_experiment_batch.call_args.args[0] == first.run_id
    assert client.record_experiment_batch.call_args.args[1][0]["variable_id"] == cost.variable_id
    assert cost.unit == "USD" and cost.metadata == {}
    first.finish()
    second = study.start()
    for label in ("cost", "new"):
        with pytest.raises(RuntimeError, match="inactive experiment"):
            first.output[label] = 14
    assert "new" not in study.output
    assert client.record_experiment_batch.call_count == 2
    second.output["cost"] = 15
    assert client.record_experiment_batch.call_args.args[0] == second.run_id


@pytest.mark.parametrize("value,kind", [(1, "scalar"), (1.5, "scalar"), ("note", "text"), (True, "scalar"), (None, "json"), ([1, 2], "json"), ({"x": 1}, "json")])
def test_output_assignment_creates_exploratory_handle(study_api, value, kind):
    study, client = study_api.study, study_api.client
    run = study.start()
    with pytest.warns(UserWarning, match="New variable 'new'"):
        run.output["new"] = value
    handle = study.output["new"]
    assert handle.kind == kind
    assert run.output["new"] is handle
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        run.output["new"] = value
        handle.record(value)
    assert client.record_experiment_batch.call_count == 3
    assert client.record_experiment_batch.call_args.args[1][0]["value"] == value


def test_output_assignment_rejects_unsupported_values_and_conflicts(study_api, tmp_path):
    study, client = study_api.study, study_api.client
    study.input("config").json()
    run = study.start()
    for value in (object(), tmp_path, float("nan")):
        with pytest.raises(TypeError, match="JSON-compatible"):
            run.output["unsupported"] = value
    assert "unsupported" not in study.output
    with pytest.raises(HTTPError, match="declared differently"):
        run.output["config"] = {}
    client.record_experiment_batch.assert_not_called()


class _Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __eq__(self, other):
        return (self.x, self.y) == (other.x, other.y)


class PointCodec:
    """A custom codec: how a project saves objects only it understands."""
    media_type = "text/x-point"
    suffix = ".pt"

    def dumps(self, value):
        return f"{value.x},{value.y}".encode()

    def loads(self, data):
        return _Point(*map(int, data.decode().split(",")))


class TextCodec:
    media_type = "text/plain"

    def dumps(self, value):
        return value  # a str, not bytes

    def loads(self, data):
        return data.decode()


def test_object_variables_round_trip_through_their_codec(study_api):
    import acquirium as aq
    study = study_api.study
    state = study.output("state").object(codec=PointCodec)
    blob = study.output("blob").object(codec=aq.PickleCodec)
    config = study.input("config").file(media_type="application/json")
    run = study.start()
    saved = state.record(_Point(1, 2))
    assert saved["filename"] == "state.pt" and saved["byte_length"] == 3
    blob.record({"a": [1, 2], "b": (3, 4)})
    with pytest.raises(TypeError, match="occurred_at"):
        state.record(_Point(0, 0), occurred_at=T0)
    run.finish()
    observation = run.observations(state).latest()
    assert observation.load() == _Point(1, 2)
    assert observation.media_type == "text/x-point" and observation.read_bytes() == b"1,2"
    assert repr(observation).startswith("<Observation 'state' = object 'state.pt' (3 bytes)")
    assert run.observations(blob).latest().load() == {"a": [1, 2], "b": (3, 4)}
    assert state.codec.endswith(":PointCodec") and state.media_type == "text/x-point"
    # Redeclaring with another codec is a different declaration.
    with pytest.raises(HTTPError, match="declared differently"):
        study.output("state").object(codec=aq.JsonCodec)
    assert study.output("state").object(codec=PointCodec).variable_id == state.variable_id


def test_object_codecs_are_classes_that_load_can_import(study_api):
    import acquirium as aq
    study = study_api.study
    with pytest.raises(TypeError, match="pass the codec class"):
        study.output("x").object(codec=aq.PickleCodec())
    class Local(PointCodec):
        pass
    with pytest.raises(ValueError, match="module level"):
        study.output("x").object(codec=Local)
    broken = study.output("text").object(codec=TextCodec)
    profile = study.output("profile").table()
    run = study.start()
    with pytest.raises(TypeError, match=r"'text': TextCodec.dumps\(\) returned str, not bytes"):
        broken.record("hello")
    profile.record(pl.DataFrame({"flow": [3]}))
    observation = run.observations(profile).latest()
    with pytest.raises(TypeError, match="pass load\\(codec=...\\)"):
        observation.load()
    # Any artifact's bytes decode with an explicit codec.
    assert observation.load(codec=aq.BytesCodec)[:4] == b"PAR1"


def test_ledger_requires_a_codec_for_objects_only(ac):
    client = ac.client
    study = client.define_experiment("codecs")["template_id"]
    with pytest.raises(HTTPError, match="needs a codec import path"):
        _declare(client, study, "state", "output", "object")
    with pytest.raises(HTTPError, match="only object variables take a codec"):
        _declare(client, study, "cost", "output", "scalar", {"codec": "acquirium.ObjectCodecs:PickleCodec"})
    declared = _declare(client, study, "state", "output", "object", {"codec": "acquirium.ObjectCodecs:PickleCodec"})
    assert declared["metadata"]["codec"] == "acquirium.ObjectCodecs:PickleCodec"


def test_builtin_codecs():
    import pickle
    import acquirium as aq
    assert aq.BytesCodec().loads(aq.BytesCodec().dumps(bytearray(b"ab"))) == b"ab"
    with pytest.raises(TypeError, match="records bytes"):
        aq.BytesCodec().dumps("ab")
    assert aq.JsonCodec().loads(aq.JsonCodec().dumps({"when": T0, "n": [1]})) == {"when": T0.isoformat(), "n": [1]}
    assert isinstance(aq.PickleCodec(), aq.Codec)


def test_cloudpickle_codec_saves_what_pickle_cannot():
    import pickle
    import acquirium as aq
    pytest.importorskip("cloudpickle")
    double = lambda value: 2 * value
    assert pickle.loads(aq.CloudpickleCodec().dumps(double))(4) == 8


def test_a_read_up_to_end_includes_every_sample_within_its_microsecond():
    end = datetime(2026, 1, 2, 0, 0, 0, 123456, tzinfo=timezone.utc)
    end_us = int(end.timestamp()) * 1_000_000 + end.microsecond
    # Reads are half-open, so samples at end_us*1000 up to +999 ns fall inside.
    assert _after_ns(end) == end_us * 1000 + 1000
    assert _after_ns(end.isoformat()) == _after_ns(end)
