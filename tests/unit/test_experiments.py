from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.parse import quote
from unittest.mock import Mock
import warnings

import polars as pl
import pytest
from requests import HTTPError

from acquirium.Client.streams import Stream, _after_ns
from acquirium.Experiments import Study, StudyService, timestamp
from acquirium.internals.models import compute_stream_uri


T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _stream_frame(uri, rows):
    """Return the time-series response for one stream."""
    return pl.DataFrame({
        "ts": [when for when, _ in rows],
        "value": [value for _, value in rows],
        "uri": [uri] * len(rows),
    })


def _declare(client, template_id, label, role, kind, metadata=None):
    return client.declare_experiment_variable(template_id, label=label, role=role, kind=kind, metadata=metadata or {})


def _status(error):
    return error.value.response.status_code


def _recorded_stream(ac, run, variable, rows):
    """Record samples with a run, as a time-series record() does."""
    receipt = ac.client.record_experiment_batch(run["run_id"], [{
        "form": "samples", "variable_id": variable["variable_id"],
        "samples": [[when.isoformat(), value] for when, value in rows]}])
    return receipt[0]["stream_uri"]


def _site_stream(ac, name, rows):
    """A stream a driver wrote, outside any experiment."""
    ac.register_streams([{"source_id": "site", "stream_name": name}])
    ac.insert_timeseries("site", name, rows)
    return str(ac.reference_uri("site", name))


def _samples(ac, ref):
    return ac.client.timeseries_df(ref).height


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


def test_rejecting_a_trailing_record_leaves_no_samples_or_observations(ac):
    client = ac.client
    study = client.define_experiment("samples")["template_id"]
    full = _declare(client, study, "full", "output", "timeseries")["variable_id"]
    empty = _declare(client, study, "empty", "output", "timeseries")["variable_id"]
    cost = _declare(client, study, "cost", "output", "scalar")["variable_id"]
    run = client.start_experiment(study, {})["run_id"]
    valid = {"form": "samples", "variable_id": full, "samples": [[T0.isoformat(), 1.0]]}

    # Prepare valid samples first, then reject the batch at its trailing entry.
    for invalid, reason in [
        ({"form": "samples", "variable_id": empty, "samples": []}, "at least one sample"),
        ({"form": "value", "variable_id": cost, "value": "bad"}, "records a finite number"),
    ]:
        with pytest.raises(HTTPError, match=reason):
            client.record_experiment_batch(run, [valid, invalid])
        assert client.list_experiment_observations(study, run_id=run) == []
        assert client.experiment_usage(run_ids=[run])["samples"] == 0
    client.finish_experiment(run)
    assert client.delete_experiments([run])["samples"] == 0


def test_delete_keeps_streams_that_other_runs_reference(ac):
    client = ac.client
    simulation, analysis = client.define_experiment("simulation"), client.define_experiment("analysis")
    level = _declare(client, simulation["template_id"], "level", "output", "timeseries")
    used = _declare(client, analysis["template_id"], "level", "input", "timeseries")
    source = client.start_experiment(simulation["template_id"], {})
    ref = _recorded_stream(ac, source, level, [(T0, 1.0)])
    reader = client.start_experiment(analysis["template_id"], {})
    client.observe_experiment(reader["run_id"], used["variable_id"], stream_uri=ref)
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
        (scalar, {"value": 1, "stream_uri": "urn:ref"}), (series, {"value": 1}), (file, {"value": "x"}),
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


def test_http_artifacts_download_with_their_saved_names(study_api, ac, tmp_path):
    study, client = study_api.study, study_api.client
    report = study.output("report").file(media_type="text/plain")
    cost = study.output("cost").scalar()
    run = study.start()

    # The Python file API sends the bytes and media type to the current recording route.
    source = tmp_path / "report.txt"
    source.write_bytes(b"new report")
    result = report.record(source)
    assert client.experiment_artifact(result["digest"]) == b"new report"
    client.attach_experiment_file.assert_called_once_with(
        run.run_id, report.variable_id, source, media_type="text/plain",
    )
    with pytest.raises(TypeError, match="occurred_at"):
        report.record(source, occurred_at=T0)

    # Keep the raw file route's Unicode header and content type contract.
    path = f"/experiments/runs/{run.run_id}/variables/{quote(report.variable_id, safe='')}/file"
    filename = quote("débit.txt", safe="")
    saved = _post(ac, path, data=b"report", headers={"X-Filename": filename, "Content-Type": "text/plain"})
    assert saved.status_code == 200
    download = _get(ac, f"/experiments/artifacts/{saved.json()['digest']}")
    assert (download.status_code, download.content) == (200, b"report")
    assert download.headers["content-type"].startswith("text/plain")
    assert download.headers["content-disposition"] == f"attachment; filename*=UTF-8''{filename}"
    assert run.observations(report).latest().filename == "débit.txt"
    for digest in ("0" * 64, "..%2F..%2Fdata.duckdb", "not-a-digest"):
        assert _get(ac, f"/experiments/artifacts/{digest}").status_code == 404
    scalar_path = f"/experiments/runs/{run.run_id}/variables/{quote(cost.variable_id, safe='')}"
    assert _post(ac, f"{scalar_path}/file", data=b"x").status_code == 400
    assert _post(ac, f"{scalar_path}/observations", json={"value": "abc"}).status_code == 400
    run.finish()
    with pytest.raises(RuntimeError, match="start an experiment"):
        report.record(source)


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
    assert cost.variable_id == str(compute_stream_uri(f"study/{study.study_id}", "cost"))
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


def test_resumed_run_keeps_historical_and_live_stream_annotations(study_api):
    study, ac = study_api.study, study_api.ac
    start, end = T0, T0.replace(day=2)
    uri = "urn:plant:gas-flow"
    efficiency = study.output("efficiency").scalar()

    # Save a historical annotation and leave a second annotation open.
    run = study.start({"digester": "A"})
    historical = run.annotate_stream(uri, start=start, end=end, metadata={"treatment": "new membrane"})
    live = run.annotate_stream(uri, start=end)
    assert run.observations().all() == []

    # Reopen the Study and continue recording against the same run.
    reopened = StudyService(ac).get("study")
    resumed = reopened.resume(run.run_id)
    reopened.output["efficiency"].record(0.82)
    [recorded] = resumed.observations("efficiency").all()
    assert recorded.variable.variable_id == efficiency.variable_id
    assert recorded.value == 0.82 and recorded.sequence == 3
    assert recorded.recorded_at.utcoffset() is not None
    assert resumed.stream_annotations()[1].range_end is None
    resumed.finish()
    with pytest.raises(ValueError, match="running"):
        reopened.resume(run.run_id)

    matches = reopened.stream_annotations(stream_uri=uri, start=end)
    assert [annotation.annotation_id for annotation in matches] == [historical.annotation_id, live.annotation_id]
    assert resumed.stream_annotations()[0].metadata == {"treatment": "new membrane"}
    assert StudyService(ac).stream_annotations(stream_uri=uri, end=start)[0].run_id == run.run_id
    assert not reopened.stream_annotations(stream_uri="urn:another-stream")

    study_api.client.timeseries_df.return_value = _stream_frame(uri, [(start, 1.0)])
    assert historical.dataframe().rows() == [(start, 1.0)]
    assert historical.stream == Stream(uri)
    study_api.client.timeseries_df.assert_called_once_with(uri, start=start.isoformat(), end_ns=_after_ns(end))

    # Closing and retrospectively annotating are still possible after completion.
    live.close(T0.replace(day=3))
    assert live.range_end == T0.replace(day=3)
    with pytest.raises(HTTPError, match="already closed"):
        live.close(T0.replace(day=4))
    assert not reopened.stream_annotations(start=T0.replace(day=4))
    retrospective = resumed.annotate_stream(uri, start=start, end=end, metadata={"identified_later": True})
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
    assert len(StudyService(study_api.ac).stream_annotations(stream_uri="urn:plant:flow")) == 2


@pytest.mark.parametrize("completion", ["finish", "fail"])
def test_completing_a_retrieved_run_keeps_other_output_handles_active(completion):
    client = Mock()
    client.start_experiment.side_effect = [{"run_id": "active"}, {"run_id": "later"}]
    client.get_experiment.side_effect = lambda run_id: {"run_id": run_id, "template_id": "study"}
    client.finish_experiment.return_value = {
        "status": "failed" if completion == "fail" else "succeeded", "finished_at": T0,
    }
    client.record_experiment_batch.return_value = [{"sequence": 1}]
    study = Study(SimpleNamespace(client=client), {"template_id": "study", "name": "results"}, [
        {"variable_id": "cost-id", "label": "cost", "role": "output", "kind": "scalar",
         "metadata": {"unit": "USD"}},
    ])

    def complete(run):
        """Complete the retrieved handle through the selected public method."""
        if completion == "fail":
            run.fail(RuntimeError("failed"))
        else:
            run.finish()

    # Completing another run leaves this Study's active output binding in place.
    active = study.start()
    other = study.experiments.get("other")
    complete(other)
    assert other.status == client.finish_experiment.return_value["status"]
    assert other.finished_at == T0
    with pytest.raises(RuntimeError, match="finish the active experiment"):
        study.start()
    cost = study.output["cost"]
    assert dict(active.output) == {"cost": cost} and cost.unit == "USD"
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        active.output["cost"] = 12
    cost.record(13, occurred_at=T0)
    client.record_experiment_batch.assert_called_with("active", [{
        "form": "value", "variable_id": "cost-id", "value": 13, "occurred_at": T0.isoformat(),
    }])

    # A distinct handle for the active run clears its binding by run ID.
    retrieved = study.experiments.get(active.run_id)
    assert retrieved is not active
    complete(retrieved)
    if completion == "fail":
        client.finish_experiment.assert_called_with(
            "active", failed=True, error={"type": "RuntimeError", "message": "failed"},
        )
    else:
        client.finish_experiment.assert_called_with("active")
    with pytest.raises(RuntimeError, match="start an experiment"):
        cost.record(14)
    later = study.start()
    calls = client.record_experiment_batch.call_count
    for label in ("cost", "new"):
        with pytest.raises(RuntimeError, match="inactive experiment"):
            active.output[label] = 15
    assert client.record_experiment_batch.call_count == calls
    assert "new" not in study.output
    later.output["cost"] = 16
    assert client.record_experiment_batch.call_args.args[0] == "later"


def test_reopened_study_hydrates_variables_and_reads_experiments(study_api):
    study = study_api.study
    cost = study.output("operating cost").scalar(unit="USD", objective="minimize")
    power = study.output("electrical power").timeseries(observed="urn:plant:tank", unit="kW")
    run = study.start({"scenario": "baseline"})
    cost.record(12.5)
    run.finish()

    # Reopen the Study so the saved declarations populate new handles.
    reopened = StudyService(study_api.ac).get("study")
    assert reopened.study_id == study.study_id
    saved = reopened.variables["operating cost"]
    assert saved.variable_id == cost.variable_id
    assert (saved.unit, saved.metadata) == ("USD", {"objective": "minimize"})
    saved_power = reopened.variables[power.variable_id]
    assert (saved_power.label, saved_power.unit, saved_power.observed) == (
        "electrical power", "kW", "urn:plant:tank",
    )

    selected = reopened.experiments.where(status="succeeded", metadata={"scenario": "baseline"})
    assert [item.run_id for item in selected.all()] == [run.run_id]
    assert saved.observations(selected).latest().value == 12.5


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
    pytest.param([12.5, {"event": "done"}], id="number-first"),
    pytest.param([{"event": "done"}, 12.5], id="object-first"),
    pytest.param([{"x": 1}, {"y": [2, 3]}], id="different-fields"),
    pytest.param([[1, 2], ["three", {"x": 4}]], id="nested-arrays"),
    pytest.param([None, True, 42, "note", {"nested": [1, None]}], id="mixed"),
    pytest.param([*range(101), {"after_inference_window": True}], id="late-object"),
])
def test_records_frame_preserves_heterogeneous_values(values):
    from acquirium.ExperimentMetadata import records_frame

    assert records_frame([{"value": value} for value in values])["value"].to_list() == values


def test_scalar_observation_frame_supports_numeric_analysis(study_api):
    result = study_api.study.output("cost").scalar()
    run = study_api.study.start()
    result.record(12.5)
    result.record(7.5)
    assert run.observations().frame().select(pl.col("value").sum()).item() == 20.0


def test_observations_refresh_variables_declared_in_another_session(study_api):
    study = study_api.study
    known = study.output("known").scalar()
    reader = StudyService(study_api.ac).get("study")
    known_handle = reader.output["known"]
    added = study.output("added later").scalar(unit="kW")
    run = study.start()
    known.record(1)
    added.record(2)
    study_api.client.list_experiment_variables.reset_mock()

    # The first read hydrates the new handle. Later accessors reuse both handles.
    observations = reader.experiments.get(run.run_id).observations()
    assert observations.frame()["value"].to_list() == [1, 2]
    assert [item.value for item in observations.all()] == [1, 2]
    latest = observations.latest()
    assert latest.value == 2 and latest.variable is reader.output["added later"]
    assert latest.variable.unit == "kW" and latest.variable.created_at is not None
    assert reader.output["known"] is known_handle
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


def test_recorded_stream_reads_after_its_accepted_receipt():
    client = Mock()
    client.start_experiment.return_value = {"run_id": "run"}
    client.record_experiment_batch.return_value = [{"stream_uri": "urn:volume", "accepted": 5}]
    ac = Mock(client=client)
    study = Study(ac, {"template_id": "study", "name": "samples"}, [
        {"variable_id": "volume-id", "label": "volume", "role": "output", "kind": "timeseries"},
    ])
    volume = study.output["volume"]
    with pytest.raises(RuntimeError):
        volume.record([])
    study.start()
    assert volume.record([]) is None
    with pytest.raises(TypeError, match="occurred_at"):
        volume.record(None, occurred_at=T0)
    client.record_experiment_batch.assert_not_called()

    recorded = volume.record([(T0, 10)])
    client.record_experiment_batch.assert_called_once_with("run", [{
        "form": "samples", "variable_id": "volume-id", "key": None,
        "samples": [[T0.isoformat(), 10]],
    }])
    ac.register_streams.assert_not_called()

    # The returned handle waits for its own samples to publish before reading.
    client.timeseries_df.return_value = _stream_frame("urn:volume", [(T0, 10.0)])
    assert str(recorded) == "urn:volume"
    assert recorded.dataframe(limit=5).rows() == [(T0, 10.0)]
    client.timeseries_df.assert_called_once_with("urn:volume", start=None, end_ns=None, after=5, limit=5)


def test_http_observation_routes_distinguish_missing_and_foreign_variables(ac):
    client = ac.client
    study = client.define_experiment("http")["template_id"]
    variable = _declare(client, study, "power (kW)", "output", "scalar")["variable_id"]
    assert variable.startswith("urn:acquirium#")
    run = client.start_experiment(study, {})["run_id"]
    other = client.define_experiment("other")["template_id"]
    foreign = _declare(client, other, "foreign", "output", "file")["variable_id"]

    # Legacy item routes and the current recording route translate ownership errors.
    for invalid_id, expected_status in (("missing", 404), (foreign, 400)):
        item = f"/experiments/runs/{run}/variables/{quote(invalid_id, safe='')}"
        assert _post(ac, f"{item}/observations", json={"value": 99}).status_code == expected_status
        assert _post(ac, f"{item}/file", data=b"invalid").status_code == expected_status
        recorded = _post(ac, f"/experiments/runs/{run}/record", json=[{
            "form": "artifact", "variable_id": invalid_id,
            "artifact": {"filename": "invalid.txt", "data": "aW52YWxpZA=="},
        }])
        assert recorded.status_code == expected_status
    assert client.list_experiment_observations(study) == []
    assert client.experiment_usage()["files"] == 0

    # A percent-escaped valid ID is accepted and gets the first sequence.
    item = f"/experiments/runs/{run}/variables/{quote(variable, safe='')}/observations"
    observed = _post(ac, item, json={"value": 42.0})
    assert observed.status_code == 200 and observed.json()["sequence"] == 1
    runs = f"/experiments/templates/{study}/runs/query"
    observations = f"/experiments/templates/{study}/observations/query"
    assert [row["run_id"] for row in _post(ac, runs, json={"run_ids": [run]}).json()] == [run]
    selected = _post(ac, observations, json={"run_ids": [run], "variable_id": variable})
    assert selected.status_code == 200
    assert [row["value"] for row in selected.json()] == [42.0]
    assert _post(ac, runs, json={"run_ids": []}).json() == []
    assert _post(ac, observations, json={"run_ids": ["missing"]}).json() == []


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


def test_legacy_batch_observation_ranges_require_both_bounds(ac):
    client = ac.client
    study = client.define_experiment("flow intervals")["template_id"]
    flow = _declare(client, study, "flow", "input", "timeseries")["variable_id"]
    run = client.start_experiment(study, {})["run_id"]
    path = f"/experiments/runs/{run}/observations/batch"
    start, end = T0.isoformat(), T0.replace(hour=1).isoformat()
    reference = {"stream_uri": "urn:site:flow"}

    # A single bound is invalid whether the other bound was omitted or sent as null.
    for bounds in ({"start": start}, {"end": end}, {"start": start, "end": None}, {"start": None, "end": end}):
        response = _post(ac, path, json=[{"variable_id": flow, **reference, **bounds}])
        assert response.status_code == 400
        assert response.json()["detail"] == "stream ranges require both start and end"
    single = f"/experiments/runs/{run}/variables/{quote(flow, safe='')}/observations"
    assert _post(ac, single, json={**reference, "start": start}).status_code == 400
    assert client.list_experiment_observations(study) == []

    # Omitted bounds and explicit nulls save an unbounded reference. A pair saves a range.
    for bounds in ({}, {"start": None, "end": None}, {"start": start, "end": end}):
        response = _post(ac, path, json=[{"variable_id": flow, **reference, **bounds}])
        assert response.status_code == 200
    rows = client.list_experiment_observations(study)
    assert [item["sequence"] for item in rows] == [1, 2, 3]
    ranges = [(item["range_start"], item["range_end"]) for item in rows]
    assert [tuple(timestamp(value) if value else None for value in pair) for pair in ranges] == [
        (None, None), (None, None), (T0, T0.replace(hour=1)),
    ]


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
    note.record("ok")
    events.record({"at": datetime(2026, 1, 1, 6), "step": np.int64(3)}, occurred_at=T0)
    values = [item.value for item in run.observations().all()]
    assert values == [0.5, True, {"levels": [1.0, 2.0], "window": [1, 2]}, "ok", {"at": "2026-01-01T06:00:00+00:00", "step": 3}]
    assert run.observations().frame()["value"].to_list() == values
    assert events.observations().latest().occurred_at == T0
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
    import acquirium as aq

    study = study_api.study
    profile = study.output("profile").table(unit="M3")
    run = study.start()
    frame = pl.DataFrame({"hour": [0, 1], "flow": [1.5, 2.0], "pump": ["a", "b"]})
    profile.record(frame)
    with pytest.raises(TypeError, match="occurred_at"):
        profile.record(frame, occurred_at=T0)
    run.finish()

    observed = run.observations(profile).latest()
    assert observed.dataframe().equals(frame)
    assert observed.media_type == "application/vnd.apache.parquet"
    assert repr(observed).startswith("<Observation 'profile' = table 'profile.parquet' (")
    with pytest.raises(TypeError, match="no options"):
        observed.dataframe(limit=1)
    with pytest.raises(TypeError, match=r"pass load\(codec=...\)"):
        observed.load()
    assert observed.load(codec=aq.BytesCodec)[:4] == b"PAR1"


def test_parquet_serializes_polars_and_arrow_identically():
    from acquirium.ExperimentValues import parquet

    frame = pl.DataFrame({"hour": [0, 1], "flow": [1.5, 2.0]})
    assert parquet(frame) == parquet(frame.to_arrow())
    with pytest.raises(TypeError, match="Polars, pandas, or Arrow"):
        parquet({"hour": [0]})


def test_parquet_serializes_pandas():
    from io import BytesIO

    from acquirium.ExperimentValues import parquet

    pandas = pytest.importorskip("pandas")
    assert pl.read_parquet(BytesIO(parquet(pandas.DataFrame({"flow": [3.0]}))))["flow"].to_list() == [3.0]


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
    ac = SimpleNamespace(client=client)
    by_name = Acquirium.stream(ac, source="site-driver", name="biogas-flow")
    assert by_name == Stream(str(compute_stream_uri("site-driver", "biogas-flow")))
    assert Acquirium.stream(ac, uri=by_name.stream_uri) == by_name
    assert Acquirium.stream(ac, "site-driver", "biogas-flow") == by_name
    assert Acquirium.stream(ac, by_name.stream_uri) == by_name
    assert Acquirium.stream(ac, source_id_or_uri="site-driver", name="biogas-flow") == by_name
    with pytest.raises(TypeError, match="either uri or source and name"):
        Acquirium.stream(ac, source="site-driver", name="biogas-flow", uri=by_name.stream_uri)
    assert str(by_name) == by_name.stream_uri and repr(by_name) == f"<Stream {by_name.stream_uri}>"
    interval = by_name.between("2026-01-01T00:00:00", "2026-01-02T00:00:00Z")
    assert (interval.start, interval.end) == (T0, T0.replace(day=2))  # naive times are UTC
    assert isinstance(interval, StreamInterval) and interval.stream_uri == by_name.stream_uri
    with pytest.raises(ValueError, match="end at or after"):
        by_name.between(T0.replace(day=2), T0)
    client.timeseries_df.return_value = pl.DataFrame()
    empty = interval.dataframe()
    assert (empty.height, empty.schema["time"], empty.schema["value"]) == (0, pl.Datetime("us", "UTC"), pl.Float64)
    client.timeseries_df.assert_called_once_with(by_name.stream_uri, start=T0.isoformat(), end_ns=_after_ns(T0.replace(day=2)))
    with pytest.raises(RuntimeError, match="not bound"):
        Stream("urn:x").dataframe()


def test_sample_rows_accepts_frames_with_one_stream():
    from acquirium.ExperimentValues import sample_rows

    narrow = pl.DataFrame({"time": [T0], "value_numeric": [3.0], "stream_uri": ["urn:one"]})
    wide = pl.DataFrame({"time": [T0], "flow": [3.0]})
    standard = pl.DataFrame({"ts": [T0], "value": [3.0]})
    for frame in (narrow, narrow.to_arrow(), wide, standard):
        assert sample_rows(frame) == [(T0, 3.0)]


@pytest.mark.parametrize("value", [None, "high"], ids=["missing", "text"])
def test_sample_rows_requires_numeric_values(value):
    from acquirium.ExperimentValues import sample_rows

    with pytest.raises(ValueError, match="numeric value"):
        sample_rows(pl.DataFrame({"time": [T0], "value": [value]}))


def test_sample_rows_requires_time_and_one_value_column():
    from acquirium.ExperimentValues import sample_rows

    for frame in (
        pl.DataFrame({"when": [T0], "value": [1.0]}),
        pl.DataFrame({"time": [T0], "a": [1.0], "b": [2.0]}),
    ):
        with pytest.raises(ValueError, match=r"time \(or ts\)"):
            sample_rows(frame)


def test_record_distinguishes_reference_from_samples():
    client = Mock()
    client.start_experiment.return_value = {"run_id": "run"}
    client.record_experiment_batch.return_value = [{"sequence": 1}]
    study = Study(SimpleNamespace(client=client), {"template_id": "study", "name": "inputs"}, [
        {"variable_id": "flow-id", "label": "flow", "role": "input", "kind": "timeseries"},
        {"variable_id": "cost-id", "label": "cost", "role": "output", "kind": "scalar"},
    ])
    site = Stream("urn:site:flow")
    end = T0.replace(hour=2)
    interval = site.between(T0, end)
    study.start()
    flow = study.input["flow"]
    with pytest.raises(TypeError, match="records rows"):
        flow.add(interval)
    with pytest.raises(TypeError, match="between"):
        flow.record(site)
    with pytest.raises(TypeError, match="only time-series"):
        study.output["cost"].record(interval)
    client.record_experiment_batch.assert_not_called()

    flow.record(interval)
    client.record_experiment_batch.assert_called_once_with("run", [{
        "form": "reference", "variable_id": "flow-id", "stream_uri": "urn:site:flow",
        "interval": [T0.isoformat(), end.isoformat()],
    }])


@pytest.mark.parametrize("arrow", [False, True], ids=["polars", "arrow"])
@pytest.mark.parametrize("identifier", ["property_uri", "stream_uri"])
def test_sample_rows_rejects_multiple_query_streams(arrow, identifier):
    from acquirium.ExperimentValues import sample_rows

    # One query alias can hold several streams at the same timestamp.
    frame = pl.DataFrame({"data_alias": ["data"] * 2, "property_uri": ["urn:point"] * 2,
                          "stream_uri": ["urn:ref"] * 2, "time": [T0] * 2, "value_numeric": [1.0, 2.0]})
    frame = frame.with_columns(pl.Series(identifier, ["urn:first", "urn:second"]))
    with pytest.raises(ValueError, match="several streams"):
        sample_rows(frame.to_arrow() if arrow else frame)


def test_output_assignment_infers_kinds_and_reuses_handles():
    client = Mock()
    client.start_experiment.return_value = {"run_id": "run"}
    client.declare_experiment_variable.side_effect = lambda _study_id, **item: {
        **item, "variable_id": item["label"], "created": True,
    }
    client.record_experiment_batch.return_value = [{"sequence": 1}]
    study = Study(SimpleNamespace(client=client), {"template_id": "study", "name": "results"})
    run = study.start()
    for label, value, kind in [
        ("integer", 1, "scalar"), ("decimal", 1.5, "scalar"), ("note", "note", "text"),
        ("flag", True, "scalar"), ("null", None, "json"), ("array", [1, 2], "json"), ("map", {"x": 1}, "json"),
    ]:
        with pytest.warns(UserWarning, match=f"New variable '{label}'"):
            run.output[label] = value
        assert run.output[label] is study.output[label]
        assert run.output[label].kind == kind
        assert client.record_experiment_batch.call_args.args[1][0]["value"] == value

    # Assignment to an existing output and direct recording use the same handle.
    integer = run.output["integer"]
    declarations = client.declare_experiment_variable.call_count
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        run.output["integer"] = 2
        integer.record(3)
    assert run.output["integer"] is integer
    assert client.declare_experiment_variable.call_count == declarations
    assert client.record_experiment_batch.call_args.args[1][0]["value"] == 3


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


def test_object_codecs_are_classes_that_load_can_import():
    import acquirium as aq

    from acquirium.ObjectCodecs import codec_path, encode

    with pytest.raises(TypeError, match="pass the codec class"):
        codec_path(aq.PickleCodec())

    class Local(PointCodec):
        pass

    with pytest.raises(ValueError, match="module level"):
        codec_path(Local)
    with pytest.raises(TypeError, match=r"'text': TextCodec.dumps\(\) returned str, not bytes"):
        encode(TextCodec(), "hello", label="text")


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
