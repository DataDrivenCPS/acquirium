from datetime import datetime, timezone
from importlib import import_module
from types import SimpleNamespace
from urllib.parse import quote
from unittest.mock import Mock
import warnings

import polars as pl
import pytest

from acquirium.Experiments import ExperimentStore, Study, StudyService, timestamp
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.models import compute_ref_uri


def test_reusable_template_variables_are_isolated_and_timestamped(tmp_path):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        experiments = ExperimentStore(store, tmp_path / "artifacts")
        template = experiments.define("load-shift")
        variable = experiments.declare(template["template_id"], "configuration", "input", "json", {})
        first = experiments.start(template["template_id"], {"case": 1})
        first_observation = experiments.observe(first["run_id"], variable["variable_id"], value={"flow": 10})
        assert first_observation["sequence"] == 1
        assert first_observation["recorded_at"].tzinfo is not None
        experiments.finish(first["run_id"], "succeeded")
        with pytest.raises(ValueError, match="terminal"):
            experiments.observe(first["run_id"], variable["variable_id"], value={"flow": 20})
        second = experiments.start(template["template_id"], {"case": 2})
        second_observation = experiments.observe(second["run_id"], variable["variable_id"], value={"flow": 20})
        assert second_observation["sequence"] == 1
    finally:
        store.close()


def test_file_attachment_is_content_addressed(tmp_path):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        experiments = ExperimentStore(store, tmp_path / "artifacts")
        template = experiments.define("files")
        variable = experiments.declare(template["template_id"], "config", "input", "file", {})
        run = experiments.start(template["template_id"], {})
        first = experiments.attach_file(run["run_id"], variable["variable_id"], "a.json", "application/json", b"{}")
        second = experiments.attach_file(run["run_id"], variable["variable_id"], "b.json", "application/json", b"{}")
        assert first["digest"] == second["digest"]
        assert (tmp_path / "artifacts" / first["digest"]).read_bytes() == b"{}"
    finally:
        store.close()


@pytest.mark.parametrize("attachment", [False, True])
@pytest.mark.parametrize("foreign", [False, True])
def test_observation_rejects_unknown_or_foreign_variable(tmp_path, attachment, foreign):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        ledger = ExperimentStore(store, tmp_path / "artifacts")
        study = ledger.define("owner")
        other = ledger.define("other")
        variable = ledger.declare(other["template_id"], "result", "output", "file" if attachment else "scalar", {})
        run = ledger.start(study["template_id"], {})
        variable_id = variable["variable_id"] if foreign else "missing"
        with pytest.raises(ValueError if foreign else KeyError):
            if attachment:
                ledger.attach_file(run["run_id"], variable_id, "result.txt", "text/plain", b"result")
            else:
                ledger.observe(run["run_id"], variable_id, value=12.5)
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM experiment_observations").fetchone()[0] == 0
            assert conn.execute("SELECT count(*) FROM experiment_artifacts").fetchone()[0] == 0
        assert list(ledger.artifact_dir.iterdir()) == []
        owned = ledger.declare(study["template_id"], "valid", "output", "scalar", {})
        assert ledger.observe(run["run_id"], owned["variable_id"], value=42)["sequence"] == 1
    finally:
        store.close()


@pytest.fixture
def study_api(tmp_path):
    store = DuckDBStore(tmp_path / "api.duckdb", recreate=True)
    ledger = ExperimentStore(store, tmp_path / "artifacts")
    client = Mock()
    client.define_experiment.side_effect = ledger.define
    client.declare_experiment_variable.side_effect = ledger.declare
    client.list_experiment_studies.side_effect = ledger.studies
    client.get_experiment_study.side_effect = ledger.study
    client.list_experiment_variables.side_effect = ledger.variables
    client.start_experiment.side_effect = ledger.start
    client.list_experiments.side_effect = ledger.runs
    client.get_experiment.side_effect = ledger.run
    client.list_experiment_observations.side_effect = ledger.observations
    def observe(run_id, variable_id, *, start=None, end=None, occurred_at=None, **body):
        interval = (timestamp(start), timestamp(end)) if start and end else None
        return ledger.observe(
            run_id,
            variable_id,
            occurred_at=timestamp(occurred_at) if occurred_at else None,
            interval=interval,
            **body,
        )
    client.observe_experiment.side_effect = observe
    client.finish_experiment.side_effect = lambda run_id, failed=False, error=None: ledger.finish(run_id, "failed" if failed else "succeeded", error)
    client.annotate_experiment_stream.side_effect = lambda run_id, ref_uri, *, start, end=None, metadata=None: ledger.annotate_stream(run_id, ref_uri, start, end, metadata)
    client.close_experiment_stream_annotation.side_effect = lambda annotation_id, *, end: ledger.close_stream_annotation(annotation_id, end)
    client.list_experiment_stream_annotations.side_effect = ledger.stream_annotations
    ac = Mock(client=client)
    item = ledger.define("study")
    try:
        yield SimpleNamespace(study=Study(ac, item), ac=ac, client=client, item=item)
    finally:
        store.close()


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
    with pytest.raises(ValueError, match="declared differently"):
        study.output("cost").scalar(unit="EUR")
    with pytest.raises(ValueError, match="declared differently"):
        study.input("cost").scalar(unit="USD")
    assert study.output["cost"] is cost


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

    expected = object()
    study_api.client.timeseries_df.return_value = expected
    assert historical.dataframe() is expected
    study_api.client.timeseries_df.assert_called_once_with(
        "urn:plant:gas-flow", start=start.isoformat(), end=end.isoformat())

    live.close(datetime(2026, 1, 3, tzinfo=timezone.utc))
    assert live.range_end == datetime(2026, 1, 3, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="already closed"):
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
    with pytest.raises(ValueError, match="at or after start"):
        first.annotate_stream("urn:plant:flow", start=start, end=before)
    with pytest.raises(ValueError, match="reference URI"):
        first.annotate_stream(" ", start=start)
    annotation = first.annotate_stream("urn:plant:flow", start=start)
    with pytest.raises(ValueError, match="at or after start"):
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
    power.use("urn:acquirium:power-1", interval=(start, end))
    first.finish()

    reopened = StudyService(ac).get("study")

    assert reopened.study_id == study.study_id
    assert reopened.variables["operating cost"].variable_id == cost.variable_id
    assert reopened.variables[power.variable_id].label == "electrical power"
    assert reopened.variables["operating cost"].metadata == {
        "objective": "minimize",
        "unit": "USD",
    }
    assert reopened.output["electrical power"].metadata == {
        "observed": "urn:plant:tank",
        "unit": "kW",
    }
    assert reopened.variables.where(role="output").frame().select(
        "label", "role", "kind", "metadata"
    ).to_dicts() == [
        {
            "label": "operating cost",
            "role": "output",
            "kind": "scalar",
            "metadata": {"objective": "minimize", "unit": "USD", "observed": None},
        },
        {
            "label": "electrical power",
            "role": "output",
            "kind": "timeseries",
            "metadata": {"objective": None, "observed": "urn:plant:tank", "unit": "kW"},
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
    expected_frame = object()
    client.timeseries_df.return_value = expected_frame
    assert power_observation.ref_uri == "urn:acquirium:power-1"
    assert power_observation.dataframe() is expected_frame
    client.timeseries_df.assert_called_once_with(
        power_observation.ref_uri,
        start=start.isoformat(),
        end=end.isoformat(),
    )


def test_variable_catalog_expands_non_null_metadata_without_overwriting_columns(study_api):
    study = study_api.study
    study.input("configuration").json(
        category="simulation",
        objective=None,
        only_null=None,
        label="metadata label",
        created_at="metadata timestamp",
    )
    study.output("cost").scalar(unit="USD", objective="minimize")

    frame = StudyService(study_api.ac).get("study").variables.frame()

    assert frame.columns == [
        "variable_id", "label", "role", "kind", "metadata", "created_at",
        "category", "objective", "unit",
    ]
    assert frame.select("label", "category", "objective", "unit").to_dicts() == [
        {
            "label": "configuration",
            "category": "simulation",
            "objective": None,
            "unit": None,
        },
        {
            "label": "cost",
            "category": None,
            "objective": "minimize",
            "unit": "USD",
        },
    ]
    assert frame["metadata"].to_list()[0]["label"] == "metadata label"
    assert frame["metadata"].to_list()[0]["created_at"] == "metadata timestamp"
    assert frame["metadata"].to_list()[0]["only_null"] is None
    assert frame["metadata"].to_list()[1]["objective"] == "minimize"
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
    assert reader.output["added later"].metadata == {"unit": "kW"}
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
    assert client.observe_experiment.call_count == 2
    assert client.observe_experiment.call_args_list[0].args == (first.run_id, variable.variable_id)
    assert client.observe_experiment.call_args_list[0].kwargs == {"value": value, "occurred_at": when.isoformat()}
    first.finish()
    with pytest.raises(RuntimeError):
        variable.record(value)
    second = study.start()
    variable.record(value)
    assert client.observe_experiment.call_args.args[0] == second.run_id
    assert first.run_id != second.run_id


def test_record_files_and_streams(study_api, tmp_path):
    study, client, ac = study_api.study, study_api.client, study_api.ac
    file = study.input("config").file(media_type="application/json")
    stream = study.output("volume").timeseries(observed="urn:tank", unit="M3")
    with pytest.raises(RuntimeError):
        stream.record([])
    run = study.start()
    path = tmp_path / "config.json"
    file.record(path)
    client.attach_experiment_file.assert_called_once_with(run.run_id, file.variable_id, path, media_type="application/json")
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ac.reference_uri.return_value = "urn:acquirium#recorded-volume"
    recorded = stream.record([(when.isoformat(), 10)])
    ac.insert_timeseries.assert_called_once_with(f"experiment/{run.run_id}", "volume", [(when, 10)], point_uri="urn:tank")
    assert ac.register_streams.call_args.args[0][0]["unit"] == "M3"
    assert ac.register_streams.call_args.args[0][0]["value_kind"] == "numeric"
    client.observe_experiment.assert_called_once_with(run.run_id, stream.variable_id, ref_uri=recorded.ref_uri, start=when.isoformat(), end=when.isoformat())
    expected_frame = object()
    client.timeseries_df.return_value = expected_frame
    assert recorded.dataframe(limit=5) is expected_frame
    client.timeseries_df.assert_called_once_with(recorded.ref_uri, limit=5)
    assert str(recorded) == recorded.ref_uri
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
    assert ac.register_streams.call_args.args[0][0]["point_uri"] is None
    assert ac.insert_timeseries.call_args.kwargs["point_uri"] is None
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

    ac.register_streams.assert_called_once()
    assert ac.insert_timeseries.call_count == 2
    assert recorded.ref_uri == appended.ref_uri
    observations = first.observations(power).all()
    assert [item.sequence for item in observations] == [1, 2]
    assert [item.range_start.hour for item in observations] == [0, 1]
    volume.record([("2026-01-01T00:00:00Z", 30)])
    volume.record([("2026-01-01T01:00:00Z", 40)])
    assert ac.register_streams.call_count == 2
    assert ac.insert_timeseries.call_count == 4

    first.finish()
    second = study.start()
    new_recording = power.record([("2026-01-01T00:00:00Z", 50)])
    power.record([("2026-01-01T01:00:00Z", 60)])
    assert ac.register_streams.call_count == 3
    assert ac.insert_timeseries.call_count == 6
    assert new_recording.ref_uri != recorded.ref_uri
    assert ac.register_streams.call_args.args[0][0]["source_id"] == f"experiment/{second.run_id}"
    assert len(second.observations(power).all()) == 2


@pytest.mark.parametrize("failure_phase", ["registration", "insertion"])
def test_timeseries_registration_cache_handles_retries(study_api, failure_phase):
    study, ac = study_api.study, study_api.ac
    power = study.output("power").timeseries(unit="kW")
    ac.reference_uri.side_effect = compute_ref_uri
    run = study.start()
    operation = ac.register_streams if failure_phase == "registration" else ac.insert_timeseries
    operation.side_effect = [RuntimeError("temporary failure"), None, None]
    rows = [("2026-01-01T00:00:00Z", 10)]

    with pytest.raises(RuntimeError, match="temporary failure"):
        power.record(rows)
    assert run.observations(power).all() == []
    power.record(rows)
    power.record(rows)

    assert ac.register_streams.call_count == (2 if failure_phase == "registration" else 1)
    assert ac.insert_timeseries.call_count == (2 if failure_phase == "registration" else 3)
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

    ac.register_streams.assert_called_once()
    assert recorded.ref_uri == original.ref_uri
    assert [item.range_start.hour for item in resumed.observations("power").all()] == [0, 1, 2]


def test_http_routes_accept_reference_uri_variable_ids_and_stream_annotations(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    store = DuckDBStore(tmp_path / "http.duckdb", recreate=True)
    try:
        ledger = ExperimentStore(store, tmp_path / "artifacts")
        module = import_module("acquirium.Server.app")
        monkeypatch.setattr(module.app.state, "manager", SimpleNamespace(experiments=ledger), raising=False)
        http = TestClient(module.app)
        study = http.post("/experiments/templates", json={"name": "http"}).json()
        variable = http.post(
            f"/experiments/templates/{study['template_id']}/variables",
            json={"label": "power (kW)", "role": "output", "kind": "scalar", "metadata": {}},
        ).json()
        assert variable["variable_id"].startswith("urn:acquirium#")
        run = http.post(f"/experiments/templates/{study['template_id']}/runs", json={"metadata": {}}).json()
        observed = http.post(
            f"/experiments/runs/{run['run_id']}/variables/{quote(variable['variable_id'], safe='')}/observations",
            json={"value": 42.0},
        )
        assert observed.status_code == 200
        other = ledger.define("other")
        foreign = ledger.declare(other["template_id"], "foreign", "output", "scalar", {})
        for invalid_id, expected_status in (("missing", 404), (foreign["variable_id"], 400)):
            path = f"/experiments/runs/{run['run_id']}/variables/{quote(invalid_id, safe='')}"
            assert http.post(f"{path}/observations", json={"value": 99}).status_code == expected_status
            assert http.post(f"{path}/file", content=b"invalid").status_code == expected_status
        assert len(ledger.observations(study["template_id"])) == 1
        assert list(ledger.artifact_dir.iterdir()) == []
        tagged = http.post(
            f"/experiments/runs/{run['run_id']}/stream-annotations",
            json={"ref_uri": "urn:plant:flow", "start": "2026-01-01T00:00:00Z", "metadata": {"phase": "trial"}},
        )
        assert tagged.status_code == 200
        listed = http.get("/experiments/stream-annotations", params={"ref_uri": "urn:plant:flow"})
        assert listed.status_code == 200
        assert listed.json()[0]["metadata"] == {"phase": "trial"}
        assert http.post(
            f"/experiments/stream-annotations/{tagged.json()['annotation_id']}/close",
            json={"end": "2026-01-02T00:00:00Z"},
        ).status_code == 200
    finally:
        store.close()


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
    assert client.observe_experiment.call_count == 2
    assert client.observe_experiment.call_args.args == (first.run_id, cost.variable_id)
    assert study._metadata[cost.variable_id] == {"unit": "USD"}
    first.finish()
    second = study.start()
    for label in ("cost", "new"):
        with pytest.raises(RuntimeError, match="inactive experiment"):
            first.output[label] = 14
    assert "new" not in study.output
    assert client.observe_experiment.call_count == 2
    second.output["cost"] = 15
    assert client.observe_experiment.call_args.args == (second.run_id, cost.variable_id)


@pytest.mark.parametrize("value,kind", [(1, "scalar"), (1.5, "scalar"), ("note", "text"), (True, "json"), (None, "json"), ([1, 2], "json"), ({"x": 1}, "json")])
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
    assert client.observe_experiment.call_count == 3
    assert client.observe_experiment.call_args.kwargs["value"] == value


def test_output_assignment_rejects_unsupported_values_and_conflicts(study_api, tmp_path):
    study, client = study_api.study, study_api.client
    study.input("config").json()
    run = study.start()
    for value in (object(), tmp_path, float("nan")):
        with pytest.raises(TypeError, match="JSON-compatible"):
            run.output["unsupported"] = value
    assert "unsupported" not in study.output
    with pytest.raises(ValueError, match="declared differently"):
        run.output["config"] = {}
    client.observe_experiment.assert_not_called()
