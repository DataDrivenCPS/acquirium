from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
import warnings

import pytest

from acquirium.Experiments import ExperimentStore, Study
from acquirium.Storage.duckdb_store import DuckDBStore


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


@pytest.fixture
def study_api(tmp_path):
    store = DuckDBStore(tmp_path / "api.duckdb", recreate=True)
    ledger = ExperimentStore(store, tmp_path / "artifacts")
    client = Mock()
    client.declare_experiment_variable.side_effect = ledger.declare
    client.start_experiment.side_effect = ledger.start
    client.finish_experiment.side_effect = lambda run_id: ledger.finish(run_id, "succeeded")
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
    stream.record([(when.isoformat(), 10)])
    ac.insert_timeseries.assert_called_once_with(f"experiment/{run.run_id}", "volume", [(when, 10)], point_uri="urn:tank")
    assert ac.register_streams.call_args.args[0][0]["unit"] == "M3"
    assert ac.register_streams.call_args.args[0][0]["value_kind"] == "numeric"
    client.observe_experiment.assert_called_once_with(run.run_id, stream.variable_id, ref_uri=str(ac.reference_uri.return_value), start=when.isoformat(), end=when.isoformat())
    for variable in (file, stream):
        with pytest.raises(TypeError, match="occurred_at"):
            variable.record(None, occurred_at=when)
    run.finish()
    with pytest.raises(RuntimeError):
        file.record(path)


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
