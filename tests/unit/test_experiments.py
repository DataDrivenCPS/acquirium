from datetime import datetime, timezone
from hashlib import sha256
from importlib import import_module
from types import SimpleNamespace
from urllib.parse import quote
from unittest.mock import Mock
import warnings

import polars as pl
import pytest

from acquirium.Client.streams import Stream
from acquirium.Experiments import ExperimentStore, Study, StudyService, timestamp, _stream_name
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.models import compute_ref_uri


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


T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _recorded_stream(store, ledger, run, variable, rows, name="level"):
    """Store samples under the run's source, as a time-series record() does."""
    ref = store.ensure_stream_ref(None, f"experiment/{run['run_id']}", name, value_kind="numeric")
    store.upsert_rows(ref, rows, value_kind="numeric")
    ledger.observe(run["run_id"], variable["variable_id"], ref_uri=ref, interval=(rows[0][0], rows[-1][0]))
    return ref


def _stream_frame(ref, rows):
    """What the time-series client returns for one stream."""
    return pl.DataFrame({"ts": [when for when, _ in rows], "value": [value for _, value in rows], "uri": [ref] * len(rows)})


def _samples(store, ref):
    with store._own_conn() as conn:
        return conn.execute("SELECT count(*) FROM timeseries_streams WHERE ref_uri=?", [ref]).fetchone()[0]


def test_delete_runs_removes_what_they_recorded_and_keeps_shared_files(tmp_path):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        ledger = ExperimentStore(store, tmp_path / "artifacts")
        study = ledger.define("tank")
        config = ledger.declare(study["template_id"], "config", "input", "file", {})
        level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
        site = store.ensure_stream_ref("urn:site#level", "site", "level", value_kind="numeric")
        store.upsert_rows(site, [(T0, 1000.0)], value_kind="numeric")
        runs, refs = [], []
        for index in range(2):
            run = ledger.start(study["template_id"], {"case": index})
            ledger.attach_file(run["run_id"], config["variable_id"], "shared.json", None, b"{}")
            refs.append(_recorded_stream(store, ledger, run, level, [(T0, float(index))]))
            ledger.annotate_stream(run["run_id"], site, T0)
            runs.append(run)
        ledger.attach_file(runs[0]["run_id"], config["variable_id"], "own.json", None, b"[]")
        with pytest.raises(ValueError, match="finish or fail"):
            ledger.delete_runs([runs[0]["run_id"]])
        assert ledger.usage(template_id=study["template_id"])["observations"] == 5
        for run in runs:
            ledger.finish(run["run_id"], "succeeded")

        removed = ledger.delete_runs([runs[0]["run_id"]])
        assert removed == {"runs": 1, "observations": 3, "annotations": 1, "samples": 1, "files": 1}
        assert [run["run_id"] for run in ledger.runs(study["template_id"])] == [runs[1]["run_id"]]
        assert ledger.stream_annotations(run_id=runs[0]["run_id"]) == []
        assert (_samples(store, refs[0]), _samples(store, refs[1]), _samples(store, site)) == (0, 1, 1)
        assert [path.name for path in ledger.artifact_dir.iterdir()] == [sha256(b"{}").hexdigest()]
        with store._own_conn() as conn:
            # The reset marker tells materializers to rebuild what they derived.
            assert conn.execute("SELECT count(*) FROM stream_resets WHERE ref_uri=?", [refs[0]]).fetchone()[0] == 1
            assert conn.execute("SELECT count(*) FROM streams WHERE ref_uri=?", [refs[0]]).fetchone()[0] == 0
        ledger.delete_runs([runs[1]["run_id"]])
        assert list(ledger.artifact_dir.iterdir()) == []
        with pytest.raises(KeyError):
            ledger.delete_runs([runs[1]["run_id"]])
    finally:
        store.close()


def test_delete_keeps_streams_that_other_runs_reference(tmp_path):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        ledger = ExperimentStore(store, tmp_path / "artifacts")
        simulation, analysis = ledger.define("simulation"), ledger.define("analysis")
        level = ledger.declare(simulation["template_id"], "level", "output", "timeseries", {})
        used = ledger.declare(analysis["template_id"], "level", "input", "timeseries", {})
        source = ledger.start(simulation["template_id"], {})
        ref = _recorded_stream(store, ledger, source, level, [(T0, 1.0)])
        reader = ledger.start(analysis["template_id"], {})
        ledger.observe(reader["run_id"], used["variable_id"], ref_uri=ref)
        for run in (source, reader):
            ledger.finish(run["run_id"], "succeeded")
        for delete in (lambda: ledger.delete_runs([source["run_id"]]),
                       lambda: ledger.delete_study(simulation["template_id"])):
            with pytest.raises(ValueError, match=reader["run_id"]):
                delete()
        assert _samples(store, ref) == 1
        # Deleting the reader with the run it read from is allowed.
        assert ledger.delete_runs([source["run_id"], reader["run_id"]])["runs"] == 2
        assert _samples(store, ref) == 0
    finally:
        store.close()


def test_delete_study_clear_and_usage_cover_experiments_only(tmp_path):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        ledger = ExperimentStore(store, tmp_path / "artifacts", resolve=lambda text, kind: "urn:unit:kW")
        assert ledger._resolve("kW", "unit") == "urn:unit:kW"
        site = store.ensure_stream_ref("urn:site#level", "site", "level", value_kind="numeric")
        store.upsert_rows(site, [(T0, 1000.0)], value_kind="numeric")
        studies, runs = [], []
        for name in ("first", "second"):
            study = ledger.define(name)
            config = ledger.declare(study["template_id"], "config", "input", "file", {})
            level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
            run = ledger.start(study["template_id"], {})
            ledger.attach_file(run["run_id"], config["variable_id"], "shared.json", None, b"shared")
            _recorded_stream(store, ledger, run, level, [(T0, 1.0), (T0.replace(minute=1), 2.0)])
            studies.append(study); runs.append(run)

        everything = ledger.usage()
        assert {key: everything[key] for key in ("studies", "variables", "runs", "observations", "samples", "files", "file_bytes")} == {
            "studies": 2, "variables": 4, "runs": 2, "observations": 4, "samples": 4, "files": 1, "file_bytes": 6,
        }
        assert everything["sample_bytes"] == 4 * 16
        assert everything["total_bytes"] == everything["sample_bytes"] + everything["file_bytes"] + everything["ledger_bytes"]
        first = ledger.usage(template_id=studies[0]["template_id"])
        assert (first["studies"], first["runs"], first["files"], first["file_bytes"]) == (1, 1, 1, 6)
        selected = ledger.usage(run_ids=[runs[0]["run_id"]])
        assert (selected["studies"], selected["variables"], selected["runs"], selected["samples"]) == (0, 0, 1, 2)
        assert ledger.usage(run_ids=[])["runs"] == 0
        with pytest.raises(KeyError):
            ledger.usage(template_id="missing")

        with pytest.raises(ValueError, match=runs[0]["run_id"]):
            ledger.delete_study(studies[0]["template_id"])
        ledger.finish(runs[0]["run_id"], "succeeded")
        removed = ledger.delete_study(studies[0]["template_id"])
        assert removed == {"studies": 1, "variables": 2, "runs": 1, "observations": 2, "annotations": 0, "samples": 2, "files": 0}
        with pytest.raises(KeyError):
            ledger.study(studies[0]["template_id"])
        assert len(list(ledger.artifact_dir.iterdir())) == 1  # the second study still uses it

        # A stream left by a deletion that failed after its ledger commit.
        orphan = store.ensure_stream_ref(None, "experiment/gone", "level", value_kind="numeric")
        store.upsert_rows(orphan, [(T0, 3.0)], value_kind="numeric")
        with pytest.raises(ValueError, match="finish or fail"):
            ledger.clear()
        ledger.finish(runs[1]["run_id"], "failed", {"message": "abandoned"})
        assert ledger.clear() == {"studies": 1, "variables": 2, "runs": 1, "observations": 2, "annotations": 0, "samples": 3, "files": 1}
        assert ledger.studies() == []
        assert ledger.usage()["total_bytes"] == 0
        assert list(ledger.artifact_dir.iterdir()) == []
        assert (_samples(store, orphan), _samples(store, site)) == (0, 1)
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] == 0
        assert ledger._resolutions == {}
        fresh = ledger.define("first")
        assert ledger.variables(fresh["template_id"]) == []
    finally:
        store.close()


def test_http_delete_routes_report_conflicts_and_require_confirmation(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    store = DuckDBStore(tmp_path / "http.duckdb", recreate=True)
    try:
        ledger = ExperimentStore(store, tmp_path / "artifacts")
        module = import_module("acquirium.Server.app")
        monkeypatch.setattr(module.app.state, "manager", SimpleNamespace(experiments=ledger), raising=False)
        http = TestClient(module.app)
        study = ledger.define("http")
        run = ledger.start(study["template_id"], {})
        assert http.post("/experiments/runs/delete", json={"run_ids": ["missing"]}).status_code == 404
        assert http.post("/experiments/runs/delete", json={"run_ids": [run["run_id"]]}).status_code == 409
        assert http.post(f"/experiments/templates/{study['template_id']}/delete").status_code == 409
        assert http.post("/experiments/clear", json={}).status_code == 400
        assert http.post("/experiments/clear", json={"confirm": True}).status_code == 409
        assert http.post("/experiments/usage", json={}).json()["runs"] == 1
        ledger.finish(run["run_id"], "succeeded")
        assert http.post("/experiments/runs/delete", json={"run_ids": [run["run_id"]]}).json()["runs"] == 1
        assert http.post(f"/experiments/templates/{study['template_id']}/delete").json()["studies"] == 1
        assert http.post("/experiments/templates/missing/delete").status_code == 404
        assert http.post("/experiments/clear", json={"confirm": True}).json()["studies"] == 0
    finally:
        store.close()


def test_ledger_enforces_what_each_kind_stores(tmp_path):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        ledger = ExperimentStore(store, tmp_path / "artifacts")
        study = ledger.define("kinds")
        declare = lambda label, kind: ledger.declare(study["template_id"], label, "output", kind, {})["variable_id"]
        with pytest.raises(ValueError, match="kind must be one of"):
            declare("bogus", "object")
        scalar, text, data, file, table, series = (declare(kind, kind) for kind in ("scalar", "text", "json", "file", "table", "timeseries"))
        run = ledger.start(study["template_id"], {})["run_id"]
        rejected = [
            (scalar, {"value": "abc"}), (scalar, {"value": None}), (scalar, {"value": float("nan")}),
            (scalar, {"value": [1]}), (text, {"value": 5}), (data, {"value": {"x": float("inf")}}),
            (scalar, {"value": 1, "ref_uri": "urn:ref"}), (series, {"value": 1}), (file, {"value": "x"}),
        ]
        for variable, body in rejected:
            with pytest.raises(ValueError):
                ledger.observe(run, variable, **body)
        for variable, value in ((scalar, 2), (scalar, 2.5), (scalar, True), (text, "ok"), (data, None), (data, [1, {"a": "b"}])):
            ledger.observe(run, variable, value=value)
        with pytest.raises(ValueError, match="only file and table"):
            ledger.attach_file(run, scalar, "x.txt", None, b"x")
        with pytest.raises(ValueError, match="Parquet"):
            ledger.attach_file(run, table, "x.parquet", None, b"not parquet")
        assert len(ledger.observations(study["template_id"])) == 6
        assert list(ledger.artifact_dir.iterdir()) == []
    finally:
        store.close()


def test_http_artifacts_download_with_their_saved_names(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    store = DuckDBStore(tmp_path / "http.duckdb", recreate=True)
    try:
        ledger = ExperimentStore(store, tmp_path / "artifacts")
        module = import_module("acquirium.Server.app")
        monkeypatch.setattr(module.app.state, "manager", SimpleNamespace(experiments=ledger), raising=False)
        http = TestClient(module.app)
        study = ledger.define("http")
        report = ledger.declare(study["template_id"], "report", "output", "file", {})
        cost = ledger.declare(study["template_id"], "cost", "output", "scalar", {})
        run = ledger.start(study["template_id"], {})
        path = f"/experiments/runs/{run['run_id']}/variables/{quote(report['variable_id'], safe='')}/file"
        saved = http.post(path, content=b"report", headers={"X-Filename": quote("débit.txt", safe=""), "Content-Type": "text/plain"}).json()
        download = http.get(f"/experiments/artifacts/{saved['digest']}")
        assert (download.status_code, download.content) == (200, b"report")
        assert download.headers["content-type"].startswith("text/plain")
        assert ledger.observations(study["template_id"])[0]["filename"] == "débit.txt"
        for digest in ("0" * 64, "..%2F..%2Fdata.duckdb", "not-a-digest"):
            assert http.get(f"/experiments/artifacts/{digest}").status_code == 404
        scalar_path = f"/experiments/runs/{run['run_id']}/variables/{quote(cost['variable_id'], safe='')}"
        assert http.post(f"{scalar_path}/file", content=b"x").status_code == 400
        assert http.post(f"{scalar_path}/observations", json={"value": "abc"}).status_code == 400
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
    client.delete_experiments.side_effect = ledger.delete_runs
    client.delete_experiment_study.side_effect = ledger.delete_study
    client.clear_experiments.side_effect = lambda *, confirm: ledger.clear()
    client.experiment_usage.side_effect = ledger.usage
    client.attach_experiment_bytes.side_effect = (
        lambda run_id, variable_id, data, *, filename, media_type=None:
        ledger.attach_file(run_id, variable_id, filename, media_type, data))
    client.experiment_artifact.side_effect = lambda digest: ledger.artifact(digest)["data"]
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
    assert repr(cost) == "<ExperimentVariable output 'cost' scalar {'unit': 'USD'}>"
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
    power.record(Stream("urn:acquirium:power-1").between(start, end))
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
            "metadata": {"objective": "minimize", "unit": "USD"},
        },
        {
            "label": "electrical power",
            "role": "output",
            "kind": "timeseries",
            "metadata": {"observed": "urn:plant:tank", "unit": "kW"},
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
        end=end.isoformat(),
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
    assert "only_null" not in frame["metadata"].to_list()[0]
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
    ac.insert_timeseries.assert_called_once_with(f"experiment/{run.run_id}", _stream_name(stream.variable_id, ()), [(when, 10)])
    registration = ac.register_streams.call_args.args[0][0]
    assert registration["unit"] == "M3"
    assert registration["value_kind"] == "numeric"
    # The run gets its own point; the plant property is an attribute of it.
    assert "point_uri" not in registration
    assert registration["metadata"] == {
        "study_id": study.study_id, "run_id": run.run_id, "variable_id": stream.variable_id, "observed": "urn:tank",
    }
    client.observe_experiment.assert_called_once_with(run.run_id, stream.variable_id, ref_uri=recorded.ref_uri, start=when.isoformat(), end=when.isoformat())
    client.timeseries_df.return_value = _stream_frame(recorded.ref_uri, [(when, 10.0)])
    assert recorded.dataframe(limit=5).rows() == [(when, 10.0)]
    client.timeseries_df.assert_called_once_with(recorded.ref_uri, start=None, end=None, limit=5)
    assert str(recorded) == recorded.ref_uri == "urn:acquirium#recorded-volume"
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
    registration = ac.register_streams.call_args.args[0][0]
    assert "point_uri" not in registration
    assert registration["metadata"] == {
        "study_id": study.study_id, "run_id": run.run_id, "variable_id": simulated_power.variable_id,
    }
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
        selected_runs = http.post(
            f"/experiments/templates/{study['template_id']}/runs/query",
            json={"run_ids": [run["run_id"]]},
        )
        assert selected_runs.status_code == 200
        assert [item["run_id"] for item in selected_runs.json()] == [run["run_id"]]
        selected_observations = http.post(
            f"/experiments/templates/{study['template_id']}/observations/query",
            json={"run_ids": [run["run_id"]], "variable_id": variable["variable_id"]},
        )
        assert selected_observations.status_code == 200
        assert selected_observations.json()[0]["value"] == 42.0
        assert http.post(
            f"/experiments/templates/{study['template_id']}/runs/query",
            json={"run_ids": []},
        ).json() == []
        assert http.post(
            f"/experiments/templates/{study['template_id']}/observations/query",
            json={"run_ids": ["missing"]},
        ).json() == []
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
    calls = client.observe_experiment.call_count
    for variable, value, error in ((flow, "fast", TypeError), (flow, np.nan, ValueError), (note, 5, TypeError),
                                   (settings, {"x": float("nan")}, ValueError), (settings, {1: "a"}, TypeError)):
        with pytest.raises(error):
            variable.record(value)
    assert client.observe_experiment.call_count == calls  # rejected before sending


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
    client.timeseries_df.assert_called_once_with(by_name.ref_uri, start=T0.isoformat(), end=T0.replace(day=2).isoformat())
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
    client.observe_experiment.assert_called_with(
        run.run_id, biogas.variable_id, ref_uri="urn:site:biogas",
        start=T0.isoformat(), end=T0.replace(hour=2).isoformat())
    ac.insert_timeseries.assert_not_called()

    # Rows read from the range are stored with the run, under its own source.
    client.timeseries_df.return_value = _stream_frame("urn:site:biogas", [(T0, 1.0), (T0.replace(hour=1), 2.0)])
    ac.reference_uri.return_value = "urn:acquirium#copied"
    copied = biogas.record(window.dataframe())
    ac.insert_timeseries.assert_called_once_with(f"experiment/{run.run_id}", _stream_name(biogas.variable_id, ()), [(T0, 1.0), (T0.replace(hour=1), 2.0)])
    assert copied == Stream("urn:acquirium#copied")

    # Stream query frames use ts or time, and value or value_numeric.
    narrow = pl.DataFrame({"data_alias": ["flow"] * 2, "point_id": ["p"] * 2,
                           "time": [T0, T0.replace(hour=1)], "value_numeric": [3.0, 4.0], "value_text": [None, None]})
    trajectory.record(narrow)
    assert ac.insert_timeseries.call_args.args[2] == [(T0, 3.0), (T0.replace(hour=1), 4.0)]
    # So do wide query frames with one stream column.
    trajectory.record(pl.DataFrame({"time": [T0], "biogas": [5.0]}))
    assert ac.insert_timeseries.call_args.args[2] == [(T0, 5.0)]
    for rows, message in ((narrow.with_columns(point_id=pl.Series(["p", "q"])), "several streams"),
                          (pl.DataFrame({"time": [T0], "value_numeric": [None]}, schema_overrides={"value_numeric": pl.Float64}), "numeric value"),
                          (pl.DataFrame({"time": [T0], "value": ["high"]}), "numeric value"),
                          (pl.DataFrame({"time": [T0], "a": [1.0], "b": [2.0]}), "time \\(or ts\\)"),
                          (pl.DataFrame({"when": [T0], "value": [1.0]}), "time \\(or ts\\)")):
        with pytest.raises(ValueError, match=message):
            trajectory.record(rows)
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
    assert client.observe_experiment.call_args.kwargs["ref_uri"] == "urn:acquirium#copied"
    second.finish()


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
