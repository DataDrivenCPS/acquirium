"""Experiment recreation contract for DuckDB and isolated Timescale schemas."""

import pytest

from acquirium.Experiments import ExperimentStore
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.timescale_store import TimescaleStore


@pytest.mark.parametrize("recreate", [False, True])
def test_backend_reopen_preserves_or_resets_experiment_ledger(materialization_store, tmp_path, recreate):
    store = materialization_store
    ledger = ExperimentStore(store, tmp_path / "artifacts")
    study = ledger.define("experiment reset contract")
    variable = ledger.declare(study["template_id"], "input", "input", "file", {})
    run = ledger.start(study["template_id"], {})
    ledger.attach_file(run["run_id"], variable["variable_id"], "input.txt", "text/plain", b"input")
    ledger.annotate_stream(run["run_id"], "urn:contract:stream", "2026-01-01T00:00:00Z")

    if isinstance(store, DuckDBStore):
        reopened = DuckDBStore(store.db_path, recreate=recreate)
    else:
        # The fixture's DSN scopes both connections to a disposable schema.
        reopened = TimescaleStore(dsn=store.dsn, recreate=recreate)
    try:
        ledger = ExperimentStore(reopened, tmp_path / "artifacts")
        if recreate:
            assert ledger.studies() == []
            assert ledger.stream_annotations() == []
            with reopened._own_conn() as conn:
                for table in ("experiment_templates", "experiment_variables", "experiment_runs", "experiment_observations", "experiment_artifacts"):
                    assert ledger._execute(conn, f"SELECT count(*) FROM {table}").fetchone()[0] == 0
            fresh = ledger.define("fresh study")
            variable = ledger.declare(fresh["template_id"], "output", "output", "scalar", {})
            run = ledger.start(fresh["template_id"], {})
            assert ledger.observe(run["run_id"], variable["variable_id"], value=42)["sequence"] == 1
        else:
            assert ledger.study(study["template_id"])["name"] == "experiment reset contract"
            assert len(ledger.variables(study["template_id"])) == 1
            observation = ledger.observations(study["template_id"])[0]
            assert observation["filename"] == "input.txt"
            assert observation["byte_length"] == 5
            assert ledger.run(run["run_id"])["status"] == "running"
            assert len(ledger.stream_annotations()) == 1
    finally:
        reopened.close()


def test_backend_deletes_runs_studies_and_clears_experiments(materialization_store, tmp_path):
    from datetime import datetime, timezone

    store = materialization_store
    ledger = ExperimentStore(store, tmp_path / "artifacts")
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    site = store.ensure_stream_ref("urn:contract:site", "site", "level", value_kind="numeric")
    store.upsert_rows(site, [(when, 1000.0)], value_kind="numeric")
    study = ledger.define("experiment delete contract")
    config = ledger.declare(study["template_id"], "config", "input", "file", {})
    level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    runs = []
    for index in range(2):
        run = ledger.start(study["template_id"], {"case": index})
        ledger.attach_file(run["run_id"], config["variable_id"], "shared.txt", "text/plain", b"shared")
        ref = store.ensure_stream_ref(None, f"experiment/{run['run_id']}", "level", value_kind="numeric")
        store.upsert_rows(ref, [(when, float(index))], value_kind="numeric")
        ledger.observe(run["run_id"], level["variable_id"], ref_uri=ref, interval=(when, when))
        ledger.annotate_stream(run["run_id"], site, when)
        runs.append(run)

    usage = ledger.usage()
    assert (usage["runs"], usage["observations"], usage["annotations"], usage["samples"], usage["files"], usage["file_bytes"]) == (2, 4, 2, 2, 1, 6)
    with pytest.raises(ValueError, match="finish or fail"):
        ledger.delete_runs([runs[0]["run_id"]])
    for run in runs:
        ledger.finish(run["run_id"], "succeeded")
    assert ledger.delete_runs([runs[0]["run_id"]]) == {"runs": 1, "observations": 2, "annotations": 1, "samples": 1, "files": 0}
    assert ledger.delete_study(study["template_id"])["files"] == 1
    assert ledger.clear()["studies"] == 0
    assert ledger.usage()["total_bytes"] == 0
    assert store.timeseries_info(site).row_count == 1


def test_batch_declarations_and_observations_are_ordered_and_atomic(materialization_store, tmp_path, monkeypatch):
    """Both HTTP batches use the same single-item checks on both backends."""
    from importlib import import_module
    from types import SimpleNamespace
    from fastapi.testclient import TestClient

    ledger = ExperimentStore(materialization_store, tmp_path / "batch-artifacts")
    module = import_module("acquirium.Server.app")
    monkeypatch.setattr(module.app.state, "manager", SimpleNamespace(experiments=ledger), raising=False)
    http = TestClient(module.app)
    study = ledger.define("batches")
    template_id = study["template_id"]
    declarations_url = f"/experiments/templates/{template_id}/variables/batch"
    declarations = [dict(label=label, role="output", kind=kind, metadata=metadata) for label, kind, metadata in (
        ("cost", "scalar", {}), ("flow", "timeseries", {"keys": ["pump"]}), ("config", "json", {}))]
    invalid = [*declarations, dict(label="bad", role="output", kind="unknown")]
    assert http.post(declarations_url, json=invalid).status_code == 400
    assert ledger.variables(template_id) == []
    execute = ledger._execute
    def fail_second_declaration(conn, statement, parameters=None):
        if statement.startswith("INSERT INTO experiment_variables") and parameters[2] == "flow":
            raise ValueError("injected declaration failure")
        return execute(conn, statement, parameters)
    with monkeypatch.context() as patch:
        patch.setattr(ledger, "_execute", fail_second_declaration)
        assert http.post(declarations_url, json=declarations).status_code == 400
    assert ledger.variables(template_id) == []
    response = http.post(declarations_url, json=declarations)
    assert response.status_code == 200, response.text
    variables = response.json()
    assert [item["label"] for item in variables] == ["cost", "flow", "config"]
    assert [item["created"] for item in http.post(declarations_url, json=declarations).json()] == [False] * 3
    conflict = [dict(label="fresh", role="input", kind="text"), {**declarations[0], "kind": "json"}]
    assert http.post(declarations_url, json=conflict).status_code == 400
    assert len(ledger.variables(template_id)) == 3
    run = ledger.start(template_id, {})
    url = f"/experiments/runs/{run['run_id']}/observations/batch"
    observations = [dict(variable_id=variables[0]["variable_id"], value=4),
                    dict(variable_id=variables[1]["variable_id"], ref_uri="urn:test:flow", key=["p1"]),
                    dict(variable_id=variables[2]["variable_id"], value={"case": 1})]
    # The last value is invalid: no prefix of the batch may be committed.
    assert http.post(url, json=[*observations, {**observations[0], "value": "bad"}]).status_code == 400
    assert ledger.observations(template_id) == []
    assert http.post(url, json=[{**observations[1], "key": None}]).status_code == 400
    assert http.post(url, json=[{**observations[0], "key": ["p1"]}]).status_code == 400
    assert http.post(url, json=[{**observations[1], "key": ["p1", "p2"]}]).status_code == 400
    assert http.post(url, json=[{**observations[1], "start": "2026-01-01T00:00:00Z"}]).status_code == 400
    assert http.post(url, json=[{**observations[0], "variable_id": "missing"}]).status_code == 404
    other = ledger.define("other batches")
    foreign = ledger.declare(other["template_id"], "foreign", "output", "scalar", {})
    assert http.post(url, json=[{**observations[0], "variable_id": foreign["variable_id"]}]).status_code == 400

    # Exercise transaction rollback after an INSERT, beyond prevalidation.
    execute = ledger._execute
    def fail_second_insert(conn, statement, parameters=None):
        if statement.startswith("INSERT INTO experiment_observations") and parameters[2] == 2:
            raise ValueError("injected failure")
        return execute(conn, statement, parameters)
    with monkeypatch.context() as patch:
        patch.setattr(ledger, "_execute", fail_second_insert)
        assert http.post(url, json=observations).status_code == 400
    assert ledger.observations(template_id) == []
    response = http.post(url, json=observations)
    assert response.status_code == 200, response.text
    assert [item["sequence"] for item in response.json()] == [1, 2, 3]
    assert [item["key"] for item in ledger.observations(template_id)] == [None, ["p1"], None]
    ledger.finish(run["run_id"], "succeeded")
    assert http.post(url, json=observations).status_code == 400
    assert len(ledger.observations(template_id)) == 3


def test_key_column_migrates_existing_ledger(materialization_store, tmp_path):
    store = materialization_store
    ledger = ExperimentStore(store, tmp_path / "migration-artifacts")
    study = ledger.define("old ledger")
    variable = ledger.declare(study["template_id"], "cost", "output", "scalar", {})
    run = ledger.start(study["template_id"], {})
    ledger.observe(run["run_id"], variable["variable_id"], value=2)
    with store._lock, store._write_conn() as conn:
        # DuckDB requires dropping secondary indexes before dropping columns.
        ledger._execute(conn, "DROP INDEX experiment_observations_variable_idx")
        ledger._execute(conn, "DROP INDEX experiment_observations_ref_idx")
        ledger._execute(conn, "ALTER TABLE experiment_observations DROP COLUMN key_json")
        ledger._execute(conn, "DELETE FROM schema_migrations WHERE component='experiments' AND version=1")
        ledger._execute(conn, "CREATE INDEX experiment_observations_variable_idx ON experiment_observations(variable_id, run_id, sequence)")
        ledger._execute(conn, "CREATE INDEX experiment_observations_ref_idx ON experiment_observations(ref_uri, range_start, range_end)")
    migrated = ExperimentStore(store, tmp_path / "migration-artifacts")
    observations = migrated.observations(study["template_id"])
    assert observations[0]["value"] == 2
    assert observations[0]["key"] is None
    flow = migrated.declare(study["template_id"], "flow", "output", "timeseries", {"keys": ["pump"]})
    refs = []
    for pump in ("p1", "p2"):
        ref = store.ensure_stream_ref(None, f"experiment/{run['run_id']}", f"flow[{pump}]", value_kind="numeric")
        refs.append(ref)
        migrated.observe(run["run_id"], flow["variable_id"], ref_uri=ref, key=(pump,))
    assert [item["key"] for item in migrated.observations(study["template_id"])][1:] == [["p1"], ["p2"]]
    migrated.finish(run["run_id"], "succeeded")
    migrated.delete_runs([run["run_id"]])
    with store._own_conn() as conn:
        assert migrated._execute(conn, "SELECT count(*) FROM streams WHERE ref_uri IN (?, ?)", refs).fetchone()[0] == 0


def test_table_artifact_batch_validation_and_atomicity(materialization_store, tmp_path):
    import base64
    import io
    import polars as pl

    ledger = ExperimentStore(materialization_store, tmp_path / "table-artifacts")
    study = ledger.define("table batch")
    scalar = ledger.declare(study["template_id"], "scalar", "output", "scalar", {})
    table = ledger.declare(study["template_id"], "table", "output", "table", {})
    run = ledger.start(study["template_id"], {})
    buffer = io.BytesIO()
    pl.DataFrame({"x": [1, 2]}).write_parquet(buffer)
    attachment = dict(variable_id=table["variable_id"], artifact=dict(filename="table.parquet",
        media_type="application/vnd.apache.parquet", data=base64.b64encode(buffer.getvalue()).decode()))
    with pytest.raises(ValueError):
        ledger.observe_batch(run["run_id"], [attachment, dict(variable_id=scalar["variable_id"], value="bad")])
    assert ledger.observations(study["template_id"]) == []
    assert list(ledger.artifact_dir.iterdir()) == []
    malformed = {**attachment, "artifact": {**attachment["artifact"], "data": base64.b64encode(b"not parquet").decode()}}
    with pytest.raises(ValueError, match="Parquet"):
        ledger.observe_batch(run["run_id"], [malformed])
    ledger.observe_batch(run["run_id"], [attachment, dict(variable_id=scalar["variable_id"], value=3)])
    rows = ledger.observations(study["template_id"])
    assert [row["sequence"] for row in rows] == [1, 2]
    saved = ledger.artifact(rows[0]["artifact_digest"])["data"]
    assert pl.read_parquet(io.BytesIO(saved))["x"].to_list() == [1, 2]
    with pytest.raises(ValueError, match="unknown artifact"):
        ledger.observe_batch(run["run_id"], [dict(variable_id=table["variable_id"], artifact_digest="missing")])


@pytest.mark.parametrize("sweep", [False, True])
@pytest.mark.parametrize("batch", [False, True])
def test_cleanup_preserves_artifact_reattached_after_delete_commit(materialization_store, tmp_path, monkeypatch, sweep, batch):
    import base64
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    import threading

    store = materialization_store
    ledger = ExperimentStore(store, tmp_path / "race-artifacts")
    study = ledger.define("artifact race")
    variable = ledger.declare(study["template_id"], "file", "input", "file", {})
    old = ledger.start(study["template_id"], {})
    data = b"shared bytes"
    saved = ledger.attach_file(old["run_id"], variable["variable_id"], "old.txt", "text/plain", data)
    ledger.finish(old["run_id"], "succeeded")
    # Separate ledger instances represent concurrent callers sharing a store.
    # Metadata transactions share delivery serialization; attaching bytes to an
    # already running experiment only needs the writer lock.
    other = ExperimentStore(store, ledger.artifact_dir)
    active = None
    if not sweep:
        active = other.start(study["template_id"], {})
    after_delete = ledger._after_delete
    started = threading.Event()
    future = None
    def interleave(result, **kwargs):
        nonlocal future
        def attach():
            nonlocal active, variable
            started.set()
            if sweep:
                new_study = other.define("new study")
                variable = other.declare(new_study["template_id"], "file", "input", "file", {})
                active = other.start(new_study["template_id"], {})
            if batch:
                other.observe_batch(active["run_id"], [{"variable_id": variable["variable_id"], "artifact": {
                    "filename": "new.txt", "media_type": "text/plain", "data": base64.b64encode(data).decode("ascii")}}])
            else:
                other.attach_file(active["run_id"], variable["variable_id"], "new.txt", "text/plain", data)
        # SQL deletion has committed; cleanup has not yet checked the file.
        future = executor.submit(attach)
        assert started.wait(5)
        if sweep:
            # Creating a new study waits for clear's metadata transaction.
            # It proceeds after clear returns, then recreates its shared bytes.
            with pytest.raises(TimeoutError):
                future.result(timeout=.1)
        else:
            future.result(timeout=10)
        return after_delete(result, **kwargs)
    monkeypatch.setattr(ledger, "_after_delete", interleave)
    with ThreadPoolExecutor(max_workers=1) as executor:
        removed = ledger.clear() if sweep else ledger.delete_runs([old["run_id"]])
        future.result(timeout=10)
    assert removed["files"] == int(sweep)
    assert other.artifact(saved["digest"])["data"] == data
    assert other.observations(active["template_id"], run_id=active["run_id"])[0]["artifact_digest"] == saved["digest"]
