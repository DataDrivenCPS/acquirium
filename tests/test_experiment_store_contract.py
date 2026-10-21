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
