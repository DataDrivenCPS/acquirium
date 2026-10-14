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
