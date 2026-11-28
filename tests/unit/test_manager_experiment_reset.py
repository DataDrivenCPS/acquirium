from contextlib import closing
from datetime import datetime, timezone

import pytest

from acquirium.Server.manager import Manager
from acquirium.Storage import graph_store


def test_manager_refuses_legacy_duckdb_before_creating_new_store(tmp_path):
    legacy = tmp_path / "timeseries.duckdb"
    legacy.write_bytes(b"legacy data")
    with pytest.raises(ValueError, match="Legacy Acquirium storage"):
        Manager(data_dir=tmp_path, exact_only=True)
    assert list(tmp_path.iterdir()) == [legacy]
    assert legacy.read_bytes() == b"legacy data"


def test_manager_refuses_legacy_location_from_active_config(tmp_path, monkeypatch):
    config = tmp_path / "acquirium.toml"
    config.write_text('[server]\ntimeseries_backend = "timescale"\n')
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(config))
    with pytest.raises(ValueError, match="Legacy DuckDB or TimescaleDB storage settings"):
        Manager.from_env()


def test_external_experiment_ledger_persists_until_manager_recreates_it(tmp_path, monkeypatch):
    # The external ledger and graph are real. This reset requires no ontology.
    monkeypatch.setattr(graph_store, "BUNDLED_FILES", ())
    monkeypatch.setattr(Manager, "_build_embedding_indexes", lambda self: None)
    options = {
        "data_dir": tmp_path / "server-data",
        "experiments_path": tmp_path / "external-storage" / "experiments.duckdb",
        "exact_only": True,
    }
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with closing(Manager(**options)) as manager:
        ledger = manager.experiments
        study = ledger.define("retained study")
        variable = ledger.declare(study["template_id"], "file", "input", "file", {})
        run = ledger.start(study["template_id"], {})
        artifact = ledger.attach_file(run["run_id"], variable["variable_id"], "input.txt", "text/plain", b"input")
        ledger.annotate_stream(run["run_id"], "urn:plant:flow", now)

    # A normal reopen keeps records even though their ledger is outside data_dir.
    with closing(Manager(**options)) as reopened:
        ledger = reopened.experiments
        assert ledger.study("retained study")["template_id"] == study["template_id"]
        assert len(ledger.observations(study["template_id"])) == 1
        assert len(ledger.stream_annotations()) == 1
        assert ledger.artifact(artifact["digest"])["data"] == b"input"

    # An explicit recreate clears that external ledger and leaves it ready for new runs.
    with closing(Manager(**options, recreate=True)) as cleared:
        ledger = cleared.experiments
        assert ledger.studies() == []
        assert ledger.stream_annotations() == []
        with cleared.ledger._own_conn() as conn:
            for table in ("experiment_templates", "experiment_variables", "experiment_runs",
                          "experiment_observations", "experiment_artifacts", "experiment_artifact_chunks"):
                assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
        with pytest.raises(KeyError):
            ledger.artifact(artifact["digest"])
        fresh = ledger.define("fresh study")
        result = ledger.declare(fresh["template_id"], "cost", "output", "scalar", {})
        new_run = ledger.start(fresh["template_id"], {})
        assert ledger.observe(new_run["run_id"], result["variable_id"], value=12.5)["sequence"] == 1
