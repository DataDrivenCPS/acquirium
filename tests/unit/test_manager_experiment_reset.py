from datetime import datetime, timezone
from importlib import import_module
from unittest.mock import Mock

import pytest

from acquirium.Server.manager import Manager


@pytest.mark.parametrize("recreate", [False, True])
def test_manager_recreation_resets_experiments_in_external_duckdb(tmp_path, monkeypatch, recreate):
    # Exercise the real storage/reset path without loading ontology indexes.
    module = import_module("acquirium.Server.manager")
    from tests.unit.test_experiment_metadata import MemoryGraphStore
    source = MemoryGraphStore()
    graph = Mock()
    for name in ("sparql_query", "sparql_update", "source_graph_uri", "clear_graphs", "graph_status", "metadata_documents", "metadata_triples", "flush_metadata"):
        setattr(graph, name, getattr(source, name))
    monkeypatch.setattr(module, "OxigraphGraphStore", Mock(return_value=graph))
    monkeypatch.setattr(Manager, "_build_embedding_indexes", lambda self: None)
    data_dir = tmp_path / "server-data"
    db_path = tmp_path / "external-storage" / "data.duckdb"
    manager = Manager(data_dir=data_dir, duckdb_path=db_path, exact_only=True)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    try:
        ledger = manager.experiments
        study = ledger.define("retained study")
        variable = ledger.declare(study["template_id"], "file", "input", "file", {})
        run = ledger.start(study["template_id"], {})
        artifact = ledger.attach_file(run["run_id"], variable["variable_id"], "input.txt", "text/plain", b"input")
        ledger.annotate_stream(run["run_id"], "urn:plant:flow", now)
        manager.timeseries_store.upsert_rows("urn:plant:flow", [(now, 42)], value_kind="numeric")
    finally:
        manager.close()

    reopened = Manager(data_dir=data_dir, duckdb_path=db_path, exact_only=True, recreate=recreate)
    try:
        ledger = reopened.experiments
        if recreate:
            assert ledger.studies() == []
            assert ledger.stream_annotations() == []
            with reopened.timeseries_store._own_conn() as conn:
                for table in ("experiment_templates", "experiment_variables", "experiment_runs", "experiment_observations", "experiment_artifacts", "experiment_artifact_chunks"):
                    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
            with pytest.raises(KeyError):
                ledger.artifact(artifact["digest"])
            assert reopened.timeseries_store.timeseries_info("urn:plant:flow").row_count == 0
            # The recreated ledger is ready for new runs.
            fresh = ledger.define("fresh study")
            result = ledger.declare(fresh["template_id"], "cost", "output", "scalar", {})
            new_run = ledger.start(fresh["template_id"], {})
            assert ledger.observe(new_run["run_id"], result["variable_id"], value=12.5)["sequence"] == 1
        else:
            assert ledger.study("retained study")["template_id"] == study["template_id"]
            assert len(ledger.observations(study["template_id"])) == 1
            assert len(ledger.stream_annotations()) == 1
            assert ledger.artifact(artifact["digest"])["data"] == b"input"
            assert reopened.timeseries_store.timeseries_info("urn:plant:flow").row_count == 1
    finally:
        reopened.close()
