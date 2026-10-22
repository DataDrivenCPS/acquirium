"""Durable graph handoff and SQL recording transactions on both backends."""
import pytest
from rdflib import RDF, URIRef

from acquirium.Experiments import ExperimentStore
from acquirium.ExperimentMetadata import EXPERIMENT, run_uri
from acquirium.internals.internals_namespaces import HAS_UNIT
from tests.unit.test_experiment_metadata import MemoryGraphStore


def pending(store):
    with store._own_conn() as conn:
        return conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0]


def test_restart_replays_resolved_documents_without_sql_snapshots(materialization_store, tmp_path, monkeypatch):
    store, graph = materialization_store, MemoryGraphStore()
    ledger = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph, resolve=lambda *args: "urn:unit:PSI")
    study = ledger.define("restart")
    variable = ledger.declare(study["template_id"], "pressure", "output", "scalar", {"unit": "psi", "metadata": {"any.key": [None, {}, []]}})
    run = ledger.start(study["template_id"], {"tags": ["A", None, "B"]})
    with monkeypatch.context() as patch:
        patch.setattr(graph, "sparql_update", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("offline")))
        with pytest.raises(OSError, match="offline"):
            ledger.sync_metadata_graph()
    assert pending(store) == 3
    restarted = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph, resolve=lambda *args: None)
    assert restarted.variables(study["template_id"])[0]["metadata"] == {"unit": "psi", "metadata": {"any.key": [None, {}, []]}}
    assert restarted.run(run["run_id"])["metadata"] == {"tags": ["A", None, "B"]}
    assert (URIRef(variable["variable_id"]), HAS_UNIT, URIRef("urn:unit:PSI")) in graph.dataset
    assert pending(store) == 0
    with store._own_conn() as conn:
        columns = conn.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name IN ('experiment_runs','experiment_variables')").fetchall()
        assert all(column != "metadata_json" for _, column in columns)
    restarted.observe(run["run_id"], variable["variable_id"], value=12)
    assert restarted.observations(study["template_id"])[0]["value"] == 12


def test_graph_commit_before_failed_acknowledgment_is_replayable(materialization_store, tmp_path, monkeypatch):
    store, graph = materialization_store, MemoryGraphStore()
    ledger = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    study = ledger.define("ack")
    ledger.sync_metadata_graph()
    run = ledger.start(study["template_id"], {"scenario": "baseline"})
    ledger.finish(run["run_id"], "succeeded")
    execute = ledger._execute
    def fail_ack(conn, statement, parameters=None):
        if statement.startswith("DELETE FROM experiment_metadata_outbox"):
            raise OSError("ack failed")
        return execute(conn, statement, parameters)
    with monkeypatch.context() as patch:
        patch.setattr(ledger, "_execute", fail_ack)
        with pytest.raises(OSError, match="ack failed"):
            ledger.sync_metadata_graph()
    assert (URIRef(run_uri(run["run_id"])), RDF.type, EXPERIMENT.Run) in graph.dataset
    assert pending(store) == 2
    restarted = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    assert restarted.run(run["run_id"])["status"] == "succeeded"
    assert restarted.run(run["run_id"])["metadata"] == {"scenario": "baseline"}
    assert pending(store) == 0


def test_outbox_failure_rolls_back_lifecycle_write(materialization_store, tmp_path, monkeypatch):
    store = materialization_store
    ledger = ExperimentStore(store, tmp_path / "artifacts")
    study = ledger.define("rollback")
    ledger.sync_metadata_graph()
    queue = ledger._queue_metadata
    def fail(conn, record):
        queue(conn, record)
        raise OSError("transaction failed")
    with monkeypatch.context() as patch:
        patch.setattr(ledger, "_queue_metadata", fail)
        with pytest.raises(OSError, match="transaction failed"):
            ledger.start(study["template_id"], {})
    assert ledger.runs(study["template_id"]) == []
    assert pending(store) == 0


def test_committed_deletion_is_retried_after_restart(materialization_store, tmp_path, monkeypatch):
    store, graph = materialization_store, MemoryGraphStore()
    ledger = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    study = ledger.define("delete")
    run = ledger.start(study["template_id"], {"scenario": "baseline"})
    ledger.finish(run["run_id"], "succeeded")
    ledger.sync_metadata_graph()
    with monkeypatch.context() as patch:
        patch.setattr(graph, "sparql_update", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("offline")))
        with pytest.raises(OSError, match="offline"):
            ledger.delete_runs([run["run_id"]])
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_runs").fetchone()[0] == 0
    assert pending(store) > 0
    restarted = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    restarted.sync_metadata_graph()
    assert not list(graph.dataset.triples((URIRef(run_uri(run["run_id"])), None, None)))
    assert pending(store) == 0


def seed_legacy_ledger(store, *, invalid=False):
    import json
    from datetime import datetime
    from acquirium.Storage.migrations import migrate, EXPERIMENT_MIGRATIONS
    from acquirium.Storage.metadata_outbox import execute
    from acquirium.Storage.duckdb_store import DuckDBStore
    stamp = datetime(2026, 1, 1)
    metadata = {"unit": ["psi", "PSI"], "tags": ["A", None, "B"], "type": "urn:example:Result",
                "_adapter_format": 1, "adapter_metadata": json.dumps({"any.key": [None, {}, []]}),
                "metadata": {"old_projection": "discard"}}
    if invalid:
        metadata["status"] = "reserved"
    with store._lock, store._write_conn() as conn:
        statements = [
            "CREATE TABLE experiment_templates (template_id VARCHAR PRIMARY KEY, name VARCHAR UNIQUE NOT NULL, created_at TIMESTAMP NOT NULL)",
            "CREATE TABLE experiment_variables (variable_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, label VARCHAR NOT NULL, role VARCHAR NOT NULL, kind VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, UNIQUE(template_id,label))",
            "CREATE TABLE experiment_runs (run_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, status VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, error_json VARCHAR)",
            "CREATE TABLE experiment_observations (run_id VARCHAR NOT NULL, variable_id VARCHAR, sequence BIGINT NOT NULL, recorded_at TIMESTAMP NOT NULL, occurred_at TIMESTAMP, value_json VARCHAR, artifact_digest VARCHAR, ref_uri VARCHAR, range_start TIMESTAMP, range_end TIMESTAMP, PRIMARY KEY(run_id,sequence))",
            "CREATE TABLE experiment_metadata_resolutions (text VARCHAR NOT NULL, kind VARCHAR NOT NULL, uri VARCHAR NOT NULL, PRIMARY KEY(text,kind))",
        ]
        for statement in statements:
            execute(conn, statement)
        migrate(conn, backend="duckdb" if isinstance(store, DuckDBStore) else "postgres", component="experiments", migrations=EXPERIMENT_MIGRATIONS[:1])
        execute(conn, "INSERT INTO experiment_templates VALUES ('legacy-study','legacy',?)", [stamp])
        execute(conn, "INSERT INTO experiment_variables VALUES ('legacy-variable','legacy-study','pressure','output','scalar',?,?)", [json.dumps(metadata), stamp])
        execute(conn, "INSERT INTO experiment_runs VALUES ('legacy-run','legacy-study','succeeded',?,?,?,NULL)", [json.dumps({"tags": ["A", None, "B"]}), stamp, stamp])
        execute(conn, "INSERT INTO experiment_observations (run_id,variable_id,sequence,recorded_at,value_json) VALUES ('legacy-run','legacy-variable',1,?,'12.5')", [stamp])
        execute(conn, "INSERT INTO experiment_observations (run_id,sequence,recorded_at,value_json,ref_uri,range_start) VALUES ('legacy-run',2,?,?,'urn:legacy:stream',?)", [stamp, json.dumps({"treatment": "old", "tags": [None, "A"]}), stamp])
        for text in ("psi", "PSI"):
            execute(conn, "INSERT INTO experiment_metadata_resolutions VALUES (?, 'unit', 'urn:unit:PSI')", [text])
    return metadata


def test_upgrade_moves_snapshots_to_resolved_outbox_and_preserves_recordings(materialization_store, tmp_path):
    store, graph = materialization_store, MemoryGraphStore()
    seed_legacy_ledger(store)
    ledger = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph, resolve=lambda *args: None)
    assert pending(store) == 4
    values = ledger.variables("legacy-study")[0]["metadata"]
    assert values == {"unit": ["psi", "PSI"], "tags": ["A", None, "B"], "type": "urn:example:Result",
                      "_adapter_format": 2, "metadata": {"any.key": [None, {}, []]}}
    assert ledger.run("legacy-run")["metadata"] == {"tags": ["A", None, "B"]}
    assert ledger.stream_annotations(run_id="legacy-run")[0]["metadata"] == {"treatment": "old", "tags": [None, "A"]}
    assert ledger.observations("legacy-study")[0]["value"] == 12.5
    assert pending(store) == 0
    with store._own_conn() as conn:
        columns = conn.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=current_schema()").fetchall()
        assert not any(table in {"experiment_runs", "experiment_variables"} and column == "metadata_json" for table, column in columns)
        assert not any(table == "experiment_metadata_resolutions" for table, _ in columns)
        assert conn.execute("SELECT value_json FROM experiment_observations WHERE variable_id IS NULL").fetchone()[0] is None
    restarted = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    assert restarted.variables("legacy-study")[0]["metadata"] == values
    assert pending(store) == 0


def test_failed_upgrade_preserves_original_sql_metadata(materialization_store, tmp_path):
    store = materialization_store
    original = seed_legacy_ledger(store, invalid=True)
    with pytest.raises(ValueError, match="reserved"):
        ExperimentStore(store, tmp_path / "artifacts", graph_store=MemoryGraphStore())
    import json
    with store._own_conn() as conn:
        assert json.loads(conn.execute("SELECT metadata_json FROM experiment_variables").fetchone()[0]) == original
        assert conn.execute("SELECT count(*) FROM schema_migrations WHERE component='experiments' AND version=2").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM experiment_metadata_resolutions").fetchone()[0] == 2


def test_delivery_is_serialized_across_ledgers_without_blocking_sample_writes(materialization_store, tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from datetime import datetime, timezone
    import threading
    store, graph = materialization_store, MemoryGraphStore()
    first = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    study = first.define("concurrent")
    first.sync_metadata_graph()
    run = first.start(study["template_id"], {})
    second = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    entered, release, finishing = threading.Event(), threading.Event(), threading.Event()
    update = graph.sparql_update
    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return update(*args, **kwargs)
    monkeypatch.setattr(graph, "sparql_update", slow)
    def finish():
        finishing.set()
        second.finish(run["run_id"], "succeeded")
    with ThreadPoolExecutor(max_workers=2) as pool:
        delivery = pool.submit(first.sync_metadata_graph)
        assert entered.wait(5)
        transition = pool.submit(finish)
        assert finishing.wait(5)
        try:
            with pytest.raises(TimeoutError):
                transition.result(timeout=.1)
            # Graph delivery owns no SQL writer lock: sample ingestion proceeds.
            ref = store.ensure_stream_ref(None, "site", "sensor", value_kind="numeric")
            store.upsert_rows(ref, [(datetime(2026, 1, 1, tzinfo=timezone.utc), 1.)], value_kind="numeric")
        finally:
            release.set()
        delivery.result(timeout=5)
        transition.result(timeout=5)
    second.sync_metadata_graph()
    assert second.run(run["run_id"])["status"] == "succeeded"
    assert pending(store) == 0


def test_missing_graph_cannot_be_reinitialized_from_only_new_pending_writes(materialization_store, tmp_path):
    store, graph = materialization_store, MemoryGraphStore()
    ledger = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    study = ledger.define("backup boundary")
    first = ledger.start(study["template_id"], {"scenario": "already acknowledged"})
    ledger.sync_metadata_graph()
    ledger.start(study["template_id"], {"scenario": "pending"})
    assert pending(store) == 1
    restarted = ExperimentStore(store, tmp_path / "artifacts", graph_store=MemoryGraphStore())
    with pytest.raises(RuntimeError, match="restore its graph backup"):
        restarted.run(first["run_id"])
    assert pending(store) == 1


def test_failed_source_flush_keeps_the_outbox_unacknowledged(materialization_store, tmp_path):
    class GraphWithFlush(MemoryGraphStore):
        fail_flush = True
        def flush_metadata(self):
            if self.fail_flush:
                raise OSError("flush failed")
    graph = GraphWithFlush()
    ledger = ExperimentStore(materialization_store, tmp_path / "artifacts", graph_store=graph)
    study = ledger.define("flush")
    with pytest.raises(OSError, match="flush failed"):
        ledger.sync_metadata_graph()
    assert pending(materialization_store) == 1
    graph.fail_flush = False
    ledger.sync_metadata_graph()
    assert pending(materialization_store) == 0
    assert ledger.study(study["template_id"])["name"] == "flush"
