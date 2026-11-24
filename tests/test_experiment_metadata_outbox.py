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


def test_restart_replays_resolved_documents_without_sql_snapshots(experiment_ledger, tmp_path, monkeypatch):
    store, graph = experiment_ledger, MemoryGraphStore()
    ledger = ExperimentStore(store, graph_store=graph, resolve=lambda *args: "urn:unit:PSI")
    study = ledger.define("restart")
    variable = ledger.declare(study["template_id"], "pressure", "output", "scalar", {"unit": "psi", "metadata": {"any.key": [None, {}, []]}})
    run = ledger.start(study["template_id"], {"tags": ["A", None, "B"]})
    with monkeypatch.context() as patch:
        patch.setattr(graph, "metadata_update", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("offline")))
        with pytest.raises(OSError, match="offline"):
            ledger.sync_metadata_graph()
    assert pending(store) == 3
    restarted = ExperimentStore(store, graph_store=graph, resolve=lambda *args: None)
    assert restarted.variables(study["template_id"])[0]["metadata"] == variable["metadata"]
    assert restarted.run(run["run_id"])["metadata"] == {"tags": ["A", None, "B"]}
    assert (URIRef(variable["variable_id"]), HAS_UNIT, URIRef("urn:unit:PSI")) in graph.dataset
    assert pending(store) == 0
    restarted.observe(run["run_id"], variable["variable_id"], value=12)
    assert restarted.observations(study["template_id"])[0]["value"] == 12


def test_graph_commit_before_failed_acknowledgment_is_replayable(experiment_ledger, tmp_path, monkeypatch):
    store, graph = experiment_ledger, MemoryGraphStore()
    ledger = ExperimentStore(store, graph_store=graph)
    study = ledger.define("ack")
    ledger.sync_metadata_graph()
    run = ledger.start(study["template_id"], {"scenario": "baseline"})
    ledger.finish(run["run_id"], "succeeded")
    from acquirium import ExperimentMetadata as metadata_module
    execute = metadata_module.execute
    def fail_ack(conn, statement, parameters=None):
        if statement.startswith("DELETE FROM experiment_metadata_outbox"):
            raise OSError("ack failed")
        return execute(conn, statement, parameters)
    with monkeypatch.context() as patch:
        patch.setattr(metadata_module, "execute", fail_ack)
        with pytest.raises(OSError, match="ack failed"):
            ledger.sync_metadata_graph()
    assert (URIRef(run_uri(run["run_id"])), RDF.type, EXPERIMENT.Run) in graph.dataset
    assert pending(store) == 2
    restarted = ExperimentStore(store, graph_store=graph)
    assert restarted.run(run["run_id"])["status"] == "succeeded"
    assert restarted.run(run["run_id"])["metadata"] == {"scenario": "baseline"}
    assert pending(store) == 0


def test_committed_deletion_is_retried_after_restart(experiment_ledger, tmp_path, monkeypatch):
    store, graph = experiment_ledger, MemoryGraphStore()
    ledger = ExperimentStore(store, graph_store=graph)
    study = ledger.define("delete")
    run = ledger.start(study["template_id"], {"scenario": "baseline"})
    ledger.finish(run["run_id"], "succeeded")
    ledger.sync_metadata_graph()
    with monkeypatch.context() as patch:
        patch.setattr(graph, "metadata_update", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("offline")))
        with pytest.raises(OSError, match="offline"):
            ledger.delete_runs([run["run_id"]])
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_runs").fetchone()[0] == 0
    assert pending(store) > 0
    restarted = ExperimentStore(store, graph_store=graph)
    restarted.sync_metadata_graph()
    assert not list(graph.dataset.triples((URIRef(run_uri(run["run_id"])), None, None)))
    assert pending(store) == 0


def test_ledger_with_another_migration_history_is_refused(experiment_ledger):
    from acquirium.Storage.ledger import DuckDBLedger
    from acquirium.Storage.migrations import Migration, migrate
    with experiment_ledger._lock, experiment_ledger._write_conn() as conn:
        migrate(conn, backend="duckdb" if isinstance(experiment_ledger, DuckDBLedger) else "postgres",
                component="experiments", migrations=(Migration(1, "observation_keys", ()),))
    with pytest.raises(RuntimeError, match="fresh data directory"):
        ExperimentStore(experiment_ledger, graph_store=MemoryGraphStore())


def test_delivery_is_serialized_across_ledgers_without_blocking_sample_writes(experiment_ledger, tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from datetime import datetime, timezone
    import threading
    store, graph = experiment_ledger, MemoryGraphStore()
    first = ExperimentStore(store, graph_store=graph)
    study = first.define("concurrent")
    first.sync_metadata_graph()
    run = first.start(study["template_id"], {})
    second = ExperimentStore(store, graph_store=graph)
    entered, release, finishing = threading.Event(), threading.Event(), threading.Event()
    update = graph.metadata_update
    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return update(*args, **kwargs)
    monkeypatch.setattr(graph, "metadata_update", slow)
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
            # Graph delivery holds no lock that sample ingestion needs.
            from acquirium.ExperimentSamples import sample_frame
            store.samples.write("site", sample_frame("urn:site:sensor", [(datetime(2026, 1, 1, tzinfo=timezone.utc), 1.)]))
        finally:
            release.set()
        delivery.result(timeout=5)
        transition.result(timeout=5)
    second.sync_metadata_graph()
    assert second.run(run["run_id"])["status"] == "succeeded"
    assert pending(store) == 0


def test_missing_graph_cannot_be_reinitialized_from_only_new_pending_writes(experiment_ledger, tmp_path):
    store, graph = experiment_ledger, MemoryGraphStore()
    ledger = ExperimentStore(store, graph_store=graph)
    study = ledger.define("backup boundary")
    first = ledger.start(study["template_id"], {"scenario": "already acknowledged"})
    ledger.sync_metadata_graph()
    ledger.start(study["template_id"], {"scenario": "pending"})
    assert pending(store) == 1
    restarted = ExperimentStore(store, graph_store=MemoryGraphStore())
    with pytest.raises(RuntimeError, match="restore its graph backup"):
        restarted.run(first["run_id"])
    assert pending(store) == 1


def test_failed_source_flush_keeps_the_outbox_unacknowledged(experiment_ledger, tmp_path):
    class GraphWithFlush(MemoryGraphStore):
        fail_flush = True
        def flush_metadata(self):
            if self.fail_flush:
                raise OSError("flush failed")
    graph = GraphWithFlush()
    ledger = ExperimentStore(experiment_ledger, graph_store=graph)
    study = ledger.define("flush")
    with pytest.raises(OSError, match="flush failed"):
        ledger.sync_metadata_graph()
    assert pending(experiment_ledger) == 1
    graph.fail_flush = False
    ledger.sync_metadata_graph()
    assert pending(experiment_ledger) == 0
    assert ledger.study(study["template_id"])["name"] == "flush"
