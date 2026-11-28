"""Durable graph handoff and SQL recording transactions on both backends."""
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timezone
from threading import Event
from unittest.mock import Mock

import pytest
from rdflib import RDF, URIRef

from acquirium.ExperimentSamples import sample_frame
from acquirium.Experiments import ExperimentStore
from acquirium.ExperimentMetadata import EXPERIMENT, run_uri
from acquirium.internals.internals_namespaces import HAS_UNIT
from tests.epoch_helpers import all_readings

WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


def pending(store):
    """Count metadata updates that the ledger still needs to deliver or acknowledge."""
    with store._own_conn() as conn:
        return conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0]


def graph_offline(*args, **kwargs):
    """Simulate a graph store that rejects an update."""
    raise OSError("offline")


def test_restart_replays_resolved_documents_without_a_resolver(experiment_ledger, source_graph, monkeypatch):
    store, graph = experiment_ledger, source_graph
    ledger = ExperimentStore(store, graph_store=graph, resolve=lambda *args: "urn:unit:PSI")
    study = ledger.define("restart")
    variable = ledger.declare(study["template_id"], "pressure", "output", "scalar", {"unit": "psi", "metadata": {"any.key": [None, {}, []]}})
    run = ledger.start(study["template_id"], {"tags": ["A", None, "B"]})
    # Leave the Study, variable, and run committed only to the ledger and its outbox.
    with monkeypatch.context() as patch:
        patch.setattr(graph, "metadata_update", graph_offline)
        with pytest.raises(OSError, match="offline"):
            ledger.run(run["run_id"])
    assert pending(store) == 3
    # Replay the already-resolved metadata with a resolver that returns no matches.
    restarted = ExperimentStore(store, graph_store=graph, resolve=lambda *args: None)
    assert restarted.variables(study["template_id"])[0]["metadata"] == variable["metadata"]
    assert restarted.run(run["run_id"])["metadata"] == {"tags": ["A", None, "B"]}
    assert pending(store) == 0
    restarted.observe(run["run_id"], variable["variable_id"], value=12)
    assert restarted.observations(study["template_id"])[0]["value"] == 12
    restarted.finish(run["run_id"], "succeeded")
    assert restarted.run(run["run_id"])["metadata"] == {"tags": ["A", None, "B"]}
    declared = graph.data_graph("experiments")
    assert (URIRef(variable["variable_id"]), HAS_UNIT, URIRef("urn:unit:PSI")) in declared

    monkeypatch.setattr(graph, "metadata_documents", Mock(side_effect=OSError("read failed")))
    with pytest.raises(OSError, match="read failed"):
        restarted.run(run["run_id"])


def test_graph_commit_before_failed_acknowledgment_is_replayable(experiment_ledger, source_graph, monkeypatch):
    store, graph = experiment_ledger, source_graph
    ledger = ExperimentStore(store, graph_store=graph)
    study = ledger.define("ack")
    ledger.sync_metadata_graph()
    run = ledger.start(study["template_id"], {"scenario": "baseline"})
    ledger.finish(run["run_id"], "succeeded")
    # Let the graph commit and fail only when the ledger acknowledges those updates.
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
    assert (URIRef(run_uri(run["run_id"])), RDF.type, EXPERIMENT.Run) in graph.data_graph("experiments")
    assert pending(store) == 2
    restarted = ExperimentStore(store, graph_store=graph)
    assert restarted.run(run["run_id"])["status"] == "succeeded"
    assert restarted.run(run["run_id"])["metadata"] == {"scenario": "baseline"}
    assert pending(store) == 0


def test_committed_deletion_is_retried_after_restart(experiment_ledger, source_graph, monkeypatch):
    store, graph = experiment_ledger, source_graph
    ledger = ExperimentStore(store, graph_store=graph)
    study = ledger.define("delete")
    run = ledger.start(study["template_id"], {"scenario": "baseline"})
    ledger.finish(run["run_id"], "succeeded")
    ledger.sync_metadata_graph()
    # Delete the run while the graph is offline. Restart must finish the graph removal.
    with monkeypatch.context() as patch:
        patch.setattr(graph, "metadata_update", graph_offline)
        with pytest.raises(OSError, match="offline"):
            ledger.delete_runs([run["run_id"]])
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_runs").fetchone()[0] == 0
    assert pending(store) > 0
    restarted = ExperimentStore(store, graph_store=graph)
    restarted.sync_metadata_graph()
    assert not list(graph.data_graph("experiments").triples((URIRef(run_uri(run["run_id"])), None, None)))
    assert pending(store) == 0


def test_ledger_with_another_migration_history_is_refused(experiment_ledger, source_graph):
    from acquirium.Storage.ledger import DuckDBLedger
    from acquirium.Storage.migrations import Migration, migrate
    with experiment_ledger._lock, experiment_ledger._write_conn() as conn:
        migrate(conn, backend="duckdb" if isinstance(experiment_ledger, DuckDBLedger) else "postgres",
                component="experiments", migrations=(Migration(1, "observation_keys", ()),))
    with pytest.raises(RuntimeError, match="fresh data directory"):
        ExperimentStore(experiment_ledger, graph_store=source_graph)


def test_graph_delivery_serializes_reads_and_lifecycle_writes(experiment_ledger, source_graph, monkeypatch):
    store, graph = experiment_ledger, source_graph
    first = ExperimentStore(store, graph_store=graph)
    study = first.define("concurrent")["template_id"]
    first.sync_metadata_graph()
    run = first.start(study, {"scenario": "baseline"})["run_id"]
    second = ExperimentStore(store, graph_store=graph)
    entered, release, reading, finishing = Event(), Event(), Event(), Event()
    update = graph.metadata_update

    def hold_update(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return update(*args, **kwargs)

    def read():
        reading.set()
        return second.run(run)

    def finish():
        finishing.set()
        second.finish(run, "succeeded")

    # Hold the first read inside graph delivery. A second read and lifecycle
    # update share that boundary, while samples can still be accepted.
    monkeypatch.setattr(graph, "metadata_update", hold_update)
    with ThreadPoolExecutor(max_workers=3) as pool:
        delivery = pool.submit(first.run, run)
        try:
            assert entered.wait(5)
            reader = pool.submit(read)
            transition = pool.submit(finish)
            assert reading.wait(5) and finishing.wait(5)
            with pytest.raises(TimeoutError):
                reader.result(timeout=.1)
            with pytest.raises(TimeoutError):
                transition.result(timeout=.1)
            store.samples.write("site", sample_frame("urn:site:sensor", [(WHEN, 1.0)]))
        finally:
            release.set()
        assert delivery.result(timeout=5)["metadata"] == {"scenario": "baseline"}
        assert reader.result(timeout=5)["metadata"] == {"scenario": "baseline"}
        transition.result(timeout=5)
    second.sync_metadata_graph()
    assert second.run(run)["status"] == "succeeded"
    assert pending(store) == 0


def test_missing_graph_cannot_be_reinitialized_from_only_new_pending_writes(experiment_ledger, open_source_graph):
    store, graph = experiment_ledger, open_source_graph()
    ledger = ExperimentStore(store, graph_store=graph)
    study = ledger.define("backup boundary")
    first = ledger.start(study["template_id"], {"scenario": "already acknowledged"})
    ledger.sync_metadata_graph()
    ledger.start(study["template_id"], {"scenario": "pending"})
    assert pending(store) == 1
    restarted = ExperimentStore(store, graph_store=open_source_graph())
    with pytest.raises(RuntimeError, match="restore its graph backup"):
        restarted.run(first["run_id"])
    assert pending(store) == 1


def test_failed_source_flush_replays_committed_sample_intent(experiment_ledger, source_graph, monkeypatch):
    store = experiment_ledger
    ledger = ExperimentStore(store, graph_store=source_graph)
    study = ledger.define("flush")["template_id"]
    flow = ledger.declare(study, "flow", "output", "timeseries", {})["variable_id"]
    run = ledger.start(study, {})["run_id"]
    [receipt] = ledger.record_batch(run, [
        {"form": "samples", "variable_id": flow, "samples": [(WHEN, 3)]},
    ])

    # Samples and observations have committed. Graph intent cannot be
    # acknowledged until its source write has flushed.
    with monkeypatch.context() as patch:
        patch.setattr(source_graph, "flush_metadata", Mock(side_effect=OSError("flush failed")))
        with pytest.raises(OSError, match="flush failed"):
            ledger.sync_metadata_graph()
    assert len(all_readings(store.samples.core)) == 1
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_observations").fetchone()[0] == 1
    assert pending(store) == 1

    reopened = ExperimentStore(store, graph_store=source_graph)
    assert reopened.observations(study)[0]["stream_uri"] == receipt["stream_uri"]
    assert pending(store) == 0
    assert reopened.study(study)["name"] == "flush"
