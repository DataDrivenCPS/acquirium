"""The ledger's own transactions, on both backends.

Users reach the ledger only through a server started with aq.init(); the
tests that go through it live with the rest of the experiment tests. These
build the server's ledger in-process instead, because what they check, a
failure partway through a write or the order two writers commit in, cannot
be arranged from a client.
"""
from datetime import datetime, timezone

import pytest

from acquirium.Experiments import ExperimentStore
from acquirium.Storage.metadata_outbox import execute
from tests.test_timeseries_store_contract import _snapshot
from tests.unit.test_experiment_metadata import MemoryGraphStore

WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def ledger(materialization_store):
    return ExperimentStore(materialization_store, graph_store=MemoryGraphStore())


def _count(store, table):
    with store._own_conn() as conn:
        return execute(conn, f"SELECT count(*) FROM {table}").fetchone()[0]


def test_outbox_failure_rolls_back_lifecycle_write(ledger, monkeypatch):
    study = ledger.define("rollback")
    ledger.sync_metadata_graph()
    queue = ledger._queue_metadata

    def fail(conn, record):
        queue(conn, record)
        raise OSError("transaction failed")
    monkeypatch.setattr(ledger, "_queue_metadata", fail)
    with pytest.raises(OSError, match="transaction failed"):
        ledger.start(study["template_id"], {})
    assert ledger.runs(study["template_id"]) == []
    assert _count(ledger.store, "experiment_metadata_outbox") == 0


@pytest.mark.parametrize("failure", ["registration", "insertion", "observation", "outbox"])
def test_recording_batch_failure_leaves_nothing_behind(ledger, monkeypatch, failure):
    study = ledger.define("atomic recording")
    level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    cost = ledger.declare(study["template_id"], "cost", "output", "scalar", {})
    run = ledger.start(study["template_id"], {})
    ledger.sync_metadata_graph()
    entries = [dict(form="samples", variable_id=level["variable_id"], samples=[[WHEN.isoformat(), 1.0]]),
               dict(form="value", variable_id=cost["variable_id"], value=3)]

    def fail(*args, **kwargs):
        raise RuntimeError("temporary")
    with monkeypatch.context() as patch:
        if failure == "registration":
            patch.setattr(ledger.store, "_ensure_stream_refs", fail)
        elif failure == "insertion":
            patch.setattr(ledger.store, "_insert_frame", fail)
        elif failure == "outbox":
            patch.setattr(ledger.metadata, "enqueue", fail)
        else:
            sql = ledger._execute

            def insert(conn, statement, parameters=None):
                if statement.startswith("INSERT INTO experiment_observations"):
                    raise RuntimeError("temporary")
                return sql(conn, statement, parameters)
            patch.setattr(ledger, "_execute", insert)
        with pytest.raises(RuntimeError, match="temporary"):
            ledger.record_batch(run["run_id"], entries)
    assert ledger.observations(study["template_id"]) == []
    for table in ("timeseries", "streams", "experiment_metadata_outbox"):
        assert _count(ledger.store, table) == 0
    assert len(ledger.record_batch(run["run_id"], entries)) == 2


def test_batch_deletion_rolls_back_resets_with_ledger_and_outbox(ledger, monkeypatch):
    from acquirium.Storage.publication.revision import RevisionPublisher

    store = ledger.store
    study = ledger.define("batch reset")
    variable = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    runs, refs = [], []
    for _ in range(2):
        run = ledger.start(study["template_id"], {})
        ref = str(store.ensure_stream_ref(None, f"experiment/{run['run_id']}", "level", value_kind="numeric"))
        store.upsert_rows(ref, [(WHEN, 1.0)], value_kind="numeric")
        ledger.observe(run["run_id"], variable["variable_id"], ref_uri=ref, interval=(WHEN, WHEN))
        ledger.finish(run["run_id"], "succeeded")
        runs.append(run["run_id"])
        refs.append(ref)
    with store._lock, store._write_conn() as conn:
        execute(conn, "UPDATE timeseries SET deleted=TRUE WHERE ref_id=(SELECT ref_id FROM ref_ids WHERE ref_uri=?)", [refs[1]])
    before = _snapshot(store)
    reset = RevisionPublisher.reset

    def fail_after_reset(*args):
        reset(*args)
        raise RuntimeError("reset failure")
    with monkeypatch.context() as patch:
        patch.setattr(RevisionPublisher, "reset", fail_after_reset)
        with pytest.raises(RuntimeError, match="reset failure"):
            ledger.delete_runs(runs)
    assert _snapshot(store) == before
    assert {run["run_id"] for run in ledger.runs(study["template_id"])} == set(runs)
    assert _count(store, "experiment_observations") == 2
    assert _count(store, "experiment_metadata_outbox") == 0
    assert ledger.delete_runs(runs) == {"runs": 2, "observations": 2, "annotations": 0, "samples": 1, "files": 0}
    revision, rows, resets = _snapshot(store)
    assert revision == before[0] + 1
    assert rows == []
    assert resets == sorted((ref, revision) for ref in refs)


def test_declaration_racing_the_same_label_is_idempotent(materialization_store, ledger, monkeypatch):
    other = ExperimentStore(materialization_store, graph_store=ledger.metadata_graph.graph_store)
    study = ledger.define("race")
    record_update = ledger.metadata_graph.record_update

    def interleave(*args, **kwargs):
        # Another caller commits the same declaration after this one has
        # read the existing variables but before it inserts.
        monkeypatch.setattr(ledger.metadata_graph, "record_update", record_update)
        other.declare(study["template_id"], "flow", "input", "scalar", {})
        return record_update(*args, **kwargs)
    monkeypatch.setattr(ledger.metadata_graph, "record_update", interleave)
    variable = ledger.declare(study["template_id"], "flow", "input", "scalar", {})
    assert variable["created"] is False
    assert [item["label"] for item in ledger.variables(study["template_id"])] == ["flow"]
    with pytest.raises(ValueError, match="declared differently"):
        ledger.declare(study["template_id"], "flow", "output", "scalar", {})


def test_declarations_load_only_the_labels_they_name(ledger, monkeypatch):
    study = ledger.define("scaling")["template_id"]
    ledger.declare_batch(study, [dict(label=f"x{i}", role="input", kind="scalar") for i in range(20)])
    hydrated, delivered = [], []
    read = ledger.metadata.read
    monkeypatch.setattr(ledger.metadata, "read", lambda kind, ids: hydrated.append(list(ids)) or read(kind, ids))
    update = ledger.metadata_graph.graph_store.metadata_update
    monkeypatch.setattr(ledger.metadata_graph.graph_store, "metadata_update",
                        lambda *args, **kwargs: delivered.append(args) or update(*args, **kwargs))
    # A new label compares against nothing, so it neither reads nor waits for
    # graph delivery of the Study's other declarations.
    ledger.declare(study, "new", "input", "scalar", {})
    assert hydrated == [] and delivered == []
    again = ledger.declare(study, "x3", "input", "scalar", {})
    assert again["created"] is False and len(hydrated) == 1 and len(hydrated[0]) == 1


def test_values_that_json_cannot_carry_are_rejected(ledger):
    # A client cannot send NaN or infinity, but the ledger must refuse them
    # from any caller.
    study = ledger.define("non-finite")
    scalar = ledger.declare(study["template_id"], "scalar", "output", "scalar", {})["variable_id"]
    data = ledger.declare(study["template_id"], "json", "output", "json", {})["variable_id"]
    run = ledger.start(study["template_id"], {})["run_id"]
    for variable, value in ((scalar, float("nan")), (scalar, float("inf")), (data, {"x": float("inf")})):
        with pytest.raises(ValueError):
            ledger.observe(run, variable, value=value)
    assert ledger.observations(study["template_id"]) == []
