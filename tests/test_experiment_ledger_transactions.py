"""The ledger's own transactions, on both backends.

Users reach the ledger only through a server started with aq.init(); the
tests that go through it live with the rest of the experiment tests. These
build the server's ledger in-process instead, because what they check, a
failure partway through a write or the order two writers commit in, cannot
be arranged from a client.
"""
import threading
from datetime import datetime, timezone

import pytest

from acquirium.ExperimentSamples import sample_frame
from acquirium.Experiments import ExperimentStore
from acquirium.Storage.metadata_outbox import execute
from tests.unit.test_experiment_metadata import MemoryGraphStore

WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def ledger(experiment_ledger):
    return ExperimentStore(experiment_ledger, graph_store=MemoryGraphStore())


def _count(store, table):
    with store._own_conn() as conn:
        return execute(conn, f"SELECT count(*) FROM {table}").fetchone()[0]


def _samples(store):
    """Every sample in the ledger's Escapement, as (stream, time, value)."""
    return [(r.stream, r.event_time_ns, r.value) for r in store.samples.core.engine.snapshot().readings()]


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
def test_failed_recording_leaves_no_observation_and_a_retry_succeeds(ledger, monkeypatch, failure):
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
            patch.setattr(ledger, "_own_streams", fail)
        elif failure == "insertion":
            patch.setattr(ledger.store.samples, "write", fail)
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
    # No observation points at anything. Samples written before the failure
    # sit in the reserved stream, unreferenced, until a retry or a deletion.
    assert ledger.observations(study["template_id"]) == []
    assert _count(ledger.store, "experiment_metadata_outbox") == 0
    written = failure in {"observation", "outbox"}
    assert len(_samples(ledger.store)) == (1 if written else 0)
    assert _count(ledger.store, "experiment_streams") == (0 if failure == "registration" else 1)
    assert len(ledger.record_batch(run["run_id"], entries)) == 2
    assert len(_samples(ledger.store)) == 1


def _recorded_runs(ledger, count):
    study = ledger.define("batch reset")
    variable = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    runs = []
    for value in range(count):
        run = ledger.start(study["template_id"], {})
        ledger.record_batch(run["run_id"], [dict(form="samples", variable_id=variable["variable_id"],
                                                 samples=[[WHEN.isoformat(), float(value)]])])
        ledger.finish(run["run_id"], "succeeded")
        runs.append(run["run_id"])
    return study, runs


def test_deleting_runs_clears_their_samples_after_the_commit(ledger):
    study, runs = _recorded_runs(ledger, 2)
    assert len(_samples(ledger.store)) == 2
    assert ledger.delete_runs(runs) == {"runs": 2, "observations": 2, "annotations": 0, "samples": 2, "files": 0}
    assert ledger.runs(study["template_id"]) == []
    assert _samples(ledger.store) == []
    assert _count(ledger.store, "experiment_streams") == 0


def test_an_interrupted_cleanup_is_finished_later(experiment_ledger, ledger, monkeypatch):
    store = ledger.store
    study, runs = _recorded_runs(ledger, 2)

    # The deletion commits first; a failure clearing the samples afterwards
    # leaves the runs deleted and their stream rows in place.
    def fail(refs):
        raise RuntimeError("clear failure")
    with monkeypatch.context() as patch:
        patch.setattr(store.samples, "clear", fail)
        with pytest.raises(RuntimeError, match="clear failure"):
            ledger.delete_runs(runs)
    assert ledger.runs(study["template_id"]) == []
    assert _count(store, "experiment_observations") == 0
    assert _count(store, "experiment_streams") == 2
    assert len(_samples(store)) == 2

    # A start that cannot clear them still starts, and keeps the rows.
    with monkeypatch.context() as patch:
        patch.setattr(store.samples, "clear", fail)
        ExperimentStore(experiment_ledger, graph_store=ledger.metadata_graph.graph_store)
    assert _count(store, "experiment_streams") == 2

    # A ledger without Escapement, which could not clear them, is refused
    # before it touches them.
    samples, store.samples = store.samples, None
    try:
        with pytest.raises(ValueError, match="sample store"):
            ExperimentStore(experiment_ledger, graph_store=ledger.metadata_graph.graph_store)
    finally:
        store.samples = samples
    assert _count(store, "experiment_streams") == 2

    # A later deletion finishes the earlier one but reports only its own.
    _, later = _recorded_runs(ledger, 1)
    assert ledger.delete_runs(later)["samples"] == 1
    assert _samples(store) == []
    assert _count(store, "experiment_streams") == 0


def test_a_start_finishes_an_interrupted_cleanup(experiment_ledger, ledger, monkeypatch):
    store = ledger.store
    _, runs = _recorded_runs(ledger, 1)
    with monkeypatch.context() as patch:
        patch.setattr(store.samples, "clear", lambda refs: (_ for _ in ()).throw(RuntimeError("clear failure")))
        with pytest.raises(RuntimeError):
            ledger.delete_runs(runs)
    ExperimentStore(experiment_ledger, graph_store=ledger.metadata_graph.graph_store)
    assert _samples(store) == []
    assert _count(store, "experiment_streams") == 0


def test_clearing_a_run_is_one_request_however_many_samples(ledger, monkeypatch):
    study = ledger.define("one request")
    level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    run = ledger.start(study["template_id"], {})["run_id"]
    ledger.record_batch(run, [dict(form="samples", variable_id=level["variable_id"],
                                   samples=[[WHEN.replace(second=i).isoformat(), float(i)] for i in range(50)])])
    ledger.finish(run, "succeeded")
    clear, requests = ledger.store.samples.core.clear, []
    monkeypatch.setattr(ledger.store.samples.core, "clear", lambda *a: requests.append(a[2]) or clear(*a))
    assert ledger.delete_runs([run])["samples"] == 50
    assert len(requests) == 1 and _samples(ledger.store) == []


def test_declaration_racing_the_same_label_is_idempotent(experiment_ledger, ledger, monkeypatch):
    other = ExperimentStore(experiment_ledger, graph_store=ledger.metadata_graph.graph_store)
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


def test_time_series_observations_record_their_epoch(ledger):
    study = ledger.define("epochs")
    level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    used = ledger.declare(study["template_id"], "used", "input", "timeseries", {})
    cost = ledger.declare(study["template_id"], "cost", "output", "scalar", {})
    run = ledger.start(study["template_id"], {})["run_id"]
    [recorded] = ledger.record_batch(run, [dict(form="samples", variable_id=level["variable_id"],
                                                samples=[[WHEN.isoformat(), 1.0]])])
    ledger.record_batch(run, [dict(form="reference", variable_id=used["variable_id"], ref_uri=recorded["ref_uri"],
                                   interval=[WHEN.isoformat(), WHEN.isoformat()]),
                              dict(form="value", variable_id=cost["variable_id"], value=3)])
    epochs = {item["variable"]: item["epoch"] for item in ledger.observations(study["template_id"])}
    published = ledger.store.samples.core.engine.snapshot().epoch
    # Samples record the epoch that published them; a reference, the epoch
    # that published everything written before it; a plain value, none.
    assert epochs == {"level": published, "used": published, "cost": None}


def test_recording_does_not_wait_for_escapement_to_publish(ledger):
    study = ledger.define("no wait")
    level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    used = ledger.declare(study["template_id"], "used", "input", "timeseries", {})
    run = ledger.start(study["template_id"], {})["run_id"]
    core = ledger.store.samples.core
    # With Escapement publishing nothing, recordings still commit.
    core.processor.stop(drain=False)
    core.ingest("plant", "flow-1", sample_frame("urn:plant:flow", [(WHEN, 2.0)]))
    ledger.record_batch(run, [dict(form="samples", variable_id=level["variable_id"], samples=[[WHEN.isoformat(), 1.0]])])
    ledger.record_batch(run, [dict(form="reference", variable_id=used["variable_id"], ref_uri="urn:plant:flow",
                                   interval=[WHEN.isoformat(), WHEN.isoformat()])])
    assert core.engine.snapshot().applied == 0
    # Reading the observations waits for their data to publish; the
    # reference's epoch includes the plant data written before it.
    core.processor = core.engine.start()
    epochs = {item["variable"]: item["epoch"] for item in ledger.observations(study["template_id"])}
    assert epochs["level"] is not None and epochs["used"] >= epochs["level"]
    assert [r.value for r in core.engine.query("urn:plant:flow", epoch=epochs["used"])] == [2.0]


def test_runs_record_without_waiting_on_each_other(ledger, monkeypatch):
    study = ledger.define("concurrent")
    level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    first, second = (ledger.start(study["template_id"], {})["run_id"] for _ in range(2))
    write, other = ledger.store.samples.write, []

    def record_other_meanwhile(source, frame):
        # The first run is mid-recording; the second records and finishes.
        if source == f"experiment/{first}":
            other.append(threading.Thread(target=lambda: (
                ledger.record_batch(second, [dict(form="samples", variable_id=level["variable_id"],
                                                  samples=[[WHEN.isoformat(), 2.0]])]),
                ledger.finish(second, "succeeded"))))
            other[0].start()
            other[0].join(5)
            assert not other[0].is_alive()
        return write(source, frame)
    monkeypatch.setattr(ledger.store.samples, "write", record_other_meanwhile)
    ledger.record_batch(first, [dict(form="samples", variable_id=level["variable_id"], samples=[[WHEN.isoformat(), 1.0]])])
    assert len(ledger.observations(study["template_id"])) == 2


def test_finishing_a_run_waits_for_a_recording_in_progress(ledger, monkeypatch):
    study = ledger.define("finish race")
    level = ledger.declare(study["template_id"], "level", "output", "timeseries", {})
    run = ledger.start(study["template_id"], {})["run_id"]
    write, finishing = ledger.store.samples.write, []

    def finish_meanwhile(source, frame):
        # The streams are reserved; a finish now must wait for the commit.
        finishing.append(threading.Thread(target=ledger.finish, args=(run, "succeeded")))
        finishing[0].start()
        finishing[0].join(0.2)
        assert finishing[0].is_alive()
        return write(source, frame)
    monkeypatch.setattr(ledger.store.samples, "write", finish_meanwhile)
    ledger.record_batch(run, [dict(form="samples", variable_id=level["variable_id"], samples=[[WHEN.isoformat(), 1.0]])])
    finishing[0].join()
    assert [item["variable"] for item in ledger.observations(study["template_id"])] == ["level"]
    assert ledger.runs(study["template_id"])[0]["status"] == "succeeded"
