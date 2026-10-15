"""A step applies the inbox and evaluates the plan in one transaction."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from acquirium.Materialization import ApplicationGraph, Binding, Engine, RevisionStore, Scheduler, StreamDescriptor
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.publication.revision import RevisionPublisher
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest
from tests.unit.test_incremental_materialization import Copy, _port

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def mutations(ref: str, values: list[float]) -> pa.Table:
    times = [T0 + timedelta(minutes=i) for i in range(len(values))]
    return pa.table({"operation": ["upsert"] * len(values), "ref_uri": [ref] * len(values),
                     "ts": pa.array(times, pa.timestamp("us", tz="UTC")),
                     "numeric_value": values, "text_value": [None] * len(values)}, schema=MUTATION_SCHEMA)


def copy_binding(name: str, source: str) -> Binding:
    return Binding(name, "digest", {"source": (StreamDescriptor(source),)}, {"out": _port(ref=f"urn:{name}")})


def stored(store, ref: str) -> list[float]:
    return [row["value"] for batch in store.timeseries(ref, value_mode="numeric") for row in batch.to_pylist()]


class GraphEvaluator:
    """A fixed plan, the way the materializer hands one to the engine."""
    def __init__(self, scheduler: Scheduler, graph: ApplicationGraph, applications: dict) -> None:
        self.scheduler, self.graph, self.applications = scheduler, graph, applications

    def prepare(self):
        return self.graph

    def evaluate(self, conn, plan) -> bool:
        return self.scheduler.evaluate(conn, plan, self.applications)


@pytest.fixture
def chain(tmp_path):
    """input -> copy -> child, evaluated by the engine's steps."""
    store = DuckDBStore(tmp_path / "step.duckdb")
    root, child = copy_binding("copy", "urn:input"), copy_binding("child", "urn:copy")
    scheduler = Scheduler(RevisionStore(store))
    engine = Engine(store, GraphEvaluator(scheduler, ApplicationGraph([root, child]), {root.signature: Copy(), child.signature: Copy()}))
    engine.deferred = True
    yield store, engine, RevisionPublisher(engine), scheduler
    scheduler.close()
    store.close()


def test_raw_and_derived_rows_of_a_chain_commit_in_one_step(chain):
    store, engine, publisher, _ = chain
    publisher.publish(PublicationRequest("p1", mutations("urn:input", [1.0, 2.0])))
    assert stored(store, "urn:input") == [] and stored(store, "urn:child") == []

    report = engine.step()

    assert report.entries == 1 and report.published
    assert stored(store, "urn:input") == [1.0, 2.0]
    assert stored(store, "urn:copy") == [1.0, 2.0]
    assert stored(store, "urn:child") == [1.0, 2.0]
    assert engine.step() is None


def test_a_derived_write_failure_rolls_the_whole_step_back(chain, monkeypatch):
    store, engine, publisher, _ = chain
    publisher.publish(PublicationRequest("p1", mutations("urn:input", [1.0])))
    real = store._insert_frame
    calls = []

    def fail_on_derived(conn, frame, revision):
        calls.append(frame["ref_uri"][0])
        if frame["ref_uri"][0] != "urn:input":
            raise RuntimeError("disk full")
        return real(conn, frame, revision)
    monkeypatch.setattr(store, "_insert_frame", fail_on_derived)

    with pytest.raises(RuntimeError, match="disk full"):
        engine.step()

    assert calls == ["urn:input", "urn:copy"]
    assert stored(store, "urn:input") == [] and engine.pending() == 1 and engine.last_step() == 0
    monkeypatch.undo()
    assert engine.step().published and stored(store, "urn:child") == [1.0]


def test_a_transform_failure_blocks_only_its_branch(tmp_path):
    class Broken(Copy):
        def transform(self, inputs, output, context):
            raise ValueError("broken sensor")

    store = DuckDBStore(tmp_path / "branch.duckdb")
    left, right, child = copy_binding("left", "urn:input"), copy_binding("right", "urn:input"), copy_binding("child", "urn:left")
    scheduler = Scheduler(RevisionStore(store))
    engine = Engine(store, GraphEvaluator(scheduler, ApplicationGraph([left, right, child]),
                                          {left.signature: Broken(), right.signature: Copy(), child.signature: Copy()}))
    engine.deferred = True
    try:
        RevisionPublisher(engine).publish(PublicationRequest("p1", mutations("urn:input", [4.0])))

        report = engine.step()

        assert report.published
        assert stored(store, "urn:input") == [4.0] and stored(store, "urn:right") == [4.0]
        assert stored(store, "urn:left") == [] and stored(store, "urn:child") == []
        assert "broken sensor" in scheduler.errors[left.signature]
    finally:
        scheduler.close()
        store.close()


def test_a_step_without_inbox_entries_still_evaluates_pending_work(chain):
    store, engine, publisher, _ = chain
    store.upsert_rows("urn:input", [(T0, 9.0)], value_kind="numeric")  # written directly, as a test fixture would

    report = engine.step()

    assert report.entries == 0 and report.published and stored(store, "urn:child") == [9.0]
