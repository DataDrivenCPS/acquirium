"""A write to the real graph store, computed as a delta first, changes exactly what the direct write would."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from rdflib import Namespace, RDF

from acquirium.Materialization.graph_history import GraphHistory
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.graph_store import OxigraphGraphStore

EX = Namespace("urn:ex:")
TTL = """@prefix ex: <urn:ex:> .
ex:A a ex:FlowMeter .
ex:B a ex:FlowMeter .
"""
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    root = tmp_path_factory.mktemp("graph")
    s = OxigraphGraphStore(store_path=root / "store", env_root=root / "env")
    try:
        yield s
    finally:
        s.close()


def plant(store):
    return set(store._registered_data_graph(None))


def meters(graph):
    rows = graph.sparql_query(f"SELECT ?m WHERE {{ ?m a <{EX.FlowMeter}> }}", include_dependencies=False, wait_for_fresh=True)["rows"]
    return sorted(str(r[0]).rsplit(":", 1)[1] for r in rows)


def test_prepare_write_then_apply_matches_insert_graph(store):
    delta = store.prepare_write(TTL, format="turtle", replace=True)
    assert {s for s, _, _ in delta.added} == {EX.A, EX.B} and delta.removed == frozenset()
    assert ("ex", "urn:ex:") in delta.prefixes
    assert plant(store) == set()                 # preparing changed nothing
    store.apply_delta(delta)
    assert plant(store) == delta.added and meters(store) == ["A", "B"]

    # Append: only the new triple is a change.
    more = store.prepare_write("@prefix ex: <urn:ex:> . ex:A a ex:FlowMeter . ex:C a ex:FlowMeter .", format="turtle")
    assert more.added == {(EX.C, RDF.type, EX.FlowMeter)} and more.removed == frozenset()
    store.apply_delta(more)
    assert meters(store) == ["A", "B", "C"]

    # Replace with a graph that keeps A and C: only B is removed.
    swap = store.prepare_write("@prefix ex: <urn:ex:> . ex:A a ex:FlowMeter . ex:C a ex:FlowMeter .", format="turtle", replace=True)
    assert swap.added == frozenset() and swap.removed == {(EX.B, RDF.type, EX.FlowMeter)}
    store.apply_delta(swap)
    assert meters(store) == ["A", "C"]


def test_prepare_update_computes_what_the_update_would_change(store):
    delta = store.prepare_update(f"DELETE {{ <{EX.C}> a <{EX.FlowMeter}> }} INSERT {{ <{EX.D}> a <{EX.FlowMeter}> }} WHERE {{ }}")
    assert delta.added == {(EX.D, RDF.type, EX.FlowMeter)} and delta.removed == {(EX.C, RDF.type, EX.FlowMeter)}
    assert meters(store) == ["A", "C"]
    store.apply_delta(delta)
    assert meters(store) == ["A", "D"]


def test_a_version_of_the_real_store_is_inferred_and_queryable(store, tmp_path):
    db = DuckDBStore(tmp_path / "h.duckdb")
    try:
        history = GraphHistory(db, store)
        delta = store.prepare_update(f"DELETE {{ <{EX.D}> a <{EX.FlowMeter}> }} WHERE {{ }}")
        change = history.record(delta, T0 + timedelta(days=5))
        store.apply_delta(delta)
        with db._lock, db._write_conn() as conn:
            history.mark_applied(conn, change, int(store.graph_status()["source_version"]))
        assert meters(store) == ["A"]
        assert meters(history.as_of(T0)) == ["A", "D"]
        assert meters(history.as_of(T0 + timedelta(days=5))) == ["A"]
    finally:
        db.close()
