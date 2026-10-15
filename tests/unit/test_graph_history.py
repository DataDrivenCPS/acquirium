"""Every data-graph write is logged, dated, before it reaches the graph; the log rebuilds the model as of any time."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import RLock

import pytest
from rdflib import Graph, Literal, Namespace, RDF, URIRef

from acquirium.Materialization.graph_history import GraphHistory, GraphHistoryConflict
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.graph_store import GraphDelta

EX = Namespace("urn:ex:")
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T = [T0 + timedelta(days=d) for d in range(10)]
DATA = "urn:acquirium:test:data"
DEPS = "urn:acquirium:test:deps"


class MemoryGraph:
    """The graph-store surface the history needs, over one rdflib graph. No inference rules."""

    def __init__(self) -> None:
        self.graph = Graph()
        self._lock = RLock()
        self._closure_version = 0
        self.version = 0
        self.inferred_data_graph_uri = URIRef(DATA)
        self.dependency_query_graph_uri = URIRef(DEPS)
        self.fail_next_apply = False

    def graph_status(self):
        return {"source_version": self.version, "published_version": self.version}

    def _source_data_graph(self):
        return Graph() + self.graph

    def _ensure_dependency_cache_current(self):
        return Graph()

    def prepare(self, add=(), remove=()):
        return GraphDelta("urn:g", frozenset(add), frozenset(remove), initial=not len(self.graph) and not remove)

    def apply_delta(self, delta):
        if self.fail_next_apply:
            self.fail_next_apply = False
            raise RuntimeError("crash before the graph write")
        for t in delta.removed:
            self.graph.remove(t)
        for t in delta.added:
            self.graph.add(t)
        self.version += 1


def meter(name):
    return (EX[name], RDF.type, EX.FlowMeter)


def members(version):
    rows = version.sparql_query(f"SELECT ?m WHERE {{ ?m a <{EX.FlowMeter}> }}")["rows"]
    return sorted(str(r[0]).rsplit(":", 1)[1] for r in rows)


@pytest.fixture
def runtime(tmp_path):
    store = DuckDBStore(tmp_path / "history.duckdb")
    graph = MemoryGraph()
    graph.graph.add((EX.plant, RDF.type, EX.Plant))   # a loaded model; the first write below is a change
    m = Materializer(store, graph)
    try:
        yield store, graph, m
    finally:
        m.close(); store.close()


def write(m, graph, effective, add=(), remove=()):
    with m.graph_change(effective) as w:
        w.apply(graph.prepare(add, remove))


def test_a_write_is_logged_then_applied_and_the_planner_sees_its_effective_time(runtime):
    store, graph, m = runtime
    write(m, graph, T[1], add=[meter("A"), meter("B")])
    assert set(graph.graph) == {(EX.plant, RDF.type, EX.Plant), meter("A"), meter("B")} and graph.version == 1
    with store._own_conn() as conn:
        heads = conn.execute("SELECT applied, source_version FROM graph_history").fetchall()
        ops = conn.execute("SELECT op, count(*) FROM graph_history_triples GROUP BY op").fetchall()
        pending = conn.execute("SELECT graph_revision, effective_at FROM materialization_graph_changes").fetchall()
    assert heads == [(True, 1)] and ops == [("+", 2)]
    assert pending == [(1, T[1].isoformat())]


def test_a_change_logged_but_not_applied_is_applied_on_recovery(runtime):
    store, graph, m = runtime
    write(m, graph, T[1], add=[meter("A")])
    graph.fail_next_apply = True
    with pytest.raises(RuntimeError):
        write(m, graph, T[2], add=[meter("B")])
    assert meter("B") not in graph.graph           # the graph missed it
    assert len(m.history.unapplied()) == 1          # the log did not
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM materialization_graph_changes").fetchone()[0] == 1

    assert m.recover_graph_writes() == 1
    assert {meter("A"), meter("B")} <= set(graph.graph) and m.history.unapplied() == []
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM materialization_graph_changes").fetchone()[0] == 2
    assert m.history.change_points() == [T[1], T[2]]


def test_recovery_is_idempotent_and_applying_twice_changes_nothing(runtime):
    store, graph, m = runtime
    write(m, graph, T[1], add=[meter("A")])
    assert m.recover_graph_writes() == 0
    graph.apply_delta(graph.prepare(add=[meter("A")]))
    assert set(graph.graph) == {(EX.plant, RDF.type, EX.Plant), meter("A")}


def test_a_write_dated_before_a_later_change_to_the_same_triple_is_rejected(runtime):
    store, graph, m = runtime
    write(m, graph, T[1], add=[meter("A"), meter("B")])
    write(m, graph, T[5], remove=[meter("B")])
    with pytest.raises(GraphHistoryConflict, match="cannot be dated before"):
        write(m, graph, T[3], add=[meter("B")])
    assert meter("B") not in graph.graph                          # nothing reached the graph
    assert len(m.history.change_points()) == 2                    # nor the log
    write(m, graph, T[3], add=[meter("C")])                       # other triples may be backdated
    assert m.history.change_points() == [T[1], T[3], T[5]]


def test_an_empty_write_is_not_a_change_point(runtime):
    store, graph, m = runtime
    write(m, graph, T[1], add=[meter("A")])
    write(m, graph, T[2])
    assert m.history.change_points() == [T[1]]


def test_the_model_as_of_a_time_undoes_later_changes(runtime):
    store, graph, m = runtime
    write(m, graph, T[1], add=[meter("A"), meter("B")])         # v1: A, B
    write(m, graph, T[3], remove=[meter("B")])                   # v2: A
    write(m, graph, T[6], add=[meter("B"), meter("C")])          # v3: A, B, C
    h = m.history
    assert members(h.as_of(None)) == []                          # before the log
    assert members(h.as_of(T[0])) == []
    assert members(h.as_of(T[1])) == ["A", "B"]
    assert members(h.as_of(T[2])) == ["A", "B"]
    assert members(h.as_of(T[3])) == ["A"]
    assert members(h.as_of(T[5])) == ["A"]
    assert members(h.as_of(T[6])) == ["A", "B", "C"]
    assert members(h.as_of(T[9])) == ["A", "B", "C"]
    assert h.as_of(T[2]) is h.as_of(T[1])                        # one version between change points


def test_a_version_is_rebuilt_after_a_new_write(runtime):
    store, graph, m = runtime
    write(m, graph, T[1], add=[meter("A")])
    before = m.history.as_of(T[1])
    write(m, graph, T[1], add=[meter("B")])                      # same effective time, later write
    after = m.history.as_of(T[1])
    assert after is not before and members(after) == ["A", "B"]


def test_a_backdated_write_shows_in_versions_after_its_time(runtime):
    store, graph, m = runtime
    write(m, graph, T[5], add=[meter("A")])
    write(m, graph, T[2], add=[meter("B")])                      # backdated, different triple
    h = m.history
    assert members(h.as_of(T[2])) == ["B"]
    assert members(h.as_of(T[5])) == ["A", "B"]


def test_the_initial_load_of_an_empty_graph_is_not_a_change_point(tmp_path):
    store = DuckDBStore(tmp_path / "initial.duckdb")
    graph = MemoryGraph()
    m = Materializer(store, graph)
    try:
        write(m, graph, T[5], add=[meter("A"), meter("B")])          # the model arrives, dated or not
        assert m.history.change_points() == []
        assert members(m.history.as_of(None)) == ["A", "B"]          # it holds before its own time
        assert members(m.history.as_of(T[0])) == ["A", "B"]
        write(m, graph, T[7], remove=[meter("B")])                   # the first real change
        assert m.history.change_points() == [T[7]]
        assert members(m.history.as_of(T[6])) == ["A", "B"] and members(m.history.as_of(T[7])) == ["A"]
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM materialization_graph_changes").fetchone()[0] == 2
    finally:
        m.close(); store.close()


def test_a_direct_graph_write_still_records_the_pending_change(runtime):
    store, graph, m = runtime
    with m.graph_change(T[4]):
        graph.graph.add(meter("A")); graph.version += 1
    with store._own_conn() as conn:
        assert conn.execute("SELECT effective_at FROM materialization_graph_changes").fetchall() == [(T[4].isoformat(),)]
    assert m.history.change_points() == []


def test_http_sparql_with_as_of_goes_to_the_history_and_change_points_are_served(monkeypatch):
    from types import SimpleNamespace
    from acquirium.Server import app as server
    calls = []
    manager = SimpleNamespace(
        sparql_json=lambda *a, **k: calls.append(("json", k)) or b"{}",
        sparql_dict=lambda q, **k: calls.append(("dict", k)) or {"columns": [], "rows": []},
        graph_change_points=lambda *, start, end: [T[3]] if start is None or start < T[3] else [],
    )
    monkeypatch.setattr(server.app.state, "manager", manager, raising=False)
    server.sparql_json_post(server.SparqlQueryRequest(query="SELECT * WHERE { ?s ?p ?o }", as_of=T[2]))
    assert calls == [("dict", {"include_dependencies": True, "wait_for_fresh": False, "as_of": T[2]})]
    assert server.graph_change_points(start=T[1]) == {"change_points": [T[3].isoformat()]}
    assert server.graph_change_points(start=T[5]) == {"change_points": []}
