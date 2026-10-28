"""Model history: the recorded model must match the formal definition.

The model at time t is every recorded change whose interval contains t,
applied in recording order. ``Reference`` implements that definition and the
write rules directly in Python; the store under test keeps a bitemporal
triple table instead and must agree with it everywhere.
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from rdflib import BNode, Graph, Literal, URIRef

import acquirium.Storage.graph_store as graph_store_module
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.graph_registry import ACQUIRIUM_GRAPH_URI, PLANT_GRAPH_URI, source_graph_uri
from acquirium.Storage.model_history import (
    NEG_INF,
    POS_INF,
    ModelHistory,
    Step,
    from_micros,
    merge,
    nt,
    parse_lines,
    symmetric_difference,
    to_micros,
)

PLANT = PLANT_GRAPH_URI
SOURCE = str(source_graph_uri("drv"))
DAY = 86_400 * 1_000_000
BASE = to_micros(datetime(2026, 1, 1, tzinfo=timezone.utc))


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


class FakeOntoEnv:
    def __init__(self, *args, **kwargs):
        self.add = MagicMock()
        self.close = MagicMock()

    @classmethod
    def connect(cls, *args, **kwargs):
        return cls()

    def import_dependencies(self, graph):
        return None


@pytest.fixture
def clock(monkeypatch):
    clock = Clock(BASE + 100 * DAY)
    monkeypatch.setattr(graph_store_module, "now_micros", clock)
    return clock


def store_opener(tmp_path, monkeypatch, timeseries_store):
    """Open (or reopen) a graph store with history over the same files.
    *timeseries_store* opens the database the history lives in."""
    monkeypatch.setattr(graph_store_module, "OntoEnv", FakeOntoEnv)
    monkeypatch.setattr(graph_store_module, "load_bundled_graph", lambda fname, canonical: Graph())
    # Inference is not under test; keep the derived-graph thread out of it.
    monkeypatch.setattr(graph_store_module.OxigraphGraphStore, "_start_query_rebuild_locked", lambda self: None)
    opened = []

    def open_(*, history: bool = True):
        timeseries = timeseries_store()
        graphs = graph_store_module.OxigraphGraphStore(
            store_path=tmp_path / "graph",
            env_root=tmp_path / "env",
            history=ModelHistory(timeseries) if history else None,
        )
        opened.append((graphs, timeseries))
        return graphs

    def close():
        for graphs, timeseries in opened:
            graphs.close()
            timeseries.close()

    return open_, close


@pytest.fixture
def open_store(tmp_path, monkeypatch, clock):
    open_, close = store_opener(tmp_path, monkeypatch, lambda: DuckDBStore(tmp_path / "ts.duckdb"))
    yield open_
    close()


def at(micros: int) -> datetime | str:
    return from_micros(micros)


def live(graphs, uri: str) -> set[str]:
    return {nt(t) for t in graphs.source_dataset.graph(URIRef(uri))}


def triple(s: str, p: str = "p", o: str = "o") -> tuple:
    return (URIRef(f"urn:{s}"), URIRef(f"urn:{p}"), URIRef(f"urn:{o}"))


def turtle(*triples) -> str:
    graph = Graph()
    for t in triples:
        graph.add(t)
    return graph.serialize(format="nt")


# -------------------- reference implementation of the definition --------------------
ALL = "ALL"


class Reference:
    """Changes ``(graph, deleted, inserted, start, end, r)``; ``deleted`` may
    be ``ALL`` (a whole-graph write). One write may add several changes."""

    def __init__(self) -> None:
        self.changes: list[tuple] = []
        self.r = 0
        self.undated: set[str] = set()

    def model(self, graph: str, t: int, as_of: int | None = None) -> set[str]:
        current: set[str] = set()
        for g, deleted, inserted, start, end, r in sorted(self.changes, key=lambda c: c[5]):
            if g != graph or not start <= t < end or (as_of is not None and r > as_of):
                continue
            current = set() if deleted == ALL else current - deleted
            current |= inserted
        return current

    def touch(self, graph: str, line: str, after: int) -> int:
        bounds = [
            b
            for g, deleted, inserted, start, end, _ in self.changes
            if g == graph and (deleted == ALL or line in deleted or line in inserted)
            for b in (start, end)
            if b > after
        ]
        return min(bounds, default=POS_INF)

    def pieces(self, graph: str, start: int, end: int) -> list[tuple[int, int]]:
        bounds = sorted({b for c in self.changes if c[0] == graph for b in c[3:5] if start < b < end})
        edges = [start, *bounds, end]
        return list(zip(edges, edges[1:]))

    def interval(self, graph: str, start, end, now: int) -> tuple[int, int | None]:
        if start is None:
            start = now if graph in self.undated else NEG_INF
            self.undated.add(graph)
        return start, end

    def insert(self, graph, lines, start, end, now, replace=False):
        start, end = self.interval(graph, start, end, now)
        self.r += 1
        if replace:
            self.changes.append((graph, ALL, set(lines), start, POS_INF if end is None else end, self.r))
        elif end is not None:
            self.changes.append((graph, set(), set(lines), start, end, self.r))
        else:
            present = self.model(graph, start)
            for line in set(lines) - present:
                self.changes.append((graph, set(), {line}, start, self.touch(graph, line, start), self.r))

    def update(self, graph, update, start, end, now):
        start, end = self.interval(graph, start, end, now)
        self.r += 1

        def run(lines):
            g = parse_lines(lines)
            g.update(update)
            return {nt(t) for t in g}

        if end is None:
            before = self.model(graph, start)
            after = run(before)
            ends = {line: self.touch(graph, line, start) for line in before ^ after}
            for line in before - after:
                self.changes.append((graph, {line}, set(), start, ends[line], self.r))
            for line in after - before:
                self.changes.append((graph, set(), {line}, start, ends[line], self.r))
        else:
            for left, right in self.pieces(graph, start, end):
                before = self.model(graph, left)
                after = run(before)
                if before != after:
                    self.changes.append((graph, before - after, after - before, left, right, self.r))


UPDATES = (
    "DELETE { ?s <urn:p0> ?o } INSERT { ?s <urn:p0> <urn:o1> } WHERE { ?s <urn:p0> ?o }",
    "INSERT { ?s <urn:p1> <urn:o0> } WHERE { ?s <urn:p0> <urn:o0> }",
    "DELETE DATA { <urn:s0> <urn:p0> <urn:o0> }",
    "INSERT DATA { <urn:s1> <urn:p1> <urn:o1> }",
    "DELETE WHERE { <urn:s2> ?p ?o }",
)
UNIVERSE = [triple(f"s{s}", f"p{p}", f"o{o}") for s in range(4) for p in range(2) for o in range(2)]


def check(graphs, reference: Reference, now: int) -> None:
    for uri in (PLANT, SOURCE):
        if uri not in graphs.history.graphs():
            assert not live(graphs, uri)
            continue
        pieces = graphs.history.versions(uri, NEG_INF, POS_INF)
        assert pieces[0][0] == NEG_INF and pieces[-1][1] == POS_INF
        for (_, right, _), (left, _, _) in zip(pieces, pieces[1:]):
            assert right == left
        for left, right, content in pieces:
            # the content is constant inside a version, and versions are maximal
            for point in {left, right - 1, (left // 2 + right // 2)}:
                assert set(content) == reference.model(uri, point), (uri, left, right, point)
        for (_, _, a), (_, _, b) in zip(pieces, pieces[1:]):
            assert a != b
        assert live(graphs, uri) == reference.model(uri, now)


@pytest.mark.parametrize("seed", range(10))
def test_random_writes_match_the_definition(open_store, clock, seed):
    rng = random.Random(seed)
    graphs = open_store()
    reference = Reference()
    snapshots = []
    for _ in range(18):
        clock.now += DAY
        now = clock.now
        graph = rng.choice((PLANT, SOURCE))
        start = rng.choice((None, None, NEG_INF, BASE + rng.randrange(0, 100) * DAY))
        lower = BASE if start in (None, NEG_INF) else start
        end = rng.choice((None, None, POS_INF, lower + rng.randrange(1, 30) * DAY))
        if start is None and end not in (None, POS_INF):
            end = None  # a default start is now, and no end may follow it but +inf
        if end not in (None, POS_INF) and end > now:
            end = now
        if start not in (None,) and end is not None and end <= start:
            end = None
        kind = rng.choice(("insert", "replace", "update"))
        kwargs = {
            "effective_from": None if start is None else at(start),
            "effective_until": None if end is None else at(end),
        }
        if kind == "update":
            update = rng.choice(UPDATES)
            graphs.sparql_update(update, graph_uri=URIRef(graph), **kwargs)
            reference.update(graph, update, start, end, now)
        else:
            chosen = rng.sample(UNIVERSE, rng.randrange(0, 6))
            graphs.insert_graph(turtle(*chosen), format="nt", replace=kind == "replace", graph_uri=URIRef(graph), **kwargs)
            reference.insert(graph, [nt(t) for t in chosen], start, end, now, replace=kind == "replace")
        check(graphs, reference, now)
        snapshots.append((graphs.history.head(), {(g, p): reference.model(g, p) for g in (PLANT, SOURCE) for p in (BASE + 10 * DAY, BASE + 50 * DAY, now)}))
    # what each earlier recording time knew stays as it was
    for head, known in snapshots:
        for (g, point), content in known.items():
            assert graphs.history.graph_at(g, point, as_of=head) == content


# -------------------- the examples --------------------
def test_a_fix_replaces_the_model_from_its_start(open_store, clock):
    graphs = open_store()
    g1, g2 = triple("pump"), triple("valve")
    graphs.insert_graph(turtle(g1), format="nt", replace=True, graph_uri=URIRef(PLANT))
    graphs.insert_graph(turtle(g2), format="nt", replace=True, graph_uri=URIRef(PLANT),
                        effective_from=at(BASE + 30 * DAY))
    history = graphs.history
    assert history.graph_at(PLANT, NEG_INF) == {nt(g1)}
    assert history.graph_at(PLANT, BASE + 30 * DAY - 1) == {nt(g1)}
    assert history.graph_at(PLANT, BASE + 30 * DAY) == {nt(g2)}
    assert live(graphs, PLANT) == {nt(g2)}


def test_a_late_entry_keeps_a_later_recorded_change(open_store, clock):
    graphs = open_store()
    basin = URIRef("urn:basin")
    role = URIRef("urn:hasRole")
    aerobic = f"<{basin}> <{role}> <urn:Aerobic>"
    anoxic = f"<{basin}> <{role}> <urn:Anoxic>"
    graphs.insert_graph(f"{aerobic} .", format="nt", replace=True, graph_uri=URIRef(PLANT))
    switch = "DELETE DATA {{ {} }} ; INSERT DATA {{ {} }}"
    june1, june15, may20 = BASE + 151 * DAY, BASE + 165 * DAY, BASE + 139 * DAY
    clock.now = june1
    graphs.sparql_update(switch.format(aerobic, anoxic), graph_uri=URIRef(PLANT))
    clock.now = june15
    graphs.sparql_update(switch.format(anoxic, aerobic), graph_uri=URIRef(PLANT))
    clock.now = june15 + 10 * DAY
    # the switch to anoxic happened on May 20, not June 1
    graphs.sparql_update(switch.format(aerobic, anoxic), graph_uri=URIRef(PLANT), effective_from=at(may20))
    history = graphs.history
    roles = lambda t: {line.split()[2] for line in history.graph_at(PLANT, t)}
    assert roles(may20 - 1) == {"<urn:Aerobic>"}
    assert roles(may20) == {"<urn:Anoxic>"}
    assert roles(june1) == {"<urn:Anoxic>"}
    assert roles(june15) == {"<urn:Aerobic>"}
    assert {line.split()[2] for line in live(graphs, PLANT)} == {"<urn:Aerobic>"}


def test_bounds_in_the_future_are_rejected(open_store, clock):
    graphs = open_store()
    graphs.insert_graph(turtle(triple("a")), format="nt", graph_uri=URIRef(PLANT))
    with pytest.raises(ValueError, match="future"):
        graphs.insert_graph(turtle(triple("b")), format="nt", graph_uri=URIRef(PLANT),
                            effective_from=at(clock.now + 1))
    with pytest.raises(ValueError, match="future"):
        graphs.insert_graph(turtle(triple("b")), format="nt", graph_uri=URIRef(PLANT),
                            effective_from=at(BASE), effective_until=at(clock.now + 1))
    with pytest.raises(ValueError, match="after effective_from"):
        graphs.insert_graph(turtle(triple("b")), format="nt", graph_uri=URIRef(PLANT),
                            effective_from=at(BASE), effective_until=at(BASE))
    with pytest.raises(ValueError, match="infinity"):
        graphs.insert_graph(turtle(triple("b")), format="nt", graph_uri=URIRef(PLANT),
                            effective_from="infinity")
    assert graphs.history.head() == 1


def test_the_first_undated_write_holds_from_minus_infinity(open_store, clock):
    graphs = open_store()
    # a dated registration first, as a driver's startup does
    graphs.insert_graph(turtle(triple("datasource")), format="nt", graph_uri=URIRef(SOURCE),
                        effective_from="-infinity", effective_until="infinity")
    graphs.insert_graph(turtle(triple("model")), format="nt", graph_uri=URIRef(SOURCE))
    clock.now += DAY
    graphs.insert_graph(turtle(triple("later")), format="nt", graph_uri=URIRef(SOURCE))
    early = graphs.history.graph_at(SOURCE, BASE)
    assert nt(triple("model")) in early and nt(triple("later")) not in early
    assert nt(triple("later")) in graphs.history.graph_at(SOURCE, clock.now)


def test_a_registration_over_all_time_returns_after_a_removal(open_store, clock):
    graphs = open_store()
    ref = triple("ref", "storedAt", "table")
    everywhere = {"effective_from": "-infinity", "effective_until": "infinity"}
    graphs.insert_graph(turtle(ref), format="nt", graph_uri=URIRef(SOURCE), **everywhere)
    clock.now += DAY
    removed_at = clock.now
    graphs.sparql_update(f"DELETE DATA {{ {nt(ref)[:-2]} }}", graph_uri=URIRef(SOURCE))
    assert nt(ref) not in live(graphs, SOURCE)
    clock.now += DAY
    graphs.insert_graph(turtle(ref), format="nt", graph_uri=URIRef(SOURCE), **everywhere)
    assert nt(ref) in graphs.history.graph_at(SOURCE, removed_at)
    assert nt(ref) in live(graphs, SOURCE)


def test_an_update_over_an_interval_runs_on_each_version(open_store, clock):
    graphs = open_store()
    p1, p2 = triple("p1", "a", "Pump"), triple("p2", "a", "Pump")
    graphs.insert_graph(turtle(p1), format="nt", replace=True, graph_uri=URIRef(PLANT))
    graphs.insert_graph(turtle(p2), format="nt", graph_uri=URIRef(PLANT), effective_from=at(BASE + 10 * DAY))
    # every pump got a sensor, over [day 5, day 20): p2 only exists from day 10
    graphs.sparql_update(
        "INSERT { ?x <urn:hasSensor> <urn:S> } WHERE { ?x <urn:a> <urn:Pump> }",
        graph_uri=URIRef(PLANT), effective_from=at(BASE + 5 * DAY), effective_until=at(BASE + 20 * DAY),
    )
    sensor = lambda s: nt((URIRef(f"urn:{s}"), URIRef("urn:hasSensor"), URIRef("urn:S")))
    history = graphs.history
    assert sensor("p1") in history.graph_at(PLANT, BASE + 5 * DAY)
    assert sensor("p2") not in history.graph_at(PLANT, BASE + 9 * DAY)
    assert sensor("p2") in history.graph_at(PLANT, BASE + 10 * DAY)
    assert not {sensor("p1"), sensor("p2")} & history.graph_at(PLANT, BASE + 20 * DAY)
    assert not {sensor("p1"), sensor("p2")} & live(graphs, PLANT)


def test_blank_nodes_keep_their_labels(open_store, clock):
    graphs = open_store()
    node = BNode("spec1")
    graphs.insert_graph(
        turtle((URIRef("urn:pump"), URIRef("urn:spec"), node), (node, URIRef("urn:rated"), Literal(5))),
        format="nt", replace=True, graph_uri=URIRef(PLANT),
    )
    graphs.sparql_update(
        "DELETE { ?b <urn:rated> ?v } WHERE { ?b <urn:rated> ?v }",
        graph_uri=URIRef(PLANT), effective_from=at(BASE), effective_until="infinity",
    )
    # the parser picks the label; history and live graph must agree on it
    (line,) = graphs.history.graph_at(PLANT, BASE)
    assert line.startswith("<urn:pump> <urn:spec> _:")
    assert live(graphs, PLANT) == {line}


def test_the_changed_region_of_a_write(open_store, clock):
    graphs = open_store()
    graphs.insert_graph(turtle(triple("a")), format="nt", replace=True, graph_uri=URIRef(PLANT))
    graphs.insert_graph(turtle(triple("a"), triple("b")), format="nt", graph_uri=URIRef(PLANT),
                        effective_from=at(BASE + 10 * DAY), effective_until=at(BASE + 20 * DAY))
    assert graphs.history.changed(2) == {PLANT: [(BASE + 10 * DAY, BASE + 20 * DAY)]}
    # writing what is already there changes nothing
    graphs.insert_graph(turtle(triple("a")), format="nt", graph_uri=URIRef(PLANT),
                        effective_from=at(BASE), effective_until=at(BASE + 5 * DAY))
    assert graphs.history.changed(3) == {}


# -------------------- recovery and scope --------------------
def test_a_restart_rebuilds_live_graphs_that_missed_a_write(open_store, clock):
    graphs = open_store()
    graphs.insert_graph(turtle(triple("a")), format="nt", replace=True, graph_uri=URIRef(PLANT))
    # recorded, then the process died before applying it to the live graph
    graphs.history.record(
        PLANT, kind="insert", start=NEG_INF, end=POS_INF, dated=True,
        steps=[Step(((nt(triple("b")), "+", NEG_INF, POS_INF),))], recorded_at=clock.now,
    )
    graphs.close()
    graphs.history.store.close()
    reopened = open_store()
    assert live(reopened, PLANT) == {nt(triple("a")), nt(triple("b"))}


def test_a_store_from_before_the_history_is_its_initial_load(open_store, clock):
    graphs = open_store(history=False)
    graphs.insert_graph(turtle(triple("a")), format="nt", replace=True, graph_uri=URIRef(PLANT))
    graphs.close()
    reopened = open_store()
    assert reopened.history.graph_at(PLANT, NEG_INF) == {nt(triple("a"))}
    assert reopened.history.graph_at(PLANT, clock.now) == {nt(triple("a"))}


def test_derived_graphs_are_not_versioned(open_store, clock):
    graphs = open_store()
    graphs.insert_graph(turtle(triple("lineage")), format="nt", graph_uri=URIRef(ACQUIRIUM_GRAPH_URI))
    graphs.insert_graph(turtle(triple("run")), format="nt", graph_uri=source_graph_uri("experiment/r1"))
    assert graphs.history.head() == 0
    with pytest.raises(ValueError, match="not versioned"):
        graphs.insert_graph(turtle(triple("x")), format="nt", graph_uri=URIRef(ACQUIRIUM_GRAPH_URI),
                            effective_from="-infinity")


def test_model_at_overlays_metadata_and_skips_derived_graphs(open_store, clock):
    graphs = open_store()
    graphs.insert_graph(turtle(triple("pump", "a", "Pump")), format="nt", replace=True, graph_uri=URIRef(PLANT))
    graphs.insert_graph(turtle(triple("lineage")), format="nt", graph_uri=URIRef(ACQUIRIUM_GRAPH_URI))
    model = graphs.model_at(clock.now)
    assert triple("pump", "a", "Pump") in model
    assert triple("lineage") not in model


# -------------------- interval helpers --------------------
def test_interval_helpers():
    assert merge([(5, 7), (0, 2), (2, 3)]) == [(0, 3), (5, 7)]
    assert symmetric_difference([(0, 10)], [(3, 5)]) == [(0, 3), (5, 10)]
    assert symmetric_difference([(0, 5)], [(5, 10)]) == [(0, 10)]
    assert symmetric_difference([(0, 5)], [(0, 5)]) == []


def test_removed_experiment_nodes_lose_their_metadata_from_now_on(open_store, clock):
    graphs = open_store()
    experiments = source_graph_uri("experiments")
    metadata = str(source_graph_uri("metadata"))
    run, plant = triple("run1", "a", "Run"), triple("pump", "a", "Pump")
    graphs.insert_graph(turtle(run), format="nt", graph_uri=experiments)
    graphs.insert_graph(turtle(plant), format="nt", replace=True, graph_uri=URIRef(PLANT))
    note = lambda s: (URIRef(f"urn:{s}"), URIRef("urn:note"), Literal("x"))
    graphs.insert_graph(turtle(note("run1"), note("pump")), format="nt", graph_uri=URIRef(metadata))
    clock.now += DAY
    graphs.clear_graphs([experiments])
    graphs.forget_metadata({"urn:run1", "urn:pump"})
    # the pump is still in the plant graph, so its metadata stays
    assert live(graphs, metadata) == {nt(note("pump"))}
    assert graphs.history.graph_at(metadata, clock.now - 1) == {nt(note("run1")), nt(note("pump"))}
