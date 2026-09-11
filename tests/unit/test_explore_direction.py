"""What ``measurement(direction=...)`` returns, checked on a real chain.

A -pipe1-> B -pipe2-> C, every node with its own property, every equipment
with an inlet and an outlet connection point that each carry a property.
Downstream of A at one step is: A's outlet, pipe1, B, B's inlet and outlet.
"""

import pytest
from rdflib import Graph, Namespace, RDF, URIRef

from acquirium.Client.explore import Query
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE, S223

X = Namespace("urn:x/")
CLS = Namespace("urn:x#")
QK = URIRef("http://qudt.org/schema/qudt/hasQuantityKind")
FLOW, PRESSURE, TEMP, PH = (URIRef(f"urn:qk#{k}") for k in ("Flow", "Pressure", "Temp", "PH"))
KINDS = {"A_out_p": FLOW, "pipe1_p": PRESSURE, "B_in_p": TEMP, "B_p": TEMP, "B_out_p": TEMP,
         "pipe2_p": PRESSURE, "C_in_p": PH, "C_p": FLOW}


def chain() -> Graph:
    g = Graph()
    eq = [X.A, X.B, X.C]
    for e in eq:
        g.add((e, RDF.type, CLS.Unit))
        for side, cls in (("in", S223.InletConnectionPoint), ("out", S223.OutletConnectionPoint)):
            cp = URIRef(f"{e}_{side}")
            g.add((cp, RDF.type, cls))
            g.add((e, S223.hasConnectionPoint, cp))
            _prop(g, cp, URIRef(f"{e}_{side}_p"))
        _prop(g, e, URIRef(f"{e}_p"))
    for i, (a, b) in enumerate(zip(eq, eq[1:]), start=1):
        pipe = URIRef(f"urn:x/pipe{i}")
        g.add((pipe, RDF.type, S223.Connection))
        g.add((pipe, S223.connectsFrom, a))
        g.add((pipe, S223.connectsTo, b))
        g.add((pipe, S223.connectsAt, URIRef(f"{a}_out")))
        g.add((pipe, S223.connectsAt, URIRef(f"{b}_in")))
        g.add((a, S223.connectedTo, b))
        g.add((b, S223.connectedFrom, a))
        _prop(g, pipe, URIRef(f"{pipe}_p"))
    return g


def _prop(g: Graph, carrier: URIRef, p: URIRef) -> None:
    g.add((carrier, S223.hasProperty, p))
    g.add((p, URIRef(HAS_EXTERNAL_REFERENCE), URIRef(f"{p}-ref")))
    kind = KINDS.get(str(p).rsplit("/", 1)[-1])
    if kind is not None:
        g.add((p, QK, kind))


class FakeClient:
    base_url = "fake://chain"

    def __init__(self, graph: Graph):
        self.graph = graph

    def sparql_query(self, sparql: str, include_dependencies: bool = True) -> dict:
        res = self.graph.query(sparql)
        cols = [str(v) for v in res.vars]
        return {"columns": cols,
                "rows": [[None if r[i] is None else str(r[i]) for i in range(len(cols))] for r in res]}

    def graph_version(self) -> int:
        return 1

    def compact_uri(self, uri: str) -> str:
        return str(uri)


def points(query: Query, col: str = "v2") -> set:
    res = query.execute()
    i = res["columns"].index(col)
    return {r[i].rsplit("/", 1)[-1] for r in res["rows"]}


@pytest.fixture
def cq() -> Query:
    return Query(client=FakeClient(chain()))


DOWN_1 = {"A_out_p", "pipe1_p", "B_p", "B_in_p", "B_out_p"}
DOWN_2 = DOWN_1 | {"pipe2_p", "C_p", "C_in_p", "C_out_p"}
UP_1 = {"C_in_p", "pipe2_p", "B_p", "B_in_p", "B_out_p"}


class TestDownstream:
    def test_one_step(self, cq):
        assert points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=1, nearest=False)) == DOWN_1

    def test_two_steps(self, cq):
        assert points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=2, nearest=False)) == DOWN_2

    def test_unbounded(self, cq):
        assert points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=0, nearest=False)) == DOWN_2

    def test_never_the_sources_own_or_inlet_points(self, cq):
        got = points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=3, nearest=False))
        assert not {"A_p", "A_in_p"} & got

    def test_from_the_last_unit_only_its_outlet(self, cq):
        assert points(cq.entity(uri=X.C, alias="c").measurement(direction="downstream", max_depth=2, nearest=False)) == {"C_out_p"}


class TestUpstream:
    def test_one_step(self, cq):
        assert points(cq.entity(uri=X.C, alias="c").measurement(direction="upstream", max_depth=1, nearest=False)) == UP_1

    def test_unbounded(self, cq):
        got = points(cq.entity(uri=X.C, alias="c").measurement(direction="upstream", max_depth=0, nearest=False))
        assert got == UP_1 | {"pipe1_p", "A_p", "A_in_p", "A_out_p"}


class TestIntermediateColumn:
    def test_exposes_what_each_point_hangs_off(self, cq):
        res = cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=1, nearest=False).execute()
        mid = res["columns"].index("v1")
        assert {r[mid].rsplit("/", 1)[-1] for r in res["rows"]} == {"A_out", "pipe1", "B"}


class TestNearest:
    """Nearest walks the places in order and keeps each source's first hit."""

    def test_default_is_nearest_and_stops_at_own_outlet(self, cq):
        assert points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream")) == {"A_out_p"}

    def test_filter_walks_past_places_without_a_match(self, cq):
        a = cq.entity(uri=X.A, alias="a")
        assert points(a.measurement(direction="downstream", quantity_kind=PRESSURE)) == {"pipe1_p"}
        assert points(a.measurement(direction="downstream", quantity_kind=TEMP)) == {"B_in_p", "B_p", "B_out_p"}
        assert points(a.measurement(direction="downstream", quantity_kind=PH, max_depth=1)) == set()
        assert points(a.measurement(direction="downstream", quantity_kind=PH, max_depth=2)) == {"C_in_p"}
        assert points(a.measurement(direction="downstream", quantity_kind=PH, max_depth=0)) == {"C_in_p"}

    def test_where_after_the_fact_also_counts(self, cq):
        q = cq.entity(uri=X.A, alias="a").measurement(direction="downstream", alias="m").where(quantity_kind=PRESSURE)
        assert points(q) == {"pipe1_p"}

    def test_each_source_keeps_its_own_first_place(self, cq):
        res = cq.entity(CLS.Unit, alias="u").measurement(direction="downstream", quantity_kind=FLOW).execute()
        cols = res["columns"]
        got = {(r[cols.index("v0")].rsplit("/", 1)[-1], r[cols.index("v2")].rsplit("/", 1)[-1]) for r in res["rows"]}
        # A: its own outlet is a flow; B: nothing until C itself; C: nothing downstream
        assert got == {("A", "A_out_p"), ("B", "C_p")}

    def test_upstream_nearest(self, cq):
        c = cq.entity(uri=X.C, alias="c")
        assert points(c.measurement(direction="upstream")) == {"C_in_p"}
        assert points(c.measurement(direction="upstream", quantity_kind=FLOW)) == {"A_out_p"}

    def test_dropped_source_column_is_stripped(self, cq):
        res = cq.entity(uri=X.A, alias="a").drop().measurement(direction="downstream", quantity_kind=PRESSURE).execute()
        assert "v0" not in res["columns"]
        assert {r[res["columns"].index("v2")].rsplit("/", 1)[-1] for r in res["rows"]} == {"pipe1_p"}

    def test_related_nearest_along_the_flow(self, cq):
        a = cq.entity(uri=X.A, alias="a")
        units = a.related(CLS.Unit, alias="t", direction="downstream", nearest=True).execute()
        assert {r[1].rsplit("/", 1)[-1] for r in units["rows"]} == {"B"}
        pipes = a.related(S223.Connection, alias="t", direction="downstream", nearest=True).execute()
        assert {r[1].rsplit("/", 1)[-1] for r in pipes["rows"]} == {"pipe1"}
        everything = a.related(CLS.Unit, alias="t", direction="downstream", nearest=False, max_depth=2).execute()
        assert {r[1].rsplit("/", 1)[-1] for r in everything["rows"]} == {"B", "C"}
