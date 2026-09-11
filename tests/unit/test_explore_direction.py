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
        assert points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=1)) == DOWN_1

    def test_two_steps(self, cq):
        assert points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=2)) == DOWN_2

    def test_unbounded(self, cq):
        assert points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=0)) == DOWN_2

    def test_never_the_sources_own_or_inlet_points(self, cq):
        got = points(cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=3))
        assert not {"A_p", "A_in_p"} & got

    def test_from_the_last_unit_only_its_outlet(self, cq):
        assert points(cq.entity(uri=X.C, alias="c").measurement(direction="downstream", max_depth=2)) == {"C_out_p"}


class TestUpstream:
    def test_one_step(self, cq):
        assert points(cq.entity(uri=X.C, alias="c").measurement(direction="upstream", max_depth=1)) == UP_1

    def test_unbounded(self, cq):
        got = points(cq.entity(uri=X.C, alias="c").measurement(direction="upstream", max_depth=0))
        assert got == UP_1 | {"pipe1_p", "A_p", "A_in_p", "A_out_p"}


class TestIntermediateColumn:
    def test_exposes_what_each_point_hangs_off(self, cq):
        res = cq.entity(uri=X.A, alias="a").measurement(direction="downstream", max_depth=1).execute()
        mid = res["columns"].index("v1")
        assert {r[mid].rsplit("/", 1)[-1] for r in res["rows"]} == {"A_out", "pipe1", "B"}
