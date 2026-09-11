"""``max_depth=0`` means unbounded on every edge form.

Program edges (``via="any"``, single predicates, nearest) already walked
unbounded client-side. Predicate lists and ``direction=`` compile to SPARQL
and used to raise or emit an empty path at ``hops=0``; they now render as
transitive property paths. Execution tests run the compiled SPARQL with
rdflib over a four-node chain.
"""

import re

import pytest
from rdflib import Graph, Namespace, RDF, URIRef

from acquirium.Client.explore import Query
from acquirium.Client.explore.compile import compile_sparql
from acquirium.Client.query_graph import QueryEdge, QueryGraph, QueryNode
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE, S223

X = Namespace("urn:x/")
CLS = Namespace("urn:x#")
P = "urn:x#next"


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def two() -> QueryGraph:
    g = QueryGraph().with_node(QueryNode(id=0, alias="a", constraints={}))
    return g.with_node(QueryNode(id=1, alias="b", constraints={}))


class TestCompile:
    def test_direction_unbounded_is_transitive(self):
        s = norm(compile_sparql(two().with_edge(QueryEdge(0, 1, hops=0, direction="downstream"))))
        assert "?v0 () ?v1" not in s
        assert f"(<{S223.connectedTo}>|^<{S223.connectedFrom}>)+" in s
        assert f"(^<{S223.connectsFrom}>/<{S223.connectsTo}>)*/^<{S223.connectsFrom}>" in s

    def test_direction_upstream_unbounded(self):
        s = norm(compile_sparql(two().with_edge(QueryEdge(0, 1, hops=0, direction="upstream"))))
        assert f"(^<{S223.connectedTo}>|<{S223.connectedFrom}>)+" in s

    def test_predicate_list_unbounded(self):
        s = norm(compile_sparql(two().with_edge(QueryEdge(0, 1, hops=0, predicates=[P, "^urn:x#q"]))))
        assert f"?v0 (<{P}>|^<urn:x#q>)+ ?v1 ." in s
        assert f"?v0 <{S223.hasConnectionPoint}>/(<{P}>|^<urn:x#q>)+ ?v1 ." in s

    def test_predicate_list_unbounded_with_cp_filter(self):
        s = norm(compile_sparql(two().with_edge(
            QueryEdge(0, 1, hops=0, predicates=[P], cp_filter="urn:x#Out"))))
        assert f"?cp_e0 a <urn:x#Out> . ?cp_e0 (<{P}>)+ ?v1 ." in s

    def test_any_predicate_unbounded_still_raises(self):
        with pytest.raises(ValueError, match="any-predicate"):
            compile_sparql(two().with_edge(QueryEdge(0, 1, hops=0)))


class TestBuilder:
    def q(self) -> Query:
        return Query(client=None)

    def test_related_predicate_list_unbounded(self):
        b = self.q().entity(CLS.A, alias="a").related(CLS.B, alias="b", via=[P], max_depth=0)
        (edge,) = b.query_graph.edges
        assert edge.hops == 0 and edge.predicates == [P]
        assert f"(<{P}>)+" in b.to_sparql()

    def test_related_direction_unbounded(self):
        b = self.q().entity(CLS.A, alias="a").related(CLS.B, alias="b", direction="upstream", max_depth=0)
        assert "+" in b.to_sparql() and "()" not in b.to_sparql()

    def test_measurement_direction_unbounded(self):
        b = self.q().entity(CLS.A, alias="a").measurement(direction="downstream", max_depth=0)
        mid_edge, _ = b.query_graph.edges
        assert mid_edge.hops == 0
        assert "()" not in b.to_sparql()

    def test_measurement_nearest_unbounded_passes_zero_through(self):
        b = self.q().entity(CLS.A, alias="a").measurement(direction="downstream", nearest=True, max_depth=0)
        (edge,) = b.query_graph.edges
        assert edge.hops == 0 and edge.nearest
        b3 = self.q().entity(CLS.A, alias="a").measurement(direction="downstream", nearest=True, max_depth=3)
        assert b3.query_graph.edges[0].hops == 4


# ------------------------------------------------------------- execution

def chain() -> Graph:
    """e1 -> e2 -> e3 -> e4 by connectedTo and by urn:x#next; a point on e4."""
    g = Graph()
    nodes = [X.e1, X.e2, X.e3, X.e4]
    for n in nodes:
        g.add((n, RDF.type, CLS.Unit))
    for a, b in zip(nodes, nodes[1:]):
        g.add((a, S223.connectedTo, b))
        g.add((a, URIRef(P), b))
    g.add((X.e4, S223.hasProperty, X.p4))
    g.add((X.p4, URIRef(HAS_EXTERNAL_REFERENCE), URIRef("urn:x/p4-ref")))
    return g


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


def targets(query: Query, col: str) -> set:
    res = query.execute()
    i = res["columns"].index(col)
    return {r[i] for r in res["rows"]}


@pytest.fixture
def cq() -> Query:
    return Query(client=FakeClient(chain()))


class TestExecution:
    def test_direction_bounded_vs_unbounded(self, cq):
        base = cq.entity(uri=X.e1, alias="s")
        assert targets(base.related(CLS.Unit, alias="t", direction="downstream", max_depth=1), "v1") == {str(X.e2)}
        assert targets(base.related(CLS.Unit, alias="t", direction="downstream", max_depth=2), "v1") == {str(X.e2), str(X.e3)}
        assert targets(base.related(CLS.Unit, alias="t", direction="downstream", max_depth=0), "v1") == {str(X.e2), str(X.e3), str(X.e4)}

    def test_predicate_list_bounded_vs_unbounded(self, cq):
        base = cq.entity(uri=X.e1, alias="s")
        assert targets(base.related(CLS.Unit, alias="t", via=[P], max_depth=1), "v1") == {str(X.e2)}
        assert targets(base.related(CLS.Unit, alias="t", via=[P], max_depth=0), "v1") == {str(X.e2), str(X.e3), str(X.e4)}

    def test_measurement_direction_unbounded_reaches_far_point(self, cq):
        base = cq.entity(uri=X.e1, alias="s")
        assert targets(base.measurement(direction="downstream", max_depth=1, alias="m"), "v2") == set()
        assert targets(base.measurement(direction="downstream", max_depth=0, alias="m"), "v2") == {str(X.p4)}

    def test_measurement_nearest_unbounded_reaches_far_point(self, cq):
        from acquirium.Client.explore.traverse import resolve_program_edges
        base = cq.entity(uri=X.e1, alias="s")
        # bounded: the walk finds nothing (rdflib cannot evaluate the empty
        # VALUES the compiler then emits, so check the resolved edge instead)
        bounded = base.measurement(direction="downstream", nearest=True, max_depth=1, alias="m")
        resolved = resolve_program_edges(bounded.query_graph, cq.client)
        assert resolved.edges[0].value_pairs == ()
        assert targets(base.measurement(direction="downstream", nearest=True, max_depth=0, alias="m"), "v1") == {str(X.p4)}
