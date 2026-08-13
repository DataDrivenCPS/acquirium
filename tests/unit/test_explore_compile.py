"""Parity tests: explore.compile.compile_sparql vs the legacy Q.to_sparql.

The legacy compiler never touches ``self.client``, so ``Q(client=None,
query_graph=g).to_sparql()`` runs without a server. Negation differs only in
the marker class (legacy private ``_Exclude`` vs explore ``Not``), so those
cases build two structurally identical graphs, one per marker.
"""

import re

import pytest

from acquirium.Client.explore.attributes import Not
from acquirium.Client.explore.compile import compile_sparql
from acquirium.Client.query import Q, _Exclude
from acquirium.Client.query_graph import DataNodeInfo, QueryEdge, QueryGraph, QueryNode

CLS_A = "urn:test#TypeA"
CLS_B = "urn:test#TypeB"
INST = "urn:test:instance#x1"
PRED_P = "urn:test#p"
PRED_Q = "urn:test#q"
CP_CLASS = "http://data.ashrae.org/standard223#InletConnectionPoint"
QK = "http://qudt.org/schema/qudt/hasQuantityKind"
MEDIUM = "http://data.ashrae.org/standard223#ofMedium"


@pytest.fixture(autouse=True)
def no_hidden_defaults():
    """Parity vs the legacy compiler needs the hidden-predicate filters off."""
    from acquirium.Client.explore.hidden import hidden_predicates, unhide
    unhide(*hidden_predicates())
    yield
    unhide()


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def legacy_sparql(g: QueryGraph) -> str:
    return Q(client=None, query_graph=g).to_sparql()


def entity(g: QueryGraph, nid: int, *, cls: str | None = None, uri: str | None = None,
           alias: str | None = None) -> QueryGraph:
    constraints = {}
    if cls:
        constraints["rdf_class"] = cls
    if uri:
        constraints["instance_uri"] = uri
    return g.with_node(QueryNode(id=nid, alias=alias, constraints=constraints))


def data_node(g: QueryGraph, nid: int, *, filters: dict) -> QueryGraph:
    g = g.with_node(QueryNode(id=nid, constraints={"is_data_node": True}))
    return g.with_data_node(DataNodeInfo(node_id=nid, filters=filters))


def assert_parity(g: QueryGraph, g_legacy: QueryGraph | None = None):
    new = compile_sparql(g)
    old = legacy_sparql(g_legacy if g_legacy is not None else g)
    assert norm(new) == norm(old)


class TestNodeConstraints:
    def test_class_only(self):
        g = entity(QueryGraph(), 0, cls=CLS_A)
        assert "subClassOf" in compile_sparql(g)
        assert_parity(g)

    def test_instance_uri_values(self):
        g = entity(QueryGraph(), 0, uri=INST)
        assert f"VALUES ?v0 {{ <{INST}> }}" in compile_sparql(g)
        assert_parity(g)

    def test_class_and_instance(self):
        g = entity(QueryGraph(), 0, cls=CLS_A, uri=INST)
        assert_parity(g)

    def test_unconstrained_node(self):
        g = entity(QueryGraph(), 0)
        assert_parity(g)


class TestEdges:
    def two_nodes(self) -> QueryGraph:
        g = entity(QueryGraph(), 0, cls=CLS_A)
        return entity(g, 1, cls=CLS_B)

    @pytest.mark.parametrize("hops", [1, 2, 3])
    def test_predicates(self, hops):
        g = self.two_nodes().with_edge(
            QueryEdge(source_id=0, target_id=1, hops=hops, predicates=[PRED_P, PRED_Q]))
        assert_parity(g)

    @pytest.mark.parametrize("hops", [1, 2, 3])
    def test_unconstrained(self, hops):
        g = self.two_nodes().with_edge(QueryEdge(source_id=0, target_id=1, hops=hops))
        assert_parity(g)

    @pytest.mark.parametrize("direction", ["upstream", "downstream"])
    @pytest.mark.parametrize("hops", [1, 3])
    def test_direction(self, direction, hops):
        g = self.two_nodes().with_edge(
            QueryEdge(source_id=0, target_id=1, hops=hops, direction=direction))
        assert_parity(g)

    @pytest.mark.parametrize("hops", [1, 2])
    @pytest.mark.parametrize("predicates", [None, [PRED_P]])
    def test_cp_filter(self, hops, predicates):
        g = self.two_nodes().with_edge(
            QueryEdge(source_id=0, target_id=1, hops=hops, predicates=predicates,
                      cp_filter=CP_CLASS))
        assert_parity(g)

    def test_inverse_predicate(self):
        g = self.two_nodes().with_edge(
            QueryEdge(source_id=0, target_id=1, hops=1, predicates=[f"^{PRED_P}"]))
        assert_parity(g)

    def test_hops_below_one_raises(self):
        g = self.two_nodes().with_edge(QueryEdge(source_id=0, target_id=1, hops=0))
        with pytest.raises(ValueError):
            compile_sparql(g)
        with pytest.raises(ValueError):
            legacy_sparql(g)


class TestDataNodes:
    def with_data(self, filters: dict) -> QueryGraph:
        g = entity(QueryGraph(), 0, cls=CLS_A)
        g = data_node(g, 1, filters=filters)
        return g.with_edge(QueryEdge(source_id=0, target_id=1, hops=1))

    def test_ext_and_unit_optionals(self):
        g = self.with_data({})
        sparql = compile_sparql(g)
        assert "?ext1" in sparql and "?unit1" in sparql and "?extunit1" in sparql
        assert_parity(g)

    def test_scalar_uri_filter(self):
        g = self.with_data({QK: "http://qudt.org/vocab/quantitykind/MassFlowRate"})
        assert_parity(g)

    def test_list_filter(self):
        g = self.with_data({MEDIUM: ["urn:test#M1", "urn:test#M2", None]})
        assert_parity(g)

    def test_literal_filter(self):
        g = self.with_data({"urn:acquirium#dataSource": "Lab"})
        assert_parity(g)

    def test_none_filter_skipped(self):
        g = self.with_data({QK: None})
        assert_parity(g)

    @pytest.mark.parametrize("value", [
        "urn:test#M1",                     # scalar URI
        ["urn:test#M1", "urn:test#M2"],    # list
        ["urn:test#M1"],                   # single-item list
        "Lab",                             # literal
    ])
    def test_negation(self, value):
        g_new = self.with_data({MEDIUM: Not(value)})
        g_old = self.with_data({MEDIUM: _Exclude(value)})
        assert "FILTER NOT EXISTS" in compile_sparql(g_new)
        assert_parity(g_new, g_old)


class TestFullChain:
    def test_entity_cp_data_chain(self):
        """The soft-sensor RO shape: entity -> CP class -> data with filters."""
        g = entity(QueryGraph(), 0, cls="urn:nawi-water-ontology#ReverseOsmosisMembrane", alias="ro")
        g = entity(g, 1, cls="http://data.ashrae.org/standard223#OutletConnectionPoint", alias="ro_out")
        g = g.with_edge(QueryEdge(source_id=0, target_id=1, hops=1))
        g = data_node(g, 2, filters={
            QK: "http://qudt.org/vocab/quantitykind/MassFlowRate",
            MEDIUM: Not("urn:nawi-water-ontology#Water-Brine"),
        })
        g = g.with_edge(QueryEdge(source_id=1, target_id=2, hops=1))

        g_old = entity(QueryGraph(), 0, cls="urn:nawi-water-ontology#ReverseOsmosisMembrane", alias="ro")
        g_old = entity(g_old, 1, cls="http://data.ashrae.org/standard223#OutletConnectionPoint", alias="ro_out")
        g_old = g_old.with_edge(QueryEdge(source_id=0, target_id=1, hops=1))
        g_old = data_node(g_old, 2, filters={
            QK: "http://qudt.org/vocab/quantitykind/MassFlowRate",
            MEDIUM: _Exclude("urn:nawi-water-ontology#Water-Brine"),
        })
        g_old = g_old.with_edge(QueryEdge(source_id=1, target_id=2, hops=1))

        assert_parity(g, g_old)


class TestMultiMeasurementUnion:
    """2+ measurement nodes compile as UNION branches, not a cross-product join."""

    def _graph(self):
        from acquirium.Client.explore.core import Query
        return (Query(client=None).entity(CLS_A, alias="pump")
                .related(CLS_B, alias="tank", via=[PRED_P])
                .measurement(frm=["pump", "tank"]))

    def test_data_blocks_are_union_branches(self):
        s = self._graph().to_sparql()
        body = s[s.index("WHERE"):]
        # both ext requirements present, in separate UNION branches
        assert "?v0 <https://brickschema.org/schema/Brick/ref#hasExternalReference>" not in body
        assert body.count("hasExternalReference") == 2
        pump_block = body.index("?ext2")
        tank_block = body.index("?ext3")
        assert pump_block < body.index("} UNION {", pump_block) < tank_block

    def test_shared_entity_pattern_compiles_once(self):
        s = self._graph().to_sparql()
        assert s.count(f"<{CLS_A}>") == 1 and s.count(f"<{CLS_B}>") == 1

    def test_parses_and_projects_all_columns(self):
        from rdflib.plugins.sparql import prepareQuery
        s = self._graph().to_sparql()
        prepareQuery(s)
        first = s.splitlines()[0]
        for var in ("?v0", "?v1", "?v2", "?v3", "?ext2", "?ext3"):
            assert var in first

    def test_data_node_include_stays_inside_branch(self):
        from acquirium.Client.explore.core import Query
        b = (Query(client=None).entity(CLS_A, alias="pump")
             .related(CLS_B, alias="tank", via=[PRED_P])
             .measurement(frm=["pump", "tank"])
             .include("unit", of="pump_data"))
        body = b.to_sparql()
        body = body[body.index("WHERE"):]
        bind = body.index("?attr2_unit .")
        assert body.index("?ext2") < bind < body.index("?ext3")  # inside pump_data's branch

    def test_single_measurement_unchanged(self):
        from acquirium.Client.explore.core import Query
        s = (Query(client=None).entity(CLS_A, alias="pump").measurement(alias="m")
             .to_sparql())
        body = s[s.index("WHERE"):]
        assert body.count("hasExternalReference") == 1  # plain join, single path
