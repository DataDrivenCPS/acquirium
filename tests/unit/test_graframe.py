"""Unit tests for the Graframe facet query interface.

These are *denotational* tests: the correctness of the fluent API is defined by
the SPARQL it compiles to, so we assert on ``to_sparql()`` / facet query text
using a fake client (no server, no network). A handful of tests exercise the
query-execution terminals against a stubbed ``sparql_query``.
"""

import re

import pytest

from acquirium.Graframe import Graframe, P, Reasoning
from acquirium.Graframe.algebra import Alt, Inv, Iri, Lit, Pred, Seq, Var, Triple
from acquirium.Graframe.facets import _facet_query

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
SUBCLASS = "http://www.w3.org/2000/01/rdf-schema#subClassOf"

PREFIXES = {
    "s223": "http://data.ashrae.org/standard223#",
    "qudt": "http://qudt.org/schema/qudt/",
    "qk": "http://qudt.org/vocab/quantitykind/",
    "watr": "urn:nawi-water-ontology#",
    "bldg": "urn:building#",
}


class FakeClient:
    """Minimal stand-in for AcquiriumClient: CURIE expansion + query capture."""

    def __init__(self, result=None):
        self._result = result or {"columns": [], "rows": []}
        self.last_query = None

    def expand_uri(self, text):
        s = str(text)
        if "://" in s or s.startswith("urn:"):
            return s
        if ":" in s:
            prefix, local = s.split(":", 1)
            if prefix in PREFIXES:
                return PREFIXES[prefix] + local
        raise ValueError(f"cannot expand {s!r}")

    def compact_uri(self, item):
        s = str(item)
        for pfx, ns in PREFIXES.items():
            if s.startswith(ns):
                return f"{pfx}:{s[len(ns):]}"
        return s

    def sparql_query(self, sparql, use_union=True):
        self.last_query = sparql
        return self._result


def norm(s: str) -> str:
    """Collapse whitespace for robust structural comparison."""
    return re.sub(r"\s+", " ", s).strip()


@pytest.fixture
def g():
    return Graframe(FakeClient())


# ---------------------------------------------------------------------------
# algebra rendering
# ---------------------------------------------------------------------------


class TestAlgebra:
    def test_pred_render(self):
        assert Pred("urn:p").render() == "<urn:p>"

    def test_inverse(self):
        assert Inv(Pred("urn:p")).render() == "^<urn:p>"

    def test_seq_and_alt_parenthesize(self):
        p = Seq(Pred("urn:a"), Alt(Pred("urn:b"), Pred("urn:c")))
        assert p.render() == "<urn:a>/(<urn:b>|<urn:c>)"

    def test_plus_star(self):
        assert Pred("urn:p").plus().render() == "<urn:p>+"
        assert Pred("urn:p").star().render() == "<urn:p>*"

    def test_literal_types(self):
        assert Lit(5).render() == "5"
        assert Lit(1.5).render() == "1.5"
        assert Lit(True).render() == "true"
        assert Lit("hi").render() == '"hi"'
        assert Lit('a"b').render() == '"a\\"b"'

    def test_triple_rename(self):
        t = Triple(Var("a"), Pred("urn:p"), Var("b"))
        r = t.rename({"a": "x"})
        assert r.render() == "?x <urn:p> ?b ."


# ---------------------------------------------------------------------------
# seeds
# ---------------------------------------------------------------------------


class TestSeeds:
    def test_instances_uses_subclass_path(self, g):
        sparql = g.instances("s223:Sensor").to_sparql()
        assert norm(sparql) == norm(
            f"SELECT DISTINCT (?n0 AS ?focus) WHERE {{ "
            f"?n0 <{RDF_TYPE}>/<{SUBCLASS}>* <{PREFIXES['s223']}Sensor> . }}"
        )

    def test_instances_no_reasoning(self):
        g = Graframe(FakeClient(), reasoning=Reasoning(subclass=False))
        sparql = g.instances("s223:Sensor").to_sparql()
        assert f"?n0 <{RDF_TYPE}> <{PREFIXES['s223']}Sensor> ." in sparql
        assert SUBCLASS not in sparql

    def test_nodes_uses_values(self, g):
        sparql = g.nodes("bldg:room_5", "bldg:room_6").to_sparql()
        assert (
            f"VALUES ?n0 {{ <{PREFIXES['bldg']}room_5> <{PREFIXES['bldg']}room_6> }}"
            in sparql
        )

    def test_everything_binds_focus(self, g):
        sparql = g.everything().to_sparql()
        assert "?n0 ?__gp0 ?n1 ." in sparql

    def test_reasoning_rejects_unimplemented(self):
        with pytest.raises(NotImplementedError):
            Reasoning(subproperty=True)


# ---------------------------------------------------------------------------
# refine vs pivot
# ---------------------------------------------------------------------------


class TestRefinePivot:
    def test_refine_is_existential(self, g):
        sel = g.instances("s223:Sensor").refine("s223:hasProperty")
        sparql = norm(sel.to_sparql())
        # focus stays n0; the edge is wrapped in FILTER EXISTS, never selected
        assert "(?n0 AS ?focus)" in sparql
        assert f"FILTER EXISTS {{ ?n0 <{PREFIXES['s223']}hasProperty> ?n1 . }}" in sparql

    def test_refine_with_is_a(self, g):
        sel = g.instances("s223:Sensor").refine("s223:hasProperty", is_a="qk:Temperature")
        sparql = norm(sel.to_sparql())
        assert "FILTER EXISTS { ?n0 <" + PREFIXES["s223"] + "hasProperty> ?n1 ." in sparql
        assert (
            f"FILTER EXISTS {{ ?n1 <{RDF_TYPE}>/<{SUBCLASS}>* <{PREFIXES['qk']}Temperature> . }}"
            in sparql
        )

    def test_pivot_moves_focus_and_adds_column(self, g):
        sel = g.instances("s223:Sensor").pivot("s223:hasProperty")
        sparql = norm(sel.to_sparql())
        assert "(?n1 AS ?focus)" in sparql  # focus moved to the new column
        assert f"?n0 <{PREFIXES['s223']}hasProperty> ?n1 ." in sparql
        assert "FILTER EXISTS" not in sparql  # pivot is inline, not existential

    def test_pivot_direction_in_inverts(self, g):
        sel = g.instances("s223:Sensor").pivot("s223:hasProperty", direction="in")
        assert f"?n0 ^<{PREFIXES['s223']}hasProperty> ?n1 ." in norm(sel.to_sparql())

    def test_caret_prefix_inverts(self, g):
        sel = g.instances("s223:Sensor").pivot("^s223:hasProperty")
        assert f"?n0 ^<{PREFIXES['s223']}hasProperty> ?n1 ." in norm(sel.to_sparql())

    def test_step_list_is_alternation(self, g):
        sel = g.instances("s223:Sensor").pivot(["s223:hasProperty", "s223:contains"])
        assert (
            f"?n0 <{PREFIXES['s223']}hasProperty>|<{PREFIXES['s223']}contains> ?n1 ."
            in norm(sel.to_sparql())
        )

    def test_step_list_inverted_wraps(self, g):
        sel = g.instances("s223:Sensor").pivot(
            ["s223:hasProperty", "s223:contains"], direction="in"
        )
        assert (
            f"?n0 ^(<{PREFIXES['s223']}hasProperty>|<{PREFIXES['s223']}contains>) ?n1 ."
            in norm(sel.to_sparql())
        )

    def test_path_object_step(self, g):
        step = P(PREFIXES["s223"] + "connectedTo").plus()
        sel = g.instances("s223:Sensor").pivot(step)
        assert f"?n0 <{PREFIXES['s223']}connectedTo>+ ?n1 ." in norm(sel.to_sparql())

    def test_pivot_value_filter(self, g):
        sel = g.instances("s223:Sensor").pivot("s223:hasLocation", value="bldg:room_5")
        sparql = norm(sel.to_sparql())
        assert f"?n0 <{PREFIXES['s223']}hasLocation> ?n1 ." in sparql
        assert f"VALUES ?n1 {{ <{PREFIXES['bldg']}room_5> }}" in sparql

    def test_numeric_range(self, g):
        sel = g.instances("watr:Reading").pivot("watr:value").in_range(min=0, max=100)
        sparql = norm(sel.to_sparql())
        assert "FILTER(?n1 >= 0)" in sparql
        assert "FILTER(?n1 <= 100)" in sparql


# ---------------------------------------------------------------------------
# correlation
# ---------------------------------------------------------------------------


class TestCorrelation:
    def test_where_is_existential_and_holds_focus(self, g):
        sel = g.instances("s223:Sensor").where(
            lambda s: s.pivot("s223:hasProperty").is_a("qk:Temperature")
        )
        sparql = norm(sel.to_sparql())
        assert "(?n0 AS ?focus)" in sparql  # focus unchanged
        # the branch pivots inside the EXISTS from the outer focus n0
        assert f"FILTER EXISTS {{ ?n0 <{PREFIXES['s223']}hasProperty> ?n1 ." in sparql

    def test_two_wheres_are_independent(self, g):
        sel = (
            g.instances("s223:Sensor")
            .where(lambda s: s.pivot("s223:hasProperty").is_a("qk:Temperature"))
            .where(lambda s: s.pivot("s223:hasLocation").is_("bldg:room_5"))
        )
        sparql = norm(sel.to_sparql())
        assert sparql.count("FILTER EXISTS") >= 2
        # both branches anchor on outer focus n0, with fresh non-colliding vars
        assert f"?n0 <{PREFIXES['s223']}hasProperty>" in sparql
        assert f"?n0 <{PREFIXES['s223']}hasLocation>" in sparql

    def test_any_of_compiles_to_or_of_exists(self, g):
        sel = g.instances("s223:Sensor").any_of(
            lambda s: s.pivot("s223:hasProperty").is_a("qk:Temperature"),
            lambda s: s.pivot("s223:hasProperty").is_a("qk:Pressure"),
        )
        sparql = norm(sel.to_sparql())
        assert "FILTER(EXISTS {" in sparql
        assert " || EXISTS {" in sparql

    def test_without_is_not_exists(self, g):
        sel = g.instances("s223:Sensor").without("s223:hasProperty")
        assert "FILTER NOT EXISTS" in norm(sel.to_sparql())

    def test_matching_membership_join(self, g):
        rooms = g.instances("s223:DomainSpace").where(
            lambda s: s.pivot("s223:hasProperty")
        )
        sel = g.instances("s223:Sensor").refine("s223:hasLocation", matching=rooms)
        sparql = norm(sel.to_sparql())
        # the other selection's focus (n0) is renamed to this edge's object (n1)
        assert f"FILTER EXISTS {{ ?n1 <{RDF_TYPE}>/<{SUBCLASS}>* <{PREFIXES['s223']}DomainSpace> ." in sparql
        # and its internal vars are offset so they don't collide with the outer query
        assert "?n0" in sparql and "(?n0 AS ?focus)" in sparql


# ---------------------------------------------------------------------------
# waypoints + projection
# ---------------------------------------------------------------------------


class TestWaypoints:
    def test_mark_and_to_return_to_column(self, g):
        sel = (
            g.instances("s223:Sensor")
            .mark("sensor")
            .pivot("s223:hasLocation")
            .mark("room")
            .to("sensor")
        )
        # focus is back on the sensor column
        assert "(?n0 AS ?focus)" in norm(sel.to_sparql())

    def test_select_multiple_columns_aliases(self, g):
        sel = (
            g.instances("s223:Sensor")
            .mark("sensor")
            .pivot("s223:hasLocation")
            .mark("room")
        )
        sparql = norm(sel.to_sparql("sensor", "room"))
        assert "(?n0 AS ?sensor)" in sparql
        assert "(?n1 AS ?room)" in sparql

    def test_to_unknown_mark_raises(self, g):
        with pytest.raises(KeyError):
            g.instances("s223:Sensor").to("nope")


# ---------------------------------------------------------------------------
# terminals (stubbed execution)
# ---------------------------------------------------------------------------


class TestTerminals:
    def test_nodes_extracts_focus_column(self):
        client = FakeClient(
            {"columns": ["focus"], "rows": [["urn:a"], ["urn:b"], ["urn:a"]]}
        )
        g = Graframe(client)
        assert g.instances("s223:Sensor").nodes() == ["urn:a", "urn:b"]

    def test_count_builds_count_query(self):
        client = FakeClient({"columns": ["count"], "rows": [[7]]})
        g = Graframe(client)
        n = g.instances("s223:Sensor").count()
        assert n == 7
        assert "COUNT(DISTINCT ?n0)" in client.last_query


# ---------------------------------------------------------------------------
# facets
# ---------------------------------------------------------------------------


class TestFacets:
    def test_by_predicate_query(self, g):
        sel = g.instances("s223:Sensor")
        q = norm(_facet_query(sel, by="predicate", direction="out", limit=10))
        assert "SELECT ?fp (COUNT(DISTINCT ?n0) AS ?support) (COUNT(*) AS ?edges)" in q
        assert "?n0 ?fp ?fo ." in q
        assert "GROUP BY ?fp" in q
        assert "LIMIT 10" in q

    def test_direction_in_flips_edge(self, g):
        sel = g.instances("s223:Sensor")
        q = norm(_facet_query(sel, by="predicate", direction="in", limit=10))
        assert "?fo ?fp ?n0 ." in q

    def test_pred_obj_type_adds_type_triple(self, g):
        sel = g.instances("s223:Sensor")
        q = norm(_facet_query(sel, by="pred-obj-type", direction="out", limit=5))
        assert f"?fo <{RDF_TYPE}> ?ft ." in q
        assert "GROUP BY ?fp ?ft" in q

    def test_facets_both_directions(self):
        client = FakeClient(
            {"columns": ["fp", "support", "edges"], "rows": [["urn:p", 3, 5]]}
        )
        g = Graframe(client)
        facets = g.instances("s223:Sensor").facets(by="predicate", direction="both")
        # one row per direction
        assert len(facets) == 2
        assert {r.direction for r in facets.rows} == {"out", "in"}

    def test_facets_invalid_by(self, g):
        with pytest.raises(ValueError):
            g.instances("s223:Sensor").facets(by="bogus")
