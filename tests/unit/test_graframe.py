"""Unit tests for the Graframe facet query interface.

These are *denotational* tests: the correctness of the fluent API is defined by
the SPARQL it compiles to, so we assert on ``to_sparql()`` / facet query text
using a fake client (no server, no network). A handful of tests exercise the
query-execution terminals against a stubbed ``sparql_query``.
"""

import re

import pytest

from acquirium.Graframe import (
    Facets, FacetRow, Graframe, P, Profile, Reasoning, like, parse_path, to_path,
)
from acquirium.Graframe.resolve import Fuzzy, resolve_iri
from acquirium.Graframe.algebra import Alt, Inv, Lit, Pred, Seq, Var, Triple
from acquirium.Graframe.facets import _facet_query, _virtual_facet

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

    def namespace_manager(self):
        prefixes = PREFIXES

        class _NM:
            def namespaces(self):
                return [(p, u) for p, u in prefixes.items()]

        return _NM()

    def timeseries_info_batch(self, uris):
        return {}

    # embedding matcher stub: a tiny name -> (uri, kind) lexicon. Value slots
    # resolve with kind=None, so a few unkinded entries are included too.
    _LEXICON = {
        ("sensor", "class"): "http://data.ashrae.org/standard223#Sensor",
        ("sensor", None): "http://data.ashrae.org/standard223#Sensor",
        ("pump", "class"): "urn:nawi-water-ontology#Pump",
        ("observes", "predicate"): "http://data.ashrae.org/standard223#observes",
        ("has property", "predicate"): "http://data.ashrae.org/standard223#hasProperty",
        ("concentration", "quantity_kind"): "http://qudt.org/vocab/quantitykind/Concentration",
        ("concentration", None): "http://qudt.org/vocab/quantitykind/Concentration",
    }

    def resolve_concept(self, text, kind=None, context=None, min_score=0.5):
        if text.startswith(("urn:", "http://", "https://")):
            return text
        return self._LEXICON.get((text.lower(), kind))

    def resolve_text(self, text, kind=None, top_k=5, min_score=0.5, context=None):
        uri = self.resolve_concept(text, kind=kind)
        return [{"uri": uri, "score": 1.0, "kind": kind}] if uri else []


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
# having (narrow) vs follow (move)
# ---------------------------------------------------------------------------


class TestHavingFollow:
    def test_having_is_existential(self, g):
        sel = g.instances("s223:Sensor").having("s223:hasProperty")
        sparql = norm(sel.to_sparql())
        # focus stays n0; the edge is wrapped in FILTER EXISTS, never selected
        assert "(?n0 AS ?focus)" in sparql
        assert f"FILTER EXISTS {{ ?n0 <{PREFIXES['s223']}hasProperty> ?n1 . }}" in sparql

    def test_refine_with_is_a(self, g):
        sel = g.instances("s223:Sensor").having("s223:hasProperty", is_a="qk:Temperature")
        sparql = norm(sel.to_sparql())
        assert "FILTER EXISTS { ?n0 <" + PREFIXES["s223"] + "hasProperty> ?n1 ." in sparql
        assert (
            f"FILTER EXISTS {{ ?n1 <{RDF_TYPE}>/<{SUBCLASS}>* <{PREFIXES['qk']}Temperature> . }}"
            in sparql
        )

    def test_pivot_moves_focus_and_adds_column(self, g):
        sel = g.instances("s223:Sensor").follow("s223:hasProperty")
        sparql = norm(sel.to_sparql())
        assert "(?n1 AS ?focus)" in sparql  # focus moved to the new column
        assert f"?n0 <{PREFIXES['s223']}hasProperty> ?n1 ." in sparql
        assert "FILTER EXISTS" not in sparql  # pivot is inline, not existential

    def test_pivot_direction_in_inverts(self, g):
        sel = g.instances("s223:Sensor").follow("s223:hasProperty", direction="in")
        assert f"?n0 ^<{PREFIXES['s223']}hasProperty> ?n1 ." in norm(sel.to_sparql())

    def test_caret_prefix_inverts(self, g):
        sel = g.instances("s223:Sensor").follow("^s223:hasProperty")
        assert f"?n0 ^<{PREFIXES['s223']}hasProperty> ?n1 ." in norm(sel.to_sparql())

    def test_step_list_is_alternation(self, g):
        sel = g.instances("s223:Sensor").follow(["s223:hasProperty", "s223:contains"])
        assert (
            f"?n0 <{PREFIXES['s223']}hasProperty>|<{PREFIXES['s223']}contains> ?n1 ."
            in norm(sel.to_sparql())
        )

    def test_step_list_inverted_wraps(self, g):
        sel = g.instances("s223:Sensor").follow(
            ["s223:hasProperty", "s223:contains"], direction="in"
        )
        assert (
            f"?n0 ^(<{PREFIXES['s223']}hasProperty>|<{PREFIXES['s223']}contains>) ?n1 ."
            in norm(sel.to_sparql())
        )

    def test_path_object_step(self, g):
        step = P(PREFIXES["s223"] + "connectedTo").plus()
        sel = g.instances("s223:Sensor").follow(step)
        assert f"?n0 <{PREFIXES['s223']}connectedTo>+ ?n1 ." in norm(sel.to_sparql())

    def test_pivot_value_filter(self, g):
        sel = g.instances("s223:Sensor").follow("s223:hasLocation", value="bldg:room_5")
        sparql = norm(sel.to_sparql())
        assert f"?n0 <{PREFIXES['s223']}hasLocation> ?n1 ." in sparql
        assert f"VALUES ?n1 {{ <{PREFIXES['bldg']}room_5> }}" in sparql

    def test_numeric_range(self, g):
        sel = g.instances("watr:Reading").follow("watr:value").in_range(min=0, max=100)
        sparql = norm(sel.to_sparql())
        assert "FILTER(?n1 >= 0)" in sparql
        assert "FILTER(?n1 <= 100)" in sparql


class TestInlinePaths:
    def test_follow_sequence_path(self, g):
        sel = g.instances("s223:Sensor").follow("s223:hasProperty/qudt:hasQuantityKind")
        assert (
            f"?n0 <{PREFIXES['s223']}hasProperty>/<{PREFIXES['qudt']}hasQuantityKind> ?n1 ."
            in norm(sel.to_sparql())
        )

    def test_having_sequence_path_filters_far_end(self, g):
        sel = g.instances("s223:DomainSpace").having(
            "s223:hasProperty/qudt:hasQuantityKind", value="qk:Temperature"
        )
        sparql = norm(sel.to_sparql())
        assert (
            f"FILTER EXISTS {{ ?n0 <{PREFIXES['s223']}hasProperty>/<{PREFIXES['qudt']}hasQuantityKind> ?n1 ."
            in sparql
        )
        assert f"VALUES ?n1 {{ <{PREFIXES['qk']}Temperature> }}" in sparql

    def test_transitive_modifier_path(self, g):
        sel = g.instances("s223:Sensor").follow("s223:connectedTo+")
        assert f"?n0 <{PREFIXES['s223']}connectedTo>+ ?n1 ." in norm(sel.to_sparql())

    def test_inline_alternation_path(self, g):
        sel = g.instances("s223:Sensor").follow("s223:hasProperty|s223:contains")
        assert (
            f"?n0 <{PREFIXES['s223']}hasProperty>|<{PREFIXES['s223']}contains> ?n1 ."
            in norm(sel.to_sparql())
        )

    def test_inline_path_direction_in_inverts_whole_path(self, g):
        sel = g.instances("s223:Sensor").follow("s223:a/s223:b", direction="in")
        assert (
            f"?n0 ^(<{PREFIXES['s223']}a>/<{PREFIXES['s223']}b>) ?n1 ."
            in norm(sel.to_sparql())
        )

    def test_full_uri_predicate_is_not_parsed_as_path(self, g):
        # a full URI contains "/" but is a single predicate, not a path
        uri = PREFIXES["s223"] + "hasProperty"
        sel = g.instances("s223:Sensor").follow(uri)
        assert f"?n0 <{uri}> ?n1 ." in norm(sel.to_sparql())

    def test_path_segments_resolve_fuzzily(self, g):
        # multi-word natural-language segments survive tokenization and resolve
        sel = g.instances("s223:Sensor").follow("observes/has property")
        assert (
            f"?n0 <{PREFIXES['s223']}observes>/<{PREFIXES['s223']}hasProperty> ?n1 ."
            in norm(sel.to_sparql())
        )

    def test_path_segments_fuzzy_off_rejects_names(self):
        g = Graframe(FakeClient(), fuzzy=False)
        with pytest.raises(ValueError):
            g.instances("s223:Sensor").follow("observes/has property").to_sparql()


# ---------------------------------------------------------------------------
# correlation
# ---------------------------------------------------------------------------


class TestCorrelation:
    def test_where_is_existential_and_holds_focus(self, g):
        sel = g.instances("s223:Sensor").where(
            lambda s: s.follow("s223:hasProperty").is_a("qk:Temperature")
        )
        sparql = norm(sel.to_sparql())
        assert "(?n0 AS ?focus)" in sparql  # focus unchanged
        # the branch pivots inside the EXISTS from the outer focus n0
        assert f"FILTER EXISTS {{ ?n0 <{PREFIXES['s223']}hasProperty> ?n1 ." in sparql

    def test_two_wheres_are_independent(self, g):
        sel = (
            g.instances("s223:Sensor")
            .where(lambda s: s.follow("s223:hasProperty").is_a("qk:Temperature"))
            .where(lambda s: s.follow("s223:hasLocation").is_one_of("bldg:room_5"))
        )
        sparql = norm(sel.to_sparql())
        assert sparql.count("FILTER EXISTS") >= 2
        # both branches anchor on outer focus n0, with fresh non-colliding vars
        assert f"?n0 <{PREFIXES['s223']}hasProperty>" in sparql
        assert f"?n0 <{PREFIXES['s223']}hasLocation>" in sparql

    def test_any_of_compiles_to_or_of_exists(self, g):
        sel = g.instances("s223:Sensor").any_of(
            lambda s: s.follow("s223:hasProperty").is_a("qk:Temperature"),
            lambda s: s.follow("s223:hasProperty").is_a("qk:Pressure"),
        )
        sparql = norm(sel.to_sparql())
        assert "FILTER(EXISTS {" in sparql
        assert " || EXISTS {" in sparql

    def test_without_is_not_exists(self, g):
        sel = g.instances("s223:Sensor").without("s223:hasProperty")
        assert "FILTER NOT EXISTS" in norm(sel.to_sparql())

    def test_matching_membership_join(self, g):
        rooms = g.instances("s223:DomainSpace").where(
            lambda s: s.follow("s223:hasProperty")
        )
        sel = g.instances("s223:Sensor").having("s223:hasLocation", matching=rooms)
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
            .follow("s223:hasLocation")
            .mark("room")
            .to("sensor")
        )
        # focus is back on the sensor column
        assert "(?n0 AS ?focus)" in norm(sel.to_sparql())

    def test_select_multiple_columns_aliases(self, g):
        sel = (
            g.instances("s223:Sensor")
            .mark("sensor")
            .follow("s223:hasLocation")
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

    def test_pred_obj_type_keys_iris_by_type_and_literals_by_datatype(self, g):
        sel = g.instances("s223:Sensor")
        q = norm(_facet_query(sel, by="pred-obj-type", direction="out", limit=5))
        # IRIs keyed by rdf:type, literals keyed by datatype, via COALESCE so
        # literal-valued edges are not dropped by an inner type join.
        assert f"OPTIONAL {{ ?fo <{RDF_TYPE}> ?fcls . }}" in q
        assert "BIND(COALESCE(?fcls, DATATYPE(?fo)) AS ?ft)" in q
        assert 'BIND(IF(BOUND(?fcls), "class", "datatype") AS ?ftkind)' in q
        assert "GROUP BY ?fp ?ft ?ftkind" in q

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


# ---------------------------------------------------------------------------
# path parsing
# ---------------------------------------------------------------------------


class TestPathParsing:
    def _expand(self, x):
        return FakeClient().expand_uri(x)

    def test_plus_path(self):
        p = parse_path("s223:connectedTo+", self._expand)
        assert p.render() == f"<{PREFIXES['s223']}connectedTo>+"

    def test_sequence(self):
        p = parse_path("s223:hasProperty/qudt:hasQuantityKind", self._expand)
        assert p.render() == f"<{PREFIXES['s223']}hasProperty>/<{PREFIXES['qudt']}hasQuantityKind>"

    def test_alternation_and_inverse(self):
        p = parse_path("^s223:a|s223:b", self._expand)
        # inverse is a composite path, so it's parenthesized inside the alternation
        assert p.render() == f"(^<{PREFIXES['s223']}a>)|<{PREFIXES['s223']}b>"

    def test_grouping(self):
        p = parse_path("s223:a/(s223:b|s223:c)", self._expand)
        assert p.render() == f"<{PREFIXES['s223']}a>/(<{PREFIXES['s223']}b>|<{PREFIXES['s223']}c>)"

    def test_to_path_accepts_builder_and_list(self):
        assert to_path(P(PREFIXES["s223"] + "x").plus(), self._expand).render() == f"<{PREFIXES['s223']}x>+"
        assert to_path(["s223:a", "s223:b"], self._expand).render() == (
            f"<{PREFIXES['s223']}a>|<{PREFIXES['s223']}b>"
        )


# ---------------------------------------------------------------------------
# profiles
# ---------------------------------------------------------------------------


class TestProfile:
    def _expand(self, x):
        return FakeClient().expand_uri(x)

    def test_base_hides_schema_namespaces(self):
        base = Profile.base()
        assert "rdf:" in base.deny and "sh:" in base.deny
        assert "sh:NodeShape" in base.deny_types

    def test_with_merges(self):
        p = Profile.base().with_(allow=["s223:"], edges={"downstream": "s223:connectedTo+"})
        assert "s223:" in p.allow
        assert "rdf:" in p.deny  # inherited
        assert p.edges["downstream"] == "s223:connectedTo+"

    def test_predicate_filter_namespace_and_exact(self):
        prof = Profile(allow=["s223:", "qudt:hasQuantityKind"], deny=["s223:cnx"])
        f = prof.predicate_filter("fp", PREFIXES, self._expand)
        assert f'STRSTARTS(STR(?fp), "{PREFIXES["s223"]}")' in f
        assert f"?fp IN (<{PREFIXES['qudt']}hasQuantityKind>)" in f
        assert f"!(?fp IN (<{PREFIXES['s223']}cnx>))" in f

    def test_no_filter_when_empty(self):
        assert Profile().predicate_filter("fp", PREFIXES, self._expand) is None

    def test_multi_term_allow_is_parenthesized(self):
        # regression: && binds tighter than ||, so a multi-term allow must be
        # wrapped or the deny only applies to the last allow term.
        prof = Profile(allow=["s223:", "nawi:"], deny=["s223:cnx"])
        f = prof.predicate_filter("fp", PREFIXES, self._expand)
        assert f.startswith("FILTER((")
        assert ") && !(" in f

    def test_named_edge_used_in_pivot(self):
        prof = Profile(edges={"downstream": "s223:connectedTo+"})
        g = Graframe(FakeClient(), profile=prof)
        sel = g.instances("s223:Sensor").follow("downstream")
        assert f"?n0 <{PREFIXES['s223']}connectedTo>+ ?n1 ." in norm(sel.to_sparql())

    def test_named_edge_in_refine(self):
        prof = Profile(edges={"measures": "s223:hasProperty"})
        g = Graframe(FakeClient(), profile=prof)
        sel = g.instances("s223:Sensor").having("measures", value="qk:Pressure")
        assert f"FILTER EXISTS {{ ?n0 <{PREFIXES['s223']}hasProperty> ?n1 ." in norm(sel.to_sparql())

    def test_facets_apply_predicate_filter(self):
        prof = Profile(allow=["s223:"])
        g = Graframe(FakeClient(), profile=prof)
        q = norm(_facet_query(
            g.instances("s223:Sensor"), by="predicate", direction="out", limit=10,
            pred_filter=prof.predicate_filter("fp", PREFIXES, g.instances("s223:Sensor")._expand),
        ))
        assert f'STRSTARTS(STR(?fp), "{PREFIXES["s223"]}")' in q
        assert "FILTER((" in q

    def test_virtual_facet_query_uses_path(self):
        client = FakeClient({"columns": ["support", "edges"], "rows": []})
        g = Graframe(client, profile=Profile(edges={"downstream": "s223:connectedTo+"}))
        sel = g.instances("s223:Sensor")
        _virtual_facet(sel, "downstream", to_path("s223:connectedTo+", sel._expand), by="predicate", limit=10)
        assert f"?n0 <{PREFIXES['s223']}connectedTo>+ ?fo ." in norm(client.last_query)

    def test_virtual_edges_surface_in_facets(self):
        class VClient(FakeClient):
            def sparql_query(self, sparql, use_union=True):
                self.last_query = sparql
                if "?fp" in sparql:  # atomic predicate facet
                    return {"columns": ["fp", "support", "edges"], "rows": [["urn:p", 2, 2]]}
                return {"columns": ["support", "edges"], "rows": [[5, 7]]}  # virtual edge

        g = Graframe(VClient(), profile=Profile(edges={"downstream": "s223:connectedTo+"}))
        facets = g.instances("s223:Sensor").facets(by="predicate", direction="out")
        assert any(
            r.is_virtual and r.predicate == "downstream" and r.support == 5
            for r in facets.rows
        )

    def test_raw_bypasses_profile(self):
        prof = Profile(allow=["s223:"], edges={"downstream": "s223:connectedTo+"})
        client = FakeClient({"columns": ["fp", "support", "edges"], "rows": [["urn:p", 1, 1]]})
        g = Graframe(client, profile=prof)
        facets = g.instances("s223:Sensor").facets(by="predicate", raw=True)
        assert not any(r.is_virtual for r in facets.rows)  # no virtual edges when raw


# ---------------------------------------------------------------------------
# data plane bridge
# ---------------------------------------------------------------------------

HAS_EXT_REF = "https://brickschema.org/schema/Brick/ref#hasExternalReference"
HAS_UNIT = "http://qudt.org/schema/qudt/hasUnit"


class TestDataBridge:
    def test_data_sparql_uses_ref_and_marks(self):
        from acquirium.Graframe.data import _data_sparql

        g = Graframe(FakeClient())
        sel = (g.instances("s223:Sensor").mark("sensor")
                 .follow("s223:hasProperty").mark("prop"))
        marks = {n: v for n, v in sel._state.marks.items() if v != sel._state.focus}
        q = norm(_data_sparql(sel, marks))
        assert f"?n1 <{HAS_EXT_REF}> ?gref ." in q          # focus (prop) is n1
        assert f"OPTIONAL {{ ?n1 <{HAS_UNIT}> ?gunit . }}" in q
        assert "OPTIONAL { ?gref <" in q                    # ext-ref unit
        assert "(?n0 AS ?entity__sensor)" in q              # mark becomes entity col

    def test_build_data_object_bindings(self):
        from acquirium.Graframe.data import build_data_object

        rows = [
            ["urn:p1", "urn:r1", "urn:unitA", None, "urn:e1"],
            ["urn:p2", "urn:r2", None, "urn:unitB", "urn:e1"],
        ]
        client = FakeClient(
            {"columns": ["point", "ref", "unit", "extunit", "entity__sensor"], "rows": rows}
        )
        g = Graframe(client)
        sel = g.instances("s223:Sensor").mark("sensor").follow("s223:hasProperty")
        d = build_data_object(sel)
        assert d._entity_columns == ["entity__sensor"]
        assert d.aliases == ["urn:p1", "urn:p2"]  # compact passthrough (no prefix match)
        by_point = {b.point_uri: b for b in d.bindings}
        assert by_point["urn:p1"].property_unit == "urn:unitA"
        assert by_point["urn:p1"].ref_unit is None
        assert by_point["urn:p2"].ref_unit == "urn:unitB"

    def test_build_data_object_empty(self):
        from acquirium.Graframe.data import build_data_object

        client = FakeClient({"columns": ["point", "ref", "unit", "extunit"], "rows": []})
        d = build_data_object(Graframe(client).instances("s223:Sensor"))
        assert d.is_empty()

    def test_metadata_columns_drop_entity_prefix(self):
        from acquirium.Graframe.data import build_data_object

        rows = [["urn:p1", "urn:r1", "urn:unitA", None, "urn:e1"]]
        client = FakeClient(
            {"columns": ["point", "ref", "unit", "extunit", "entity__sensor"], "rows": rows}
        )
        g = Graframe(client)
        sel = g.instances("s223:Sensor").mark("sensor").follow("s223:hasProperty")
        md = build_data_object(sel).metadata()
        # internal key keeps the prefix; the user-facing frame drops it
        assert "sensor" in md.columns and "entity__sensor" not in md.columns

    def test_data_rejects_mark_colliding_with_reserved_column(self):
        from acquirium.Graframe.data import build_data_object

        g = Graframe(FakeClient())
        sel = g.instances("s223:Sensor").mark("time").follow("s223:hasProperty")
        with pytest.raises(ValueError, match="reserved data column"):
            build_data_object(sel)


# ---------------------------------------------------------------------------
# fuzzy term resolution
# ---------------------------------------------------------------------------

S223 = PREFIXES["s223"]


class TestResolve:
    def _c(self):
        return FakeClient()

    def test_like_marker(self):
        f = like("concentration", "quantity_kind")
        assert isinstance(f, Fuzzy) and f.text == "concentration" and f.kind == "quantity_kind"

    def test_uri_passthrough(self):
        assert resolve_iri(self._c(), "urn:x", kind="class", fuzzy=True, min_score=0.5) == "urn:x"

    def test_curie_expands_not_fuzzy(self):
        assert resolve_iri(self._c(), "s223:Sensor", kind="class", fuzzy=True, min_score=0.5) == f"{S223}Sensor"

    def test_bad_prefix_warns_and_falls_back_to_fuzzy(self):
        # unknown prefix + resolvable local part -> warn, then resolve the local
        with pytest.warns(UserWarning):
            got = resolve_iri(self._c(), "nope:sensor", kind="class", fuzzy=True, min_score=0.5)
        assert got == f"{S223}Sensor"

    def test_bad_prefix_unresolvable_still_raises(self):
        # warns, tries fuzzy on the local part, which also fails -> raise
        with pytest.warns(UserWarning):
            with pytest.raises(ValueError):
                resolve_iri(self._c(), "nope:Thing", kind="class", fuzzy=True, min_score=0.5)

    def test_bad_prefix_raises_when_fuzzy_off(self):
        with pytest.raises(Exception):
            resolve_iri(self._c(), "nope:Thing", kind="class", fuzzy=False, min_score=0.5)

    def test_natural_language_resolves_when_fuzzy(self):
        assert resolve_iri(self._c(), "sensor", kind="class", fuzzy=True, min_score=0.5) == f"{S223}Sensor"

    def test_natural_language_rejected_when_not_fuzzy(self):
        with pytest.raises(ValueError):
            resolve_iri(self._c(), "sensor", kind="class", fuzzy=False, min_score=0.5)

    def test_unresolvable_raises_with_hint(self):
        with pytest.raises(ValueError):
            resolve_iri(self._c(), "wobblegonk", kind="class", fuzzy=True, min_score=0.5)

    def test_fuzzy_marker_uses_its_kind(self):
        # kind on the marker wins over the slot kind
        got = resolve_iri(self._c(), like("concentration", "quantity_kind"), kind="class", fuzzy=True, min_score=0.5)
        assert got == "http://qudt.org/vocab/quantitykind/Concentration"

    def test_instances_natural_language(self):
        g = Graframe(self._c())
        assert f"<{S223}Sensor>" in g.instances("sensor").to_sparql()

    def test_instances_fuzzy_off_raises(self):
        with pytest.raises(ValueError):
            Graframe(self._c(), fuzzy=False).instances("sensor")

    def test_pivot_natural_language_predicate(self):
        g = Graframe(self._c())
        sel = g.instances("sensor").follow("observes")
        assert f"?n0 <{S223}observes> ?n1 ." in norm(sel.to_sparql())

    def test_value_curie_resolves_to_iri(self):
        g = Graframe(self._c())
        # a CURIE with a bound prefix expands to an IRI
        sel = g.instances("sensor").having("s223:hasProperty", value="qk:Concentration")
        assert f"VALUES ?n1 {{ <{PREFIXES['qk']}Concentration> }}" in norm(sel.to_sparql())

    def test_value_bare_string_is_fuzzy_resolved(self):
        g = Graframe(self._c())
        # a colon-less value resolves like a concept slot (no like() needed)
        sel = g.instances("sensor").having("s223:hasProperty", value="concentration")
        assert f"VALUES ?n1 {{ <{PREFIXES['qk']}Concentration> }}" in norm(sel.to_sparql())

    def test_value_number_stays_literal(self):
        g = Graframe(self._c())
        # numbers (and Lit/Literal) are the escape hatch for real literals
        sel = g.instances("watr:Reading").having("watr:value", value=5)
        assert "VALUES ?n1 { 5 }" in norm(sel.to_sparql())

    def test_value_lit_escape_stays_literal(self):
        g = Graframe(self._c())
        sel = g.instances("sensor").having("s223:hasProperty", value=Lit("concentration"))
        assert 'VALUES ?n1 { "concentration" }' in norm(sel.to_sparql())

    def test_value_bad_prefix_warns_and_fuzzy(self):
        g = Graframe(self._c())
        # a typo'd prefix warns then fuzzy-resolves the local part ("sensor")
        with pytest.warns(UserWarning):
            sel = g.instances("pump").having("s223:hasProperty", value="nope:sensor")
        assert f"<{S223}Sensor>" in norm(sel.to_sparql())

    def test_value_like_resolves(self):
        g = Graframe(self._c())
        sel = g.instances("sensor").having(
            "s223:hasProperty", value=like("concentration", "quantity_kind")
        )
        assert "<http://qudt.org/vocab/quantitykind/Concentration>" in norm(sel.to_sparql())

    def test_suggest(self):
        out = Graframe(self._c()).suggest("sensor", kind="class")
        assert out and out[0]["curie"] == "s223:Sensor" and out[0]["score"] == 1.0


# ---------------------------------------------------------------------------
# actionable facet rows
# ---------------------------------------------------------------------------


class TestFacetRowAction:
    def test_follow_facet_row_predicate(self, g):
        row = FacetRow(direction="out", predicate=f"{S223}observes", support=3, edges=3)
        sel = g.instances("s223:Sensor").follow(row)
        assert f"?n0 <{S223}observes> ?n1 ." in norm(sel.to_sparql())

    def test_follow_facet_row_direction_in(self, g):
        row = FacetRow(direction="in", predicate=f"{S223}hasProperty", support=1, edges=1)
        sel = g.instances("s223:Sensor").follow(row)
        assert f"?n0 ^<{S223}hasProperty> ?n1 ." in norm(sel.to_sparql())

    def test_having_facet_row_pred_obj_becomes_value_filter(self, g):
        row = FacetRow(
            direction="out", predicate=f"{S223}hasLocation", support=2, edges=2,
            key=f"{PREFIXES['bldg']}room_5", key_kind="value",
        )
        sparql = norm(g.instances("s223:Sensor").having(row).to_sparql())
        assert f"FILTER EXISTS {{ ?n0 <{S223}hasLocation> ?n1 ." in sparql
        assert f"VALUES ?n1 {{ <{PREFIXES['bldg']}room_5> }}" in sparql

    def test_having_facet_row_pred_obj_type_becomes_is_a(self, g):
        row = FacetRow(
            direction="out", predicate=f"{S223}hasProperty", support=2, edges=2,
            key=f"{PREFIXES['qk']}Temperature", key_kind="type",
        )
        sparql = norm(g.instances("s223:Sensor").having(row).to_sparql())
        assert f"?n1 <{RDF_TYPE}>/<{SUBCLASS}>* <{PREFIXES['qk']}Temperature> ." in sparql

    def test_having_facet_row_datatype_becomes_datatype_filter(self, g):
        xsd_double = "http://www.w3.org/2001/XMLSchema#double"
        row = FacetRow(
            direction="out", predicate=f"{S223}hasValue", support=2, edges=2,
            key=xsd_double, key_kind="datatype",
        )
        sparql = norm(g.instances("s223:Sensor").having(row).to_sparql())
        assert f"FILTER(DATATYPE(?n1) = <{xsd_double}>)" in sparql

    def test_datatype_kwarg_on_follow(self, g):
        xsd_double = "http://www.w3.org/2001/XMLSchema#double"
        sel = g.instances("s223:Sensor").follow("s223:hasValue", datatype=xsd_double)
        assert f"FILTER(DATATYPE(?n1) = <{xsd_double}>)" in norm(sel.to_sparql())

    def test_row_selects_by_predicate_name(self):
        rows = [
            FacetRow(direction="out", predicate=f"{S223}observes", support=3, edges=3),
            FacetRow(direction="out", predicate=f"{S223}hasProperty", support=1, edges=1),
        ]
        f = Facets(Graframe(FakeClient()).instances("s223:Sensor"), "predicate", rows)
        assert f.row("s223:observes").predicate == f"{S223}observes"  # compacted match
        assert f.row(1).predicate == f"{S223}hasProperty"             # positional

    def test_row_ambiguous_raises(self):
        rows = [
            FacetRow(direction="out", predicate=f"{S223}p", support=3, edges=3),
            FacetRow(direction="in", predicate=f"{S223}p", support=1, edges=1),
        ]
        f = Facets(Graframe(FakeClient()).instances("s223:Sensor"), "predicate", rows)
        with pytest.raises(ValueError):
            f.row("s223:p")
        assert f.row("s223:p", direction="in").direction == "in"  # disambiguated

    def test_row_no_match_raises(self):
        f = Facets(Graframe(FakeClient()).instances("s223:Sensor"), "predicate", [])
        with pytest.raises(KeyError):
            f.row("s223:nope")
