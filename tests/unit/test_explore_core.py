"""Tests for acquirium.Client.explore.core — the Q builder.

Graph-shape assertions plus SPARQL parity against equivalent legacy Query
chains. All inputs are URIs, which bypass text resolution in both builders,
so no server/client is needed.
"""

import re

import pytest

from acquirium.Client.explore.core import Q
from acquirium.Client.query import Query

CLS_A = "urn:test#TypeA"
CLS_B = "urn:test#TypeB"
INST = "urn:test:instance#x1"
PRED_P = "urn:test#p"


def q() -> Q:
    return Q(client=None)


@pytest.fixture(autouse=True)
def no_hidden_defaults():
    """Parity vs the legacy compiler needs the hidden-predicate filters off."""
    from acquirium.Client.explore.hidden import hidden_predicates, unhide
    unhide(*hidden_predicates())
    yield
    unhide()


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


class TestEntity:
    def test_adds_node_with_class(self):
        b = q().entity(CLS_A, alias="a")
        g = b.query_graph
        assert g.nodes[0].constraints == {"rdf_class": CLS_A}
        assert g.current_pointer == 0
        assert g.aliases["a"] == 0

    def test_uri_only(self):
        b = q().entity(uri=INST, alias="x")
        assert b.query_graph.nodes[0].constraints == {"instance_uri": INST}

    def test_requires_cls_or_uri(self):
        with pytest.raises(ValueError):
            q().entity()

    def test_immutability(self):
        b1 = q().entity(CLS_A, alias="a")
        b2 = b1.entity(CLS_B, alias="b")
        assert len(b1.query_graph.nodes) == 1
        assert len(b2.query_graph.nodes) == 2
        assert b2.query_graph.current_pointer == 1

    def test_fresh_cache_per_step(self):
        b1 = q().entity(CLS_A)
        b1.cache["execute"] = {"columns": [], "rows": []}
        b2 = b1.entity(CLS_B)
        assert b2.cache == {}


class TestRelated:
    def test_any_defaults_to_bounded_wildcard_program(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b")
        (edge,) = b.query_graph.edges
        assert edge.patterns == ((((("*", None),),), True),)
        assert edge.hops == 3 and edge.predicates is None and edge.direction is None

    def test_any_unbounded_is_explicit(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", max_depth=0)
        assert b.query_graph.edges[0].hops == 0

    def test_predicates_default_one_hop(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=[PRED_P])
        (edge,) = b.query_graph.edges
        assert edge.hops == 1 and edge.predicates == [PRED_P]

    def test_max_depth_override(self):
        b = q().entity(CLS_A).related(CLS_B, via=[PRED_P], max_depth=2)
        assert b.query_graph.edges[0].hops == 2

    def test_inverse_predicate_passthrough(self):
        b = q().entity(CLS_A).related(CLS_B, via=[f"^{PRED_P}"])
        assert b.query_graph.edges[0].predicates == [f"^{PRED_P}"]

    def test_direction_edge(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", direction="upstream")
        (edge,) = b.query_graph.edges
        assert edge.direction == "upstream" and edge.hops == 3

    def test_frm_alias(self):
        b = (q().entity(CLS_A, alias="a").entity(CLS_B, alias="b")
             .related(CLS_A, alias="c", frm="a"))
        (edge,) = b.query_graph.edges
        assert edge.source_id == 0 and edge.target_id == 2

    def test_errors(self):
        with pytest.raises(ValueError):
            q().related(CLS_A)  # no source
        base = q().entity(CLS_A)
        with pytest.raises(ValueError):
            base.related(CLS_B, direction="sideways")
        with pytest.raises(ValueError):
            base.related(CLS_B, via=[PRED_P], direction="upstream")
        with pytest.raises(ValueError):
            base.related(CLS_B, via=42)  # not a valid via expression


class TestMeasurement:
    def test_attaches_data_node(self):
        b = q().entity(CLS_A, alias="ro").measurement()
        g = b.query_graph
        assert g.nodes[1].constraints == {"is_data_node": True}
        assert 1 in g.data_nodes
        assert g.aliases["ro_data"] == 1
        (edge,) = g.edges
        assert edge.source_id == 0 and edge.target_id == 1 and edge.hops == 1

    def test_alias_and_frm(self):
        b = (q().entity(CLS_A, alias="a").entity(CLS_B, alias="b")
             .measurement(frm="a", alias="m"))
        g = b.query_graph
        assert g.aliases["m"] == 2
        assert g.edges[0].source_id == 0

    def test_star_expands_all_entities(self):
        b = q().entity(CLS_A, alias="a").entity(CLS_B, alias="b").measurement(frm="*")
        g = b.query_graph
        assert sorted(g.data_nodes) == [2, 3]
        assert g.aliases["a_data"] == 2 and g.aliases["b_data"] == 3

    def test_direction_builds_mid_node_and_cp_filter(self):
        b = q().entity(CLS_A, alias="ro").measurement(direction="upstream", max_depth=2)
        g = b.query_graph
        assert g.aliases["ro_upstream_entity"] == 1
        assert g.aliases["ro_upstream_data"] == 2
        mid_edge, data_edge = g.edges
        assert mid_edge.direction == "upstream" and mid_edge.hops == 2
        assert data_edge.hops == 1
        assert data_edge.cp_filter == "http://data.ashrae.org/standard223#InletConnectionPoint"
        assert 2 in g.data_nodes

    def test_root_form_on_empty_query(self):
        b = q().measurement()
        g = b.query_graph
        assert g.nodes[0].constraints == {"is_data_node": True}
        assert 0 in g.data_nodes and not g.edges
        assert g.aliases["data"] == 0

    def test_root_form_with_attrs_and_alias(self):
        qk = "http://qudt.org/vocab/quantitykind/PH"
        b = q().measurement(alias="ph", quantity_kind=qk)
        g = b.query_graph
        assert g.aliases["ph"] == 0
        assert g.data_nodes[0].filters == {"quantity_kind": qk}

    def test_root_form_parity_with_find_all_data(self):
        new = q().measurement().to_sparql()
        old = Query(client=None).find_all_data().to_sparql()
        assert canon(new) == canon(old)

    def test_unknown_frm_still_errors(self):
        with pytest.raises(ValueError):
            q().entity(CLS_A).measurement(frm="nope")


class TestRefocus:
    def test_repoints(self):
        b = q().entity(CLS_A, alias="a").entity(CLS_B, alias="b")
        assert b.query_graph.current_pointer == 1
        assert b.refocus("a").query_graph.current_pointer == 0

    def test_unknown_alias(self):
        with pytest.raises(ValueError):
            q().entity(CLS_A, alias="a").refocus("nope")


def canon(s: str) -> str:
    """Normalize whitespace and node-id numbering.

    The legacy builder's id counter skips numbers (its ``_new_id`` +
    ``bump_id`` both increment), so equivalent graphs get different node ids.
    Renumber ``?v<N>/?ext<N>/?unit<N>/?extunit<N>`` by order of first
    appearance so structurally identical queries compare equal.
    """
    s = norm(s)
    mapping: dict[str, str] = {}

    def sub(m: re.Match) -> str:
        nid = m.group(2)
        if nid not in mapping:
            mapping[nid] = str(len(mapping))
        return f"?{m.group(1)}{mapping[nid]}"

    return re.sub(r"\?(v|extunit|ext|unit)(\d+)", sub, s)


class TestSparqlParityWithLegacy:
    """New verb chains must compile to the same SPARQL as the legacy builder
    (modulo node numbering, see :func:`canon`)."""

    def test_entity_related_chain(self):
        new = (q().entity(CLS_A, alias="a")
               .related(CLS_B, alias="b", via=[PRED_P]).to_sparql())
        old = (Query(client=None).find_entity(_class=CLS_A, alias="a")
               .find_related(_class=CLS_B, alias="b", predicates=[PRED_P]).to_sparql())
        assert canon(new) == canon(old)

    def test_direction_chain(self):
        new = (q().entity(CLS_A, alias="a")
               .related(CLS_B, alias="b", direction="downstream", max_depth=2).to_sparql())
        old = (Query(client=None).find_entity(_class=CLS_A, alias="a")
               .find_related(_class=CLS_B, alias="b", direction="downstream", hops=2).to_sparql())
        assert canon(new) == canon(old)

    def test_measurement_chain(self):
        new = (q().entity(CLS_A, alias="ro").measurement().to_sparql())
        old = (Query(client=None).find_entity(_class=CLS_A, alias="ro")
               .find_data().to_sparql())
        assert canon(new) == canon(old)

    def test_directional_measurement_chain(self):
        new = (q().entity(CLS_A, alias="ro")
               .measurement(direction="upstream", max_depth=3).to_sparql())
        old = (Query(client=None).find_entity(_class=CLS_A, alias="ro")
               .find_related_data(direction="upstream", hops=3).to_sparql())
        assert canon(new) == canon(old)

    def test_soft_sensor_shape(self):
        """entity -> CP class -> measurement, the copy-pasted notebook chain.

        The CP hop uses an explicit predicate: any-traversal deliberately
        diverged from the legacy compiler (it resolves client-side now).
        """
        ro = "urn:nawi-water-ontology#ReverseOsmosisMembrane"
        cp_cls = "http://data.ashrae.org/standard223#OutletConnectionPoint"
        cp_pred = "http://data.ashrae.org/standard223#hasConnectionPoint"
        new = (q().entity(ro, alias="ro")
               .related(cp_cls, alias="out", via=[cp_pred])
               .measurement(alias="permeate").to_sparql())
        old = (Query(client=None).find_entity(_class=ro, alias="ro")
               .find_related(_class=cp_cls, alias="out", predicates=[cp_pred])
               .find_data(alias="permeate").to_sparql())
        assert canon(new) == canon(old)


class TestIncludeConnectionPoints:
    def test_default_includes_cp_union(self):
        s = q().entity(CLS_A, alias="ro").measurement(alias="m").to_sparql()
        assert "hasConnectionPoint" in s and "UNION" in s

    def test_false_drops_cp_alternative(self):
        s = (q().entity(CLS_A, alias="ro")
             .measurement(alias="m", include_connection_points=False).to_sparql())
        assert "hasConnectionPoint" not in s and "UNION" not in s
        assert "?v0 ?p_e0_1 ?v1 ." in s

    def test_star_respects_flag(self):
        b = (q().entity(CLS_A, alias="a").entity(CLS_A, alias="b")
             .measurement(frm="*", include_connection_points=False))
        assert all(e.cp_union is False for e in b.query_graph.edges)

    def test_directional_rejects_flag(self):
        base = q().entity(CLS_A, alias="ro")
        with pytest.raises(ValueError, match="only applies to the"):
            base.measurement(direction="upstream", include_connection_points=False)
        with pytest.raises(ValueError, match="only applies to the"):
            base.measurement(direction="upstream", nearest=True,
                             include_connection_points=False)


class TestAliasing:
    def test_alias_names_current_node(self):
        b = q().entity(CLS_A).alias("ro")
        g = b.query_graph
        assert g.aliases["ro"] == 0 and g.aliases_reverse[0] == "ro"
        assert g.current_pointer == 0

    def test_previous_alias_still_resolves(self):
        b = q().entity(CLS_A, alias="a").alias("ro")
        g = b.query_graph
        assert g.aliases["a"] == 0 and g.aliases["ro"] == 0
        assert g.aliases_reverse[0] == "ro"  # display name is the latest

    def test_alias_on_empty_query_errors(self):
        with pytest.raises(ValueError, match="no current node"):
            q().alias("x")

    def test_default_alias_local_name_without_client(self):
        b = q().entity(CLS_A)
        assert b.query_graph.aliases_reverse[0] == "TypeA"  # no client -> no CURIE

    def test_default_alias_text_and_curie(self):
        from unittest.mock import MagicMock
        client = MagicMock()
        client.resolve.return_value = CLS_A
        client.compact_uri.side_effect = lambda u: "test:" + u.rsplit("#", 1)[-1]
        b = Q(client=client).entity("tank")
        assert b.query_graph.aliases_reverse[0] == "tank"      # the text as given
        b2 = Q(client=client).entity(CLS_A)
        assert b2.query_graph.aliases_reverse[0] == "test:TypeA"  # CURIE

    def test_default_alias_uniquified(self):
        b = q().entity(CLS_A).entity(CLS_A)
        g = b.query_graph
        assert g.aliases_reverse[0] == "TypeA" and g.aliases_reverse[1] == "TypeA_2"

    def test_uri_only_node_keeps_numeric_fallback(self):
        b = q().entity(uri=INST)
        assert b.query_graph.aliases_reverse[0] == "0"


class TestMeasurementFrmList:
    def test_list_attaches_per_named_entity(self):
        b = (q().entity(CLS_A, alias="pump").related(CLS_B, alias="tank", max_depth=1)
             .entity(CLS_A, alias="other")
             .measurement(frm=["pump", "tank"]))
        g = b.query_graph
        assert g.aliases["pump_data"] == 3 and g.aliases["tank_data"] == 4
        assert sorted(g.data_nodes) == [3, 4]
        assert {e.source_id for e in g.edges if e.target_id in g.data_nodes} == {0, 1}

    def test_list_deduplicates(self):
        b = q().entity(CLS_A, alias="pump").measurement(frm=["pump", "pump"])
        assert len(b.query_graph.data_nodes) == 1

    def test_unknown_alias_in_list(self):
        with pytest.raises(ValueError, match="unknown alias 'nope'"):
            q().entity(CLS_A, alias="pump").measurement(frm=["pump", "nope"])

    def test_empty_list(self):
        with pytest.raises(ValueError, match="frm list is empty"):
            q().entity(CLS_A, alias="pump").measurement(frm=[])

    def test_list_with_direction_errors(self):
        with pytest.raises(ValueError, match="only combines with the non-directional"):
            q().entity(CLS_A, alias="pump").measurement(frm=["pump"], direction="upstream")

    def test_attrs_apply_to_all_listed(self):
        qk = "http://qudt.org/vocab/quantitykind/PH"
        b = (q().entity(CLS_A, alias="pump").related(CLS_B, alias="tank", max_depth=1)
             .measurement(frm=["pump", "tank"], quantity_kind=qk))
        g = b.query_graph
        assert all(g.data_nodes[n].filters == {"quantity_kind": qk} for n in g.data_nodes)


class TestDrop:
    def test_drop_pointer_removes_from_select(self):
        b = (q().entity(uri=INST, alias="sys").drop()
             .related(CLS_B, alias="eq", via=[PRED_P]))
        s = b.to_sparql()
        first = s.splitlines()[0]
        assert "?v0" not in first and "?v1" in first
        assert f"VALUES ?v0 {{ <{INST}> }}" in s  # still constrains the pattern

    def test_drop_by_alias_and_multiple(self):
        b = (q().entity(CLS_A, alias="a").entity(CLS_B, alias="b")
             .entity(CLS_A, alias="c").drop("a", "b"))
        first = b.to_sparql().splitlines()[0]
        assert "?v0" not in first and "?v1" not in first and "?v2" in first
        assert b.query_graph.current_pointer == 2  # pointer untouched

    def test_dropped_data_node_hides_internals_too(self):
        b = q().entity(CLS_A, alias="ro").measurement(alias="m").drop()
        first = b.to_sparql().splitlines()[0]
        assert "?ext1" not in first and "?unit1" not in first and "?v1" not in first

    def test_drop_all_errors_at_compile(self):
        b = q().entity(CLS_A, alias="a").drop()
        with pytest.raises(ValueError, match="nothing left to select"):
            b.to_sparql()

    def test_unknown_alias(self):
        with pytest.raises(ValueError, match="unknown column"):
            q().entity(CLS_A, alias="a").drop("nope")

    def test_empty_query_errors(self):
        with pytest.raises(ValueError, match="no current node"):
            q().drop()
