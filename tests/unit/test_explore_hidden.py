"""Tests for hidden predicates, via= forms, and direction step constants."""

from unittest.mock import MagicMock

import pytest
from rdflib.plugins.sparql import prepareQuery

from acquirium.Client.explore.core import Q
from acquirium.Client.explore.directions import (
    DOWNSTREAM_EQUIPMENT,
    DOWNSTREAM_PROPERTY,
    EQUIPMENT_STEPS,
    PROPERTY_STEPS,
    UPSTREAM_EQUIPMENT,
    UPSTREAM_PROPERTY,
)
from acquirium.Client.explore.hidden import hidden_predicates, hide, unhide

CLS_A = "urn:test#TypeA"
CLS_B = "urn:test#TypeB"
CNX = "http://data.ashrae.org/standard223#cnx"
CONNECTED_TO = "http://data.ashrae.org/standard223#connectedTo"


def q() -> Q:
    return Q(client=None)


@pytest.fixture(autouse=True)
def clean_state():
    unhide()
    yield
    unhide()


class TestVia:
    def test_single_predicate_is_repeatable(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=CONNECTED_TO)
        (edge,) = b.query_graph.edges
        assert edge.patterns == (((((CONNECTED_TO, None),),), True),)
        assert edge.hops == 3  # default bound

    def test_inverted_predicate(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=f"^{CONNECTED_TO}")
        assert b.query_graph.edges[0].patterns == (((((f"^{CONNECTED_TO}", None),),), True),)

    def test_max_depth_bounds_single_predicate(self):
        b = q().entity(CLS_A).related(CLS_B, via=CONNECTED_TO, max_depth=5)
        assert b.query_graph.edges[0].hops == 5

    def test_all_is_synonym_for_any(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via="all")
        (edge,) = b.query_graph.edges
        assert edge.patterns == ((((("*", None),),), True),)

    def test_text_predicate_resolves(self):
        client = MagicMock()
        client.resolve.side_effect = lambda t, k=None, **kw: (
            CLS_A if k == "class" else "urn:test#feedsChemicalTo")
        b = (Q(client=client).entity(CLS_A, alias="a")
             .related(CLS_B, alias="b", via="feeds chemical to"))
        assert b.query_graph.edges[0].patterns == \
            ((((("urn:test#feedsChemicalTo", None),),), True),)

    def test_unresolvable_text_predicate_errors(self):
        client = MagicMock()
        client.resolve.side_effect = lambda t, k=None, **kw: (
            CLS_A if k == "class" else None)
        with pytest.raises(ValueError, match="could not resolve via predicate"):
            Q(client=client).entity(CLS_A).related(CLS_B, via="teleport beam")

    def test_bad_via_type(self):
        with pytest.raises(ValueError, match="via must be"):
            q().entity(CLS_A).related(CLS_B, via=42)

    def test_sparql_parses(self):
        s = q().entity(CLS_A, alias="a").related(CLS_B, alias="b",
                                                 via=CONNECTED_TO, max_depth=2).to_sparql()
        assert f"<{CONNECTED_TO}>" in s
        prepareQuery(s)


class TestDirectionSteps:
    def test_constants_are_program_alternatives(self):
        for alts in (DOWNSTREAM_EQUIPMENT, UPSTREAM_EQUIPMENT,
                     DOWNSTREAM_PROPERTY, UPSTREAM_PROPERTY):
            assert len(alts) == 4
            for chain in alts:
                for pred, node_cls in chain:
                    assert isinstance(pred, str)
                    assert node_cls is None or node_cls.startswith("http")

    def test_measurement_nearest_uses_direction_steps(self):
        b = q().entity(CLS_A, alias="ro").measurement(
            direction="upstream", nearest=True, max_depth=2)
        (edge,) = b.query_graph.edges
        assert edge.patterns == ((EQUIPMENT_STEPS["upstream"], True),
                                 (PROPERTY_STEPS["upstream"], False))
        assert edge.hops == 3  # equipment steps + property step

    def test_property_steps_check_cp_classes(self):
        outlet = "http://data.ashrae.org/standard223#OutletConnectionPoint"
        assert any(node == outlet for chain in DOWNSTREAM_PROPERTY
                   for _, node in chain)


class TestHide:
    def test_requires_uri(self):
        with pytest.raises(ValueError, match="not a URI"):
            hide("s223:cnx")

    def test_defaults_cover_attribute_predicates(self):
        from acquirium.Client.explore.attributes import REGISTRY
        h = hidden_predicates()
        assert {str(p) for a in REGISTRY.values() for p in a.predicates} <= h
        assert "http://www.w3.org/2000/01/rdf-schema#subClassOf" in h
        assert "http://data.ashrae.org/standard223#hasProperty" in h
        assert "https://brickschema.org/schema/Brick/ref#hasExternalReference" in h
        assert CNX in h

    def test_any_traversal_filters_defaults(self):
        s = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", max_depth=2).to_sparql()
        assert "NOT IN (" in s and f"<{CNX}>" in s
        assert "<http://www.w3.org/2000/01/rdf-schema#subClassOf>" in s
        prepareQuery(s)

    def test_data_edge_exempt_from_hiding(self):
        s = q().entity(CLS_A, alias="a").measurement(alias="m").to_sparql()
        assert "NOT IN" not in s

    def test_unhide_lifts_a_default(self):
        unhide(CNX)
        assert CNX not in hidden_predicates()

    def test_hide_adds_on_top_and_bare_unhide_resets(self):
        hide("urn:x#custom")
        unhide(CNX)
        assert "urn:x#custom" in hidden_predicates() and CNX not in hidden_predicates()
        unhide()
        h = hidden_predicates()
        assert "urn:x#custom" not in h and CNX in h

    def test_explicit_via_overrides_hiding(self):
        s = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=CNX).to_sparql()
        assert f"<{CNX}>" in s and "NOT IN" not in s
        s2 = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=[CNX]).to_sparql()
        assert f"(<{CNX}>)" in s2 and "NOT IN" not in s2


class TestViaDirectionSteps:
    def test_via_accepts_step_alternatives(self):
        b = (q().entity(CLS_A, alias="ro")
             .related(CLS_B, alias="tank", via=UPSTREAM_EQUIPMENT,
                      nearest=True, max_depth=6))
        (edge,) = b.query_graph.edges
        assert edge.patterns == ((UPSTREAM_EQUIPMENT, True),)
        assert edge.nearest and edge.hops == 6
