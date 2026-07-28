"""Tests for shortcuts (Step-based), via expressions, hide(), and compilation."""

from unittest.mock import MagicMock

import pytest
from rdflib.plugins.sparql import prepareQuery

from acquirium.Client.explore.core import Q
from acquirium.Client.explore.shortcuts import (
    SHORTCUTS,
    Shortcut,
    Step,
    get_shortcut,
    hidden_predicates,
    hide,
    register_shortcut,
    unhide,
)

CLS_A = "urn:test#TypeA"
CLS_B = "urn:test#TypeB"
CNX = "http://data.ashrae.org/standard223#cnx"
CONNECTED_TO = "http://data.ashrae.org/standard223#connectedTo"
HAS_PROPERTY = "http://data.ashrae.org/standard223#hasProperty"
OUTLET_CP = "http://data.ashrae.org/standard223#OutletConnectionPoint"


def q() -> Q:
    return Q(client=None)


@pytest.fixture(autouse=True)
def clean_state():
    unhide()
    yield
    unhide()
    SHORTCUTS.pop("dosing", None)


class TestRegistry:
    def test_defaults_present(self):
        assert {"next_equipment", "downstream_property", "upstream_property"} <= set(SHORTCUTS)

    def test_get_unknown_returns_none(self):
        assert get_shortcut("teleport") is None

    def test_register_with_text_steps(self):
        register_shortcut(Shortcut("dosing", ((Step("feeds chemical to"),),)))
        assert get_shortcut("dosing") is not None

    def test_register_validation(self):
        with pytest.raises(ValueError, match="reserved"):
            register_shortcut(Shortcut("any", ((Step("urn:x#p"),),)))
        with pytest.raises(ValueError, match="may not contain"):
            register_shortcut(Shortcut("a/b", ((Step("urn:x#p"),),)))
        with pytest.raises(ValueError, match="non-empty"):
            register_shortcut(Shortcut("empty", ()))
        with pytest.raises(ValueError, match="non-empty"):
            register_shortcut(Shortcut("hollow", ((),)))


class TestViaExpression:
    def test_single_shortcut_exact_one_step(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via="next_equipment")
        (edge,) = b.query_graph.edges
        assert edge.hops == 1 and edge.predicates is None and edge.direction is None
        assert edge.patterns == (((((CONNECTED_TO, None),),), False),)
        s = b.to_sparql()
        assert f"?v0 <{CONNECTED_TO}> ?v1 ." in s
        prepareQuery(s)

    def test_multi_alternative_shortcut_unions_with_node_check(self):
        s = q().entity(CLS_A, alias="ro").related(CLS_B, alias="p",
                                                  via="downstream_property").to_sparql()
        assert "UNION" in s
        assert (f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>/"
                f"<http://www.w3.org/2000/01/rdf-schema#subClassOf>* <{OUTLET_CP}> .") in s
        prepareQuery(s)

    def test_star_composition(self):
        b = q().entity(CLS_A, alias="a").related(
            CLS_B, alias="p", via="next_equipment*/downstream_property", max_depth=3)
        s = b.to_sparql()
        prepareQuery(s)
        # totals 1..3 -> 0, 1, or 2 equipment steps before the property step
        assert s.count(f"<{HAS_PROPERTY}>") >= 3
        # the zero-repetition variant reaches the property shortcut directly from ?v0
        assert f"?v0 ^<http://data.ashrae.org/standard223#connectsFrom>" in s

    def test_star_default_budget(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="p",
                                                 via="next_equipment*/downstream_property")
        assert b.query_graph.edges[0].hops == 4  # 1 fixed + 3 star budget

    def test_pure_star(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b",
                                                 via="next_equipment*", max_depth=2)
        s = b.to_sparql()
        prepareQuery(s)
        assert s.count(f"<{CONNECTED_TO}>") == 3  # 1-step variant + 2-step variant

    def test_fixed_composition_sets_exact_hops(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b",
                                                 via="next_equipment/next_equipment")
        assert b.query_graph.edges[0].hops == 2
        s = b.to_sparql()
        assert "?x_e0_c0_0" in s  # intermediate between the two fixed steps
        prepareQuery(s)

    def test_max_depth_below_fixed_steps_errors(self):
        with pytest.raises(ValueError, match="fixed step"):
            q().entity(CLS_A).related(CLS_B, via="next_equipment/next_equipment", max_depth=1)

    def test_uri_token_passthrough(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via="urn:test#feeds")
        assert b.query_graph.edges[0].patterns == ((((("urn:test#feeds", None),),), False),)

    def test_full_http_uri_is_single_segment(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=CONNECTED_TO)
        assert b.query_graph.edges[0].patterns == (((((CONNECTED_TO, None),),), False),)

    def test_inverted_uri_token(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=f"^{CONNECTED_TO}")
        assert b.query_graph.edges[0].patterns == (((((f"^{CONNECTED_TO}", None),),), False),)

    def test_direction_conflicts_with_via_expression(self):
        with pytest.raises(ValueError, match="direction only combines"):
            q().entity(CLS_A).related(CLS_B, via="next_equipment", direction="upstream")

    def test_bad_via_type(self):
        with pytest.raises(ValueError, match="via must be"):
            q().entity(CLS_A).related(CLS_B, via=42)


class TestViaResolution:
    def make_client(self, mapping):
        client = MagicMock()
        client.resolve.return_value = mapping
        return client

    def test_text_token_resolves_as_predicate(self):
        client = self.make_client({"p_0_0_0": "urn:test#feedsChemicalTo"})
        b = (Q(client=client).entity(CLS_A, alias="a")
             .related(CLS_B, alias="b", via="feeds chemical to"))
        client.resolve.assert_called_once_with(
            {"p_0_0_0": ("feeds chemical to", "predicate")}, min_score=0.4)
        assert b.query_graph.edges[0].patterns == ((((("urn:test#feedsChemicalTo", None),),), False),)

    def test_shortcut_with_text_steps_resolves_on_use(self):
        register_shortcut(Shortcut("dosing", ((Step("feeds chemical to", node="chemical feeder"),),)))
        client = self.make_client({"p_0_0_0": "urn:test#feeds", "n_0_0_0": "urn:test#Feeder"})
        b = Q(client=client).entity(CLS_A, alias="a").related(CLS_B, alias="b", via="dosing")
        record = client.resolve.call_args.args[0]
        assert record == {"p_0_0_0": ("feeds chemical to", "predicate"),
                          "n_0_0_0": ("chemical feeder", "class")}
        assert b.query_graph.edges[0].patterns == ((((("urn:test#feeds", "urn:test#Feeder"),),), False),)

    def test_unresolvable_token_mentions_shortcuts(self):
        client = self.make_client({"p_0_0_0": None})
        with pytest.raises(ValueError, match="not a registered shortcut"):
            Q(client=client).entity(CLS_A).related(CLS_B, via="teleport")


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
        assert "NOT IN (" in s
        assert f"<{CNX}>" in s
        assert "<http://www.w3.org/2000/01/rdf-schema#subClassOf>" in s
        assert "FILTER(isIRI(" in s  # wildcard program preview
        prepareQuery(s)

    def test_data_edge_exempt_from_hiding(self):
        s = q().entity(CLS_A, alias="a").measurement(alias="m").to_sparql()
        assert "NOT IN" not in s

    def test_unhide_lifts_a_default(self):
        unhide(CNX)
        assert CNX not in hidden_predicates()
        s = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", max_depth=1).to_sparql()
        assert f"<{CNX}>" not in s and "NOT IN" in s

    def test_hide_adds_on_top_and_bare_unhide_resets(self):
        hide("urn:x#custom")
        unhide(CNX)
        assert "urn:x#custom" in hidden_predicates() and CNX not in hidden_predicates()
        unhide()
        h = hidden_predicates()
        assert "urn:x#custom" not in h and CNX in h

    def test_explicit_via_overrides_hiding(self):
        ## naming a hidden predicate means it: no filter, plain traversal
        s = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=CNX).to_sparql()
        assert f"<{CNX}>" in s and "NOT IN" not in s
        s2 = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via=[CNX]).to_sparql()
        assert f"(<{CNX}>)" in s2 and "NOT IN" not in s2
