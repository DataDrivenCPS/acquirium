"""Tests for shortcuts (named path bundles), hide(), and their compilation."""

import pytest

from acquirium.Client.explore.core import Q
from acquirium.Client.explore.shortcuts import (
    SHORTCUTS,
    Shortcut,
    get_shortcut,
    hidden_predicates,
    hide,
    instantiate_pattern,
    register_shortcut,
    unhide,
)

CLS_A = "urn:test#TypeA"
CLS_B = "urn:test#TypeB"
CNX = "http://data.ashrae.org/standard223#cnx"


def q() -> Q:
    return Q(client=None)


@pytest.fixture(autouse=True)
def clean_hidden():
    unhide()
    yield
    unhide()


class TestRegistry:
    def test_defaults_present(self):
        assert {"next_equipment", "downstream_property", "upstream_property"} <= set(SHORTCUTS)

    def test_unknown_lists_known(self):
        with pytest.raises(ValueError, match="known:"):
            get_shortcut("teleport")

    def test_register_and_replace(self):
        register_shortcut(Shortcut("dosing", ("<urn:my#feedsChemicalTo>",)))
        assert get_shortcut("dosing").patterns == ("<urn:my#feedsChemicalTo>",)
        del SHORTCUTS["dosing"]

    def test_reserved_and_empty(self):
        with pytest.raises(ValueError, match="reserved"):
            register_shortcut(Shortcut("any", ("<urn:x#p>",)))
        with pytest.raises(ValueError, match="at least one pattern"):
            register_shortcut(Shortcut("empty", ()))


class TestInstantiate:
    def test_bare_path(self):
        assert instantiate_pattern("<urn:x#p>/<urn:x#q>", "?v0", "?v1", "u") == \
            "?v0 (<urn:x#p>/<urn:x#q>) ?v1 ."

    def test_template_substitutes_and_renames(self):
        out = instantiate_pattern("?s <urn:x#p> ?m . ?m a <urn:x#C> . ?m <urn:x#q> ?t .",
                                  "?v0", "?v1", "e0_k1_s0_p0")
        assert out == ("?v0 <urn:x#p> ?m_e0_k1_s0_p0 . ?m_e0_k1_s0_p0 a <urn:x#C> . "
                       "?m_e0_k1_s0_p0 <urn:x#q> ?v1 .")


class TestViaShortcut:
    def test_edge_carries_patterns_and_defaults_one_hop(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via="next_equipment")
        (edge,) = b.query_graph.edges
        assert edge.patterns == SHORTCUTS["next_equipment"].patterns
        assert edge.hops == 1 and edge.predicates is None and edge.direction is None

    def test_sparql_single_pattern_single_hop(self):
        s = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", via="next_equipment").to_sparql()
        assert "?v0 (<http://data.ashrae.org/standard223#connectedTo>) ?v1 ." in s

    def test_sparql_multi_pattern_unions(self):
        b = q().entity(CLS_A, alias="ro").related(CLS_B, alias="p", via="downstream_property")
        s = b.to_sparql()
        assert "OutletConnectionPoint" in s and "UNION" in s
        assert "?m_e0_k1_s0_p0" in s  # renamed template var

    def test_multi_hop_chains_steps(self):
        b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b",
                                                 via="next_equipment", max_depth=2)
        s = b.to_sparql()
        assert "?x_e0_1_k2" in s  # intermediate step var at k=2
        assert s.count("connectedTo") >= 3  # k=1 once, k=2 twice

    def test_direction_conflicts_with_shortcut(self):
        with pytest.raises(ValueError, match="direction only combines"):
            q().entity(CLS_A).related(CLS_B, via="next_equipment", direction="upstream")

    def test_bad_via_type(self):
        with pytest.raises(ValueError, match="via must be"):
            q().entity(CLS_A).related(CLS_B, via=42)


class TestHide:
    def test_requires_uri(self):
        with pytest.raises(ValueError, match="not a URI"):
            hide("s223:cnx")

    def test_hide_and_unhide(self):
        hide(CNX)
        assert CNX in hidden_predicates()
        unhide(CNX)
        assert CNX not in hidden_predicates()

    def test_any_traversal_filters_hidden(self):
        hide(CNX)
        s = q().entity(CLS_A, alias="a").related(CLS_B, alias="b", max_depth=2).to_sparql()
        assert f"FILTER(?p_e0_1 NOT IN (<{CNX}>))" in s
        assert f"FILTER(?p_e0_2 NOT IN (<{CNX}>))" in s

    def test_measurement_edge_filters_hidden(self):
        hide(CNX)
        s = q().entity(CLS_A, alias="a").measurement(alias="m").to_sparql()
        assert f"NOT IN (<{CNX}>)" in s

    def test_no_filter_when_nothing_hidden(self):
        s = q().entity(CLS_A, alias="a").related(CLS_B, alias="b").to_sparql()
        assert "NOT IN" not in s
