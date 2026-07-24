"""Tests for nearest-match traversal: walk_program BFS, pruning, resolution."""

from unittest.mock import MagicMock

import pytest

from acquirium.Client.explore.core import Q
from acquirium.Client.explore.traverse import (
    clear_segment_cache,
    materialize_segment,
    resolve_program_edges,
    walk_program,
)

CLS_A = "urn:test#TypeA"
TANK = "urn:test#Tank"
CONNECTED_TO = "http://data.ashrae.org/standard223#connectedTo"


@pytest.fixture(autouse=True)
def clean_cache():
    clear_segment_cache()
    yield
    clear_segment_cache()


def adj(*pairs):
    out = {}
    for s, t in pairs:
        out.setdefault(s, set()).add(t)
    return out


class TestWalkProgram:
    def test_linear_nearest_stops_at_first_layer(self):
        a = adj(("a", "b"), ("b", "c"), ("c", "d"))
        res = walk_program([a], [True], ["a"], 3, nearest=True)
        assert res == {"a": {"b": 1}}

    def test_cone_collects_all(self):
        a = adj(("a", "b"), ("b", "c"))
        res = walk_program([a], [True], ["a"], 3, nearest=False)
        assert res == {"a": {"b": 1, "c": 2}}

    def test_ties_all_kept(self):
        a = adj(("a", "b1"), ("a", "b2"), ("b1", "c"))
        res = walk_program([a], [True], ["a"], 3, nearest=True)
        assert res == {"a": {"b1": 1, "b2": 1}}

    def test_accept_skips_closer_nonmatching(self):
        a = adj(("a", "b"), ("b", "tank"))
        res = walk_program([a], [True], ["a"], 3, nearest=True, accept={"tank"})
        assert res == {"a": {"tank": 2}}

    def test_per_source_independence(self):
        a = adj(("a", "m"), ("b", "x"), ("x", "m"))
        res = walk_program([a], [True], ["a", "b"], 3, nearest=True, accept={"m"})
        assert res == {"a": {"m": 1}, "b": {"m": 2}}

    def test_max_total_cutoff(self):
        a = adj(("a", "b"), ("b", "c"))
        res = walk_program([a], [True], ["a"], 1, nearest=True, accept={"c"})
        assert res == {}

    def test_cycle_terminates(self):
        a = adj(("a", "b"), ("b", "a"))
        res = walk_program([a], [True], ["a"], 10, nearest=True, accept={"z"})
        assert res == {}

    def test_star_then_fixed_program(self):
        # equipment* then property: a -eq-> b -eq-> c; properties on a and c
        eq = adj(("a", "b"), ("b", "c"))
        prop = adj(("a", "pa"), ("c", "pc"))
        res = walk_program([eq, prop], [True, False], ["a"], 5, nearest=True)
        # zero equipment steps: property of a itself at depth 1
        assert res == {"a": {"pa": 1}}
        res2 = walk_program([eq, prop], [True, False], ["a"], 5, nearest=True, accept={"pc"})
        assert res2 == {"a": {"pc": 3}}  # two equipment steps + property step

    def test_fixed_segment_not_repeatable(self):
        a = adj(("a", "b"), ("b", "c"))
        res = walk_program([a], [False], ["a"], 5, nearest=False)
        assert res == {"a": {"b": 1}}  # cannot chain a non-star segment


class TestMaterialize:
    def make_client(self, rows):
        client = MagicMock()
        client.base_url = "http://test:8000"
        client.sparql_query.return_value = {"columns": ["s", "t"], "rows": rows}
        return client

    def test_builds_adjacency_and_caches(self):
        client = self.make_client([["a", "b"], ["a", "c"], [None, "x"]])
        alts = (((CONNECTED_TO, None),),)
        adj1 = materialize_segment(client, alts, version=1)
        assert adj1 == {"a": {"b", "c"}}
        materialize_segment(client, alts, version=1)
        assert client.sparql_query.call_count == 1
        materialize_segment(client, alts, version=2)  # version bump refetches
        assert client.sparql_query.call_count == 2

    def test_query_shape(self):
        client = self.make_client([])
        materialize_segment(client, (((CONNECTED_TO, None),),), version=1)
        sparql = client.sparql_query.call_args.args[0]
        assert sparql.startswith("SELECT DISTINCT ?s ?t")
        assert f"?s <{CONNECTED_TO}> ?t ." in sparql


class TestResolveNearest:
    def build_query(self, client):
        return (Q(client=client).entity(CLS_A, alias="ro")
                .related(TANK, alias="tank", via="next_equipment*",
                         nearest=True, max_depth=4))

    def make_client(self, responses):
        client = MagicMock()
        client.base_url = "http://test:8000"
        client.graph_version.return_value = 7
        client.sparql_query.side_effect = responses
        return client

    def test_end_to_end_pairs_and_values(self):
        client = self.make_client([
            # 1: source fetch (pruned graph -> v0)
            {"columns": ["v0"], "rows": [["urn:p#ro1"], ["urn:p#ro2"]]},
            # 2: target accept set (tanks)
            {"columns": ["v1"], "rows": [["urn:p#tankA"], ["urn:p#tankB"]]},
            # 3: segment materialization
            {"columns": ["s", "t"], "rows": [
                ["urn:p#ro1", "urn:p#tankA"],          # ro1 -> tankA (1 step)
                ["urn:p#ro2", "urn:p#mid"],            # ro2 -> mid -> tankB (2 steps)
                ["urn:p#mid", "urn:p#tankB"],
                ["urn:p#mid", "urn:p#other"],          # non-tank, must be ignored
            ]},
            # 4: final query
            {"columns": ["v0", "v1"], "rows": []},
        ])
        b = self.build_query(client)
        b.execute()
        final_sparql = client.sparql_query.call_args.args[0]
        assert "VALUES (?v0 ?v1) { (<urn:p#ro1> <urn:p#tankA>) (<urn:p#ro2> <urn:p#tankB>) }" in final_sparql
        assert client.sparql_query.call_count == 4

    def test_source_query_excludes_target(self):
        client = self.make_client([
            {"columns": ["v0"], "rows": []},   # no sources -> empty pairs
            {"columns": ["v0", "v1"], "rows": []},
        ])
        b = self.build_query(client)
        b.execute()
        source_sparql = client.sparql_query.call_args_list[0].args[0]
        assert "?v1" not in source_sparql and TANK not in source_sparql
        final_sparql = client.sparql_query.call_args.args[0]
        assert "VALUES (?v0 ?v1) {  }" in final_sparql

    def test_to_sparql_previews_without_resolution(self):
        client = self.make_client([])
        s = self.build_query(client).to_sparql()
        assert "VALUES" not in s and CONNECTED_TO in s
        client.sparql_query.assert_not_called()


class TestNearestValidation:
    def test_related_nearest_any_builds_wildcard_program(self):
        b = Q(client=None).entity(CLS_A).related(TANK, nearest=True)
        (edge,) = b.query_graph.edges
        alternatives = ((("*", None),),)
        assert edge.patterns == ((alternatives, True),)
        assert edge.nearest and edge.hops == 0  # unbounded by default

    def test_related_nearest_any_with_direction_errors(self):
        with pytest.raises(ValueError, match="directional shortcut"):
            Q(client=None).entity(CLS_A).related(TANK, nearest=True, direction="upstream")

    def test_related_nearest_with_predicate_list(self):
        b = Q(client=None).entity(CLS_A).related(TANK, via=["urn:test#p"],
                                                 nearest=True, max_depth=2)
        (edge,) = b.query_graph.edges
        assert edge.nearest and edge.patterns == ((((("urn:test#p", None),),), True),)

    def test_measurement_nearest_needs_direction(self):
        with pytest.raises(ValueError, match="requires direction"):
            Q(client=None).entity(CLS_A).measurement(nearest=True)

    def test_measurement_nearest_builds_program_edge(self):
        b = Q(client=None).entity(CLS_A, alias="ro").measurement(
            direction="upstream", nearest=True, max_depth=2)
        g = b.query_graph
        (edge,) = g.edges
        assert edge.nearest and edge.hops == 3  # 2 equipment steps + property step
        assert [star for _, star in edge.patterns] == [True, False]
        assert 1 in g.data_nodes and g.aliases["ro_upstream_data"] == 1
        # no intermediate entity node, unlike the non-nearest direction branch
        assert len(g.nodes) == 2


class TestWildcardSegment:
    def test_preview_sparql_has_filters(self):
        from rdflib.plugins.sparql import prepareQuery
        s = (Q(client=None).entity(CLS_A, alias="a")
             .related(TANK, alias="t", nearest=True, max_depth=2).to_sparql())
        assert "?p_e0_" in s and "NOT IN" in s and "FILTER(isIRI(" in s
        prepareQuery(s)

    def test_materialize_wildcard_query_shape(self):
        client = MagicMock()
        client.base_url = "http://test:8000"
        client.sparql_query.return_value = {"columns": ["s", "t"], "rows": []}
        materialize_segment(client, ((("*", None),),), version=1)
        sparql = client.sparql_query.call_args.args[0]
        assert sparql.startswith("SELECT DISTINCT ?s ?t")
        assert "?s ?p_seg_a0_0 ?t ." in sparql
        assert "NOT IN" in sparql and "FILTER(isIRI(?t))" in sparql

    def test_wildcard_cache_keyed_by_hidden_set(self):
        from acquirium.Client.explore.shortcuts import hide, unhide
        client = MagicMock()
        client.base_url = "http://test:8000"
        client.sparql_query.return_value = {"columns": ["s", "t"], "rows": []}
        alts = ((("*", None),),)
        materialize_segment(client, alts, version=1)
        materialize_segment(client, alts, version=1)
        assert client.sparql_query.call_count == 1
        hide("urn:x#extra")
        try:
            materialize_segment(client, alts, version=1)
            assert client.sparql_query.call_count == 2
        finally:
            unhide()


class TestConeResolution:
    """Non-nearest program edges resolve client-side too, collecting all matches."""

    def make_client(self, responses):
        client = MagicMock()
        client.base_url = "http://test:8000"
        client.graph_version.return_value = 7
        client.sparql_query.side_effect = responses
        return client

    def test_shortcut_cone_collects_all_matches(self):
        client = self.make_client([
            {"columns": ["v0"], "rows": [["urn:p#ro1"]]},                    # sources
            {"columns": ["v1"], "rows": [["urn:p#tankA"], ["urn:p#tankB"]]},  # accept
            {"columns": ["s", "t"], "rows": [                                 # edges
                ["urn:p#ro1", "urn:p#tankA"],
                ["urn:p#tankA", "urn:p#tankB"],
            ]},
            {"columns": ["v0", "v1"], "rows": []},                            # final
        ])
        b = (Q(client=client).entity(CLS_A, alias="ro")
             .related(TANK, alias="tank", via="next_equipment*", max_depth=4))
        b.execute()
        final_sparql = client.sparql_query.call_args.args[0]
        # nearest would keep only tankA; cone keeps both
        assert "(<urn:p#ro1> <urn:p#tankA>)" in final_sparql
        assert "(<urn:p#ro1> <urn:p#tankB>)" in final_sparql

    def test_any_cone_unbounded(self):
        client = self.make_client([
            {"columns": ["v0"], "rows": [["urn:p#a"]]},
            {"columns": ["v1"], "rows": [["urn:p#z"]]},
            {"columns": ["s", "t"], "rows": [
                ["urn:p#a", "urn:p#b"], ["urn:p#b", "urn:p#c"],
                ["urn:p#c", "urn:p#d"], ["urn:p#d", "urn:p#z"],
            ]},
            {"columns": ["v0", "v1"], "rows": []},
        ])
        b = Q(client=client).entity(CLS_A, alias="a").related(TANK, alias="z")
        b.execute()
        final_sparql = client.sparql_query.call_args.args[0]
        # four hops away — found because default is unbounded
        assert "(<urn:p#a> <urn:p#z>)" in final_sparql

    def test_explicit_predicate_edges_stay_sparql(self):
        client = self.make_client([{"columns": ["v0", "v1"], "rows": []}])
        b = Q(client=client).entity(CLS_A, alias="a").related(TANK, alias="t",
                                                              via=["urn:test#p"])
        b.execute()
        assert client.sparql_query.call_count == 1  # no BFS phases
        assert "VALUES" not in client.sparql_query.call_args.args[0]


class TestUnboundedWalk:
    def test_unbounded_walks_past_any_fixed_bound(self):
        chain = adj(*[(f"n{i}", f"n{i+1}") for i in range(50)])
        res = walk_program([chain], [True], ["n0"], None, nearest=True, accept={"n50"})
        assert res == {"n0": {"n50": 50}}

    def test_zero_means_unbounded(self):
        chain = adj(("a", "b"), ("b", "c"))
        res = walk_program([chain], [True], ["a"], 0, nearest=False)
        assert res == {"a": {"b": 1, "c": 2}}
