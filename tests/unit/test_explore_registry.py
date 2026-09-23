"""Tests for the dynamic attribute registry (explore.attributes.Registry).

Built-ins answer without a server; user attributes (``urn:acquirium:attr#``
predicates written by ``insert_metadata``) are discovered from the graph and
then behave like built-in literal attributes in where/include/options/facets.
"""
from unittest.mock import MagicMock

import pytest

from acquirium.Client.explore.attributes import (
    ATTR_NS,
    BOTH,
    DISCOVERY_SPARQL,
    REGISTRY,
    Registry,
    clear_registry_cache,
    user_attributes,
)
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.facets import clear_facet_cache
from acquirium.Client.explore.hidden import hidden_filter, hidden_prefixes, hide, unhide

CLS_A = "urn:test#TypeA"
PREDS = [
    f"{ATTR_NS}last_cleaned",
    f"{ATTR_NS}product_info.year",
    f"{ATTR_NS}product_info.manufacturer",
    f"{ATTR_NS}tags.0",
    f"{ATTR_NS}tags.1",
    f"{ATTR_NS}tags.10",
    f"{ATTR_NS}product_list.0.manufacturer",
    f"{ATTR_NS}product_list.1.manufacturer",
]


@pytest.fixture(autouse=True)
def clean_caches():
    clear_registry_cache()
    clear_facet_cache()
    yield
    clear_registry_cache()
    clear_facet_cache()
    unhide()


def make_client(predicates=PREDS, responder=None):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 7
    client.compact_uri.side_effect = lambda x: str(x).rsplit("#", 1)[-1]

    def default(sparql, include_dependencies=True):
        if sparql == DISCOVERY_SPARQL:
            return {"columns": ["p"], "rows": [[p] for p in predicates]}
        if responder is not None:
            return responder(sparql, include_dependencies)
        return {"columns": [], "rows": []}

    client.sparql_query.side_effect = default
    return client


class TestUserAttributes:
    def test_exact_leaf_paths(self):
        attrs = user_attributes(PREDS)
        assert attrs["last_cleaned"].predicates == (f"{ATTR_NS}last_cleaned",)
        assert attrs["product_info.year"].predicates == (f"{ATTR_NS}product_info.year",)
        assert attrs["tags.1"].predicates == (f"{ATTR_NS}tags.1",)

    def test_list_indices_collapse_in_numeric_order(self):
        attrs = user_attributes(PREDS)
        assert attrs["tags"].predicates == (
            f"{ATTR_NS}tags.0", f"{ATTR_NS}tags.1", f"{ATTR_NS}tags.10")

    def test_nested_list_collapses_index_segment(self):
        attrs = user_attributes(PREDS)
        assert attrs["product_list.manufacturer"].predicates == (
            f"{ATTR_NS}product_list.0.manufacturer",
            f"{ATTR_NS}product_list.1.manufacturer")
        assert "product_list.0" not in attrs  # not a leaf

    def test_shape_of_a_user_attribute(self):
        a = user_attributes(PREDS)["last_cleaned"]
        assert a.literal and a.kind == "any" and a.roles == BOTH and not a.via_subclass

    def test_ignores_foreign_predicates_and_builtin_names(self):
        attrs = user_attributes(["urn:x#foo", f"{ATTR_NS}unit", ATTR_NS])
        assert attrs == {}


class TestRegistry:
    def test_no_client_is_builtins_only(self):
        r = Registry(None)
        assert set(r) == set(REGISTRY)
        assert "last_cleaned" not in r
        with pytest.raises(KeyError):
            r["last_cleaned"]

    def test_builtin_lookup_never_queries(self):
        client = make_client()
        r = Registry(client)
        assert r["unit"] is REGISTRY["unit"] and "medium" in r
        client.sparql_query.assert_not_called()

    def test_discovery_runs_once_per_graph_version(self):
        client = make_client()
        r = Registry(client)
        assert "last_cleaned" in r and "tags" in r and "product_info.year" in r
        Registry(client)["tags.0"]
        assert client.sparql_query.call_count == 1
        client.sparql_query.assert_called_with(DISCOVERY_SPARQL, include_dependencies=False)
        client.graph_version.return_value = 8
        Registry(client)["tags.0"]
        assert client.sparql_query.call_count == 2

    def test_for_role_includes_discovered(self):
        names = {a.name for a in Registry(make_client()).for_role("entity")}
        assert {"type", "process", "last_cleaned", "tags"} <= names
        assert "unit" not in names


class TestQueryUsesRegistry:
    def test_where_kwarg_on_user_attribute(self):
        q = Query(client=make_client()).entity(CLS_A, alias="ro").where(last_cleaned="03-12-1999")
        assert q.query_graph.nodes[0].constraints["attrs"] == {"last_cleaned": "03-12-1999"}
        assert f'?v0 <{ATTR_NS}last_cleaned> "03-12-1999" .' in q.to_sparql()

    def test_where_on_list_matches_any_index(self):
        q = Query(client=make_client()).entity(CLS_A, alias="ro").where(tags="lab")
        s = q.to_sparql()
        assert f'{{ ?v0 <{ATTR_NS}tags.0> "lab" . }} UNION {{ ?v0 <{ATTR_NS}tags.1> "lab" . }}' in s

    def test_unknown_attribute_still_rejected(self):
        with pytest.raises(ValueError, match="unknown attribute"):
            Query(client=make_client()).entity(CLS_A).where(nope=1)

    def test_include_dotted_user_path(self):
        q = (Query(client=make_client()).entity(CLS_A, alias="ro")
             .include("product_info.year"))
        assert q.query_graph.selects == ((0, "product_info.year", False),)
        assert f"OPTIONAL {{ ?v0 (<{ATTR_NS}product_info.year>) ?attr0_product_info.year . }}" \
            in q.to_sparql()

    def test_include_alias_dot_user_path(self):
        q = (Query(client=make_client()).entity(CLS_A, alias="ro").measurement(alias="m")
             .include("ro.product_info.year"))
        assert q.query_graph.selects == ((0, "product_info.year", False),)

    def test_include_all_expands_discovered(self):
        q = Query(client=make_client()).entity(CLS_A, alias="ro").include("all")
        names = {n for _, n, _ in q.query_graph.selects}
        assert {"medium", "process", "last_cleaned", "product_info.year", "tags"} <= names

    def test_include_all_without_client_is_builtins(self):
        q = Query(client=None).entity(CLS_A, alias="ro").include("all")
        names = {n for _, n, _ in q.query_graph.selects}
        assert "last_cleaned" not in names and "medium" in names

    def test_options_on_user_attribute(self):
        def responder(sparql, include_dependencies=True):
            if sparql.startswith("SELECT ?v ?opt"):
                assert f"<{ATTR_NS}last_cleaned>" in sparql
                return {"columns": ["v", "opt"], "rows": [["urn:p#a", "1999"], ["urn:p#b", "1999"]]}
            return {"columns": ["v0"], "rows": [["urn:p#a"], ["urn:p#b"]]}
        df = Query(client=make_client(responder=responder)).entity(CLS_A, alias="ro").options("last_cleaned")
        assert df["last_cleaned"].to_list() == ["1999"] and df["count"].to_list() == [2]

    def test_facets_lists_user_attributes(self):
        def responder(sparql, include_dependencies=True):
            if "?opt" in sparql:
                return {"columns": ["opt", "count"], "rows": []}
            return {"columns": ["uri"], "rows": []}
        f = Query(client=make_client(responder=responder)).entity(CLS_A, alias="ro").facets()
        assert {"last_cleaned", "tags", "product_info.manufacturer"} <= set(f.attrs())


class TestHiddenNamespace:
    def test_attr_namespace_hidden_by_default(self):
        assert ATTR_NS in hidden_prefixes()
        assert f'!STRSTARTS(STR(?p), "{ATTR_NS}")' in hidden_filter("?p")

    def test_unhide_and_hide_prefix(self):
        unhide(ATTR_NS)
        assert ATTR_NS not in hidden_prefixes()
        assert "STRSTARTS" not in hidden_filter("?p")
        hide("urn:x#")
        assert "urn:x#" in hidden_prefixes()
        unhide()
        assert hidden_prefixes() == frozenset({ATTR_NS})

    def test_wildcard_edge_carries_namespace_filter(self):
        q = (Query(client=None).entity(CLS_A, alias="a").related(CLS_A, alias="b", via="any"))
        assert f'!STRSTARTS(STR(?p_e0_c2_g2_a0_0), "{ATTR_NS}")' in q.to_sparql()
