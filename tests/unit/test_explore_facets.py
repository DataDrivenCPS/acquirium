"""Tests for facets(): multi-attribute summary with model/vocabulary fallback."""

from unittest.mock import MagicMock

import pytest

from acquirium.Client.explore.core import Query
from acquirium.Client.explore.facets import (
    VOCAB_FRAGMENTS,
    FacetSummary,
    clear_facet_cache,
    model_options,
    vocab_options,
)
from acquirium.Client.explore.attributes import REGISTRY

CLS_A = "urn:test#TypeA"

DATA_ATTRS = ["type", "medium", "substance", "quantity_kind", "unit",
              "enumeration_kind", "data_source"]
ENTITY_ATTRS = ["type", "process", "cp_type", "medium"]


@pytest.fixture(autouse=True)
def clean_cache():
    clear_facet_cache()
    yield
    clear_facet_cache()


def make_client(responder):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 3
    client.sparql_query.side_effect = responder
    client.compact_uri.side_effect = lambda x: str(x).rsplit("#", 1)[-1]
    return client


def empty_res(sparql, include_dependencies=True):
    if "?opt" in sparql:
        return {"columns": ["opt", "count"], "rows": []}
    return {"columns": ["uri"], "rows": []}


class TestFacetsSelection:
    def test_data_node_attrs(self):
        client = make_client(empty_res)
        f = (Query(client=client).entity(CLS_A, alias="ro")
             .measurement(alias="m").facets())
        assert f.attrs() == DATA_ATTRS
        assert f.node_alias == "m"

    def test_entity_node_attrs(self):
        client = make_client(empty_res)
        f = Query(client=client).entity(CLS_A, alias="ro").facets()
        assert f.attrs() == ENTITY_ATTRS


class TestFallbackChain:
    def test_matched_wins(self):
        nodes = [f"urn:p#m{i}" for i in range(4)]

        def responder(sparql, include_dependencies=True):
            if sparql.startswith("SELECT ?v ?opt"):
                if "ofMedium" in sparql:  # medium lookup: all four carry Water
                    return {"columns": ["v", "opt"],
                            "rows": [[n, "urn:m#Water"] for n in nodes]}
                return {"columns": ["v", "opt"], "rows": []}
            if sparql.startswith("SELECT DISTINCT ?v0"):  # pattern execute
                return {"columns": ["v0", "v1"],
                        "rows": [["urn:p#ro", n] for n in nodes]}
            return empty_res(sparql)
        client = make_client(responder)
        f = Query(client=client).entity(CLS_A, alias="ro").measurement(alias="m").facets()
        assert f.scopes["medium"] == "matched"
        assert f["medium"]["medium"].to_list() == ["Water"]
        assert f["medium"]["count"].to_list() == [4]

    def test_model_fallback_when_pattern_empty(self):
        def responder(sparql, include_dependencies=True):
            if "COUNT(DISTINCT ?v1)" in sparql:
                return {"columns": ["opt", "count"], "rows": []}
            if "COUNT(DISTINCT ?x)" in sparql:
                return {"columns": ["opt", "count"], "rows": [["urn:m#Brine", 2]]}
            return empty_res(sparql)
        client = make_client(responder)
        f = Query(client=client).entity(CLS_A, alias="ro").measurement(alias="m").facets()
        assert f.scopes["medium"] == "model"
        assert f["medium"]["medium"].to_list() == ["Brine"]

    def test_vocab_fallback_last(self):
        def responder(sparql, include_dependencies=True):
            if "SELECT DISTINCT ?uri" in sparql:
                return {"columns": ["uri"], "rows": [["urn:m#Seawater"]]}
            return {"columns": ["opt", "count"], "rows": []}
        client = make_client(responder)
        f = Query(client=client).entity(CLS_A, alias="ro").measurement(alias="m").facets()
        assert f.scopes["medium"] == "vocabulary"
        assert f["medium"]["medium"].to_list() == ["Seawater"]
        assert f["medium"]["count"].to_list() == [0]
        # unit has no vocabulary fragment -> stops at model scope, empty
        assert f.scopes["unit"] == "vocabulary" if "unit" in VOCAB_FRAGMENTS else f.scopes["unit"]
        assert f["unit"].height == 0


class TestModuleCache:
    def test_model_options_cached_by_version(self):
        client = make_client(
            lambda s, include_dependencies=True: {"columns": ["opt", "count"], "rows": []},
        )
        attr = REGISTRY["unit"]
        model_options(client, attr, 1)
        model_options(client, attr, 1)
        assert client.sparql_query.call_count == 1
        model_options(client, attr, 2)
        assert client.sparql_query.call_count == 2

    def test_vocab_only_for_fragment_attrs(self):
        client = make_client(empty_res)
        assert vocab_options(client, REGISTRY["unit"], 1) == []
        client.sparql_query.assert_not_called()
        vocab_options(client, REGISTRY["substance"], 1)
        assert client.sparql_query.call_count == 1
        assert "LIMIT 200" in client.sparql_query.call_args.args[0]


class TestSummaryObject:
    def test_repr_and_indexing(self):
        nodes = [f"urn:p#m{i}" for i in range(3)]

        def responder(sparql, include_dependencies=True):
            if sparql.startswith("SELECT ?v ?opt"):
                if "hasQuantityKind" in sparql:
                    return {"columns": ["v", "opt"],
                            "rows": [[n, "urn:qk#PH"] for n in nodes]}
                return {"columns": ["v", "opt"], "rows": []}
            if sparql.startswith("SELECT DISTINCT ?v0"):
                return {"columns": ["v0", "v1"],
                        "rows": [["urn:p#ro", n] for n in nodes]}
            return empty_res(sparql)
        client = make_client(responder)
        f = Query(client=client).entity(CLS_A, alias="ro").measurement(alias="m").facets()
        assert isinstance(f, FacetSummary)
        text = repr(f)
        assert "quantity_kind [matched]: PH (3)" in text
        assert "medium" in text and "m" in text
        assert "quantity_kind" in f and f["quantity_kind"].height == 1

    def test_errors(self):
        client = make_client(empty_res)
        with pytest.raises(ValueError, match="unknown alias"):
            Query(client=client).entity(CLS_A, alias="ro").facets(of="nope")
