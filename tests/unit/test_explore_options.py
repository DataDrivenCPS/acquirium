"""Tests for options() single-facet value+count aggregation."""

from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.Client.explore.core import Q

CLS_A = "urn:test#TypeA"
OF_MEDIUM = "http://data.ashrae.org/standard223#ofMedium"
HAS_MEDIUM = "http://data.ashrae.org/standard223#hasMedium"
QK_PRED = "http://qudt.org/schema/qudt/hasQuantityKind"


def make_client(rows):
    client = MagicMock()
    client.sparql_query.return_value = {"columns": ["opt", "count"], "rows": rows}
    client.compact_uri.side_effect = lambda x: str(x).rsplit("#", 1)[-1]
    return client


def base(client) -> Q:
    return Q(client=client).entity(CLS_A, alias="ro").measurement(alias="m")


class TestOptionsQuery:
    def test_aggregation_query_shape(self):
        client = make_client([])
        base(client).options("quantity_kind")
        sparql = client.sparql_query.call_args.args[0]
        assert sparql.startswith("SELECT ?opt (COUNT(DISTINCT ?v1) AS ?count)")
        assert "GROUP BY ?opt" in sparql and "ORDER BY DESC(?count)" in sparql
        assert f"?v1 (<{QK_PRED}>) ?opt ." in sparql

    def test_includes_current_pattern(self):
        client = make_client([])
        base(client).options("quantity_kind")
        sparql = client.sparql_query.call_args.args[0]
        # entity class fence and the data-node ext-ref triple are part of the WHERE
        assert f"subClassOf>* <{CLS_A}>" in sparql
        assert "hasExternalReference" in sparql

    def test_multi_predicate_attr_unions(self):
        client = make_client([])
        base(client).options("medium")
        sparql = client.sparql_query.call_args.args[0]
        assert f"(<{OF_MEDIUM}>|<{HAS_MEDIUM}>)" in sparql

    def test_of_targets_entity_node(self):
        client = make_client([])
        base(client).options("process", of="ro")
        sparql = client.sparql_query.call_args.args[0]
        assert "COUNT(DISTINCT ?v0)" in sparql
        assert "hasProcess" in sparql


class TestOptionsResult:
    def test_dataframe_with_compacted_values(self):
        client = make_client([
            ["urn:qk#MassFlowRate", 5],
            ["urn:qk#PH", 2],
            [None, 9],
        ])
        df = base(client).options("quantity_kind")
        assert df.columns == ["quantity_kind", "count"]
        assert df["quantity_kind"].to_list() == ["MassFlowRate", "PH"]
        assert df["count"].to_list() == [5, 2]
        assert df.schema["count"] == pl.Int64

    def test_cached_per_attr_and_node(self):
        client = make_client([])
        b = base(client)
        b.options("quantity_kind")
        b.options("quantity_kind")
        assert client.sparql_query.call_count == 1
        b.options("unit")
        assert client.sparql_query.call_count == 2


class TestOptionsErrors:
    def test_unknown_attr(self):
        with pytest.raises(ValueError, match="unknown attribute"):
            base(make_client([])).options("flavour")

    def test_unknown_alias(self):
        with pytest.raises(ValueError, match="unknown alias"):
            base(make_client([])).options("medium", of="nope")

    def test_role_mismatch(self):
        with pytest.raises(ValueError, match="does not apply to data"):
            base(make_client([])).options("process")
        with pytest.raises(ValueError, match="does not apply to entity"):
            base(make_client([])).options("quantity_kind", of="ro")
