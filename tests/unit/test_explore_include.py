"""Tests for include() attribute projection and its SPARQL/column mapping."""

import pytest

from acquirium.Client.explore.core import Q
from acquirium.Client.query_graph import QueryGraph, QueryNode

CLS_A = "urn:test#TypeA"
OF_MEDIUM = "http://data.ashrae.org/standard223#ofMedium"
HAS_MEDIUM = "http://data.ashrae.org/standard223#hasMedium"


def q() -> Q:
    return Q(client=None)


def base() -> Q:
    return q().entity(CLS_A, alias="ro").measurement(alias="m")


class TestSelectStorage:
    def test_stores_selects_on_pointer(self):
        b = base().include("medium", "unit")
        assert b.query_graph.selects == ((1, "medium"), (1, "unit"))

    def test_of_targets_alias(self):
        b = base().include("process", of="ro")
        assert b.query_graph.selects == ((0, "process"),)

    def test_dedup(self):
        b = base().include("medium").include("medium")
        assert b.query_graph.selects == ((1, "medium"),)

    def test_selects_survive_later_verbs(self):
        b = base().include("medium").related(CLS_A, alias="next", frm="ro")
        assert b.query_graph.selects == ((1, "medium"),)

    def test_errors(self):
        with pytest.raises(ValueError, match="at least one"):
            base().include()
        with pytest.raises(ValueError, match="unknown attribute"):
            base().include("flavour")
        with pytest.raises(ValueError, match="unknown alias"):
            base().include("medium", of="nope")
        with pytest.raises(ValueError, match="does not apply to entity"):
            base().include("quantity_kind", of="ro")
        with pytest.raises(ValueError, match="does not apply to data"):
            base().include("process")


class TestSelectSparql:
    def test_optional_binding_and_projection(self):
        s = base().include("quantity_kind").to_sparql()
        assert "OPTIONAL { ?v1 (<http://qudt.org/schema/qudt/hasQuantityKind>) ?attr1_quantity_kind . }" in s
        first_line = s.splitlines()[0]
        assert first_line.endswith("?attr1_quantity_kind")

    def test_multi_predicate_attr_binds_path_union(self):
        s = base().include("medium").to_sparql()
        assert f"OPTIONAL {{ ?v1 (<{OF_MEDIUM}>|<{HAS_MEDIUM}>) ?attr1_medium . }}" in s

    def test_no_selects_means_no_attr_vars(self):
        assert "attr" not in base().to_sparql()


class TestColumnNaming:
    def test_attr_column_maps_to_alias_dot_attr(self):
        b = base().include("medium")
        assert b._col_name_to_alias("attr1_medium") == "m.medium"

    def test_attr_name_with_underscore(self):
        b = base().include("quantity_kind")
        assert b._col_name_to_alias("attr1_quantity_kind") == "m.quantity_kind"

    def test_unaliased_node_gets_class_default_alias(self):
        b = q().entity(CLS_A).measurement(alias="m")
        # no client -> CURIE unavailable -> local name of the class URI
        assert b._col_name_to_alias("attr0_medium") == "TypeA.medium"

    def test_garbage_passthrough(self):
        assert base()._col_name_to_alias("attrX_medium") == "attrX_medium"


class TestQueryGraphSelects:
    def test_with_select_dedup_returns_same_graph(self):
        g = QueryGraph().with_node(QueryNode(id=0)).with_select(0, "medium")
        assert g.with_select(0, "medium") is g

    def test_default_empty(self):
        assert QueryGraph().selects == ()
