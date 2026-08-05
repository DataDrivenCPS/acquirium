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
        assert b.query_graph.selects == ((1, "medium", False), (1, "unit", False))

    def test_of_targets_alias(self):
        b = base().include("process", of="ro")
        assert b.query_graph.selects == ((0, "process", False),)

    def test_dedup(self):
        b = base().include("medium").include("medium")
        assert b.query_graph.selects == ((1, "medium", False),)

    def test_required_upgrades_existing_entry(self):
        b = base().include("medium").include("medium", required=True)
        assert b.query_graph.selects == ((1, "medium", True),)

    def test_selects_survive_later_verbs(self):
        b = base().include("medium").related(CLS_A, alias="next", frm="ro")
        assert b.query_graph.selects == ((1, "medium", False),)

    def test_errors(self):
        with pytest.raises(ValueError, match="at least one"):
            base().include()
        with pytest.raises(ValueError, match="unknown column"):
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
        # attr columns directly follow their node's column
        assert "?v1 ?attr1_quantity_kind" in s.splitlines()[0]

    def test_attr_columns_interleave_per_node(self):
        b = (q().entity(CLS_A, alias="a").include("medium")
             .entity(CLS_A, alias="b").include("medium"))
        first = b.to_sparql().splitlines()[0]
        assert "?v0 ?attr0_medium ?v1 ?attr1_medium" in first

    def test_multi_predicate_attr_binds_path_union(self):
        s = base().include("medium").to_sparql()
        assert f"OPTIONAL {{ ?v1 (<{OF_MEDIUM}>|<{HAS_MEDIUM}>) ?attr1_medium . }}" in s

    def test_no_selects_means_no_attr_vars(self):
        assert "attr" not in base().to_sparql()

    def test_required_binds_without_optional(self):
        s = base().include("unit", required=True).to_sparql()
        assert "?v1 (<http://qudt.org/schema/qudt/hasUnit>) ?attr1_unit ." in s
        assert "OPTIONAL { ?v1 (<http://qudt.org/schema/qudt/hasUnit>)" not in s

    def test_default_stays_optional(self):
        s = base().include("unit").to_sparql()
        assert "OPTIONAL { ?v1 (<http://qudt.org/schema/qudt/hasUnit>) ?attr1_unit . }" in s


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


class TestColumnControl:
    """include/drop as inverses + the unified with_columns()."""

    def test_include_undrops_a_node(self):
        b = base().drop("ro").include("ro")
        assert "dropped" not in b.query_graph.nodes[0].constraints
        assert "?v0" in b.to_sparql().splitlines()[0]

    def test_drop_unincludes_an_attr(self):
        b = base().include("unit").drop("unit")
        assert b.query_graph.selects == ()

    def test_drop_dotted_targets_other_node(self):
        b = base().include("process", of="ro").drop("ro.process")
        assert b.query_graph.selects == ()

    def test_include_dotted_targets_other_node(self):
        b = base().include("ro.process")
        assert b.query_graph.selects == ((0, "process", False),)

    def test_with_columns_mixed(self):
        b = base().with_columns("unit", "-ro")
        g = b.query_graph
        assert g.selects == ((1, "unit", False),)
        assert g.nodes[0].constraints.get("dropped") is True

    def test_with_columns_undrop_and_uninclude(self):
        b = (base().drop("ro").include("unit")
             .with_columns("ro", "-unit"))
        g = b.query_graph
        assert "dropped" not in g.nodes[0].constraints
        assert g.selects == ()

    def test_with_columns_required_passthrough(self):
        b = base().with_columns("unit", required=True)
        assert b.query_graph.selects == ((1, "unit", True),)

    def test_with_columns_empty_errors(self):
        with pytest.raises(ValueError, match="at least one"):
            base().with_columns()

    def test_attr_name_wins_over_alias(self):
        ## a node aliased like a registry attr is shadowed by the attribute
        b = q().entity(CLS_A, alias="unit").measurement(alias="m").include("unit")
        assert b.query_graph.selects == ((1, "unit", False),)
