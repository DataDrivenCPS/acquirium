"""Tests for acquirium.Client.query_graph — immutable query graph builder."""

import pytest

from acquirium.Client.query_graph import QueryNode, QueryEdge, QueryGraph, DataNodeInfo


class TestQueryGraphWithNode:
    def test_adds_node(self):
        g = QueryGraph()
        node = QueryNode(id=1, alias="valve", constraints={"rdf_class": "urn:test#TypeA"})
        g2 = g.with_node(node)
        assert 1 in g2.nodes
        assert g2.nodes[1] is node

    def test_sets_pointer(self):
        g = QueryGraph()
        node = QueryNode(id=1, constraints={"rdf_class": "urn:test#TypeA"})
        g2 = g.with_node(node)
        assert g2.current_pointer == 1

    def test_registers_alias(self):
        g = QueryGraph()
        node = QueryNode(id=1, alias="valve")
        g2 = g.with_node(node)
        assert g2.aliases["valve"] == 1
        assert g2.aliases_reverse[1] == "valve"

    def test_no_alias_uses_str_id(self):
        g = QueryGraph()
        node = QueryNode(id=42)
        g2 = g.with_node(node)
        assert g2.aliases["42"] == 42
        assert g2.aliases_reverse[42] == "42"

    def test_immutability(self):
        g = QueryGraph()
        node = QueryNode(id=1, alias="v")
        g2 = g.with_node(node)
        assert 1 not in g.nodes
        assert g.current_pointer is None


class TestQueryGraphWithEdge:
    def test_adds_edge(self):
        g = QueryGraph()
        edge = QueryEdge(source_id=1, target_id=2)
        g2 = g.with_edge(edge)
        assert len(g2.edges) == 1
        assert g2.edges[0] is edge

    def test_preserves_pointer(self):
        g = QueryGraph(current_pointer=5)
        edge = QueryEdge(source_id=1, target_id=2)
        g2 = g.with_edge(edge)
        assert g2.current_pointer == 5

    def test_new_pointer(self):
        g = QueryGraph(current_pointer=5)
        edge = QueryEdge(source_id=1, target_id=2)
        g2 = g.with_edge(edge, new_pointer=2)
        assert g2.current_pointer == 2

    def test_immutability(self):
        g = QueryGraph()
        edge = QueryEdge(source_id=1, target_id=2)
        g2 = g.with_edge(edge)
        assert len(g.edges) == 0


class TestQueryGraphWithDataNode:
    def test_adds_data_node(self):
        g = QueryGraph()
        info = DataNodeInfo(node_id=10, filters={"unit": "degC"})
        g2 = g.with_data_node(info)
        assert 10 in g2.data_nodes
        assert g2.data_nodes[10].filters == {"unit": "degC"}

    def test_immutability(self):
        g = QueryGraph()
        info = DataNodeInfo(node_id=10)
        g2 = g.with_data_node(info)
        assert 10 not in g.data_nodes


class TestResolveAlias:
    def test_found(self):
        g = QueryGraph(aliases={"valve": 1})
        assert g.resolve_alias("valve") == 1

    def test_none_returns_pointer(self):
        g = QueryGraph(current_pointer=7)
        assert g.resolve_alias(None) == 7

    def test_missing_returns_none(self):
        g = QueryGraph()
        assert g.resolve_alias("nonexistent") is None


class TestFrozenDataclasses:
    def test_query_node_frozen(self):
        node = QueryNode(id=1)
        with pytest.raises(AttributeError):
            node.id = 2

    def test_query_edge_defaults(self):
        edge = QueryEdge(source_id=1, target_id=2)
        assert edge.hops == 3
        assert edge.predicates is None
        assert edge.direction is None
        assert edge.cp_filter is None

    def test_query_edge_frozen(self):
        edge = QueryEdge(source_id=1, target_id=2)
        with pytest.raises(AttributeError):
            edge.hops = 5
