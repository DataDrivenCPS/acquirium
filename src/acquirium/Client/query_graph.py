from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

@dataclass(frozen=True)
class DataNodeInfo:
    node_id: int
    filters: Dict[str, Any] = field(default_factory=dict)



@dataclass(frozen=True)
class QueryNode:
    """A node in the logical query graph.

    - id: internal identifier (stable within this QueryGraph)
    - rdf_class: ontology class URI (e.g., 'urn:nawi-water-ontology#Valve')
    - alias: user-facing name to refer to this node (e.g., 'valve'), if not provided, same as id
    - constraints: future extension (labels, numeric filters, etc.)
    """
    id: int
    rdf_class: str = None
    alias: Optional[str] = None
    constraints: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryEdge:
    """A relationship between two query nodes.

    - source_id: internal id of source node
    - target_id: internal id of target node
    - hops: maximum hop distance (1 = direct, >1 = path up to that length)
    - predicates: optional list of allowed predicates (None = any)
    """
    source_id: int
    target_id: int
    hops: int = 3
    predicates: Optional[List[str]] = None


@dataclass(frozen=True)
class QueryGraph:
    """Immutable representation of a query graph."""

    nodes: Dict[int, QueryNode] = field(default_factory=dict)
    edges: List[QueryEdge] = field(default_factory=list)
    aliases: Dict[str, int] = field(default_factory=dict)
    aliases_reverse: Dict[int, str] = field(default_factory=dict)
    current_pointer: Optional[int] = None

    data_nodes: Dict[int, DataNodeInfo] = field(default_factory=dict)

    def with_data_node(self, info: DataNodeInfo) -> "QueryGraph":
        dn = dict(self.data_nodes)
        dn[info.node_id] = info
        return QueryGraph(
            nodes=dict(self.nodes),
            edges=list(self.edges),
            aliases=dict(self.aliases),
            aliases_reverse=dict(self.aliases_reverse),
            current_pointer=self.current_pointer,
            data_nodes=dn,
        )

    def with_node(self, node: QueryNode) -> "QueryGraph":
        """Return a new graph with an added/updated node and alias."""
        nodes = dict(self.nodes)
        nodes[node.id] = node

        aliases = dict(self.aliases)
        aliases_reverse = dict(self.aliases_reverse)
        if node.alias:
            aliases[node.alias] = node.id
            aliases_reverse[node.id] = node.alias
        else:
            aliases[str(node.id)] = node.id
            aliases_reverse[node.id] = str(node.id)

        return QueryGraph(
            nodes=nodes,
            edges=list(self.edges),
            aliases=aliases,
            aliases_reverse=aliases_reverse,
            current_pointer=node.id,
            data_nodes=dict(self.data_nodes),
        )

    def with_edge(self, edge: QueryEdge, *, new_pointer: Optional[int] = None) -> "QueryGraph":
        """Return a new graph with an added edge and (optionally) new pointer."""
        edges = list(self.edges)
        edges.append(edge)
        return QueryGraph(
            nodes=dict(self.nodes),
            edges=edges,
            aliases=dict(self.aliases),
            aliases_reverse=dict(self.aliases_reverse),
            current_pointer=new_pointer if new_pointer is not None else self.current_pointer,
            data_nodes=dict(self.data_nodes),
        )

    def resolve_alias(self, alias_or_none: Optional[str]) -> Optional[int]:
        """Resolve an alias or use current pointer when None."""
        if alias_or_none is None:
            return self.current_pointer
        return self.aliases.get(alias_or_none)
