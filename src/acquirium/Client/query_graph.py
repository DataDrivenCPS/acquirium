from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Any

@dataclass(frozen=True)
class DataNodeInfo:
    node_id: int
    filters: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StreamNodeInfo:
    """A node bound to an external reference rather than to a point.

    A measurement node matches a point that *has* a reference; a stream node
    matches the reference itself, so it finds streams that no point links to
    — which is most of them when data is ingested without a model.

    - ``source_id``: node this stream hangs off, when the query chained one
      (``entity(...).measurement().streams()``). ``None`` for the root form.
    - ``filters``: same shape as ``DataNodeInfo.filters``.
    """

    node_id: int
    source_id: Optional[int] = None
    filters: Dict[str, Any] = field(default_factory=dict)



@dataclass(frozen=True)
class QueryNode:
    """A node in the logical query graph.

    - id: internal identifier (stable within this QueryGraph)
    - alias: user-facing name to refer to this node (e.g., 'valve'), if not provided, same as id
    - constraints: node constraints; well-known keys are "rdf_class"
      (ontology class URI, e.g. 'urn:nawi-water-ontology#Valve'),
      "instance_uri", "is_data_node", "path_from", and "process"
    """
    id: int
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
    direction: Optional[str] = None  # "upstream", "downstream", or None
    cp_filter: Optional[str] = None  # rdf:type URI to filter connection points in CP alternative
    # Lowered via-program snapshot (explore layer): tuple of segments
    # (alternatives, star); one segment step = UNION of its step chains.
    patterns: Optional[tuple] = None
    # Nearest-match edge (explore layer): resolved by client-side BFS at
    # execute time; value_pairs holds the (source_uri, target_uri) matches
    # injected as paired VALUES instead of the edge pattern.
    nearest: bool = False
    value_pairs: Optional[tuple] = None
    # Unconstrained-edge compilation: also try the first hop through a
    # connection point (measurement's include_connection_points flag).
    cp_union: bool = True


@dataclass(frozen=True)
class QueryGraph:
    """Immutable representation of a query graph."""

    nodes: Dict[int, QueryNode] = field(default_factory=dict)
    edges: List[QueryEdge] = field(default_factory=list)
    aliases: Dict[str, int] = field(default_factory=dict)
    aliases_reverse: Dict[int, str] = field(default_factory=dict)
    current_pointer: Optional[int] = None

    data_nodes: Dict[int, DataNodeInfo] = field(default_factory=dict)
    stream_nodes: Dict[int, StreamNodeInfo] = field(default_factory=dict)

    # Projected attribute columns: (node_id, attr_name, required) triples,
    # in order. required=True filters rows lacking the attribute.
    selects: tuple = ()

    def with_data_node(self, info: DataNodeInfo) -> "QueryGraph":
        dn = dict(self.data_nodes)
        dn[info.node_id] = info
        return replace(self, data_nodes=dn)

    def with_stream_node(self, info: StreamNodeInfo) -> "QueryGraph":
        sn = dict(self.stream_nodes)
        sn[info.node_id] = info
        return replace(self, stream_nodes=sn)

    def with_node(self, node: QueryNode) -> "QueryGraph":
        """Return a new graph with an added/updated node and alias."""
        nodes = dict(self.nodes)
        nodes[node.id] = node

        aliases = dict(self.aliases)
        aliases_reverse = dict(self.aliases_reverse)
        name = node.alias or str(node.id)
        aliases[name] = node.id
        aliases_reverse[node.id] = name

        return replace(
            self,
            nodes=nodes,
            aliases=aliases,
            aliases_reverse=aliases_reverse,
            current_pointer=node.id,
        )

    def with_edge(self, edge: QueryEdge, *, new_pointer: Optional[int] = None) -> "QueryGraph":
        """Return a new graph with an added edge and (optionally) new pointer."""
        return replace(
            self,
            edges=[*self.edges, edge],
            current_pointer=new_pointer if new_pointer is not None else self.current_pointer,
        )

    def with_select(self, node_id: int, attr_name: str, required: bool = False) -> "QueryGraph":
        """Return a new graph with an added (node, attr) projection.

        Deduplicated on (node, attr); re-adding with a different ``required``
        replaces the entry."""
        entry = (node_id, attr_name, required)
        if entry in self.selects:
            return self
        if any(n == node_id and a == attr_name for n, a, _ in self.selects):
            return replace(self, selects=tuple(
                entry if (n == node_id and a == attr_name) else (n, a, r)
                for n, a, r in self.selects
            ))
        return replace(self, selects=self.selects + (entry,))

    def resolve_alias(self, alias_or_none: Optional[str]) -> Optional[int]:
        """Resolve an alias or use current pointer when None."""
        if alias_or_none is None:
            return self.current_pointer
        return self.aliases.get(alias_or_none)

    def node_role(self, node_id: int) -> str:
        """Which attribute role a node plays: stream, data or entity.

        The single place this is decided. Attribute applicability
        (``Attr.roles``) is checked against it by ``where``/``include``/
        ``options``/``facets``.
        """
        if node_id in self.stream_nodes:
            return "stream"
        if node_id in self.data_nodes:
            return "data"
        return "entity"
