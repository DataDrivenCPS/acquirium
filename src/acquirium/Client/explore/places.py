"""Nearest along the flow, executed place by place.

A directional edge with ``nearest=True`` is resolved per source, walking
the flow one place at a time. The places, seen from a source, are its own
outlet (downstream) or inlet (upstream) connection points when the edge
carries ``own_cp_class`` (``measurement``), then for each flow step the
connection leaving the current entities and then the entities they lead
to. Each place's candidate pairs ``(source, target)`` go into the full
pattern as paired ``VALUES`` (class, attribute and data filters included),
so a place counts as a hit only when it yields a row; a source keeps the
rows of its first hit and drops out. Ties within a place all survive.

The one-step adjacency (entity to entity, entity to connection, entity to
its own connection points) is materialized once per graph version and
cached, and frontiers advance in Python with a visited set per source, so
loops terminate and no long property path is ever sent to the store. The
source is never its own target. ``max_depth`` bounds the entity steps;
``0`` walks until every pending source's frontier is empty.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Dict, List, Optional, Set, Tuple

from acquirium.Client.explore.attributes import Registry
from acquirium.Client.explore.compile import compile_sparql
from acquirium.Client.explore.traverse import _fetch_source_uris, _fetch_target_accept, _prune_target_subtree
from acquirium.Client.query_graph import QueryEdge, QueryGraph, QueryNode
from acquirium.internals.internals_namespaces import CONNECTED_THROUGH, CONNECTS_FROM, CONNECTS_TO, S223

_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_SUBCLASS = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
_SAFETY_CAP = 1000  # entity steps; only reachable with max_depth=0

# (server_key, kind, direction/class, graph_version) -> {source: {targets}}
_ADJ_CACHE: Dict[tuple, Dict[str, Set[str]]] = {}


def _server_key(client) -> str:
    return str(getattr(client, "base_url", id(client)))


def _adjacency(client, key: tuple, body: str) -> Dict[str, Set[str]]:
    cached = _ADJ_CACHE.get(key)
    if cached is not None:
        return cached
    res = client.sparql_query(f"SELECT DISTINCT ?s ?t\nWHERE {{\n  {body}\n}}", include_dependencies=True)
    cols = res.get("columns", [])
    adj: Dict[str, Set[str]] = {}
    if "s" in cols and "t" in cols:
        si, ti = cols.index("s"), cols.index("t")
        for r in res.get("rows", []):
            if r[si] is not None and r[ti] is not None and r[si] != r[ti]:
                adj.setdefault(str(r[si]), set()).add(str(r[ti]))
    _ADJ_CACHE[key] = adj
    return adj


def _steps(client, direction: str, own_cp: Optional[str], version: int):
    """The three one-step maps for a direction: entity->entity,
    entity->connection, entity->own connection point (may be empty)."""
    ct, cf = f"<{S223.connectedTo}>", f"<{S223.connectedFrom}>"
    cst, csf = f"<{CONNECTS_TO}>", f"<{CONNECTS_FROM}>"
    if direction == "downstream":
        ent = f"?s ({ct}|^{cf}) ?t ."
        conn = f"?s ^{csf} ?t ."
    else:
        ent = f"?s (^{ct}|{cf}) ?t ."
        conn = f"?s ^{cst} ?t ."
    sk = _server_key(client)
    ent_map = _adjacency(client, (sk, "ent", direction, version), ent)
    conn_map = _adjacency(client, (sk, "conn", direction, version), conn)
    own_map: Dict[str, Set[str]] = {}
    if own_cp:
        # anchored sub-SELECT fence, as the compiler does for class matches:
        # an unanchored rdf:type/subClassOf* path is far slower on Oxigraph
        own = (f"?s <{S223.hasConnectionPoint}> ?t . ?t <{_RDF_TYPE}> ?own_typ . "
               f"{{ SELECT DISTINCT ?own_typ WHERE {{ ?own_typ <{_SUBCLASS}>* <{own_cp}> . }} }}")
        own_map = _adjacency(client, (sk, "own", own_cp, version), own)
    return ent_map, conn_map, own_map


def _placed_edge(graph: QueryGraph) -> QueryEdge:
    edges = [e for e in graph.edges if e.direction is not None and e.nearest]
    if len(edges) != 1:
        raise ValueError(
            f"nearest along a direction supports one directional edge per query, got {len(edges)}"
        )
    return edges[0]


def _with_pairs(graph: QueryGraph, edge: QueryEdge, pairs: List[Tuple[str, str]], *, undrop: bool) -> QueryGraph:
    nodes = dict(graph.nodes)
    if undrop:
        node = nodes[edge.source_id]
        constraints = dict(node.constraints or {})
        constraints.pop("dropped", None)
        nodes[edge.source_id] = QueryNode(id=node.id, alias=node.alias, constraints=constraints)
    resolved = replace(edge, value_pairs=tuple(pairs))
    return QueryGraph(
        nodes=nodes,
        edges=[resolved if e is edge else e for e in graph.edges],
        aliases=dict(graph.aliases),
        aliases_reverse=dict(graph.aliases_reverse),
        current_pointer=graph.current_pointer,
        data_nodes=dict(graph.data_nodes),
        selects=graph.selects,
    )


def execute_placed(graph: QueryGraph, client, include_dependencies: bool = True) -> dict:
    """Run a query holding one nearest directional edge; return ``{"columns", "rows"}``."""
    edge = _placed_edge(graph)
    src_node = graph.nodes[edge.source_id]
    src_dropped = bool((src_node.constraints or {}).get("dropped"))
    src_col = f"v{edge.source_id}"
    version = client.graph_version()
    ent_map, conn_map, own_map = _steps(client, edge.direction, edge.own_cp_class, version)

    sources = _fetch_source_uris(client, _prune_target_subtree(graph, edge), edge.source_id)
    # nodes that can satisfy the target's own constraints (class, attributes);
    # a place whose candidates all fall outside needs no query. None when the
    # target node is unconstrained (a measurement's intermediate node).
    accept = _fetch_target_accept(client, graph, edge)
    pending: Set[str] = set(sources)
    frontier: Dict[str, Set[str]] = {s: {s} for s in sources}
    visited: Dict[str, Set[str]] = {s: {s} for s in sources}

    columns: Optional[List[str]] = None
    rows_out: List[list] = []

    def run_place(candidates: Dict[str, Set[str]]) -> None:
        nonlocal columns
        pairs = [(s, t) for s in sorted(candidates) for t in sorted(candidates[s])
                 if s in pending and (accept is None or t in accept)]
        if not pairs:
            return
        g = _with_pairs(graph, edge, pairs, undrop=src_dropped)
        res = client.sparql_query(compile_sparql(g, Registry(client)),
                                  include_dependencies=include_dependencies)
        cols = res.get("columns", [])
        if columns is None:
            columns = list(cols)
        rows = res.get("rows", [])
        if src_col in cols:
            si = cols.index(src_col)
            hit = {r[si] for r in rows if r[si] is not None}
            pending.difference_update(hit)
        rows_out.extend(rows)

    # place: the source's own connection points
    if own_map:
        run_place({s: own_map.get(s, set()) for s in pending})

    max_steps = int(edge.hops) if int(edge.hops) > 0 else _SAFETY_CAP
    for _ in range(max_steps):
        if not pending:
            break
        # place: the connections leaving the current entities
        conns = {s: {c for e in frontier[s] for c in conn_map.get(e, set())} for s in pending}
        run_place(conns)
        # place: the entities those connections (or direct links) lead to
        nxt: Dict[str, Set[str]] = {}
        for s in list(pending):
            reach = {t for e in frontier[s] for t in ent_map.get(e, set())} - visited[s]
            visited[s] |= reach
            nxt[s] = reach
        run_place(nxt)
        frontier = {s: nxt.get(s, set()) for s in pending}
        if not any(frontier.values()):
            break

    if columns is None:
        # nothing was ever queried: fall back to an empty result with the
        # ordinary column set
        probe = compile_sparql(_with_pairs(graph, edge, [], undrop=src_dropped), Registry(client))
        columns = [c.lstrip("?") for c in probe.split("\n", 1)[0].split()[2:]]
    if src_dropped and src_col in columns:
        si = columns.index(src_col)
        columns = [c for i, c in enumerate(columns) if i != si]
        rows_out = [[v for i, v in enumerate(r) if i != si] for r in rows_out]
    return {"columns": columns, "rows": rows_out}
