"""Client-side traversal for nearest-match edges.

A ``nearest`` edge is resolved in three phases at execute time:

1. **Sources** — the query graph minus the nearest edge (and everything
   hanging off its target) runs normally; the source node's bindings are the
   BFS start set.
2. **Materialize** — each segment of the edge's step program becomes one
   ``SELECT DISTINCT ?s ?t`` query (rendered by the same
   ``compile.render_alternatives`` the SPARQL path uses), yielding that
   segment's full edge list. Results are cached per
   ``(server, segment, graph_version)``, so repeat traversals in a session
   are free until the graph changes.
3. **BFS** — :func:`walk_program` walks the program like a small NFA
   (star segments may repeat or be skipped), per source, breadth-first.
   With ``nearest`` it stops at the first matching layer per source — all
   equal-distance ties are kept. Matches are written back onto the edge as
   ``value_pairs`` and compiled as paired ``VALUES``, so each source joins
   only its own nearest targets.

Distances (in program steps) are computed and returned by
:func:`walk_program` but not surfaced as columns yet.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Dict, Iterable, List, Optional, Set, Tuple

from acquirium.Client.explore.compile import compile_sparql, render_alternatives
from acquirium.Client.query_graph import QueryGraph

# (server_key, alternatives, graph_version) -> adjacency {source: {targets}}
_SEGMENT_CACHE: Dict[tuple, Dict[str, Set[str]]] = {}


def _server_key(client) -> str:
    return str(getattr(client, "base_url", id(client)))


def materialize_segment(client, alternatives: tuple, version: int) -> Dict[str, Set[str]]:
    """Fetch (and cache) the full edge list of one program segment."""
    key = (_server_key(client), alternatives, version)
    cached = _SEGMENT_CACHE.get(key)
    if cached is not None:
        return cached
    body = render_alternatives(alternatives, "?s", "?t", "seg")
    sparql = f"SELECT DISTINCT ?s ?t\nWHERE {{\n  {body}\n}}"
    res = client.sparql_query(sparql, use_union=True)
    cols = res.get("columns", [])
    try:
        si, ti = cols.index("s"), cols.index("t")
    except ValueError:
        raise RuntimeError(f"materialize_segment: unexpected columns {cols}")
    adjacency: Dict[str, Set[str]] = {}
    for row in res.get("rows", []):
        s, t = row[si], row[ti]
        if s is not None and t is not None:
            adjacency.setdefault(str(s), set()).add(str(t))
    _SEGMENT_CACHE[key] = adjacency
    return adjacency


def clear_segment_cache() -> None:
    _SEGMENT_CACHE.clear()


def walk_program(
    adjacencies: List[Dict[str, Set[str]]],
    stars: List[bool],
    sources: Iterable[str],
    max_total: int,
    nearest: bool = True,
    accept: Optional[Set[str]] = None,
) -> Dict[str, Dict[str, int]]:
    """BFS a step program from each source; returns {source: {target: distance}}.

    States are ``(node, position)``: a star segment's edge keeps the
    position (repeatable) and may be skipped for free; a fixed segment's
    edge advances it. A node in the accepting position — and in ``accept``,
    when given — is a match, so target constraints (class, attribute
    filters) participate in nearness: a closer non-matching node does not
    shadow a farther matching one. With ``nearest`` the walk stops per
    source at the first layer containing a match (equal-distance ties are
    all kept); otherwise every match within ``max_total`` steps is
    collected at its minimum distance.
    """
    n = len(adjacencies)

    def eps(states: Set[tuple]) -> Set[tuple]:
        out = set(states)
        frontier = list(states)
        while frontier:
            node, pos = frontier.pop()
            if pos < n and stars[pos] and (node, pos + 1) not in out:
                out.add((node, pos + 1))
                frontier.append((node, pos + 1))
        return out

    results: Dict[str, Dict[str, int]] = {}
    for src in sources:
        frontier = eps({(src, 0)})
        visited = set(frontier)
        found: Dict[str, int] = {}
        for depth in range(1, max_total + 1):
            nxt: Set[tuple] = set()
            for node, pos in frontier:
                if pos >= n:
                    continue
                for target in adjacencies[pos].get(node, ()):
                    state = (target, pos if stars[pos] else pos + 1)
                    if state not in visited:
                        nxt.add(state)
            nxt = eps(nxt) - visited
            if not nxt:
                break
            visited |= nxt
            matches = {node for node, pos in nxt
                       if pos == n and (accept is None or node in accept)}
            for m in matches:
                found.setdefault(m, depth)
            if matches and nearest:
                break
            frontier = nxt
        if found:
            results[src] = found
    return results


def _prune_target_subtree(graph: QueryGraph, edge) -> QueryGraph:
    """Drop the nearest edge, its target node, and everything built off it."""
    removed = {edge.target_id}
    changed = True
    while changed:
        changed = False
        for e in graph.edges:
            if e is edge:
                continue
            if e.source_id in removed and e.target_id not in removed:
                removed.add(e.target_id)
                changed = True
    return QueryGraph(
        nodes={k: v for k, v in graph.nodes.items() if k not in removed},
        edges=[e for e in graph.edges
               if e is not edge and e.source_id not in removed and e.target_id not in removed],
        aliases={a: i for a, i in graph.aliases.items() if i not in removed},
        aliases_reverse={i: a for i, a in graph.aliases_reverse.items() if i not in removed},
        current_pointer=edge.source_id,
        data_nodes={k: v for k, v in graph.data_nodes.items() if k not in removed},
        selects=tuple((nid, name) for nid, name in graph.selects if nid not in removed),
    )


def _fetch_source_uris(client, graph: QueryGraph, src_id: int) -> List[str]:
    res = client.sparql_query(compile_sparql(graph), use_union=True)
    cols = res.get("columns", [])
    col = f"v{src_id}"
    if col not in cols:
        return []
    idx = cols.index(col)
    uris: List[str] = []
    seen: Set[str] = set()
    for row in res.get("rows", []):
        val = row[idx]
        if val is None:
            continue
        s = str(val)
        if s not in seen:
            seen.add(s)
            uris.append(s)
    return uris


def _fetch_target_accept(client, graph: QueryGraph, edge) -> Optional[Set[str]]:
    """URIs satisfying the target node's own constraints, or None if unconstrained."""
    tid = edge.target_id
    node = graph.nodes[tid]
    info = graph.data_nodes.get(tid)
    if not (node.constraints or {}) and info is None:
        return None
    alias = graph.aliases_reverse.get(tid, str(tid))
    sub = QueryGraph(
        nodes={tid: node},
        edges=[],
        aliases={alias: tid},
        aliases_reverse={tid: alias},
        current_pointer=tid,
        data_nodes={tid: info} if info is not None else {},
    )
    res = client.sparql_query(compile_sparql(sub), use_union=True)
    cols = res.get("columns", [])
    col = f"v{tid}"
    if col not in cols:
        return set()
    idx = cols.index(col)
    return {str(row[idx]) for row in res.get("rows", []) if row[idx] is not None}


def resolve_nearest(graph: QueryGraph, client) -> QueryGraph:
    """Resolve every unresolved nearest edge into paired VALUES matches."""
    pending = [e for e in graph.edges
               if getattr(e, "nearest", False) and e.value_pairs is None]
    if not pending:
        return graph
    version = client.graph_version()
    for edge in pending:
        pruned = _prune_target_subtree(graph, edge)
        sources = _fetch_source_uris(client, pruned, edge.source_id)
        pairs: List[Tuple[str, str]] = []
        if sources:
            accept = _fetch_target_accept(client, graph, edge)
            adjacencies = [materialize_segment(client, alts, version)
                           for alts, _ in edge.patterns]
            stars = [star for _, star in edge.patterns]
            matches = walk_program(adjacencies, stars, sources, int(edge.hops),
                                   nearest=True, accept=accept)
            for src, targets in matches.items():
                pairs.extend((src, tgt) for tgt in sorted(targets))
        resolved_edge = replace(edge, value_pairs=tuple(pairs))
        graph = QueryGraph(
            nodes=dict(graph.nodes),
            edges=[resolved_edge if e is edge else e for e in graph.edges],
            aliases=dict(graph.aliases),
            aliases_reverse=dict(graph.aliases_reverse),
            current_pointer=graph.current_pointer,
            data_nodes=dict(graph.data_nodes),
            selects=graph.selects,
        )
    return graph
