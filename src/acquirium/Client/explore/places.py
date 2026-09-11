"""Nearest along the flow, executed place by place.

A directional edge with ``nearest=True`` is not one SPARQL query but a
sequence of them. The flow from a source is divided into *places*: for
``measurement(direction="downstream")`` the source's own outlet connection
points, then for each flow step the connection and then the entity together
with all of its connection points; for ``related(direction=...)`` the same
without the source's own connection points. Each place is queried with the
full pattern (class, attribute and data filters included), so a place counts
as a hit only when it holds a matching row. Every source keeps the rows of
the first place where it hit; sources already satisfied are ignored in
later places. Ties within a place all survive.

``max_depth`` bounds the number of entity places. ``max_depth=0`` continues
until an entity place reaches no entity that an earlier entity place has not
already reached, which ends the search on plants with loops.
"""
from __future__ import annotations

from dataclasses import replace
from typing import List, Optional, Set

from acquirium.Client.explore.compile import compile_sparql
from acquirium.Client.query_graph import QueryEdge, QueryGraph, QueryNode

_SAFETY_CAP = 400  # places; only reachable with max_depth=0 on a pathological graph


def _placed_edge(graph: QueryGraph) -> QueryEdge:
    edges = [e for e in graph.edges if e.direction is not None and e.nearest]
    if len(edges) != 1:
        raise ValueError(
            f"nearest along a direction supports one directional edge per query, got {len(edges)}"
        )
    return edges[0]


def _with_edge(graph: QueryGraph, old: QueryEdge, new: QueryEdge, *, undrop: Optional[int] = None) -> QueryGraph:
    nodes = dict(graph.nodes)
    if undrop is not None:
        node = nodes[undrop]
        constraints = dict(node.constraints or {})
        constraints.pop("dropped", None)
        nodes[undrop] = QueryNode(id=node.id, alias=node.alias, constraints=constraints)
    return QueryGraph(
        nodes=nodes,
        edges=[new if e is old else e for e in graph.edges],
        aliases=dict(graph.aliases),
        aliases_reverse=dict(graph.aliases_reverse),
        current_pointer=graph.current_pointer,
        data_nodes=dict(graph.data_nodes),
        selects=graph.selects,
    )


def _reachable(client, edge: QueryEdge, src_node: QueryNode, place: int, include_dependencies: bool) -> Set[str]:
    """Targets of one place from the source's own constraints alone; a probe
    used to end an unbounded search."""
    constraints = dict(src_node.constraints or {})
    constraints.pop("dropped", None)
    g = QueryGraph().with_node(QueryNode(id=0, alias="s", constraints=constraints))
    g = g.with_node(QueryNode(id=1, alias="t", constraints={}))
    g = g.with_edge(replace(edge, source_id=0, target_id=1, place=place, nearest=False))
    res = client.sparql_query(compile_sparql(g), include_dependencies=include_dependencies)
    cols = res.get("columns", [])
    if "v1" not in cols:
        return set()
    i = cols.index("v1")
    return {r[i] for r in res.get("rows", []) if r[i] is not None}


def execute_placed(graph: QueryGraph, client, include_dependencies: bool = True) -> dict:
    """Run a query holding one nearest directional edge; return ``{"columns", "rows"}``."""
    edge = _placed_edge(graph)
    src_node = graph.nodes[edge.source_id]
    src_dropped = bool((src_node.constraints or {}).get("dropped"))
    src_col = f"v{edge.source_id}"
    own = 1 if edge.own_cp_class else 0
    bounded = int(edge.hops) > 0
    last_place = 2 * int(edge.hops) + own if bounded else _SAFETY_CAP

    columns: Optional[List[str]] = None
    rows_out: List[list] = []
    satisfied: Set[str] = set()
    seen_entities: Set[str] = set()

    for place in range(1, last_place + 1):
        placed = replace(edge, place=place)
        g = _with_edge(graph, edge, placed, undrop=edge.source_id if src_dropped else None)
        res = client.sparql_query(compile_sparql(g), include_dependencies=include_dependencies)
        cols = res.get("columns", [])
        if columns is None:
            columns = list(cols)
        if src_col in cols:
            si = cols.index(src_col)
            new_rows = [r for r in res.get("rows", []) if r[si] not in satisfied]
            satisfied.update(r[si] for r in new_rows)
        else:
            new_rows = list(res.get("rows", []))
        rows_out.extend(new_rows)

        if not bounded:
            is_entity_place = place > own and (place - own) % 2 == 0
            if is_entity_place:
                reached = _reachable(client, edge, src_node, place, include_dependencies)
                if not reached - seen_entities:
                    break
                seen_entities |= reached

    columns = columns or []
    if src_dropped and src_col in columns:
        si = columns.index(src_col)
        columns = [c for i, c in enumerate(columns) if i != si]
        rows_out = [[v for i, v in enumerate(r) if i != si] for r in rows_out]
    return {"columns": columns, "rows": rows_out}
