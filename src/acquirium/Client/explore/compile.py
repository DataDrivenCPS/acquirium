"""Pure SPARQL compiler for the explore query layer.

``compile_sparql(graph)`` turns a :class:`QueryGraph` into a SELECT query.
Ported from ``Query.to_sparql`` / ``_edge_pattern`` / ``_direction_edge_pattern``
(``Client/query.py``) as free functions with no client or builder state, so it
can be unit-tested and reused without a running deployment. Output parity with
the legacy builder is asserted in ``tests/unit/test_explore_compile.py``.

Negation in data-node filters is expressed with the public
:class:`~acquirium.Client.explore.attributes.Not` marker (the legacy builder
used a private ``_Exclude`` wrapper with identical SPARQL output).
"""
from __future__ import annotations

from typing import Any, List

from rdflib.namespace import RDF

import itertools

from acquirium.Client.explore.attributes import REGISTRY, Attr, Not, normalize_value
from acquirium.Client.explore.shortcuts import hidden_predicates
from acquirium.Client.query_graph import QueryEdge, QueryGraph
from acquirium.internals.internals_namespaces import (
    CONNECTED_THROUGH,
    CONNECTION_POINT,
    CONNECTS_FROM,
    CONNECTS_TO,
    HAS_EXTERNAL_REFERENCE,
    HAS_UNIT,
    S223,
)


def _format_pred(p: str) -> str:
    """Format a predicate for SPARQL. Handles inverse (^) prefix."""
    if p.startswith("^"):
        return f"^<{p[1:]}>"
    return f"<{p}>"


def _is_iri(x: object) -> bool:
    return isinstance(x, str) and ("://" in x or x.startswith("urn:"))


def _term(x: object) -> str:
    """Return SPARQL term for x: <iri> or "literal"."""
    if _is_iri(x):
        return f"<{x}>"
    return f"\"{x}\""


_RDF_TYPE = str(RDF.type)
_SUBCLASS = "http://www.w3.org/2000/01/rdf-schema#subClassOf"


def _attr_clauses(var: str, attr: Attr, value: Any) -> List[str]:
    """WHERE clauses for one registry attribute constraint on ``var``.

    Subclass-matched attrs use the same anchored sub-SELECT fence as
    ``rdf_class`` (see the comment in :func:`compile_sparql`); multi-predicate
    attrs OR-union their predicates; ``Not`` values become FILTER NOT EXISTS.
    """
    values, negated = normalize_value(value)
    if not values:
        return []
    clauses: List[str] = []

    if attr.via_subclass:
        pred_path = "|".join(f"<{p}>" for p in attr.predicates)
        if negated:
            # Anchored at the constant class on the right, so no fence needed.
            step = f"<{_RDF_TYPE}>/<{_SUBCLASS}>*" if _RDF_TYPE in attr.predicates \
                else f"({pred_path})/<{_RDF_TYPE}>/<{_SUBCLASS}>*"
            for c in values:
                clauses.append(f"FILTER NOT EXISTS {{ {var} {step} <{c}> . }}")
            return clauses
        if _RDF_TYPE in attr.predicates:
            fence_var = f"{var}_{attr.name}"
            clauses.append(f"{var} <{_RDF_TYPE}> {fence_var} .")
        else:
            inst = f"{var}_{attr.name}"
            fence_var = f"{var}_{attr.name}_typ"
            clauses.append(f"{var} ({pred_path}) {inst} .")
            clauses.append(f"{inst} <{_RDF_TYPE}> {fence_var} .")
        if len(values) == 1:
            inner = f"{fence_var} <{_SUBCLASS}>* <{values[0]}> . "
        else:
            inner = " UNION ".join(f"{{ {fence_var} <{_SUBCLASS}>* <{c}> . }}" for c in values)
        clauses.append(f"{{ SELECT DISTINCT {fence_var} WHERE {{ {inner}}} }}")
        return clauses

    combos = [(p, _term(x)) for p in attr.predicates for x in values]
    if len(combos) == 1:
        p, t = combos[0]
        triple = f"{var} <{p}> {t} ."
        clauses.append(f"FILTER NOT EXISTS {{ {triple} }}" if negated else triple)
    else:
        union = " UNION ".join(f"{{ {var} <{p}> {t} . }}" for p, t in combos)
        clauses.append(f"FILTER NOT EXISTS {{ {union} }}" if negated else f"{{ {union} }}")
    return clauses


def _direction_edge_pattern(src_var: str, tgt_var: str, edge: QueryEdge, edge_idx: int) -> str:
    """Build SPARQL property-path pattern for direction-based traversal.

    The path reaches **both** entities and Connection resources along
    the directional chain so that data on pipes/connections is captured.

    Entity-reaching paths (per hop):

    **Downstream** (src → tgt):
      <connectedTo> | ^<connectedFrom>
      | <connectedThrough>/<connectsTo>
      | ^<connectsFrom>/^<connectedThrough>

    **Upstream** (finding tgt upstream of src):
      ^<connectedTo> | <connectedFrom>
      | <connectedThrough>/<connectsFrom>
      | ^<connectsTo>/^<connectedThrough>

    Connection-reaching paths (to land on the Connection resource):

    **Downstream**: ^<connectsFrom> repeated via <connectsTo>/^<connectsFrom>
    **Upstream**:   ^<connectsTo>   repeated via <connectsFrom>/^<connectsTo>

    Multi-hop spells out k=1..hops repetitions, joined with ``|``.
    """
    hops = int(edge.hops)
    direction = edge.direction

    ct  = f"<{S223.connectedTo}>"
    cf  = f"<{S223.connectedFrom}>"
    cth = f"<{CONNECTED_THROUGH}>"
    cst = f"<{CONNECTS_TO}>"
    csf = f"<{CONNECTS_FROM}>"

    # --- entity-reaching one-hop group ---
    if direction == "downstream":
        one_hop_ent = f"({ct}|^{cf})"
        ent_to_conn = f"^{csf}"   # entity → its downstream connection
        conn_to_ent = cst          # connection → downstream entity
    else:  # upstream
        one_hop_ent = f"(^{ct}|{cf})"
        ent_to_conn = f"^{cst}"   # entity → its upstream connection
        conn_to_ent = csf          # connection → upstream entity

    parts: List[str] = []

    # entity-reaching paths: 1..hops entity hops
    for k in range(1, hops + 1):
        parts.append("/".join([one_hop_ent] * k))

    # connection-reaching paths: land on Connection at hop k
    #   k=1: ent_to_conn
    #   k=2: ent_to_conn / conn_to_ent / ent_to_conn
    #   k=N: (ent_to_conn / conn_to_ent){N-1} / ent_to_conn
    for k in range(1, hops + 1):
        conn_steps = [ent_to_conn]
        for _ in range(k - 1):
            conn_steps.extend([conn_to_ent, ent_to_conn])
        parts.append("/".join(conn_steps))

    path = f"({'|'.join(parts)})"
    return f"{src_var} {path} {tgt_var} ."


def _program_edge_pattern(src_var: str, tgt_var: str, edge: QueryEdge, edge_idx: int) -> str:
    """Render a lowered ``via`` step program (see ``core.Q.related``).

    ``edge.patterns`` is a tuple of segments ``(alternatives, star)``:
    alternatives is a tuple of chains, a chain is a tuple of
    ``(predicate_uri, node_class_uri | None)`` hops. Non-star segments run
    exactly once; star segments repeat 0..N times, with ``edge.hops``
    bounding the **total** number of steps in the whole chain (min 1).
    """
    segments = list(edge.patterns)
    max_total = int(edge.hops)
    n_fixed = sum(1 for _, star in segments if not star)
    star_positions = [i for i, (_, star) in enumerate(segments) if star]

    count_combos = []
    for counts in itertools.product(range(0, max_total + 1), repeat=len(star_positions)):
        total = n_fixed + sum(counts)
        if 1 <= total <= max_total:
            count_combos.append(counts)
    if not count_combos:
        raise ValueError(
            f"via chain needs at least {max(n_fixed, 1)} step(s) but max_depth is {max_total}"
        )

    def render_group(alts, prev: str, obj: str, uid: str) -> str:
        rendered = []
        for ai, chain in enumerate(alts):
            clauses: List[str] = []
            p = prev
            for si, (pred, node_cls) in enumerate(chain):
                o = obj if si == len(chain) - 1 else f"?m_{uid}_a{ai}_{si}"
                clauses.append(f"{p} {_format_pred(pred)} {o} .")
                if node_cls:
                    clauses.append(f"{o} <{_RDF_TYPE}>/<{_SUBCLASS}>* <{node_cls}> .")
                p = o
            rendered.append(" ".join(clauses))
        if len(rendered) == 1:
            return rendered[0]
        return "{ " + " UNION ".join("{ " + r + " }" for r in rendered) + " }"

    union_blocks: List[str] = []
    for ci, counts in enumerate(count_combos):
        groups = []
        star_iter = iter(counts)
        for alts, star in segments:
            reps = next(star_iter) if star else 1
            groups.extend([alts] * reps)
        parts: List[str] = []
        prev = src_var
        for gi, alts in enumerate(groups):
            obj = tgt_var if gi == len(groups) - 1 else f"?x_e{edge_idx}_c{ci}_{gi}"
            parts.append(render_group(alts, prev, obj, f"e{edge_idx}_c{ci}_g{gi}"))
            prev = obj
        union_blocks.append("{ " + " ".join(parts) + " }")
    return " UNION ".join(union_blocks)


def _edge_pattern(src_var: str, tgt_var: str, edge: QueryEdge, edge_idx: int) -> str:
    """
    Build a WHERE fragment for one edge.

    Enhancement:
    - Whenever it emits an edge pattern, also emit an alternative where the FIRST hop
    is taken via a connection point.

    Rules:
    - If edge.patterns is set (a lowered via program): chains of shortcut steps.
    - If edge.direction is set: delegate to _direction_edge_pattern for full topology traversal.
    - If edge.predicates is present/non-empty: constrain to those predicates and allow length 1..hops.
    - Else: allow any predicates, but length <= hops, via UNION of k-step chains,
      excluding any hidden predicates (see ``shortcuts.hide``).
    """
    if getattr(edge, "patterns", None):
        return _program_edge_pattern(src_var, tgt_var, edge, edge_idx)
    if getattr(edge, "direction", None) is not None:
        return _direction_edge_pattern(src_var, tgt_var, edge, edge_idx)

    hops = int(edge.hops)
    if hops < 1:
        raise ValueError(f"edge.hops must be >= 1, got {edge.hops}")

    preds = getattr(edge, "predicates", None) or []
    preds = [p for p in preds if p]  # remove falsy

    # Case A: constrained predicate set
    if preds:
        seen = set()
        uniq = []
        for p in preds:
            if p not in seen:
                seen.add(p)
                uniq.append(p)

        # normal property path (no variables inside)
        if hops == 1:
            alt = "|".join(_format_pred(p) for p in uniq)
            path = f"({alt})"
        else:
            parts = []
            for p in uniq:
                fp = _format_pred(p)
                for k in range(1, hops + 1):
                    parts.append("/".join([fp] * k))
            path = f"({'|'.join(parts)})"

        normal = f"{src_var} {path} {tgt_var} ."

        # CP alternative:
        # - For hops==1 we can still keep it as a property path because it's all IRIs:
        #     src <cp>/<p> tgt
        # - For hops>1, rewrite as a UNION over k with explicit triples so CP only affects first hop.
        if hops == 1:
            cp_f = getattr(edge, "cp_filter", None)
            if cp_f:
                cp = f"?cp_e{edge_idx}"
                via_cp = f"{src_var} <{CONNECTION_POINT}> {cp} . {cp} a <{cp_f}> . {cp} {path} {tgt_var} ."
            else:
                via_cp = f"{src_var} <{CONNECTION_POINT}>/{path} {tgt_var} ."
            return f"{{ {normal} }} UNION {{ {via_cp} }}"
        else:
            union_blocks: List[str] = []

            for k in range(1, hops + 1):
                mids = [f"?x_e{edge_idx}_{i}_k{k}" for i in range(1, k)]  # k-1

                # We'll build a UNION per predicate for this k.
                pred_blocks = []
                for p in uniq:
                    fp = _format_pred(p)
                    triples_normal = []
                    prev = src_var
                    for step in range(k):
                        obj = tgt_var if step == k - 1 else mids[step]
                        triples_normal.append(f"{prev} {fp} {obj} .")
                        prev = obj

                    # CP version: inject cp node only on first hop
                    cp = f"?cp_e{edge_idx}_k{k}"
                    triples_cp = list(triples_normal)
                    first_obj = tgt_var if k == 1 else mids[0]
                    cp_f = getattr(edge, "cp_filter", None)
                    cp_type = f" {cp} a <{cp_f}> ." if cp_f else ""
                    triples_cp[0] = f"{src_var} <{CONNECTION_POINT}> {cp} .{cp_type} {cp} {fp} {first_obj} ."

                    block_normal = "{ " + " ".join(triples_normal) + " }"
                    block_cp = "{ " + " ".join(triples_cp) + " }"
                    pred_blocks.append(f"{block_normal} UNION {block_cp}")

                union_blocks.append("{ " + " UNION ".join(pred_blocks) + " }")

            return " UNION ".join(union_blocks)

    # Case B: unconstrained predicates -> UNION of explicit k-step chains.
    # Hidden predicates (shortcuts.hide) are excluded from every hop.
    hidden = sorted(hidden_predicates())
    hidden_filter = (
        "FILTER({pvar} NOT IN (" + ", ".join(f"<{h}>" for h in hidden) + "))"
        if hidden else None
    )
    union_blocks: List[str] = []
    for k in range(1, hops + 1):
        mids = [f"?x_e{edge_idx}_{i}" for i in range(1, k)]  # k-1 intermediates
        ps = [f"?p_e{edge_idx}_{i}" for i in range(1, k + 1)]

        # normal chain triples
        triples_normal: List[str] = []
        prev = src_var
        for step in range(k):
            pvar = ps[step]
            obj = tgt_var if step == k - 1 else mids[step]
            triples_normal.append(f"{prev} {pvar} {obj} .")
            prev = obj

        # connection-point variant: rewrite first hop using intermediate cp node (NO property path with var)
        triples_cp: List[str] = []
        cp = f"?cp_e{edge_idx}_k{k}"
        first_obj = tgt_var if k == 1 else mids[0]
        triples_cp.append(f"{src_var} <{CONNECTION_POINT}> {cp} .")
        cp_f = getattr(edge, "cp_filter", None)
        if cp_f:
            triples_cp.append(f"{cp} a <{cp_f}> .")
        triples_cp.append(f"{cp} {ps[0]} {first_obj} .")
        # remaining hops (if any) unchanged
        if k > 1:
            triples_cp.extend(triples_normal[1:])

        if hidden_filter:
            filters = [hidden_filter.format(pvar=pvar) for pvar in ps]
            triples_normal.extend(filters)
            triples_cp.extend(filters)

        block_normal = "{ " + " ".join(triples_normal) + " }"
        block_cp = "{ " + " ".join(triples_cp) + " }"
        union_blocks.append(f"{block_normal} UNION {block_cp}")

    return " UNION ".join(union_blocks)


def compile_sparql(graph: QueryGraph) -> str:
    """Compile a query graph to a SPARQL SELECT string."""
    # node id -> ?v{id}
    var_map = {nid: f"?v{nid}" for nid in graph.nodes}
    ext_vars = {}

    where_clauses: List[str] = []

    # rdf:type constraints and instance constraints
    for nid, node in graph.nodes.items():
        v = var_map[nid]
        instance_uri = (node.constraints or {}).get("instance_uri")
        rdf_class = (node.constraints or {}).get("rdf_class")
        if instance_uri is not None:
            where_clauses.append(f"VALUES {v} {{ <{instance_uri}> }}")
        if rdf_class:
            # Anchor the subClassOf* traversal at the constant class inside
            # a sub-SELECT. This fences the property path so Oxigraph
            # evaluates it *backward* from <class> (a handful of nodes)
            # instead of driving it *forward* from every ?v_typ bound by
            # rdf:type. The naive `?v rdf:type ?t . ?t subClassOf* <class>`
            # form lets the planner re-walk the whole subclass forest above
            # each typed individual — catastrophic when a deep-hierarchy
            # ontology (e.g. QUDT, with owl:Restriction skeletons over
            # ~16k unit/quantitykind individuals) is in the union graph
            # (~6s vs ~0.03s for this same query).
            typ = f"{v}_typ"
            where_clauses.append(f"{v} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> {typ} .")
            where_clauses.append(
                f"{{ SELECT DISTINCT {typ} WHERE {{ "
                f"{typ} <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <{rdf_class}> . "
                f"}} }}"
            )
        for name, aval in ((node.constraints or {}).get("attrs") or {}).items():
            where_clauses.extend(_attr_clauses(v, REGISTRY[name], aval))

    # edge constraints
    for edge_idx, edge in enumerate(graph.edges):
        src_var = var_map[edge.source_id]
        tgt_var = var_map[edge.target_id]
        where_clauses.append(_edge_pattern(src_var, tgt_var, edge, edge_idx))

    # data node constraints
    unit_vars = {}
    extunit_vars = {}
    for nid, info in graph.data_nodes.items():
        v = var_map[nid]
        ext = f"?ext{nid}"
        ext_vars[nid] = ext
        where_clauses.append(f"{v} <{HAS_EXTERNAL_REFERENCE}> {ext} .")

        # OPTIONAL unit metadata for property and external reference
        uvar = f"?unit{nid}"
        euvar = f"?extunit{nid}"
        unit_vars[nid] = uvar
        extunit_vars[nid] = euvar
        where_clauses.append(f"OPTIONAL {{ {v} <{HAS_UNIT}> {uvar} . }}")
        where_clauses.append(f"OPTIONAL {{ {ext} <{HAS_UNIT}> {euvar} . }}")

        for pred, val in (info.filters or {}).items():
            if val is None:
                continue

            # Registry-attribute keys expand via the attribute definition;
            # anything else is a raw predicate URI (legacy-shaped filters).
            if isinstance(pred, str) and pred in REGISTRY:
                where_clauses.extend(_attr_clauses(v, REGISTRY[pred], val))
                continue

            # Unwrap negation marker
            negate = isinstance(val, Not)
            if negate:
                val = val.value

            # Build the triple pattern(s)
            if isinstance(val, str) and ("://" in val or val.startswith("urn:")):
                if negate:
                    where_clauses.append(f"FILTER NOT EXISTS {{ {v} <{pred}> <{val}> . }}")
                else:
                    where_clauses.append(f"{v} <{pred}> <{val}> .")

            elif isinstance(val, list):
                items = [x for x in val if x is not None]
                if negate:
                    # Exclude any that match: FILTER NOT EXISTS with UNION
                    if len(items) == 1:
                        where_clauses.append(f"FILTER NOT EXISTS {{ {v} <{pred}> {_term(items[0])} . }}")
                    else:
                        union_block = " UNION ".join(f"{{ {v} <{pred}> {_term(x)} . }}" for x in items)
                        where_clauses.append(f"FILTER NOT EXISTS {{ {union_block} }}")
                else:
                    union_block = " UNION ".join(f"{{ {v} <{pred}> {_term(x)} . }}" for x in items)
                    where_clauses.append(f"{{ {union_block} }}")

            else:
                if negate:
                    where_clauses.append(f'FILTER NOT EXISTS {{ {v} <{pred}> "{val}" . }}')
                else:
                    where_clauses.append(f'{v} <{pred}> "{val}" .')

    # projected attribute columns (?attr<N>_<name>, OPTIONAL so rows without
    # the attribute survive; the prefix is disjoint from v/ext/unit/extunit
    # so DataObject's column parsing ignores them)
    attr_vars: List[str] = []
    for nid, name in getattr(graph, "selects", ()):
        attr = REGISTRY[name]
        avar = f"?attr{nid}_{name}"
        pred_path = "|".join(f"<{p}>" for p in attr.predicates)
        where_clauses.append(f"OPTIONAL {{ {var_map[nid]} ({pred_path}) {avar} . }}")
        attr_vars.append(avar)

    select_parts = (list(var_map.values()) + list(ext_vars.values())
                    + list(unit_vars.values()) + list(extunit_vars.values()) + attr_vars)
    select_vars = " ".join(select_parts)
    where_block = "\n  ".join(where_clauses) if where_clauses else ""
    return f"SELECT DISTINCT {select_vars}\nWHERE {{\n  {where_block}\n}}"
