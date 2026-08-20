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
from acquirium.Client.explore.hidden import hidden_predicates
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


def render_alternatives(alts, prev: str, obj: str, uid: str) -> str:
    """Render one program segment (UNION of step chains) between two vars.

    Shared by the edge compiler and the traversal materializer
    (``explore.traverse``), so both interpret programs identically. The
    wildcard predicate ``"*"`` matches any predicate except the hidden set
    (IRI objects only, so the hop lands on a node).
    """
    rendered = []
    for ai, chain in enumerate(alts):
        clauses: List[str] = []
        p = prev
        for si, (pred, node_cls) in enumerate(chain):
            o = obj if si == len(chain) - 1 else f"?m_{uid}_a{ai}_{si}"
            if pred == "*":
                pvar = f"?p_{uid}_a{ai}_{si}"
                clauses.append(f"{p} {pvar} {o} .")
                hidden = sorted(hidden_predicates())
                if hidden:
                    clauses.append(f"FILTER({pvar} NOT IN (" + ", ".join(f"<{h}>" for h in hidden) + "))")
                clauses.append(f"FILTER(isIRI({o}))")
            else:
                clauses.append(f"{p} {_format_pred(pred)} {o} .")
            if node_cls:
                clauses.append(f"{o} <{_RDF_TYPE}>/<{_SUBCLASS}>* <{node_cls}> .")
            p = o
        rendered.append(" ".join(clauses))
    if len(rendered) == 1:
        return rendered[0]
    return "{ " + " UNION ".join("{ " + r + " }" for r in rendered) + " }"


def attr_paths(attr: Attr, role: str) -> List[str]:
    """Bracketed SPARQL property paths matching one attribute, in match order.

    The single place an attribute's predicates become path syntax. Returned
    paths are ready to drop into a triple pattern as-is (already bracketed),
    so callers never re-wrap them.
    """
    return [f"<{p}>" for p in attr.predicates]


def attr_pred_path(attr: Attr, role: str) -> str:
    """``attr_paths`` as a single alternation, for use in a property path."""
    return "|".join(attr_paths(attr, role))


def _attr_clauses(var: str, attr: Attr, value: Any, role: str = "entity") -> List[str]:
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
        pred_path = attr_pred_path(attr, role)
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

    combos = [(path, _term(x)) for path in attr_paths(attr, role) for x in values]
    if len(combos) == 1:
        path, t = combos[0]
        triple = f"{var} {path} {t} ."
        clauses.append(f"FILTER NOT EXISTS {{ {triple} }}" if negated else triple)
    else:
        union = " UNION ".join(f"{{ {var} {path} {t} . }}" for path, t in combos)
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
    """Render a lowered ``via`` step program (see ``core.Query.related``).

    ``edge.patterns`` is a tuple of segments ``(alternatives, star)``:
    alternatives is a tuple of chains, a chain is a tuple of
    ``(predicate_uri, node_class_uri | None)`` hops. Non-star segments run
    exactly once; star segments repeat 0..N times, with ``edge.hops``
    bounding the **total** number of steps in the whole chain (min 1).
    """
    segments = list(edge.patterns)
    # Program edges resolve by client-side BFS at execute time; this SPARQL
    # rendering is a preview/debug aid. hops=0 means unbounded — render the
    # preview with a bound of 3.
    max_total = int(edge.hops) or 3
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
            parts.append(render_alternatives(alts, prev, obj, f"e{edge_idx}_c{ci}_g{gi}"))
            prev = obj
        union_blocks.append("{ " + " ".join(parts) + " }")
    return " UNION ".join(union_blocks)


def _edge_pattern(src_var: str, tgt_var: str, edge: QueryEdge, edge_idx: int,
                  is_data_edge: bool = False) -> str:
    """
    Build a WHERE fragment for one edge.

    Enhancement:
    - Whenever it emits an edge pattern, also emit an alternative where the FIRST hop
    is taken via a connection point.

    Rules:
    - If edge.value_pairs is set (a resolved nearest edge): paired VALUES.
    - If edge.patterns is set (a lowered via program): chains of via steps.
    - If edge.direction is set: delegate to _direction_edge_pattern for full topology traversal.
    - If edge.predicates is present/non-empty: constrain to those predicates and allow length 1..hops.
    - Else: allow any predicates, but length <= hops, via UNION of k-step chains,
      excluding any hidden predicates (see ``hidden.hide``). Edges that
      target a measurement node are exempt from hiding — that's how data
      attaches (hasProperty/observes/...), and the external-reference
      requirement bounds them.
    """
    if getattr(edge, "value_pairs", None) is not None:
        pairs = " ".join(f"(<{s}> <{t}>)" for s, t in edge.value_pairs)
        return f"VALUES ({src_var} {tgt_var}) {{ {pairs} }}"
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
    # Hidden predicates (hidden.hide) are excluded from every hop, except
    # on data-node edges.
    hidden = [] if is_data_edge else sorted(hidden_predicates())
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
        if not getattr(edge, "cp_union", True):
            union_blocks.append(block_normal)
            continue
        block_cp = "{ " + " ".join(triples_cp) + " }"
        union_blocks.append(f"{block_normal} UNION {block_cp}")

    return " UNION ".join(union_blocks)


def _node_constraint_clauses(v: str, node) -> List[str]:
    """VALUES / class-fence / attribute clauses for one node."""
    clauses: List[str] = []
    instance_uri = (node.constraints or {}).get("instance_uri")
    rdf_class = (node.constraints or {}).get("rdf_class")
    if instance_uri is not None:
        clauses.append(f"VALUES {v} {{ <{instance_uri}> }}")
    if rdf_class:
        # Anchor the subClassOf* traversal at the constant class inside
        # a sub-SELECT. This fences the property path so Oxigraph
        # evaluates it *backward* from <class> (a handful of nodes)
        # instead of driving it *forward* from every ?v_typ bound by
        # rdf:type (catastrophic with QUDT in the union graph).
        typ = f"{v}_typ"
        clauses.append(f"{v} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> {typ} .")
        clauses.append(
            f"{{ SELECT DISTINCT {typ} WHERE {{ "
            f"{typ} <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <{rdf_class}> . "
            f"}} }}"
        )
    for name, aval in ((node.constraints or {}).get("attrs") or {}).items():
        clauses.extend(_attr_clauses(v, REGISTRY[name], aval, "entity"))
    return clauses


def _data_node_clauses(v: str, nid: int, info) -> List[str]:
    """ext-ref triple, unit OPTIONALs, and filters for one data node."""
    clauses: List[str] = [f"{v} <{HAS_EXTERNAL_REFERENCE}> ?ext{nid} ."]
    clauses.append(f"OPTIONAL {{ {v} <{HAS_UNIT}> ?unit{nid} . }}")
    clauses.append(f"OPTIONAL {{ ?ext{nid} <{HAS_UNIT}> ?extunit{nid} . }}")

    for pred, val in (info.filters or {}).items():
        if val is None:
            continue
        if isinstance(pred, str) and pred in REGISTRY:
            clauses.extend(_attr_clauses(v, REGISTRY[pred], val, "data"))
            continue
        negate = isinstance(val, Not)
        if negate:
            val = val.value
        if isinstance(val, str) and ("://" in val or val.startswith("urn:")):
            if negate:
                clauses.append(f"FILTER NOT EXISTS {{ {v} <{pred}> <{val}> . }}")
            else:
                clauses.append(f"{v} <{pred}> <{val}> .")
        elif isinstance(val, list):
            items = [x for x in val if x is not None]
            if negate:
                if len(items) == 1:
                    clauses.append(f"FILTER NOT EXISTS {{ {v} <{pred}> {_term(items[0])} . }}")
                else:
                    union_block = " UNION ".join(f"{{ {v} <{pred}> {_term(x)} . }}" for x in items)
                    clauses.append(f"FILTER NOT EXISTS {{ {union_block} }}")
            else:
                union_block = " UNION ".join(f"{{ {v} <{pred}> {_term(x)} . }}" for x in items)
                clauses.append(f"{{ {union_block} }}")
        else:
            if negate:
                clauses.append(f'FILTER NOT EXISTS {{ {v} <{pred}> "{val}" . }}')
            else:
                clauses.append(f'{v} <{pred}> "{val}" .')
    return clauses


def _attr_select_clause(v: str, nid: int, name: str, required: bool, role: str = "entity") -> tuple:
    attr = REGISTRY[name]
    avar = f"?attr{nid}_{name}"
    pred_path = attr_pred_path(attr, role)
    clause = f"{v} ({pred_path}) {avar} ."
    return (clause if required else f"OPTIONAL {{ {clause} }}"), avar


def compile_parts(graph: QueryGraph) -> tuple:
    """Compile a query graph into ``(var_map, select_parts, where_clauses)``.

    ``compile_sparql`` assembles these into the standard SELECT; facet
    aggregations (``Query.options``) reuse the WHERE body with their own
    projection and GROUP BY. Queries with **multiple** measurement nodes
    compile their data blocks as UNION branches over the shared entity
    pattern, so results are the union of the per-node matches (M+N rows,
    empty nodes contribute None columns) instead of a cross-product join.
    """
    if len(graph.data_nodes) > 1:
        return _compile_parts_multi(graph)
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
            where_clauses.extend(_attr_clauses(v, REGISTRY[name], aval, "entity"))

    # edge constraints
    for edge_idx, edge in enumerate(graph.edges):
        src_var = var_map[edge.source_id]
        tgt_var = var_map[edge.target_id]
        where_clauses.append(_edge_pattern(
            src_var, tgt_var, edge, edge_idx,
            is_data_edge=edge.target_id in graph.data_nodes,
        ))

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
                where_clauses.extend(_attr_clauses(v, REGISTRY[pred], val, "data"))
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
    attr_var_pairs: List[tuple] = []  # (node_id, var) in selects order
    for nid, name, required in getattr(graph, "selects", ()):
        clause, avar = _attr_select_clause(
            var_map[nid], nid, name, required, graph.node_role(nid)
        )
        where_clauses.append(clause)
        attr_var_pairs.append((nid, avar))

    # drop(): nodes stay in the pattern (WHERE) but leave the projection —
    # which also collapses DISTINCT rows that differed only in them.
    dropped = {nid for nid, node in graph.nodes.items()
               if (node.constraints or {}).get("dropped")}
    # Each node's attribute columns follow its own column, so metadata reads
    # Equipment, Equipment.process, Equipment_2, Equipment_2.process, ...
    select_parts: List[str] = []
    for nid, v in var_map.items():
        if nid not in dropped:
            select_parts.append(v)
        select_parts.extend(avar for anid, avar in attr_var_pairs if anid == nid)
    select_parts += (
        [v for nid, v in ext_vars.items() if nid not in dropped]
        + [v for nid, v in unit_vars.items() if nid not in dropped]
        + [v for nid, v in extunit_vars.items() if nid not in dropped]
    )
    if not select_parts:
        raise ValueError("drop(): every node is dropped — nothing left to select")
    return var_map, select_parts, where_clauses


def _compile_parts_multi(graph: QueryGraph) -> tuple:
    """compile_parts for graphs with 2+ measurement nodes.

    The shared entity pattern compiles once; each data node's block (its
    edge, ext-ref requirement, unit OPTIONALs, filters, and projected
    attributes) becomes one UNION branch. Rows therefore bind exactly one
    measurement node (the others' columns are None), the result is the
    union of per-node matches instead of their cross product, and a node
    with no matches contributes nothing without emptying the rest.
    """
    var_map = {nid: f"?v{nid}" for nid in graph.nodes}
    data_ids = set(graph.data_nodes)

    where_clauses: List[str] = []
    for nid, node in graph.nodes.items():
        if nid in data_ids:
            continue
        where_clauses.extend(_node_constraint_clauses(var_map[nid], node))

    for edge_idx, edge in enumerate(graph.edges):
        if edge.target_id in data_ids:
            continue
        where_clauses.append(_edge_pattern(
            var_map[edge.source_id], var_map[edge.target_id], edge, edge_idx,
            is_data_edge=False,
        ))

    attr_var_pairs: List[tuple] = []  # (node_id, var) in selects order
    for nid, name, required in getattr(graph, "selects", ()):
        if nid in data_ids:
            continue
        clause, avar = _attr_select_clause(var_map[nid], nid, name, required, "entity")
        where_clauses.append(clause)
        attr_var_pairs.append((nid, avar))

    branches: List[str] = []
    for nid, info in graph.data_nodes.items():
        v = var_map[nid]
        b: List[str] = list(_node_constraint_clauses(v, graph.nodes[nid]))
        for edge_idx, edge in enumerate(graph.edges):
            if edge.target_id == nid:
                b.append(_edge_pattern(
                    var_map[edge.source_id], v, edge, edge_idx, is_data_edge=True))
        b.extend(_data_node_clauses(v, nid, info))
        # this node's projected attributes live inside its branch: outside it
        # the unbound variable would turn the binding into an open pattern
        for snid, name, required in getattr(graph, "selects", ()):
            if snid == nid:
                clause, avar = _attr_select_clause(v, nid, name, required, "data")
                b.append(clause)
                attr_var_pairs.append((nid, avar))
        branches.append("{ " + " ".join(b) + " }")
    where_clauses.append(" UNION ".join(branches))

    dropped = {nid for nid, node in graph.nodes.items()
               if (node.constraints or {}).get("dropped")}
    select_parts: List[str] = []
    for nid, v in var_map.items():
        if nid not in dropped:
            select_parts.append(v)
        select_parts.extend(avar for anid, avar in attr_var_pairs if anid == nid)
    select_parts += (
        [f"?ext{nid}" for nid in graph.data_nodes if nid not in dropped]
        + [f"?unit{nid}" for nid in graph.data_nodes if nid not in dropped]
        + [f"?extunit{nid}" for nid in graph.data_nodes if nid not in dropped]
    )
    if not select_parts:
        raise ValueError("drop(): every node is dropped — nothing left to select")
    return var_map, select_parts, where_clauses


def compile_sparql(graph: QueryGraph) -> str:
    """Compile a query graph to a SPARQL SELECT string."""
    _, select_parts, where_clauses = compile_parts(graph)
    select_vars = " ".join(select_parts)
    where_block = "\n  ".join(where_clauses) if where_clauses else ""
    return f"SELECT DISTINCT {select_vars}\nWHERE {{\n  {where_block}\n}}"
