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

from rdflib.namespace import RDF, RDFS

import itertools

from acquirium.Client.explore.attributes import REGISTRY, Attr, Not, normalize_value
from acquirium.Client.explore.hidden import hidden_predicates
from acquirium.Client.query_graph import QueryEdge, QueryGraph
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_SOURCE_ID,
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
RDFS_LABEL = str(RDFS.label)
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


def attr_path_groups(attr: Attr, role: str) -> tuple[List[str], List[str]]:
    """One attribute's paths, split into ``(on the node, through its reference)``.

    The single place an attribute's predicates become path syntax. Paths come
    back already bracketed, ready to drop into a triple pattern.

    A ``via_ref`` attribute on a measurement node also matches one hop through
    ``ref:hasExternalReference``: stream registration writes semantics on the
    reference while a user's model puts them on the point, and a query should
    find either. Everything else has an empty second group.
    """
    direct = [f"<{p}>" for p in attr.predicates]
    if attr.via_ref and role == "data":
        return direct, [f"<{HAS_EXTERNAL_REFERENCE}>/<{p}>" for p in attr.predicates]
    return direct, []


def attr_paths(attr: Attr, role: str) -> List[str]:
    """Every path matching one attribute, node-side first."""
    direct, through_ref = attr_path_groups(attr, role)
    return direct + through_ref


def attr_pred_path(attr: Attr, role: str) -> str:
    """``attr_paths`` as a single alternation, for use in a property path.

    Right for *filtering*, where matching either side is the whole point.
    Wrong for *projection* — see ``_attr_select_clause``.
    """
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


def _filter_clauses(v: str, filters: dict | None, role: str) -> List[str]:
    """WHERE clauses for a data or stream node's filters.

    A key naming a registry attribute expands through its definition; anything
    else is a raw predicate URI, which the legacy builder allowed and app
    specs may still carry.
    """
    clauses: List[str] = []
    for pred, val in (filters or {}).items():
        if val is None:
            continue
        if isinstance(pred, str) and pred in REGISTRY:
            clauses.extend(_attr_clauses(v, REGISTRY[pred], val, role))
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


def _data_node_clauses(v: str, nid: int, info) -> List[str]:
    """ext-ref triple, unit OPTIONALs, and filters for one data node."""
    return [
        f"{v} <{HAS_EXTERNAL_REFERENCE}> ?ext{nid} .",
        f"OPTIONAL {{ {v} <{HAS_UNIT}> ?unit{nid} . }}",
        f"OPTIONAL {{ ?ext{nid} <{HAS_UNIT}> ?extunit{nid} . }}",
        *_filter_clauses(v, info.filters, "data"),
    ]


def _stream_node_clauses(
    v: str, nid: int, info, src_var: str | None, src_ext_var: str | None
) -> List[str]:
    """Anchor, point OPTIONAL, unit OPTIONALs and filters for one stream node.

    ``v`` binds the external reference itself, so registration-written
    attributes apply to it directly and a stream with no point still matches.
    ``acq:sourceId`` is the anchor: every Acquirium-managed reference carries
    it, and it is what ``_sync_stream_refs_from_graph`` treats as canonical.

    Chaining off a measurement node reuses that node's already-bound
    ``?ext`` rather than re-walking ``hasExternalReference``, which would
    self-join the same triples. Chaining off an entity walks the edge.
    """
    clauses: List[str] = []
    point = f"?pt{nid}"

    if src_ext_var is not None:
        # The source measurement already bound this reference and its point.
        clauses.append(f"BIND({src_ext_var} AS {v})")
        clauses.append(f"BIND({src_var} AS {point})")
        clauses.append(f"{v} <{ACQUIRIUM_SOURCE_ID}> ?sid{nid} .")
        clauses.append(f"OPTIONAL {{ {point} <{HAS_UNIT}> ?unit{nid} . }}")
    else:
        if src_var is not None:
            clauses.append(f"{src_var} <{HAS_EXTERNAL_REFERENCE}> {v} .")
        clauses.append(f"{v} <{ACQUIRIUM_SOURCE_ID}> ?sid{nid} .")
        # A point is optional; its unit only means anything when it exists, so
        # nest rather than leaving ?pt unbound in a sibling OPTIONAL.
        clauses.append(
            f"OPTIONAL {{ {point} <{HAS_EXTERNAL_REFERENCE}> {v} . "
            f"OPTIONAL {{ {point} <{HAS_UNIT}> ?unit{nid} . }} }}"
        )

    clauses.append(f"OPTIONAL {{ {v} <{HAS_UNIT}> ?extunit{nid} . }}")
    # A reference URI is a UUID, so the label is what identifies the stream in
    # anything a human reads.
    clauses.append(f"OPTIONAL {{ {v} <{RDFS_LABEL}> ?label{nid} . }}")
    clauses.extend(_filter_clauses(v, info.filters, "stream"))
    return clauses


def _attr_select_clause(
    v: str, nid: int, name: str, required: bool, role: str = "entity"
) -> tuple[List[str], str]:
    """Clauses projecting one attribute as ``?attr{nid}_{name}``.

    A ``via_ref`` attribute cannot project as a plain alternation: when the
    point and its reference both carry a value the alternation binds twice and
    the measurement comes back as two rows. Reconciliation deliberately allows
    exactly that — a convertible pair such as a point in Celsius against a
    reference in Fahrenheit is accepted at registration — so it is a case the
    read path has to handle, not a hypothetical.

    Each side is bound separately and COALESCEd instead, which yields one row
    and makes the point win. That matches ``DataObject._resolve_effective_units``,
    which reads the point's unit in preference to the reference's.
    """
    attr = REGISTRY[name]
    avar = f"?attr{nid}_{name}"
    direct, through_ref = attr_path_groups(attr, role)

    if not through_ref:
        clause = f"{v} ({'|'.join(direct)}) {avar} ."
        return [clause if required else f"OPTIONAL {{ {clause} }}"], avar

    point_var, ref_var = f"{avar}__point", f"{avar}__ref"
    clauses = [
        f"OPTIONAL {{ {v} ({'|'.join(direct)}) {point_var} . }}",
        f"OPTIONAL {{ {v} ({'|'.join(through_ref)}) {ref_var} . }}",
        f"BIND(COALESCE({point_var}, {ref_var}) AS {avar})",
    ]
    if required:
        clauses.append(f"FILTER(BOUND({avar}))")
    return clauses, avar


def compile_parts(graph: QueryGraph) -> tuple:
    """Compile a query graph into ``(var_map, select_parts, where_clauses)``.

    ``compile_sparql`` assembles these into the standard SELECT; facet
    aggregations (``Query.options``) reuse the WHERE body with their own
    projection and GROUP BY. Queries with **multiple** measurement nodes
    compile their data blocks as UNION branches over the shared entity
    pattern, so results are the union of the per-node matches (M+N rows,
    empty nodes contribute None columns) instead of a cross-product join.
    """
    # Several measurement nodes must union rather than cross-product. A stream
    # node never triggers this: chained, it refines one measurement and belongs
    # in the same block; standalone, it is the only such node. Query.streams()
    # refuses the combination the multi path could not express.
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

        where_clauses.extend(_filter_clauses(v, info.filters, "data"))

    # stream node constraints — the reference itself, with an optional point
    point_vars: dict[int, str] = {}
    label_vars: dict[int, str] = {}
    for nid, info in graph.stream_nodes.items():
        v = var_map[nid]
        src_id = info.source_id
        src_var = var_map[src_id] if src_id is not None else None
        src_ext = ext_vars.get(src_id) if src_id is not None else None
        where_clauses.extend(_stream_node_clauses(v, nid, info, src_var, src_ext))
        # No ext var: the stream node *is* the reference, so DataObject reads
        # it straight off ?v{nid} and only needs the point projected.
        point_vars[nid] = f"?pt{nid}"
        label_vars[nid] = f"?label{nid}"
        unit_vars[nid] = f"?unit{nid}"
        extunit_vars[nid] = f"?extunit{nid}"

    # projected attribute columns (?attr<N>_<name>, OPTIONAL so rows without
    # the attribute survive; the prefix is disjoint from v/ext/unit/extunit
    # so DataObject's column parsing ignores them)
    attr_var_pairs: List[tuple] = []  # (node_id, var) in selects order
    for nid, name, required in getattr(graph, "selects", ()):
        clauses, avar = _attr_select_clause(
            var_map[nid], nid, name, required, graph.node_role(nid)
        )
        where_clauses.extend(clauses)
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
        [v for nid, v in label_vars.items() if nid not in dropped]
        + [v for nid, v in point_vars.items() if nid not in dropped]
        + [v for nid, v in ext_vars.items() if nid not in dropped]
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
        clauses, avar = _attr_select_clause(var_map[nid], nid, name, required, "entity")
        where_clauses.extend(clauses)
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
                clauses, avar = _attr_select_clause(v, nid, name, required, "data")
                b.extend(clauses)
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
