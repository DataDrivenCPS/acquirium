"""Core immutable query builder for the explore layer.

``Query`` is the clean replacement for the legacy builder (now ``Q``): short verbs
(``entity`` / ``related`` / ``measurement`` / ``where`` / ``include`` /
``alias`` / ``drop`` / ``refocus``) build an immutable :class:`QueryGraph`, and terminals
(``metadata`` / ``data`` / ``dataframe`` / ``execute`` / ``to_sparql``) run
it. Compilation is delegated to the pure
:func:`~acquirium.Client.explore.compile.compile_sparql`.

Every verb returns a **new** ``Query`` with a fresh result cache, so variants can
be kept side by side::

    ro = aq.query().entity("reverse osmosis membrane").alias("ro")
    out = ro.related("outlet connection point", alias="out")
    permeate = out.measurement(alias="permeate")
    permeate.metadata()
    permeate.data(start=t0, end=t1)["permeate"]
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import polars as pl
from rdflib import URIRef

from acquirium.Client.explore.attributes import REGISTRY, Not, attributes_doc, normalize_value
from acquirium.Client.explore.compile import compile_sparql
from acquirium.Client.explore.directions import EQUIPMENT_STEPS, PROPERTY_STEPS
from acquirium.Client.query_graph import DataNodeInfo, QueryEdge, QueryGraph, QueryNode
from acquirium.internals.internals_namespaces import S223

if TYPE_CHECKING:
    from acquirium.Client.client import AcquiriumClient
    from acquirium.Client.data_object import DataObject
    from acquirium.Client.explore.facets import FacetSummary

_DIRECTIONS = ("upstream", "downstream")


def _is_uri(text: Any) -> bool:
    return isinstance(text, str) and (
        text.startswith("urn:") or text.startswith("http://") or text.startswith("https://")
    )


@dataclass(frozen=True)
class Query:
    """Immutable explore query bound to an Acquirium client."""

    client: "AcquiriumClient | None"
    query_graph: QueryGraph = field(default_factory=QueryGraph)
    cache: Dict[str, Any] = field(default_factory=dict, compare=False)

    # ---------- internal helpers ----------

    def _next_id(self) -> int:
        return max(self.query_graph.nodes, default=-1) + 1

    def _with_graph(self, g: QueryGraph) -> "Query":
        return Query(client=self.client, query_graph=g)

    def _as_uri(self, value: str | URIRef, kind: str) -> str:
        """Coerce a class/predicate input to a URI: passthrough or text-resolve."""
        if isinstance(value, URIRef) or _is_uri(value):
            return str(value)
        uri = self.client.resolve(str(value), kind, min_score=0.4)
        if uri is None:
            raise ValueError(f"Could not resolve {value!r} as {kind}")
        return uri

    def _normalize_instance_uri(self, uri: str | URIRef | None, *, param: str = "uri") -> str | None:
        if uri is None:
            return None
        if isinstance(uri, URIRef) or _is_uri(uri):
            return str(uri)
        if isinstance(uri, str):
            try:
                return self.client.expand_uri(uri)
            except Exception as e:
                raise ValueError(f"Invalid URI or CURIE '{uri}' for parameter '{param}': {e}")
        raise ValueError(
            f"{param} must be a URI (urn:..., http://..., https://...) "
            f"or a CURIE 'prefix:local' with a bound prefix"
        )

    def _source_id(self, frm: Optional[str], *, verb: str) -> int:
        src_id = self.query_graph.resolve_alias(frm)
        if src_id is None:
            raise ValueError(f"{verb}: no source node (set frm or start with entity())")
        return src_id

    def _src_alias(self, src_id: int) -> str:
        return self.query_graph.aliases_reverse.get(src_id, str(src_id))

    def _unique_alias(self, g: QueryGraph, base: str) -> str:
        if base not in g.aliases:
            return base
        i = 2
        while f"{base}_{i}" in g.aliases:
            i += 1
        return f"{base}_{i}"

    def _default_alias(self, g: QueryGraph, cls: Any) -> Optional[str]:
        """Default node alias: the class text as given; a CURIE for a class
        URI (local name when no prefix is bound); None -> numeric fallback."""
        if cls is None:
            return None
        s = str(cls)
        if isinstance(cls, URIRef) or _is_uri(s):
            try:
                name = self.client.compact_uri(s)
            except Exception:
                name = s.split("#")[-1].rsplit("/", 1)[-1]
        else:
            name = s
        return self._unique_alias(g, name)

    def _lower_via(self, via: str) -> tuple:
        """Lower a single-predicate ``via`` into a repeatable step program.

        The predicate may be a URI or free text (server-resolved as kind
        "predicate"); a ``"^"`` prefix inverts it. The step repeats up to
        ``max_depth`` times (default 3), so ``via="hasMember"`` walks nested
        membership. Returns ``(program, default_hops)``.
        """
        pred = via.strip()
        inverted = pred.startswith("^")
        core = pred[1:] if inverted else pred
        if not core:
            raise ValueError("related: empty via predicate")
        if not _is_uri(core):
            resolved = self.client.resolve(core, "predicate", min_score=0.4)
            if resolved is None:
                raise ValueError(
                    f"related: could not resolve via predicate {core!r} "
                    f"(pass 'any', a predicate URI/text, or a list of predicates)"
                )
            core = resolved
        pred_uri = f"^{core}" if inverted else core
        program = ((((pred_uri, None),),), True),
        return program, 3

    def _resolve_attr_values(self, attrs: Dict[str, Any]) -> Dict[str, Any]:
        """Validate attr names and resolve text values to URIs in one joint call.

        URIs/URIRefs pass through; literal attrs keep their raw value; the
        remaining text values are resolved together via
        ``client.resolve`` so siblings disambiguate each other.
        ``Not`` markers are preserved around the resolved value.
        """
        unknown = [k for k in attrs if k not in REGISTRY]
        if unknown:
            raise ValueError(f"unknown attribute(s) {unknown}; known: {sorted(REGISTRY)}")

        record: Dict[str, Any] = {}
        for name, raw in attrs.items():
            attr = REGISTRY[name]
            values, _ = normalize_value(raw)
            for i, v in enumerate(values):
                if attr.literal or isinstance(v, URIRef) or _is_uri(v):
                    continue
                record[f"{name}_{i}"] = (str(v), attr.kind)
        resolved_uris = self.client.resolve(record, min_score=0.4) if record else {}

        out: Dict[str, Any] = {}
        for name, raw in attrs.items():
            attr = REGISTRY[name]
            values, negated = normalize_value(raw)
            if not values:
                continue
            coerced: List[Any] = []
            for i, v in enumerate(values):
                if attr.literal:
                    coerced.append(v)
                elif isinstance(v, URIRef) or _is_uri(v):
                    coerced.append(str(v))
                else:
                    uri = resolved_uris.get(f"{name}_{i}")
                    if uri is None:
                        raise ValueError(f"Could not resolve {v!r} as {attr.kind} for attribute {name!r}")
                    coerced.append(uri)
            value = coerced[0] if len(coerced) == 1 else coerced
            out[name] = Not(value) if negated else value
        return out

    def _apply_attrs(self, g: QueryGraph, node_ids: List[int], resolved: Dict[str, Any]) -> QueryGraph:
        """Store resolved attribute constraints on the given nodes (role-checked)."""
        ptr = g.current_pointer
        for nid in node_ids:
            is_data = nid in g.data_nodes
            role = "data" if is_data else "entity"
            for name in resolved:
                if role not in REGISTRY[name].roles:
                    alias = g.aliases_reverse.get(nid, str(nid))
                    raise ValueError(f"attribute {name!r} does not apply to {role} node {alias!r}")
            if is_data:
                info = g.data_nodes[nid]
                filters = dict(info.filters)
                filters.update(resolved)
                g = g.with_data_node(replace(info, filters=filters))
            else:
                node = g.nodes[nid]
                constraints = dict(node.constraints)
                merged = dict(constraints.get("attrs") or {})
                merged.update(resolved)
                constraints["attrs"] = merged
                g = g.with_node(replace(node, constraints=constraints))
        return replace(g, current_pointer=ptr)

    # ---------- verbs ----------

    def entity(self, cls: str | URIRef | None = None, *, uri: str | URIRef | None = None,
               alias: Optional[str] = None, **attrs: Any) -> "Query":
        """Add a new entity node to the pattern and point at it.

        ``cls`` is a class URI or free text (resolved via the server);
        ``uri`` pins a specific instance. Extra keyword arguments are
        attribute filters applied to the new node (same as ``where()``)::

            aq.query().entity("Equipment", process="ozonation")
        """
        instance_uri = self._normalize_instance_uri(uri)
        if cls is None and instance_uri is None and not attrs:
            raise ValueError("entity: provide cls, uri, or attribute filters")
        constraints: Dict[str, Any] = {}
        if cls is not None:
            constraints["rdf_class"] = self._as_uri(cls, "class")
        if instance_uri is not None:
            constraints["instance_uri"] = instance_uri
        alias = alias or self._default_alias(self.query_graph, cls)
        node = QueryNode(id=self._next_id(), alias=alias, constraints=constraints)
        q2 = self._with_graph(self.query_graph.with_node(node))
        if attrs:
            resolved = q2._resolve_attr_values(attrs)
            q2 = q2._with_graph(q2._apply_attrs(q2.query_graph, [node.id], resolved))
        return q2

    def related(self, cls: str | URIRef | None = None, *, uri: str | URIRef | None = None,
                alias: Optional[str] = None, frm: Optional[str] = None,
                via: Any = "any", direction: Optional[str] = None,
                max_depth: Optional[int] = None, nearest: Optional[bool] = None,
                **attrs: Any) -> "Query":
        """Add an entity related to an existing node and point at it.

        - ``frm``: alias of the source node (default: current pointer).
        - ``via``: ``"any"``/``"all"`` (any predicate except hidden ones,
          see ``hidden.hide``), a single predicate (URI or free text,
          ``"^"`` inverts, repeatable up to ``max_depth``), or a list of
          predicate URIs / free-text names (compiled as SPARQL chains).
        - ``direction``: ``"upstream"``/``"downstream"`` s223 topology
          traversal (``via="any"`` only); the exact step patterns each
          direction infers are documented in ``explore.directions``.
        - ``max_depth``: bound on total steps; ``max_depth=0`` means
          unbounded (explicit opt-in). Defaults: 3 for ``"any"``/single
          predicates/directional, 1 for predicate lists.
        - ``nearest``: keep only the closest match(es) per source instead of
          all reachable ones (equal-distance ties all survive). With
          ``via="any"`` distance means raw RDF hops over all non-hidden
          predicates (graph-nearest); pass a direction's steps
          (``via=UPSTREAM_EQUIPMENT``, see ``explore.directions``) when you
          mean nearest along the process flow.

        Via-expression and ``via="any"`` edges are resolved by client-side
        BFS at execute time (SPARQL cannot evaluate multi-hop any-predicate
        chains); predicate lists and directional traversal compile to SPARQL
        directly.
        """
        instance_uri = self._normalize_instance_uri(uri)
        if cls is None and instance_uri is None and not attrs:
            raise ValueError("related: provide cls, uri, or attribute filters")
        src_id = self._source_id(frm, verb="related")

        if direction is not None and direction not in _DIRECTIONS:
            raise ValueError(f"related: direction must be one of {_DIRECTIONS}, got {direction!r}")

        preds: Optional[List[str]] = None
        patterns: Optional[tuple] = None
        default_hops = 3
        if isinstance(via, (list, tuple)) and via and all(
                isinstance(c, tuple) for c in via):
            # step alternatives in program-IR form, e.g. the exported
            # direction constants: via=UPSTREAM_EQUIPMENT (repeatable)
            patterns = ((tuple(via), True),)
        elif isinstance(via, (list, tuple)):
            preds = [
                f"^{self._as_uri(str(p)[1:], 'predicate')}" if str(p).startswith("^")
                else self._as_uri(p, "predicate")
                for p in via
            ]
            default_hops = 1
        elif via in ("any", "all"):
            if direction is None:
                # Wildcard traversal program: any predicate except the hidden
                # set. Multi-hop any-predicate chains are join-explosive in
                # SPARQL, so program edges resolve by client-side BFS at
                # execute time; distance means raw RDF hops. Bounded to 3 by
                # default; pass max_depth=0 for unbounded reachability.
                patterns = (((("*", None),),), True),
        elif isinstance(via, str):
            patterns, default_hops = self._lower_via(via)
        else:
            raise ValueError(
                f"related: via must be 'any'/'all', a predicate, or a list of predicates, got {via!r}"
            )

        if direction is not None and (preds is not None or patterns is not None):
            raise ValueError(
                "related: direction only combines with via='any'; explicit "
                "predicates/steps encode their own direction"
            )

        if nearest is None:
            # default: plain any-relatedness means the *nearest* matches;
            # via expressions / predicate lists / direction default to all
            nearest = via == "any" and direction is None
        if nearest:
            if preds is not None:
                # a predicate list is a one-segment repeatable program
                patterns = ((tuple(((p, None),) for p in preds), True),)
                preds = None
            if patterns is None:  # only reachable when direction is set
                raise ValueError(
                    "related: nearest=True with direction is not supported; "
                    "pass the direction steps instead, e.g. "
                    "via=UPSTREAM_EQUIPMENT (see explore.directions)"
                )

        hops = max_depth if max_depth is not None else default_hops

        constraints: Dict[str, Any] = {}
        if cls is not None:
            constraints["rdf_class"] = self._as_uri(cls, "class")
        if instance_uri is not None:
            constraints["instance_uri"] = instance_uri
        new_id = self._next_id()
        alias = alias or self._default_alias(self.query_graph, cls)
        g = self.query_graph.with_node(QueryNode(id=new_id, alias=alias, constraints=constraints))
        edge = QueryEdge(source_id=src_id, target_id=new_id, hops=hops,
                         predicates=preds, direction=direction, patterns=patterns,
                         nearest=nearest)
        q2 = self._with_graph(g.with_edge(edge, new_pointer=new_id))
        if attrs:
            resolved = q2._resolve_attr_values(attrs)
            q2 = q2._with_graph(q2._apply_attrs(q2.query_graph, [new_id], resolved))
        return q2

    def measurement(self, *, frm: "str | list | tuple | None" = None, alias: Optional[str] = None,
                    direction: Optional[str] = None, max_depth: int = 3,
                    nearest: bool = False, include_connection_points: bool = True,
                    **attrs: Any) -> "Query":
        """Attach a measurement point (data node) to the pattern and point at it.

        Matches nodes carrying an external reference one hop from the source.
        By default that includes measurements on the source's connection
        points (inlet, outlet, bidirectional) as well as on the source
        itself; pass ``include_connection_points=False`` for only the
        source's own measurements. ``frm`` accepts an alias, ``None``
        (current pointer), a list of aliases, or ``"*"`` — one measurement
        node is attached per named entity (``Pump_data``, ``Tank_data``,
        ...). On an **empty query** this is the root
        form — every measurement point in the plant, no entity anchor
        (default alias ``"data"``)::

            aq.query().measurement()                    # all registered streams
            aq.query().measurement(quantity_kind="ph")  # filtered

        With ``direction`` set, first traverses up to ``max_depth`` topology
        hops upstream/downstream through an intermediate entity, then looks
        for measurements one hop away (inlet connection points for upstream,
        outlet for downstream).

        With ``nearest=True`` (requires ``direction``), the closest matching
        measurement per source is found by client-side BFS over the
        ``<direction>_equipment*/<direction>_property`` program —
        ``max_depth`` bounds the equipment steps, attribute filters
        participate in nearness (a closer non-matching property does not
        shadow a farther matching one), and equal-distance ties are kept::

            q.measurement(direction="upstream", nearest=True, quantity_kind="ph")

        Extra keyword arguments are attribute filters applied to the new
        measurement node(s), same as ``where()``::

            q.measurement(quantity_kind="mass flow rate", medium=Not("brine"))
        """
        g = self.query_graph

        if isinstance(frm, (list, tuple)) and direction is not None:
            raise ValueError("measurement: frm list only combines with the non-directional form")

        if direction is not None and not include_connection_points:
            raise ValueError(
                "measurement: include_connection_points=False only applies to the "
                "non-directional form (directional traversal scopes connection "
                "points by the direction rule)"
            )

        if nearest:
            if direction is None:
                raise ValueError("measurement: nearest=True requires direction ('upstream' or 'downstream')")
            if direction not in _DIRECTIONS:
                raise ValueError(f"measurement: direction must be one of {_DIRECTIONS}, got {direction!r}")
            src_id = self._source_id(frm, verb="measurement")
            src_alias = self._src_alias(src_id)
            program = ((EQUIPMENT_STEPS[direction], True),
                       (PROPERTY_STEPS[direction], False))
            data_id = self._next_id()
            g = g.with_node(QueryNode(id=data_id, alias=alias or f"{src_alias}_{direction}_data",
                                      constraints={"is_data_node": True}))
            g = g.with_edge(QueryEdge(source_id=src_id, target_id=data_id,
                                      hops=max_depth + 1, patterns=program, nearest=True),
                            new_pointer=data_id)
            g = g.with_data_node(DataNodeInfo(node_id=data_id))
            if attrs:
                g = self._apply_attrs(g, [data_id], self._resolve_attr_values(attrs))
            return self._with_graph(g)

        if direction is not None:
            if direction not in _DIRECTIONS:
                raise ValueError(f"measurement: direction must be one of {_DIRECTIONS}, got {direction!r}")
            src_id = self._source_id(frm, verb="measurement")
            src_alias = self._src_alias(src_id)

            mid_id = self._next_id()
            g = g.with_node(QueryNode(id=mid_id, alias=f"{src_alias}_{direction}_entity"))
            g = g.with_edge(QueryEdge(source_id=src_id, target_id=mid_id,
                                      hops=max_depth, direction=direction),
                            new_pointer=mid_id)

            cp_filter = str(S223.InletConnectionPoint if direction == "upstream"
                            else S223.OutletConnectionPoint)
            data_id = mid_id + 1
            g = g.with_node(QueryNode(id=data_id, alias=alias or f"{src_alias}_{direction}_data",
                                      constraints={"is_data_node": True}))
            g = g.with_edge(QueryEdge(source_id=mid_id, target_id=data_id, hops=1,
                                      cp_filter=cp_filter),
                            new_pointer=data_id)
            g = g.with_data_node(DataNodeInfo(node_id=data_id))
            if attrs:
                g = self._apply_attrs(g, [data_id], self._resolve_attr_values(attrs))
            return self._with_graph(g)

        if frm is None and not g.nodes:
            # Root form: every measurement point in the plant, no entity
            # anchor (the legacy find_all_data). A standalone data node —
            # still bounded by the external-reference requirement.
            new_id = self._next_id()
            g = g.with_node(QueryNode(id=new_id, alias=alias or "data",
                                      constraints={"is_data_node": True}))
            g = g.with_data_node(DataNodeInfo(node_id=new_id))
            if attrs:
                g = self._apply_attrs(g, [new_id], self._resolve_attr_values(attrs))
            return self._with_graph(g)

        if isinstance(frm, str) and frm.strip().lower() in {"*", "all"}:
            if not g.nodes:
                raise ValueError("measurement(frm='*'): query has no nodes to expand from")
            src_ids = sorted(g.nodes.keys())
        elif isinstance(frm, (list, tuple)):
            src_ids = []
            for f in frm:
                sid = g.aliases.get(f) if isinstance(f, str) else None
                if sid is None:
                    raise ValueError(f"measurement: unknown alias {f!r} in frm list")
                if sid not in src_ids:
                    src_ids.append(sid)
            if not src_ids:
                raise ValueError("measurement: frm list is empty")
        else:
            src_ids = [self._source_id(frm, verb="measurement")]

        created: List[int] = []
        for i, src_id in enumerate(src_ids):
            src_alias = g.aliases_reverse.get(src_id, str(src_id))
            a = alias
            if a is None:
                a = f"{src_alias}_data"
            elif len(src_ids) > 1 and i > 0:
                a = f"{a}_{i}"
            new_id = max(g.nodes, default=-1) + 1
            g = g.with_node(QueryNode(id=new_id, alias=a, constraints={"is_data_node": True}))
            g = g.with_edge(QueryEdge(source_id=src_id, target_id=new_id, hops=1,
                                      cp_union=include_connection_points),
                            new_pointer=new_id)
            g = g.with_data_node(DataNodeInfo(node_id=new_id))
            created.append(new_id)

        if attrs:
            g = self._apply_attrs(g, created, self._resolve_attr_values(attrs))
        return self._with_graph(g)

    def alias(self, name: str) -> "Query":
        """Name the current node (Cypher AS / Gremlin as-step).

        The previous alias keeps working as an alternative handle; display
        uses the latest name::

            aq.query().entity("reverse osmosis membrane").alias("ro")
        """
        g = self.query_graph
        if g.current_pointer is None:
            raise ValueError("alias: no current node (start with entity())")
        node = g.nodes[g.current_pointer]
        return self._with_graph(g.with_node(replace(node, alias=name)))

    def where(self, target: Optional[str] = None, **attrs: Any) -> "Query":
        """Filter a node by registry attributes (see ``explore.attributes.REGISTRY``).

        - ``target``: alias to filter (default: current pointer); ``"*"``
          applies to every measurement node.
        - Values may be URIs, free text (server-resolved), lists (OR), or
          wrapped in :class:`Not` to exclude::

              q.where(quantity_kind="mass flow rate", medium=Not("brine"))
        """
        if not attrs:
            raise ValueError('where: provide at least one attribute filter, e.g. where(medium="water")')
        resolved = self._resolve_attr_values(attrs)
        g = self.query_graph
        if isinstance(target, str) and target.strip().lower() in {"*", "all"}:
            ids = sorted(g.data_nodes)
            if not ids:
                raise ValueError("where: no measurement nodes to filter")
        else:
            nid = g.resolve_alias(target)
            if nid is None:
                raise ValueError(
                    f"where: unknown target alias {target!r}" if target is not None
                    else "where: no current node (start with entity())"
                )
            ids = [nid]
        return self._with_graph(self._apply_attrs(g, ids, resolved))

    def _column_target(self, g: QueryGraph, name: str, of: Optional[str]) -> tuple:
        """Resolve a column spec to ("attr", nid, attr_name) or ("node", nid).

        Bare registry attribute names win over aliases; ``"alias.attr"``
        targets an attribute of a specific node.
        """
        if "." in name:
            alias, _, attr_name = name.rpartition(".")
            if attr_name in REGISTRY and alias in g.aliases:
                return ("attr", g.aliases[alias], attr_name)
        if name in REGISTRY:
            nid = g.resolve_alias(of)
            if nid is None:
                raise ValueError(
                    f"unknown alias {of!r}" if of is not None
                    else "no current node (start with entity())"
                )
            return ("attr", nid, name)
        if name in g.aliases:
            return ("node", g.aliases[name])
        raise ValueError(
            f"unknown column {name!r}: not an attribute ({sorted(REGISTRY)}) "
            f"or a node alias ({sorted(g.aliases)})"
        )

    def _set_dropped(self, g: QueryGraph, nid: int, dropped: bool) -> QueryGraph:
        node = g.nodes[nid]
        constraints = dict(node.constraints)
        if dropped:
            constraints["dropped"] = True
        else:
            constraints.pop("dropped", None)
        ptr = g.current_pointer
        return replace(g.with_node(replace(node, constraints=constraints)),
                       current_pointer=ptr)

    def include(self, *names: str, of: Optional[str] = None,
                required: bool = False) -> "Query":
        """Include columns: attribute values (``alias.attr`` columns) or a
        previously ``drop()``ed node's column (un-drop).

        ``of`` targets a node by alias (default: current pointer); dotted
        ``"alias.attr"`` targets explicitly. Attribute values bind OPTIONALly
        (``None`` where absent) unless ``required=True``::

            q.include("medium", "unit")             # attrs of the current node
            q.include("unit", required=True)        # only rows with a unit
            q.include("ro.process")                 # attr of another node
            q.include("Backwash")                   # un-drop a hidden node
        """
        if not names:
            raise ValueError('include: provide at least one column name, e.g. include("medium")')
        g = self.query_graph
        for name in names:
            try:
                target = self._column_target(g, name, of)
            except ValueError as e:
                raise ValueError(f"include: {e}") from None
            if target[0] == "node":
                g = self._set_dropped(g, target[1], False)
                continue
            _, nid, attr_name = target
            role = "data" if nid in g.data_nodes else "entity"
            if role not in REGISTRY[attr_name].roles:
                alias = g.aliases_reverse.get(nid, str(nid))
                raise ValueError(
                    f"include: attribute {attr_name!r} does not apply to {role} node {alias!r}")
            g = g.with_select(nid, attr_name, required)
        return self._with_graph(g)

    def drop(self, *names: str) -> "Query":
        """Drop columns: a node (kept in the pattern, hidden from the
        output) or a previously ``include()``d attribute (un-include).

        No arguments drops the current node. Since a dropped node's variable
        leaves the SELECT, rows that differed only in it collapse::

            q.drop()                 # current node
            q.drop("Backwash")       # node by alias
            q.drop("unit")           # un-include an attr of the current node
            q.drop("ro.process")     # un-include an attr of another node
        """
        g = self.query_graph
        if not names:
            if g.current_pointer is None:
                raise ValueError("drop: no current node (start with entity())")
            return self._with_graph(self._set_dropped(g, g.current_pointer, True))
        for name in names:
            try:
                target = self._column_target(g, name, None)
            except ValueError as e:
                raise ValueError(f"drop: {e}") from None
            if target[0] == "node":
                g = self._set_dropped(g, target[1], True)
            else:
                _, nid, attr_name = target
                g = replace(g, selects=tuple(
                    entry for entry in g.selects
                    if not (entry[0] == nid and entry[1] == attr_name)))
        return self._with_graph(g)

    def with_columns(self, *specs: str, of: Optional[str] = None,
                     required: bool = False) -> "Query":
        """Unified column control: plain names include, ``"-"``-prefixed drop.

        Each spec is an attribute name, a node alias, or ``"alias.attr"`` —
        exactly what ``include()``/``drop()`` accept::

            q.with_columns("unit", "-Backwash")       # add unit, hide node
            q.with_columns("ro.process", "-m.unit")   # targeted add/remove
            q.with_columns("Backwash")                # un-drop
        """
        if not specs:
            raise ValueError("with_columns: provide at least one column spec")
        q = self
        for spec in specs:
            if spec.startswith("-"):
                q = q.drop(spec[1:])
            else:
                q = q.include(spec, of=of, required=required)
        return q

    def refocus(self, alias: str) -> "Query":
        """Repoint the query at an existing node by alias."""
        nid = self.query_graph.aliases.get(alias)
        if nid is None:
            raise ValueError(f"at: unknown alias {alias!r}")
        return self._with_graph(replace(self.query_graph, current_pointer=nid))

    # ---------- faceted exploration ----------

    def options(self, attr_name: str, *, of: Optional[str] = None,
                use_union: bool = True) -> pl.DataFrame:
        """Distinct values of one attribute across the current matches, with counts.

        Runs immediately (one aggregation query) and returns a polars frame
        ``[<attr_name>, count]`` sorted by count, where count is the number
        of distinct matched nodes carrying that value::

            aq.query().entity("Equipment").measurement(frm="*").options("quantity_kind")
        """
        if attr_name not in REGISTRY:
            raise ValueError(f"unknown attribute {attr_name!r}; known: {sorted(REGISTRY)}")
        g = self.query_graph
        nid = g.resolve_alias(of)
        if nid is None:
            raise ValueError(
                f"options: unknown alias {of!r}" if of is not None
                else "options: no current node (start with entity())"
            )
        attr = REGISTRY[attr_name]
        role = "data" if nid in g.data_nodes else "entity"
        if role not in attr.roles:
            alias = g.aliases_reverse.get(nid, str(nid))
            raise ValueError(f"options: attribute {attr_name!r} does not apply to {role} node {alias!r}")

        cache_key = f"options:{attr_name}:{nid}:{use_union}"
        if self.cache.get(cache_key) is None:
            # Two-phase: the pattern runs once through execute() (program
            # edges BFS-resolved, result cached and shared with metadata());
            # attribute values come from a flat VALUES-anchored lookup and
            # the counting happens here. The store only does index lookups —
            # no server-side GROUP BY over the whole pattern.
            res = self.execute(use_union=use_union)
            cols = res.get("columns", [])
            col = f"v{nid}"
            uris: List[str] = []
            if col in cols:
                idx = cols.index(col)
                seen: set = set()
                for r in res.get("rows", []):
                    val = r[idx]
                    if val is None:
                        continue
                    s = str(val)
                    if s not in seen and _is_uri(s):
                        seen.add(s)
                        uris.append(s)

            pred_path = "|".join(f"<{p}>" for p in attr.predicates)
            counts_by_value: Dict[str, set] = {}
            for i in range(0, len(uris), 500):
                chunk = " ".join(f"<{u}>" for u in uris[i:i + 500])
                sparql = (
                    f"SELECT ?v ?opt\nWHERE {{\n  VALUES ?v {{ {chunk} }}\n"
                    f"  ?v ({pred_path}) ?opt .\n}}"
                )
                lookup = self.client.sparql_query(sparql, use_union=use_union)
                lcols = lookup.get("columns", [])
                vi = lcols.index("v") if "v" in lcols else 0
                oi = lcols.index("opt") if "opt" in lcols else 1
                for r in lookup.get("rows", []):
                    if r[oi] is not None and r[vi] is not None:
                        counts_by_value.setdefault(str(r[oi]), set()).add(str(r[vi]))

            ranked = sorted(counts_by_value.items(), key=lambda kv: (-len(kv[1]), kv[0]))
            self.cache[cache_key] = pl.DataFrame(
                {attr_name: [self._compact_uri_safe(v) for v, _ in ranked],
                 "count": [len(nodes) for _, nodes in ranked]},
                schema={attr_name: pl.String, "count": pl.Int64},
            )
        return self.cache[cache_key]

    def facets(self, *, of: Optional[str] = None, use_union: bool = True) -> "FacetSummary":
        """Value counts for every attribute applicable to a node, with fallback.

        For each applicable registry attribute: values matched by the
        current pattern; if none, model-wide usage; if still none (and the
        attribute has a bounded vocabulary), the ontology's taxonomy. The
        summary prints compactly and indexes like a dict::

            f = q.facets()
            f                    # notebook overview
            f["quantity_kind"]   # full polars frame [value, count]
        """
        from acquirium.Client.explore.facets import FacetSummary, model_options, vocab_options

        g = self.query_graph
        nid = g.resolve_alias(of)
        if nid is None:
            raise ValueError(
                f"facets: unknown alias {of!r}" if of is not None
                else "facets: no current node (start with entity())"
            )
        role = "data" if nid in g.data_nodes else "entity"
        alias = g.aliases_reverse.get(nid, str(nid))
        version = self.client.graph_version()

        summary = FacetSummary(node_alias=alias)
        for name, attr in REGISTRY.items():
            if role not in attr.roles:
                continue
            df = self.options(name, of=alias, use_union=use_union)
            scope = "matched"
            if df.height == 0:
                pairs = model_options(self.client, attr, version)
                scope = "model"
                if not pairs:
                    pairs = vocab_options(self.client, attr, version)
                    scope = "vocabulary"
                df = pl.DataFrame(
                    {name: [self._compact_uri_safe(v) for v, _ in pairs],
                     "count": [c for _, c in pairs]},
                    schema={name: pl.String, "count": pl.Int64},
                )
            summary.frames[name] = df
            summary.scopes[name] = scope
        return summary

    # ---------- terminals ----------

    def to_sparql(self) -> str:
        return compile_sparql(self.query_graph)

    def execute(self, use_union: bool = True) -> dict:
        """Execute the compiled SPARQL against the metadata graph (cached).

        Traversal-program edges (via expressions / ``via="any"``) are
        resolved first by client-side BFS (see ``explore.traverse``) —
        nearest edges keep only the closest matches, others all reachable
        matches. The final result always comes from one SPARQL query with
        the matches injected as paired VALUES.
        """
        if self.cache.get(f"execute_{use_union}") is None:
            g = self.query_graph
            if any(getattr(e, "patterns", None) and e.value_pairs is None for e in g.edges):
                from acquirium.Client.explore.traverse import resolve_program_edges
                g = resolve_program_edges(g, self.client)
            self.cache[f"execute_{use_union}"] = self.client.sparql_query(compile_sparql(g), use_union=use_union)
        return self.cache[f"execute_{use_union}"]

    def to_dict(self) -> dict:
        """Return a JSON-serializable representation of this query graph.

        Used by app registration (:meth:`Acquirium.register_app`) to store
        the query alongside the app spec. ``Not`` markers serialize as
        ``{"not": value}``; via-programs serialize as nested lists.
        """
        def safe(v: Any) -> Any:
            if isinstance(v, Not):
                return {"not": safe(v.value)}
            if isinstance(v, (list, tuple)):
                return [safe(x) for x in v]
            if isinstance(v, dict):
                return {str(k): safe(x) for k, x in v.items()}
            if v is None or isinstance(v, (str, int, float, bool)):
                return v
            return str(v)

        g = self.query_graph
        return {
            "nodes": [
                {
                    "id": n.id,
                    "rdf_class": (n.constraints or {}).get("rdf_class"),
                    "alias": n.alias,
                    "constraints": safe(dict(n.constraints or {})),
                }
                for n in g.nodes.values()
            ],
            "edges": [
                {
                    "source_id": e.source_id,
                    "target_id": e.target_id,
                    "hops": e.hops,
                    "predicates": list(e.predicates) if e.predicates else None,
                    "direction": e.direction,
                    "patterns": safe(e.patterns) if e.patterns else None,
                    "nearest": e.nearest,
                }
                for e in g.edges
            ],
            "aliases": dict(g.aliases),
            "aliases_reverse": dict(g.aliases_reverse),
            "current_pointer": g.current_pointer,
            "selects": safe(g.selects),
            "data_nodes": [
                {
                    "id": nid,
                    "alias": g.aliases_reverse.get(nid, f"v{nid}"),
                    "filters": safe(dict(info.filters or {})),
                }
                for nid, info in g.data_nodes.items()
            ],
        }

    def resolved_nodes(
        self,
        *,
        alias: Optional[str] = None,
        only_data_nodes: bool = False,
        use_union: bool = True,
    ) -> List[str]:
        """URIs the pattern currently matches (all nodes, one alias, or data nodes only)."""
        res = self.execute(use_union=use_union)
        cols = res.get("columns", [])
        rows = res.get("rows", [])

        if alias is not None:
            nid = self.query_graph.resolve_alias(alias)
            if nid is None:
                raise ValueError("resolved_nodes: alias not found")
            target_cols = [f"v{nid}"]
        else:
            node_ids = self.query_graph.data_nodes.keys() if only_data_nodes else self.query_graph.nodes.keys()
            target_cols = [f"v{nid}" for nid in node_ids]

        col_indices = [i for i, c in enumerate(cols) if c in target_cols]
        uris: set = set()
        for row in rows:
            for i in col_indices:
                if i >= len(row):
                    continue
                val = row[i]
                if val is not None and _is_uri(str(val)):
                    uris.add(str(val))
        return sorted(uris)

    def metadata(self, *, include_internals: bool = False, use_union: bool = True) -> pl.DataFrame:
        """Return the pattern matches as a polars table with alias column names.

        The internal SPARQL columns driving ``data()`` (``ext<nid>``,
        ``unit<nid>``, ``extunit<nid>``) are hidden unless
        ``include_internals=True``.
        """
        cache_key = f"metadata_table:{include_internals}:{use_union}"
        if self.cache.get(cache_key) is None:
            res = self.execute(use_union=use_union)
            cols = res.get("columns", [])
            rows = res.get("rows", [])
            keep_idx = list(range(len(cols)))
            if not include_internals:
                keep_idx = [
                    i for i, c in enumerate(cols)
                    if not (isinstance(c, str) and (c.startswith("ext") or c.startswith("unit")))
                ]
            cols_kept = [cols[i] for i in keep_idx]
            rows_kept = [[r[i] for i in keep_idx] for r in rows]
            cols_w_alias = [self._col_name_to_alias(c) for c in cols_kept]

            pl_table = pl.DataFrame(rows_kept, schema=cols_w_alias, orient="row")
            pl_table = pl_table.with_columns([
                pl.col(c)
                .map_elements(
                    lambda x: self._compact_uri_safe(x) if isinstance(x, str)
                    else (None if x is None else str(x)),
                    return_dtype=pl.String,
                    skip_nulls=False,
                )
                .alias(c)
                for c in cols_w_alias
            ]).unique()
            self.cache[cache_key] = pl_table
        return self.cache[cache_key]

    def data(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        use_union: bool = True,
        cast_value: str | None = "float",
        value_mode: str = "default",
    ) -> "DataObject":
        """Return a lazy DataObject with alias-driven access to the matched streams."""
        from acquirium.Client.data_object import DataObject
        return DataObject._from_query(
            self,
            start=start,
            end=end,
            limit=limit,
            order=order,
            use_union=use_union,
            cast_value=cast_value,
            value_mode=value_mode,
        )

    def dataframe(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        use_union: bool = True,
        shape: str = "narrow",
        cast_value: str | None = "str",
        value_mode: str = "default",
        include_ref: bool = False,
    ) -> pl.DataFrame:
        """Fetch the matched streams' values as one polars frame (wide or narrow)."""
        return self.data(
            start=start,
            end=end,
            limit=limit,
            order=order,
            use_union=use_union,
            cast_value=cast_value,
            value_mode=value_mode,
        ).dataframe(shape=shape, include_ref=include_ref, compact=True)

    # ---------- display helpers ----------

    def _col_name_to_alias(self, col_name: str) -> str:
        if col_name.startswith("attr"):
            head, _, attr_name = col_name[4:].partition("_")
            try:
                node_id = int(head)
            except ValueError:
                return col_name
            base_alias = self.query_graph.aliases_reverse.get(node_id, f"v{node_id}")
            return f"{base_alias}.{attr_name}"
        if col_name.startswith("ext"):
            try:
                node_id = int(col_name[3:])
            except ValueError:
                return col_name
            base_alias = self.query_graph.aliases_reverse.get(node_id, col_name)
            return f"{base_alias}_ref"
        if not col_name.startswith("v"):
            return col_name
        try:
            node_id = int(col_name[1:])
        except ValueError:
            return col_name
        return self.query_graph.aliases_reverse.get(node_id, col_name)

    def _compact_uri_safe(self, uri: str) -> str:
        try:
            return self.client.compact_uri(uri)
        except Exception:
            return str(uri)


# Append the attribute registry (single source of truth) to every method
# that accepts attributes, so help(Query.where) etc. always list the current set.
for _fn in (Query.entity, Query.related, Query.measurement, Query.where, Query.include, Query.drop,
            Query.with_columns, Query.options, Query.facets):
    _fn.__doc__ = (_fn.__doc__ or "") + "\n" + attributes_doc(indent=8) + "\n"
del _fn
