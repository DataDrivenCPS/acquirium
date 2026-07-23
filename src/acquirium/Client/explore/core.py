"""Core immutable query builder for the explore layer.

``Q`` is the clean replacement for the legacy ``Query`` builder: short verbs
(``entity`` / ``related`` / ``measurement`` / ``where`` / ``include`` /
``refocus``) build an immutable :class:`QueryGraph`, and terminals
(``metadata`` / ``data`` / ``dataframe`` / ``execute`` / ``to_sparql``) run
it. Compilation is delegated to the pure
:func:`~acquirium.Client.explore.compile.compile_sparql`.

Every verb returns a **new** ``Q`` with a fresh result cache, so variants can
be kept side by side::

    ro = aq.explore().entity("reverse osmosis membrane", alias="ro")
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

from acquirium.Client.explore.attributes import REGISTRY, Not, normalize_value
from acquirium.Client.explore.compile import compile_sparql
from acquirium.Client.query_graph import DataNodeInfo, QueryEdge, QueryGraph, QueryNode
from acquirium.internals.internals_namespaces import S223

if TYPE_CHECKING:
    from acquirium.Client.client import AcquiriumClient
    from acquirium.Client.data_object import DataObject

_DIRECTIONS = ("upstream", "downstream")


def _is_uri(text: Any) -> bool:
    return isinstance(text, str) and (
        text.startswith("urn:") or text.startswith("http://") or text.startswith("https://")
    )


@dataclass(frozen=True)
class Q:
    """Immutable explore query bound to an Acquirium client."""

    client: "AcquiriumClient | None"
    query_graph: QueryGraph = field(default_factory=QueryGraph)
    cache: Dict[str, Any] = field(default_factory=dict, compare=False)

    # ---------- internal helpers ----------

    def _next_id(self) -> int:
        return max(self.query_graph.nodes, default=-1) + 1

    def _with_graph(self, g: QueryGraph) -> "Q":
        return Q(client=self.client, query_graph=g)

    def _as_uri(self, value: str | URIRef, kind: str) -> str:
        """Coerce a class/predicate input to a URI: passthrough or text-resolve."""
        if isinstance(value, URIRef) or _is_uri(value):
            return str(value)
        uri = self.client.resolve_concept(str(value), kind=kind, min_score=0.4)
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

    def _resolve_attr_values(self, attrs: Dict[str, Any]) -> Dict[str, Any]:
        """Validate attr names and resolve text values to URIs in one joint call.

        URIs/URIRefs pass through; literal attrs keep their raw value; the
        remaining text values are resolved together via
        ``client.resolve_record_uris`` so siblings disambiguate each other.
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
        resolved_uris = self.client.resolve_record_uris(record, min_score=0.4) if record else {}

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
               alias: Optional[str] = None, **attrs: Any) -> "Q":
        """Add a new entity node to the pattern and point at it.

        ``cls`` is a class URI or free text (resolved via the server);
        ``uri`` pins a specific instance. Extra keyword arguments are
        attribute filters applied to the new node (same as ``where()``)::

            aq.explore().entity("Equipment", process="ozonation")
        """
        instance_uri = self._normalize_instance_uri(uri)
        if cls is None and instance_uri is None and not attrs:
            raise ValueError("entity: provide cls, uri, or attribute filters")
        constraints: Dict[str, Any] = {}
        if cls is not None:
            constraints["rdf_class"] = self._as_uri(cls, "class")
        if instance_uri is not None:
            constraints["instance_uri"] = instance_uri
        node = QueryNode(id=self._next_id(), alias=alias, constraints=constraints)
        q2 = self._with_graph(self.query_graph.with_node(node))
        if attrs:
            resolved = q2._resolve_attr_values(attrs)
            q2 = q2._with_graph(q2._apply_attrs(q2.query_graph, [node.id], resolved))
        return q2

    def related(self, cls: str | URIRef | None = None, *, uri: str | URIRef | None = None,
                alias: Optional[str] = None, frm: Optional[str] = None,
                via: Any = "any", direction: Optional[str] = None,
                max_depth: Optional[int] = None, **attrs: Any) -> "Q":
        """Add an entity related to an existing node and point at it.

        - ``frm``: alias of the source node (default: current pointer).
        - ``via``: ``"any"`` (any predicates) or a list of predicate URIs /
          free-text names (``"^..."`` prefix inverts). Named traversal
          profiles land in a later step.
        - ``direction``: ``"upstream"``/``"downstream"`` topology traversal
          (mutually exclusive with a ``via`` predicate list).
        - ``max_depth``: traversal bound; defaults to 1 for a predicate list
          and 3 for ``"any"``/directional traversal.
        """
        instance_uri = self._normalize_instance_uri(uri)
        if cls is None and instance_uri is None and not attrs:
            raise ValueError("related: provide cls, uri, or attribute filters")
        src_id = self._source_id(frm, verb="related")

        preds: Optional[List[str]] = None
        if isinstance(via, (list, tuple)):
            preds = [
                f"^{self._as_uri(str(p)[1:], 'predicate')}" if str(p).startswith("^")
                else self._as_uri(p, "predicate")
                for p in via
            ]
        elif via != "any":
            raise ValueError(
                f"related: unknown via {via!r} (pass 'any' or a list of predicates; "
                f"named traversal profiles are not available yet)"
            )

        if direction is not None:
            if direction not in _DIRECTIONS:
                raise ValueError(f"related: direction must be one of {_DIRECTIONS}, got {direction!r}")
            if preds is not None:
                raise ValueError("related: pass either direction or a via predicate list, not both")

        hops = max_depth if max_depth is not None else (1 if preds else 3)

        constraints: Dict[str, Any] = {}
        if cls is not None:
            constraints["rdf_class"] = self._as_uri(cls, "class")
        if instance_uri is not None:
            constraints["instance_uri"] = instance_uri
        new_id = self._next_id()
        g = self.query_graph.with_node(QueryNode(id=new_id, alias=alias, constraints=constraints))
        edge = QueryEdge(source_id=src_id, target_id=new_id, hops=hops,
                         predicates=preds, direction=direction)
        q2 = self._with_graph(g.with_edge(edge, new_pointer=new_id))
        if attrs:
            resolved = q2._resolve_attr_values(attrs)
            q2 = q2._with_graph(q2._apply_attrs(q2.query_graph, [new_id], resolved))
        return q2

    def measurement(self, *, frm: Optional[str] = None, alias: Optional[str] = None,
                    direction: Optional[str] = None, max_depth: int = 3,
                    **attrs: Any) -> "Q":
        """Attach a measurement point (data node) to the pattern and point at it.

        Matches nodes carrying an external reference one hop from the source.
        ``frm`` accepts an alias, ``None`` (current pointer), or ``"*"`` to
        attach one measurement node to every entity in the pattern.

        With ``direction`` set, first traverses up to ``max_depth`` topology
        hops upstream/downstream through an intermediate entity, then looks
        for measurements one hop away (inlet connection points for upstream,
        outlet for downstream).

        Extra keyword arguments are attribute filters applied to the new
        measurement node(s), same as ``where()``::

            q.measurement(quantity_kind="mass flow rate", medium=Not("brine"))
        """
        g = self.query_graph

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

        if isinstance(frm, str) and frm.strip().lower() in {"*", "all"}:
            if not g.nodes:
                raise ValueError("measurement(frm='*'): query has no nodes to expand from")
            src_ids = sorted(g.nodes.keys())
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
            g = g.with_edge(QueryEdge(source_id=src_id, target_id=new_id, hops=1),
                            new_pointer=new_id)
            g = g.with_data_node(DataNodeInfo(node_id=new_id))
            created.append(new_id)

        if attrs:
            g = self._apply_attrs(g, created, self._resolve_attr_values(attrs))
        return self._with_graph(g)

    def where(self, target: Optional[str] = None, **attrs: Any) -> "Q":
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

    def include(self, *attr_names: str, of: Optional[str] = None) -> "Q":
        """Include attribute values as extra metadata columns named ``alias.attr``.

        Additive: the regular node columns stay; each named attribute adds a
        column. ``of`` targets a node by alias (default: current pointer).
        Values bind OPTIONALly, so rows without the attribute are kept::

            q.include("medium", "unit")          # of the current node
            q.include("process", of="ro")        # of another node
        """
        if not attr_names:
            raise ValueError('include: provide at least one attribute name, e.g. include("medium")')
        g = self.query_graph
        nid = g.resolve_alias(of)
        if nid is None:
            raise ValueError(
                f"include: unknown alias {of!r}" if of is not None
                else "include: no current node (start with entity())"
            )
        role = "data" if nid in g.data_nodes else "entity"
        for name in attr_names:
            if name not in REGISTRY:
                raise ValueError(f"unknown attribute {name!r}; known: {sorted(REGISTRY)}")
            if role not in REGISTRY[name].roles:
                alias = g.aliases_reverse.get(nid, str(nid))
                raise ValueError(f"include: attribute {name!r} does not apply to {role} node {alias!r}")
        for name in attr_names:
            g = g.with_select(nid, name)
        return self._with_graph(g)

    def refocus(self, alias: str) -> "Q":
        """Repoint the query at an existing node by alias."""
        nid = self.query_graph.aliases.get(alias)
        if nid is None:
            raise ValueError(f"at: unknown alias {alias!r}")
        return self._with_graph(replace(self.query_graph, current_pointer=nid))

    # ---------- terminals ----------

    def to_sparql(self) -> str:
        return compile_sparql(self.query_graph)

    def execute(self, use_union: bool = True) -> dict:
        """Execute the compiled SPARQL against the metadata graph (cached)."""
        if self.cache.get("execute") is None:
            self.cache["execute"] = self.client.sparql_query(self.to_sparql(), use_union=use_union)
        return self.cache["execute"]

    def metadata(self, *, include_internals: bool = False, use_union: bool = True) -> pl.DataFrame:
        """Return the pattern matches as a polars table with alias column names.

        The internal SPARQL columns driving ``data()`` (``ext<nid>``,
        ``unit<nid>``, ``extunit<nid>``) are hidden unless
        ``include_internals=True``.
        """
        cache_key = f"metadata_table:{include_internals}"
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
                    lambda x: self._compact_uri_safe(x) if isinstance(x, str) else str(x),
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
