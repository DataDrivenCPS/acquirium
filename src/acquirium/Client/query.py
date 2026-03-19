from __future__ import annotations
from rich.console import Console
from rich.table import Table
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional , Union
from acquirium.internals.internals_namespaces import *
from acquirium.internals.models import LogEntry
import polars as pl
from datetime import datetime
from acquirium.TextMatch.decorators import flex_query_rdf_inputs, FlexSpec
from acquirium.Client.query_graph import QueryGraph, QueryNode, QueryEdge, DataNodeInfo
from acquirium.Client.client import AcquiriumClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Query:
    """Query builder for Acquirium.

    This object is immutable: each operation returns a **new** Query with an
    updated internal QueryGraph, so you can safely keep multiple variants:

        q1 = aq.query().find_entity(_class=Valve, alias="valve")
        q2 = aq.query().find_entity(_class=Pump, alias="pump")
        q3 = q1.relate_to(q2)
    """

    client: AcquiriumClient
    query_graph: QueryGraph = field(default_factory=QueryGraph)
    _next_id: int = 0
    cache: Dict[str, Any] = field(default_factory=dict, compare=False)

    # ---------- internal helpers ----------

    def _new_id(self) -> int:
        nid = self._next_id
        # IMPORTANT: must mutate the counter
        object.__setattr__(self, "_next_id", self._next_id + 1)  # because Query is frozen
        return nid
        

    def _with_incremented_id(self) -> "Query":
        return Query(
            client=self.client,
            query_graph=self.query_graph,
            _next_id=self._next_id + 1,
        )

    def _clone_with_graph(self, new_graph: QueryGraph, *, bump_id: bool = False) -> "Query":
        return Query(
            client=self.client,
            query_graph=new_graph,
            _next_id=self._next_id + (1 if bump_id else 0),
        )
    
    def _add_data_node(
    self,
    *,
    g: QueryGraph,
    src_id: int | None,
    path: str | None,
    _class: str | None,
    alias: str | None,
    hops: int,
    filters_dict: Dict[str, Any] | None,
    instance_uri: str | None,
    force_one_hop: bool = False,
    ) -> tuple["QueryGraph", int]:
        new_id = self._new_id()

        constraints = {
            "is_data_node": True,
            "path_from": path,
        }
        if instance_uri is not None:
            constraints["instance_uri"] = instance_uri

        node = QueryNode(
            id=new_id,
            rdf_class=_class or None,
            alias=alias,
            constraints=constraints,
        )
        g2 = g.with_node(node)

        if src_id is not None:
            if path:
                g2 = g2.with_edge(
                    QueryEdge(source_id=src_id, target_id=new_id, hops=1, predicates=[path]),
                    new_pointer=new_id,
                )
            else:
                eff_hops = 1 if force_one_hop else hops
                g2 = g2.with_edge(
                    QueryEdge(source_id=src_id, target_id=new_id, hops=eff_hops, predicates=None),
                    new_pointer=new_id,
                )
        else:
            g2 = QueryGraph(
                nodes=dict(g2.nodes),
                edges=list(g2.edges),
                aliases=dict(g2.aliases),
                aliases_reverse=dict(g2.aliases_reverse),
                current_pointer=new_id,
                data_nodes=dict(getattr(g2, "data_nodes", {})),
            )

        info = DataNodeInfo(
            node_id=new_id,
            filters=dict(filters_dict or {}),
        )
        g2 = g2.with_data_node(info)
        return g2, new_id

    def _select_data_node_ids(self, _from: Optional[str]) -> List[int]:
        g = self.query_graph

        def is_all(x: Optional[str]) -> bool:
            return isinstance(x, str) and x.strip().lower() in {"*", "all"}

        if not g.data_nodes:
            raise ValueError("No data nodes exist in the query graph to filter")

        if is_all(_from):
            return sorted(g.data_nodes.keys())

        if _from is None:
            pid = g.current_pointer
            if pid in g.data_nodes:
                return [pid]
            # fallback: apply to all data nodes if pointer is not a data node
            return sorted(g.data_nodes.keys())

        rid = g.resolve_alias(_from)
        if rid is None:
            raise ValueError("filter: _from alias not found")

        if rid in g.data_nodes:
            return [rid]

        # If _from refers to an entity node, apply to its directly attached data nodes (1 hop)
        attached = []
        for e in g.edges:
            if e.source_id == rid and e.target_id in g.data_nodes:
                attached.append(e.target_id)
        return sorted(set(attached)) if attached else []

    def _resolve_rdf(self, text: str, kind: str) -> str:
        matches = self.client.resolve_text(text, kind=kind, top_k=1, min_score=0.4)
        if not matches:
            raise ValueError(f"Could not resolve '{text}' as {kind}")
        return matches[0]["uri"]
    
    def _query_resolver_adapter(self, text: str, kind: str) -> str:
        return self._resolve_rdf(text, kind)

    def _is_uri(self, text: str) -> bool:
        return isinstance(text, str) and (text.startswith("urn:") or text.startswith("http://") or text.startswith("https://"))

    def _normalize_instance_uri(self, uri: str | URIRef | None, *, param: str = "uri") -> str | None:
        if uri is None:
            return None
        if isinstance(uri, URIRef):
            return str(uri)
        if isinstance(uri, str) and self._is_uri(uri):
            return uri
        raise ValueError(f"{param} must be a URI (urn:..., http://..., or https://...)")

    def _find_all_nodes(self, id_val=None) -> list[URIRef]:
        '''
        Finds all nodes in the query result, except literals.
        If alias is provided, only nodes with that alias are returned.

        '''
        
        target_col = None
        if id_val is not None:
            target_col = f"v{id_val}"
        query_result = self.execute(use_union=True)
        nodes: set[URIRef] = set()
        if target_col:
            col_index = query_result["columns"].index(target_col)
            for row in query_result.get("rows", []):
                cell = row[col_index]
                if isinstance(cell, str) and (cell.startswith("urn:")):
                    nodes.add(URIRef(cell))
            return list(nodes)
        else:
            for row in query_result.get("rows", []):
                for cell in row:
                    if isinstance(cell, str) and (cell.startswith("urn:")):
                        nodes.add(URIRef(cell))
            return nodes

    # ----------------------------------------------------
    # ----------  API ----------
    # ----------------------------------------------------


    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class")])
    def find_entity(self, _class: Optional[str] = None, alias: Optional[str] = None, uri: str | URIRef | None = None) -> "Query":
        """Add a new entity node to the query and set it as the current pointer.

        Example:
            q = aq.query().find_entity(
                _class="urn:nawi-water-ontology#Valve",
                alias="valve",
            )

        You can also target a specific instance by URI:
            q = aq.query().find_entity(
                uri="urn:acquirium:point#MyPump_1",
                alias="pump_1",
            )

        This creates a new QueryNode and makes it the default pointer.
        """
        self.cache.clear()
        instance_uri = self._normalize_instance_uri(uri, param="uri")
        if _class is None and instance_uri is None:
            raise ValueError("find_entity: provide _class, uri, or both")
        node_id = self._new_id()
        constraints = {}
        if instance_uri is not None:
            constraints["instance_uri"] = instance_uri
        node = QueryNode(id=node_id, rdf_class=_class, alias=alias, constraints=constraints)
        new_graph = self.query_graph.with_node(node)
        # bump internal id counter
        return self._clone_with_graph(new_graph, bump_id=True)

    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class"), FlexSpec("predicates", "predicate")])
    def find_related(
        self,
        *,
        _class: Optional[str] = None,
        uri: str | URIRef | None = None,
        alias: Optional[str] = None,
        _from: Optional[str] = None,
        hops: int = 3,
        predicates: Optional[List[str]] = None,
        multi_hop_predicates: bool = False,
        direction: Optional[str] = None,
    ) -> "Query":
        """Add a related entity node, connected from an existing node.

        Semantics:
        - `_from` is an alias of an existing node; if omitted, uses current pointer.
        - Adds a new node of type `_class` with the given alias.
        - Adds an edge from the `_from` node to the new node, with a hop limit.
        - `direction` can be "upstream" or "downstream" to traverse the S223
          connection topology.  When set, overrides `predicates`.

          Each logical hop is a UNION of 4 patterns:

          **Downstream** (per hop, src → tgt):
            1. src connectedTo tgt                                       (direct)
            2. tgt connectedFrom src                                     (direct inverse)
            3. src connectedThrough ?cp . ?cp connectsTo tgt             (via connection)
            4. tgt connectedThrough ?cp . ?cp connectsFrom src           (inverse of upstream CP)

          **Upstream** (per hop, finding tgt upstream of src):
            1. tgt connectedTo src                                       (inverse of direct)
            2. src connectedFrom tgt                                     (direct)
            3. src connectedThrough ?cp . ?cp connectsFrom tgt           (via connection)
            4. tgt connectedThrough ?cp . ?cp connectsTo src             (inverse of downstream CP)

        Example:
            q1 = aq.query().find_entity(_class=Valve, alias="valve")
            q1 = q1.find_related(_class=Pump, alias="related_pump", _from="valve")

        You can also relate to a specific instance:
            q1 = q1.find_related(uri="urn:acquirium:point#Pump_42", alias="pump_42")

        Direction example:
            # At pump, find upstream tank (hops>=2):
            q.find_related(_class=Tank, alias="tank", direction="upstream", hops=3)
            # At pump, find downstream tank2:
            q.find_related(_class=Tank, alias="tank2", direction="downstream", hops=2)
        """
        self.cache.clear()
        instance_uri = self._normalize_instance_uri(uri, param="uri")
        if _class is None and instance_uri is None:
            raise ValueError("find_related: provide _class, uri, or both")
        src_id = self.query_graph.resolve_alias(_from)
        if src_id is None:
            raise ValueError("find_related: no source node to relate from (pointer is None and _from not set)")

        if direction is not None and direction not in ("upstream", "downstream"):
            raise ValueError(f"find_related: direction must be 'upstream', 'downstream', or None, got '{direction}'")

        new_id = self._new_id()
        constraints = {}
        if instance_uri is not None:
            constraints["instance_uri"] = instance_uri
        new_node = QueryNode(id=new_id, rdf_class=_class, alias=alias, constraints=constraints)
        g = self.query_graph.with_node(new_node)

        if direction is not None:
            edge = QueryEdge(source_id=src_id, target_id=new_id, hops=hops, direction=direction)
        elif predicates and multi_hop_predicates:
            edge = QueryEdge(source_id=src_id, target_id=new_id, hops=hops, predicates=predicates)
        elif predicates and not multi_hop_predicates:
            edge = QueryEdge(source_id=src_id, target_id=new_id, hops=1, predicates=predicates)
        else:
            edge = QueryEdge(source_id=src_id, target_id=new_id, hops=hops, predicates=None)

        g2 = g.with_edge(edge, new_pointer=new_id)
        return self._clone_with_graph(g2, bump_id=True)
    
    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class"), FlexSpec("predicates", "predicate")])
    def relate_to(
        self,
        other: "Query",
        _from: Optional[str] = None,
        _to: Optional[str] = None,
        *,
        hops: int = 3,
        predicates: Optional[List[str]] = None,
    ) -> "Query":
        """Relate the current pointer of this query to the current pointer of another query.

        Example:
            q1 = aq.query().find_entity(_class=Valve, alias="valve")
            q2 = aq.query().find_entity(_class=Pump, alias="pump")
            q3 = q1.relate_to(q2)

        Interpretation:
        - `q1` has pointer at 'valve', `q2` has pointer at 'pump'.
        - `q3` will contain the union of both query graphs and an edge between
          valve and pump (default up to 3 hops).
        """
        self.cache.clear()
        other.cache.clear()
        src_id = self.query_graph.current_pointer if _from is None else self.query_graph.resolve_alias(_from)
        if src_id is None:
            raise ValueError("relate_to: current query has no pointer")

        tgt_id = other.query_graph.current_pointer if _to is None else other.query_graph.resolve_alias(_to)
        if tgt_id is None:
            raise ValueError("relate_to: other query has no pointer")

        # Merge node/alias spaces naïvely; in a real system you may want a
        # more sophisticated merge strategy or id remapping
        # For now we assume these queries were created from the same base and
        # have disjoint id spaces or compatible semantics.
        merged_nodes = dict(self.query_graph.nodes)
        merged_edges = list(self.query_graph.edges)
        merged_aliases = dict(self.query_graph.aliases)
        merged_aliases_reverse = dict(self.query_graph.aliases_reverse)

        max_id_self = max(self.query_graph.nodes.keys(), default=-1)
        other_mapping = {}
        # Bring in nodes/aliases from other; if ids collide, this is a TODO
        for nid, node in other.query_graph.nodes.items():
            other_mapping[nid] = max_id_self + 1 + nid
            merged_nodes[other_mapping[nid]] = node
        for edge in other.query_graph.edges:
            mapped_edge = QueryEdge(
                source_id=other_mapping[edge.source_id],
                target_id=other_mapping[edge.target_id],
                hops=edge.hops,
                predicates=edge.predicates,
            )
            merged_edges.append(mapped_edge)
        for alias_name, nid in other.query_graph.aliases.items():
            # if alias exists and points somewhere else, last write wins for now
            merged_aliases[alias_name] = other_mapping[nid]
            merged_aliases_reverse = {v: k for k, v in merged_aliases.items()}

        merged_graph = QueryGraph(
            nodes=merged_nodes,
            edges=merged_edges,
            aliases=merged_aliases,
            aliases_reverse=merged_aliases_reverse,
            current_pointer=src_id,
        )

        # Optionally add a relationship node or just a direct edge.
        edge = QueryEdge(source_id=src_id, target_id=other_mapping[tgt_id], hops=hops, predicates=predicates)
        merged_graph = merged_graph.with_edge(edge, new_pointer=other_mapping[tgt_id])

        return Query(
            client=self.client,
            query_graph=merged_graph,
            _next_id=max(self._next_id, other._next_id)
        )


    @flex_query_rdf_inputs(
        specs=[
            FlexSpec("_class", "class"),
            FlexSpec("quantity_kind", "quantity_kind"),
            FlexSpec("enumeration_kind", "class"),
            FlexSpec("unit", "unit"),
            FlexSpec("data_source", "class"),
            FlexSpec("substance", "class"),
            FlexSpec("medium", "class"),
        ]
    )
    def find_related_data(
        self,
        *,
        _from: Optional[str] = None,
        path: Optional[str] = None,
        _class: Optional[str] = None,
        uri: str | URIRef | None = None,
        quantity_kind: Optional[str] = None,
        enumeration_kind: Optional[str] = None,
        unit: Optional[str] = None,
        data_source: Optional[str] = None,
        substance: Optional[str] = None,
        medium: Optional[str] = None,
        alias: Optional[str] = None,
        hops: int = 1,
        direction: Optional[str] = None,
    ) -> "Query":
        """Find data related to an entity, optionally traversing upstream/downstream.

        When ``direction`` is set ("upstream" or "downstream"), ``hops`` controls
        how many entity-level steps along s223:connectedTo / s223:connectedFrom
        to search through.  At each reachable intermediate entity the query looks
        for data nodes 1 hop away.

        Example (tank --connectedTo--> sth --connectedTo--> pump --connectedTo--> tank2):
            # At pump, find data from up to 3 hops upstream:
            q.find_related_data(direction="upstream", hops=3, quantity_kind=...)
        """
        filters: Dict[str, Any] = {}
        if quantity_kind:
            filters[HAS_QUANTITY_KIND] = quantity_kind
        if enumeration_kind:
            filters[HAS_ENUMERATION_KIND] = enumeration_kind
        if unit:
            filters[HAS_UNIT] = unit
        if data_source:
            filters[DATA_SOURCE] = data_source
        if substance:
            filters[OF_SUBSTANCE] = substance
        if medium:
            filters[HAS_MEDIUM] = medium

        if direction is not None:
            self.cache.clear()
            g = self.query_graph
            instance_uri = self._normalize_instance_uri(uri, param="uri")

            src_id = g.resolve_alias(_from)
            if src_id is None:
                raise ValueError("find_related_data: no source node (set _from or ensure pointer is set)")

            if direction not in ("upstream", "downstream"):
                raise ValueError(f"find_related_data: direction must be 'upstream', 'downstream', or None, got '{direction}'")

            # Create an intermediate entity node (unconstrained class) reachable
            # via 1‥hops directional steps from the source.
            mid_id = self._new_id()
            src_alias = self.query_graph.aliases_reverse.get(src_id, str(src_id))
            mid_alias = f"{src_alias}_{direction}_entity"
            mid_node = QueryNode(id=mid_id, rdf_class=None, alias=mid_alias, constraints={})
            g = g.with_node(mid_node)

            edge = QueryEdge(source_id=src_id, target_id=mid_id, hops=hops, direction=direction)
            g = g.with_edge(edge, new_pointer=mid_id)

            # Attach a data node 1 hop from the intermediate entity.
            src_alias = self.query_graph.aliases_reverse.get(src_id, str(src_id))
            data_alias = alias or f"{src_alias}_{direction}_data"

            g, _ = self._add_data_node(
                g=g,
                src_id=mid_id,
                path=path,
                _class=_class,
                alias=data_alias,
                hops=1,
                filters_dict=filters or None,
                instance_uri=instance_uri,
                force_one_hop=True,
            )

            return Query(
                client=self.client,
                query_graph=g,
                _next_id=self._next_id,
            )

        return self.find_data(
            _from=_from,
            path=path,
            _class=_class,
            uri=uri,
            hops=hops,
            filters_dict=filters or None,
            alias=alias,
        )

    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class")])
    def find_data(
        self,
        *,
        _from: Optional[str] = None,     # None, alias, "*" or "All"
        path: Optional[str] = None,
        _class: Optional[str] = None,
        uri: str | URIRef | None = None,
        hops: int = 1,
        filters_dict: Optional[Dict[str, Any]] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        self.cache.clear()
        g = self.query_graph
        instance_uri = self._normalize_instance_uri(uri, param="uri")

        def is_all(x: Optional[str]) -> bool:
            return isinstance(x, str) and x.strip().lower() in {"*", "all"}

        # Decide sources
        if is_all(_from):
            if not g.nodes:
                raise ValueError("find_data(from='*'): query graph has no nodes to expand from")
            if instance_uri is not None:
                raise ValueError("find_data: uri cannot be used with _from='*'")
            src_ids = sorted(g.nodes.keys())

        else:
            src_id = g.resolve_alias(_from)  # if _from None -> current_pointer
            if src_id is None:
                raise ValueError("find_data: no source node (set _from or ensure pointer is set)")
            src_ids = [src_id]

        last_graph = g
        created = 0
        for i, src_id in enumerate(src_ids):
            src_alias = g.aliases_reverse.get(src_id, str(src_id))
            a = alias
            if a is None:
                a = f"{src_alias}_data"
            elif len(src_ids) > 1:
                a = a if i == 0 else f"{a}_{i}"

            last_graph, _ = self._add_data_node(
                g=last_graph,
                src_id=src_id,
                path=path,
                _class=_class,
                alias=a,
                hops=hops,
                filters_dict=filters_dict,
                instance_uri=instance_uri,
                force_one_hop=True,   # force 1 hop as requested
            )
            created += 1

            # Important: advance ids as we go, since _new_id() reads _next_id
            # We do it by updating "self" logically through _next_id in a local counter:
            self = Query(client=self.client, query_graph=last_graph, _next_id=self._next_id + created)

        return Query(
            client=self.client,
            query_graph=last_graph,
            _next_id=self._next_id + created
        )

    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class")])
    def find_all_data(
        self,
        *,
        _class: Optional[str] = None,
        uri: str | URIRef | None = None,
        hops: int = 1,
        filters_dict: Optional[Dict[str, Any]] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        self.cache.clear()
        g = self.query_graph
        instance_uri = self._normalize_instance_uri(uri, param="uri")

        if not g.nodes:
            g2, _ = self._add_data_node(
                g=g,
                src_id=None,
                path=None,
                _class=_class,
                alias=alias,
                hops=hops,
                filters_dict=filters_dict,
                instance_uri=instance_uri,
                force_one_hop=False,
            )
            return self._clone_with_graph(g2, bump_id=True)

        return self.find_data(_from="*", path=None, _class=_class, uri=uri, hops=hops, filters_dict=filters_dict, alias=alias)

    def dataframe(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        use_union: bool = True,
        shape: str = "narrow",          # "wide" or "narrow"
        cast_value: str | None = "str",  # "float", "int", or None to keep string
    ) -> pl.DataFrame:
        """
        Fetch time series for all bound data nodes in this query result.

        Returns:
        - wide: columns ["time", <data_node_alias_1>, <data_node_alias_2>, ...]
        - narrow: columns ["time", "value", "id"]
        """
        if not getattr(self.query_graph, "data_nodes", None):
            return pl.DataFrame({"time": [], "value": [], "point_id": [], "ref": []})

        res = self.execute(use_union=use_union)
        cols: list[str] = res.get("columns", []) #v0, v1, ext1, ...
        rows: list[list[Any]] = res.get("rows", []) 

        # map "v<ID>" column -> ID
        col_to_id: dict[int, int] = {}
        ext_ref_col_to_id: dict[int, int] = {}
        for i, c in enumerate(cols):
            if isinstance(c, str) and c.startswith("v"):
                try:
                    nid = int(c[1:])
                    col_to_id[i] = nid
                except ValueError:
                    pass
            elif isinstance(c, str) and c.startswith("ext"):
                try:
                    nid = int(c[3:])
                    ext_ref_col_to_id[i] = nid
                except ValueError:
                    pass
        nid_to_ext_ref_col: dict[int, int] = {v: k for k, v in ext_ref_col_to_id.items()}

        data_node_ids = set(self.query_graph.data_nodes.keys())
        data_col_indices = [i for i, nid in col_to_id.items() if nid in data_node_ids]
        ref_col_indices = [i for i, nid in ext_ref_col_to_id.items() if nid in data_node_ids]
        if not data_col_indices or not ref_col_indices:
            return pl.DataFrame({"point_id": [], "ref": [], "time": [], "value": []})

        # gather unique point URIs bound to data nodes
        point_ref_uris: list[tuple[int, str, str]] = []
        seen = set()
        for r in rows:
            for i in data_col_indices:
                nid = col_to_id[i]
                uri = r[i]
                if uri is None:
                    continue
                uri_s = str(uri)
                if nid in nid_to_ext_ref_col:
                    ref_col_idx = nid_to_ext_ref_col[nid]
                    if ref_col_idx >= len(r):
                        continue
                    ref_uri = r[ref_col_idx]
                    if ref_uri is None:
                        continue
                    ref_uri_s = str(ref_uri)
                    key = (nid, uri_s, ref_uri_s)
                    if key not in seen:
                        seen.add(key)
                        point_ref_uris.append((nid, uri_s, ref_uri_s))

        if not point_ref_uris:
            return pl.DataFrame({"point_id": [], "ref": [], "time": [], "value": []})

        # fetch time series for each point URI and build tall frame
        frames: list[pl.DataFrame] = []
        for nid, point_uri, ref_uri in point_ref_uris:

            df = self.client.timeseries_df(
                ref_uri,
                start=start,
                end=end,
                limit=limit,
                order=order,
            )
            if df.is_empty():
                continue

            df = df.rename({"value": "value", "ts": "time","uri": "ref"})
            df = df.with_columns(pl.lit(point_uri).alias("point_id"))
            frames.append(df)

        if not frames:
            return pl.DataFrame({"point_id": [], "ref": [], "time": [], "value": []})

        tall = pl.concat(frames, how="vertical")

        # optional casting
        if cast_value == "float":
            try:
                tall = tall.with_columns(pl.col("value").cast(pl.Float64, strict=True))
            except Exception:
                logging.warning("casting to float failed")
                pass
        elif cast_value == "int":
            try:
                tall = tall.with_columns(pl.col("value").cast(pl.Int64, strict=True))
            except Exception:
                logging.warning("casting to int failed")
                pass
        tall = tall.with_columns(pl.col("point_id").map_elements(lambda x: self._remove_prefixes(x),return_dtype=pl.Utf8).alias("point_id"))
        tall = tall.with_columns(pl.col("ref").map_elements(lambda x: self._remove_prefixes(x),return_dtype=pl.Utf8).alias("ref"))
        # else: keep as string
        if shape == "narrow":
            return tall.select("point_id","ref","time", "value").sort("time")

        # wide
        wide = tall.pivot(values="value", index="time", on=["ref"], aggregate_function="first")
        wide.columns = [self._clean_column_name(c) for c in wide.columns]
        # wide.columns = ["time"] + [self._remove_prefixes(c) for c in wide.columns[1:]]
        return wide.sort("time")

    def _clean_column_name(self, col_name: str) -> str:
        if col_name == "time":
            return col_name
        if isinstance(col_name, str):
            return self._remove_prefixes(col_name)
        if isinstance(col_name, set):
            return list(col_name)[1]
        return str(col_name)
    
    def metadata(self) -> pl.DataFrame:
        """
        Execute the SPARQL query to get the query graph results.
        Returns:
            A polars table.
        """
        if self.cache.get("metadata_table") is None:
            res = self.execute(use_union=True)
            cols_w_alias = [self._col_name_to_alias(c) for c in res.get("columns", [])]
            rows_clean = [[self._remove_prefixes(i) for i in r] for r in res.get("rows", [])]
            pl_table = pl.DataFrame(rows_clean, schema=cols_w_alias, orient="row")
            self.cache["metadata_table"] = pl_table
        return self.cache["metadata_table"]

    def latest_data(
        self,
        *,
        use_union: bool = True,
        limit: int = 1,
        shape: str = "wide",          # "wide" or "narrow"
        cast_value: str | None = "str",  # "float", "int", or None to keep string
    ) -> pl.DataFrame:
        """Fetch latest data point for all bound data nodes in this query result."""
        return self.dataframe(
            start=None,
            end=None,
            limit=limit,
            order="desc",
            use_union=use_union,
            shape=shape,
            cast_value=cast_value,
        )

    def data(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        use_union: bool = True,
        cast_value: str | None = "float",
    ) -> "DataObject":
        """Return a DataObject with alias-driven, structured access to sensor data.

        Example::

            data = query.data(start=..., end=..., cast_value="float")
            data["chlorine"]             # pl.DataFrame [time, value]
            for uri, group in data.by("basin"):
                cl = group["chlorine"]   # scoped to this basin
            df = data.dataframe()        # [time, alias_col_1, alias_col_2, ...]
        """
        from acquirium.Client.data_object import DataObject
        return DataObject._from_query(
            self,
            start=start,
            end=end,
            limit=limit,
            order=order,
            use_union=use_union,
            cast_value=cast_value,
        )


    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class")])
    def filter_data_nodes(
        self,
        *,
        predicate: str ,
        value: Any,
        _from: Optional[str] = None,
    ) -> "Query":
        self.cache.clear()
        g = self.query_graph
        targets = self._select_data_node_ids(_from)
        if not targets:
            raise ValueError("filter_data_nodes: no target data nodes selected")

        dn2 = dict(g.data_nodes)
        for nid in targets:
            info = dn2[nid]
            new_filters = dict(info.filters)
            new_filters[predicate] = value
            dn2[nid] = replace(info, filters=new_filters)

        g2 = QueryGraph(
            nodes=dict(g.nodes),
            edges=list(g.edges),
            aliases=dict(g.aliases),
            aliases_reverse=dict(g.aliases_reverse),
            current_pointer=g.current_pointer,
            data_nodes=dn2,
        )
        return self._clone_with_graph(g2, bump_id=False)

    @flex_query_rdf_inputs(specs=[FlexSpec("unit", "unit")])
    def filter_by_unit(self, unit: str | list, *, _from: Optional[str] = None) -> "Query":
        if isinstance(unit, str):
            unit = [unit]
        return self.filter_data_nodes(predicate=HAS_UNIT, value=unit, _from=_from)

    @flex_query_rdf_inputs(specs=[FlexSpec("medium", "class")])
    def filter_by_medium(self, medium: str | list, *, _from: Optional[str] = None) -> "Query":
        if isinstance(medium, str):
            medium = [medium]
        return self.filter_data_nodes(predicate=HAS_MEDIUM, value=medium, _from=_from)

    @flex_query_rdf_inputs(specs=[FlexSpec("substance", "class")])
    def filter_by_substance(self, substance: str | list, *, _from: Optional[str] = None) -> "Query":
        if isinstance(substance, str):
            substance = [substance]
        return self.filter_data_nodes(predicate=OF_SUBSTANCE, value=substance, _from=_from)

    @flex_query_rdf_inputs(specs=[FlexSpec("qk", "quantity_kind")])
    def filter_by_quantity_kind(self, qk: str | list, *, _from: Optional[str] = None) -> "Query":
        if isinstance(qk, str):
            qk = [qk]
        return self.filter_data_nodes(predicate=HAS_QUANTITY_KIND, value=qk, _from=_from)

    @flex_query_rdf_inputs(specs=[FlexSpec("ek", "class")])
    def filter_by_enumeration_kind(self, ek: str | list, *, _from: Optional[str] = None) -> "Query":
        if isinstance(ek, str):
            ek = [ek]
        return self.filter_data_nodes(predicate=HAS_ENUMERATION_KIND, value=ek, _from=_from)

    #TODO: Not working yet
    # def filter_by_data_source(self, data_source: str, *, _from: Optional[str] = None) -> "Query":
        # return self.filter_data_nodes(predicate=DATA_SOURCE, value=data_source, _from=_from)


    # ------------------ LOGGING -------------------------
    
    
    
    def insert_log(
        self,
        message: str,
        alias: Optional[str] = None,
        observation_start: datetime | None = None,
        observation_end: datetime | None = None,
    ) -> None:
        """Insert a log entry for the specified alias. If not specified, defaults to current pointer alias, if '*', logs to all entities in the query."""
        
        pids = []
        if alias == "*":
            pids = list(self._find_all_nodes())
        elif alias is not None:
            id = self.query_graph.resolve_alias(alias)
            if id is None:
                logger.warning(f"Alias {alias} not found")
                pids = []
            else:
                pids = list(self._find_all_nodes(id))
        else:
            pids = list(self._find_all_nodes(self.query_graph.current_pointer))

        if not pids:
            logger.warning("no target alias specified or found; skipping log insertion")
            return
        for pid in pids:
            
            self.client.insert_log(
                point_uri=pid,
                log_message=message,
                log_time=datetime.now().isoformat(),
                observation_start=observation_start,
                observation_end=observation_end,
            )

    def read_logs(
        self,
        alias: Optional[str] = None,
        log_time_start: Union[datetime, str, None] = None,
        log_time_end: Union[datetime, str, None] = None,
        observation_start: Union[datetime, str, None] = None,
        observation_end: Union[datetime, str, None] = None
    ) -> pl.DataFrame:
        """Read log entries for the specified alias. If not specified, defaults to current pointer alias, if '*', reads logs for all entities in the query."""
        
        pids = []
        if alias == "*":
            pids = list(self._find_all_nodes())
        elif alias is not None:
            id = self.query_graph.resolve_alias(alias)
            if id is None:
                logger.warning(f"Alias {alias} not found")
                pids = []
            else:
                pids = list(self._find_all_nodes(id))
        else:
            pids = list(self._find_all_nodes(self.query_graph.current_pointer))
    
        if not pids:
            logger.warning("No entities found, skipping log querying")
            return pl.DataFrame({"point_uri": [], "message": [], "log_time": [], "observation_start": [], "observation_end": []})

        frames: list[dict] = []
        for pid in pids:
            logs: LogEntry = self.client.query_logs(
                point_uri=pid,
                log_time_start=log_time_start,
                log_time_end=log_time_end,
                observation_start=observation_start,
                observation_end=observation_end
            )
            for log in logs:
                frames.append(log.to_dict())

        if not frames:
            return pl.DataFrame({"point_uri": [], "message": [], "log_time": [], "observation_start": [], "observation_end": []})
        schema = {
            "point_uri": pl.Utf8,
            "message": pl.Utf8,
            "log_time": pl.Datetime(time_zone="UTC"),
            "observation_start": pl.Datetime(time_zone="UTC"),
            "observation_end": pl.Datetime(time_zone="UTC")
        }
        combined = pl.concat([pl.DataFrame(f, schema=schema) for f in frames], how="vertical").select(
            "point_uri", "message", "log_time", "observation_start", "observation_end"
        ).sort("log_time")
        return combined

    def resolved_nodes(
        self,
        *,
        alias: Optional[str] = None,
        only_data_nodes: bool = False,
        use_union: bool = True,
    ) -> list[str]:
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
        if not col_indices:
            return []

        uris: set[str] = set()
        for row in rows:
            for i in col_indices:
                if i >= len(row):
                    continue
                val = row[i]
                if isinstance(val, str) and self._is_uri(val):
                    uris.add(val)
                elif val is not None:
                    val_s = str(val)
                    if self._is_uri(val_s):
                        uris.add(val_s)

        return sorted(uris)



    # ----------------------------------------------------
    # ---------- compilation / execution hooks ----------
    # ----------------------------------------------------

    def to_dict(self) -> dict:
        """Return a JSON serializable representation of this query graph."""
        return {
            "nodes": [
                {
                    "id": n.id,
                    "rdf_class": n.rdf_class,
                    "alias": n.alias,
                    "constraints": dict(n.constraints or {}),
                }
                for n in self.query_graph.nodes.values()
            ],
            "edges": [
                {
                    "source_id": e.source_id,
                    "target_id": e.target_id,
                    "hops": e.hops,
                    "predicates": list(e.predicates) if e.predicates else None,
                    "direction": e.direction,
                }
                for e in self.query_graph.edges
            ],
            "aliases": dict(self.query_graph.aliases),
            "aliases_reverse": dict(self.query_graph.aliases_reverse),
            "current_pointer": self.query_graph.current_pointer,
            "data_nodes": [
                {
                    "id": nid,
                    "alias": self.query_graph.aliases_reverse.get(nid, f"v{nid}"),
                    "filters": dict(info.filters or {}),
                }
                for nid, info in self.query_graph.data_nodes.items()
            ],
        }


    # ----------------------------------------------------
    # --------- SPARQL compilation / execution  ----------
    # ----------------------------------------------------

    from typing import List

    def _direction_edge_pattern(self, src_var: str, tgt_var: str, edge, edge_idx: int) -> str:
        """Build SPARQL property-path pattern for direction-based traversal.

        Each logical hop is expressed as a property-path alternation of 4 routes:

        **Downstream** (per hop, src → tgt):
          1. <connectedTo>                                     (direct)
          2. ^<connectedFrom>                                  (direct inverse)
          3. <connectedThrough>/<connectsTo>                   (via connection)
          4. ^<connectsFrom>/^<connectedThrough>               (inverse of upstream CP)

        **Upstream** (per hop, finding tgt upstream of src):
          1. ^<connectedTo>                                    (inverse of direct)
          2. <connectedFrom>                                   (direct)
          3. <connectedThrough>/<connectsFrom>                 (via connection)
          4. ^<connectsTo>/^<connectedThrough>                 (inverse of downstream CP)

        Multi-hop uses k=1..hops repetitions of the one-hop group, joined with ``|``.
        The entire expression is a single property path — no intermediate variables.
        """
        hops = int(edge.hops)
        direction = edge.direction

        ct  = f"<{S223.connectedTo}>"
        cf  = f"<{S223.connectedFrom}>"
        cth = f"<{CONNECTED_THROUGH}>"
        cst = f"<{CONNECTS_TO}>"
        csf = f"<{CONNECTS_FROM}>"

        if direction == "downstream":
            one_hop = f"({ct}|^{cf}|{cth}/{cst}|^{csf}/^{cth})"
        else:  # upstream
            one_hop = f"(^{ct}|{cf}|{cth}/{csf}|^{cst}/^{cth})"

        if hops == 1:
            path = one_hop
        else:
            parts = ["/".join([one_hop] * k) for k in range(1, hops + 1)]
            path = f"({'|'.join(parts)})"

        return f"{src_var} {path} {tgt_var} ."

    def _edge_pattern(self, src_var: str, tgt_var: str, edge, edge_idx: int) -> str:
        """
        Build a WHERE fragment for one edge.

        Enhancement:
        - Whenever it emits an edge pattern, also emit an alternative where the FIRST hop
        is taken via a connection point.

        Rules:
        - If edge.direction is set: delegate to _direction_edge_pattern for full topology traversal.
        - If edge.predicates is present/non-empty: constrain to those predicates and allow length 1..hops.
        - Else: allow any predicates, but length <= hops, via UNION of k-step chains.
        """
        if getattr(edge, "direction", None) is not None:
            return self._direction_edge_pattern(src_var, tgt_var, edge, edge_idx)

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
                alt = "|".join(self._format_pred(p) for p in uniq)
                path = f"({alt})"
            else:
                parts = []
                for p in uniq:
                    fp = self._format_pred(p)
                    for k in range(1, hops + 1):
                        parts.append("/".join([fp] * k))
                path = f"({'|'.join(parts)})"

            normal = f"{src_var} {path} {tgt_var} ."

            # CP alternative:
            # - For hops==1 we can still keep it as a property path because it's all IRIs:
            #     src <cp>/<p> tgt
            # - For hops>1, rewrite as a UNION over k with explicit triples so CP only affects first hop.
            if hops == 1:
                via_cp = f"{src_var} <{CONNECTION_POINT}>/{path} {tgt_var} ."
                return f"{{ {normal} }} UNION {{ {via_cp} }}"
            else:
                union_blocks: List[str] = []

                for k in range(1, hops + 1):
                    # normal k-hop chain with fixed predicate <p0>
                    triples_normal: List[str] = []
                    prev = src_var
                    mids = [f"?x_e{edge_idx}_{i}_k{k}" for i in range(1, k)]  # k-1
                    p0 = uniq[0]  # placeholder; we'll emit UNION across p below

                    # We'll build a UNION per predicate for this k.
                    pred_blocks = []
                    for p in uniq:
                        fp = self._format_pred(p)
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
                        triples_cp[0] = f"{src_var} <{CONNECTION_POINT}> {cp} . {cp} {fp} {first_obj} ."

                        block_normal = "{ " + " ".join(triples_normal) + " }"
                        block_cp = "{ " + " ".join(triples_cp) + " }"
                        pred_blocks.append(f"{block_normal} UNION {block_cp}")

                    union_blocks.append("{ " + " UNION ".join(pred_blocks) + " }")

                return " UNION ".join(union_blocks)

        # Case B: unconstrained predicates -> UNION of explicit k-step chains
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
            triples_cp.append(f"{cp} {ps[0]} {first_obj} .")
            # remaining hops (if any) unchanged
            if k > 1:
                triples_cp.extend(triples_normal[1:])

            block_normal = "{ " + " ".join(triples_normal) + " }"
            block_cp = "{ " + " ".join(triples_cp) + " }"
            union_blocks.append(f"{block_normal} UNION {block_cp}")

        return " UNION ".join(union_blocks)


    def _format_pred(self, p: str) -> str:
        """Format a predicate for SPARQL. Handles inverse (^) prefix."""
        if p.startswith("^"):
            return f"^<{p[1:]}>"
        return f"<{p}>"

    def _is_iri(self,x: object) -> bool:
        return isinstance(x, str) and ("://" in x or x.startswith("urn:"))
    def _term(self,x: object) -> str:
        """Return SPARQL term for x: <iri> or "literal"."""
        if self._is_iri(x):
            return f"<{x}>"
        # booleans and numbers can be unquoted, but keeping quoted is usually OK.
        # If you want typed literals, adjust here.
        return f"\"{x}\""
    def to_sparql(self) -> str:
        # node id -> ?v{id}
        var_map = {nid: f"?v{nid}" for nid in self.query_graph.nodes}
        ext_vars = {}

        where_clauses: List[str] = []

        # rdf:type constraints and instance constraints
        for nid, node in self.query_graph.nodes.items():
            v = var_map[nid]
            instance_uri = (node.constraints or {}).get("instance_uri")
            if instance_uri is not None:
                where_clauses.append(f"VALUES {v} {{ <{instance_uri}> }}")
            if node.rdf_class:
                where_clauses.append(f"{v} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>/<http://www.w3.org/2000/01/rdf-schema#subClassOf>* <{node.rdf_class}> .")

        # edge constraints
        for edge_idx, edge in enumerate(self.query_graph.edges):
            src_var = var_map[edge.source_id]
            tgt_var = var_map[edge.target_id]
            where_clauses.append(self._edge_pattern(src_var, tgt_var, edge, edge_idx))
        
        
        # data node constraints
        for nid, info in self.query_graph.data_nodes.items():
            v = var_map[nid]
            ext = f"?ext{nid}"
            ext_vars[nid] = ext
            where_clauses.append(f"{v} <{HAS_EXTERNAL_REFERENCE}> {ext} .")

            for pred, val in (info.filters or {}).items():
                if val is None:
                    continue

                # If value looks like a URI, emit <...>, otherwise emit a literal
                if isinstance(val, str) and ("://" in val or val.startswith("urn:")):
                    where_clauses.append(f"{v} <{pred}> <{val}> .")

                elif isinstance(val, list):
                    # If value is a list, emit a UNION of literals
                    union_block = " UNION ".join(f"{{ {v} <{pred}> {self._term(x)} . }}" for x in val if x is not None)
                    where_clauses.append(f"{{ {union_block} }}")

                else:
                    # numbers and booleans become literals too
                    where_clauses.append(f'{v} <{pred}> "{val}" .')



        select_vars = " ".join(var_map.values()) + " " + " ".join(ext_vars.values())
        where_block = "\n  ".join(where_clauses) if where_clauses else ""
        return f"SELECT DISTINCT {select_vars}\nWHERE {{\n  {where_block}\n}}"


    def execute(self,use_union = True) -> dict:
        """Execute this query against the metadata graph.

        Currently, this uses to_sparql() and OxigraphGraphStore.sparql_query().
        Later you can redirect to your VF2-based matcher.
        """
        if self.cache.get("execute") is None:
            sparql = self.to_sparql()
            # print("Executing SPARQL:\n",sparql)
            self.cache["execute"] = self.client.sparql_query(sparql, use_union=use_union)
        return self.cache["execute"]

    # ----------------------------------------------------
    # ---------- visualization / debugging ---------------
    # ----------------------------------------------------

    # -------- internal helpers --------
    def _col_name_to_alias(self, col_name: str) -> Optional[str]:
        """Map a SPARQL result column name back to an alias, if any."""
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

    def _remove_prefixes(self, item: Union[str, Any]) -> str:
        """Remove common URI prefixes for display."""
        try:
            s = str(item)
            s = s.split("#")
            if len(s) == 2:
                return s[1]
            raise ValueError()
        except:
            try:
                s = str(item)
                s = s.split("/")
                return s[-1]
            except:
                return str(item)


    def _show_head_cli(self,columns, rows, n, title=None):
        console = Console()

        table = Table(title=title or f"DataFrame head({n})")

        for col in columns:
            table.add_column(self._col_name_to_alias(col))

        for row in rows[:n]:
            table.add_row(*[self._remove_prefixes(x) for x in row])

        console.print(table)

    def _pretty_print_graph(self) -> None:
        print("QUERY GRAPH")

        print("\nNodes:")
        for nid, node in self.query_graph.nodes.items():
            alias = node.alias or self.query_graph.aliases_reverse.get(nid, str(nid))
            flags = []
            if node.constraints.get("is_data_node"):
                flags.append("DATA")
            cls = node.rdf_class or "*"
            flags_s = f" [{'|'.join(flags)}]" if flags else ""
            inst = node.constraints.get("instance_uri")
            inst_s = f"  instance={inst}" if inst else ""
            print(f"  {nid} [{alias}]{flags_s}  class={cls}{inst_s}")

        print("\nEdges:")
        for e in self.query_graph.edges:
            src = self.query_graph.aliases_reverse.get(e.source_id, str(e.source_id))
            tgt = self.query_graph.aliases_reverse.get(e.target_id, str(e.target_id))
            if e.direction:
                label = f"direction={e.direction}, hops={e.hops}"
            elif e.predicates:
                label = f"{', '.join(e.predicates)}, hops={e.hops}"
            else:
                label = f"*, hops={e.hops}"
            print(f"  {src} --({label})--> {tgt}")

        if self.query_graph.data_nodes:
            print("\nData nodes:")
            for nid, info in self.query_graph.data_nodes.items():
                alias = self.query_graph.aliases_reverse.get(nid, f"v{nid}")
                filt = dict(info.filters or {})
                if not filt:
                    print(f"  {nid} [{alias}]  filters={{}}")
                else:
                    # stable display
                    parts = [f"{k}={v}" for k, v in sorted(filt.items(), key=lambda kv: str(kv[0]))]
                    print(f"  {nid} [{alias}]  filters={{" + ", ".join(parts) + "}}")
        else:
            print("\nData nodes: (none)")

        ptr = self.query_graph.current_pointer
        ptr_alias = self.query_graph.aliases_reverse.get(ptr, None) if ptr is not None else None
        print(f"\nCurrent pointer: {ptr_alias if ptr_alias is not None else ptr}\n")



    # ----------- public visualization API ----------
    def metadata_head(self, limit = 10) -> dict:
        """
        Execute the SPARQL query to get a sample of the query graph results.
        Returns:
            A pandas-like table view (dict with 'columns' and 'rows').
        """
        if self.cache.get("metadata_head") is None:
            self.cache["metadata_head"] = self.execute()
        Query._show_head_cli(
                self,
                columns=self.cache["metadata_head"]["columns"],
                rows=self.cache["metadata_head"]["rows"],
                n=limit,
                title=f"Metadata First {limit} Rows",
            )
        return self.cache["metadata_head"]

    def show_query_graph(self) -> None:
        """Print a human-readable representation of the internal query graph."""
        self._pretty_print_graph()

    def data_head(
        self,
        k: int = 10,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        order: str = "asc",
        use_union: bool = True,
        shape: str = "wide",
    ) -> pl.DataFrame:
        """
        Print and return the head of the time series DataFrame for this query.
        """
        df = self.dataframe(start=start, end=end, order=order, use_union=use_union, shape=shape)
        print(df.head(k))
        return df.head(k)


    ### GRAFANA INTEGRATION

    def _create_prop_dicts(self) -> dict:
        """
        Create a property dictionary for the Grafana panel based on the query graph.

        out:
        {
        'point_uri': 'http://example.com/point1',
        'ref_uri': 'http://example.com/ref1',
        }
        """
        
        #check if there're any data nodes in the query graph
        if not self.query_graph.data_nodes:
            return False
              
        res = self.execute()
        cols: list[str] = res.get("columns", []) #v0, v1, ext1, ...
        rows: list[list[Any]] = res.get("rows", []) 

        # map "v<ID>" column -> ID
        col_to_id: dict[int, int] = {}
        ext_ref_col_to_id: dict[int, int] = {}
        for i, c in enumerate(cols):
            if isinstance(c, str) and c.startswith("v"):
                try:
                    nid = int(c[1:])
                    col_to_id[i] = nid
                except ValueError:
                    pass
            elif isinstance(c, str) and c.startswith("ext"):
                try:
                    nid = int(c[3:])
                    ext_ref_col_to_id[i] = nid
                except ValueError:
                    pass
        nid_to_ext_ref_col: dict[int, int] = {v: k for k, v in ext_ref_col_to_id.items()}

        data_node_ids = set(self.query_graph.data_nodes.keys())
        data_col_indices = [i for i, nid in col_to_id.items() if nid in data_node_ids]
        if not data_col_indices:
            return False
        # gather unique point URIs bound to data nodes
        point_ref_uris: list[tuple[int, str, str]] = []
        seen = set()
        for r in rows:
            for i in data_col_indices:
                nid = col_to_id[i]
                uri = r[i]
                if uri is None:
                    continue
                uri_s = str(uri)
                if nid in nid_to_ext_ref_col:
                    ref_col_idx = nid_to_ext_ref_col[nid]
                    if ref_col_idx >= len(r):
                        continue
                    ref_uri = r[ref_col_idx]
                    if ref_uri is None:
                        continue
                    ref_uri_s = str(ref_uri)
                    key = (nid, uri_s, ref_uri_s)
                    if key not in seen:
                        seen.add(key)
                        point_ref_uris.append((nid, uri_s, ref_uri_s))

        if not point_ref_uris:
            return False

        prop_dicts = []
        for nid, point_uri, ref_uri in point_ref_uris:
            prop_dicts.append({
                "point_uri": point_uri,
                "ref_uri": ref_uri
            })
        return prop_dicts
        
        

        


    def add_grafana_panel(self, panel_title: str = None, type = "Gauge"):
        '''
        Add a new panel to the Grafana dashboard.
        type can be "Gauge" or "TimeSeries"

        title not necessary for gauge, but necessary for timeseries 
        '''

        prop_dicts = self._create_prop_dicts()
        if not prop_dicts:
            logger.warning("No data nodes with external references found in query graph; cannot create Grafana panel")
            return
        

        if type == "Gauge":
            for prop_dict in prop_dicts:
                self.client.add_gauge_panel(prop_dict)
        elif type == "TimeSeries":
            if not panel_title:
                raise ValueError("Panel title is required for TimeSeries panels")
            self.client.add_time_series_panel(panel_title, prop_dicts)
        else:
            raise ValueError(f"Unsupported panel type: {type}")
