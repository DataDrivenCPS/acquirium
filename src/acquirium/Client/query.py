from __future__ import annotations
from rich.console import Console
from rich.table import Table
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional , Union
from acquirium.internals.internals_namespaces import *
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
        self._next_id + 1
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
    force_one_hop: bool = False,
    ) -> tuple["QueryGraph", int]:
        new_id = self._new_id()

        node = QueryNode(
            id=new_id,
            rdf_class=_class or None,
            alias=alias,
            constraints={
                "is_data_node": True,
                "path_from": path,
            },
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
        svc = self.client.textmatch_service

        from acquirium.TextMatch.service import make_resolver_from_service
        resolver = make_resolver_from_service(svc)
        return resolver(text, kind)
    
    def _query_resolver_adapter(self, text: str, kind: str) -> str:
        return self._resolve_rdf(self,text, kind)

    # ----------------------------------------------------
    # ----------  API ----------
    # ----------------------------------------------------


    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class")])
    def find_entity(self, _class: str, alias: Optional[str] = None) -> "Query":
        """Add a new entity node to the query and set it as the current pointer.

        Example:
            q = aq.query().find_entity(
                _class="urn:nawi-water-ontology#Valve",
                alias="valve",
            )

        This creates a new QueryNode and makes it the default pointer.
        """
        self.cache.clear()
        node_id = self._new_id()
        node = QueryNode(id=node_id, rdf_class=_class, alias=alias)
        new_graph = self.query_graph.with_node(node)
        # bump internal id counter
        return self._clone_with_graph(new_graph, bump_id=True)

    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class"), FlexSpec("predicates", "predicate")])
    def find_related(
        self,
        *,
        _class: str,
        alias: Optional[str] = None,
        _from: Optional[str] = None,
        hops: int = 3,
        predicates: Optional[List[str]] = None,
        multi_hop_predicates: bool = False,
    ) -> "Query":
        """Add a related entity node, connected from an existing node.

        Semantics:
        - `_from` is an alias of an existing node; if omitted, uses current pointer.
        - Adds a new node of type `_class` with the given alias.
        - Adds an edge from the `_from` node to the new node, with a hop limit.

        Example:
            q1 = aq.query().find_entity(_class=Valve, alias="valve")
            q1 = q1.find_related(_class=Pump, alias="related_pump", _from="valve")

        If `_from` is omitted, this is equivalent because the pointer is 'valve'.
        """
        self.cache.clear()
        src_id = self.query_graph.resolve_alias(_from)
        if src_id is None:
            raise ValueError("find_related: no source node to relate from (pointer is None and _from not set)")

        new_id = self._new_id()
        new_node = QueryNode(id=new_id, rdf_class=_class, alias=alias)
        g = self.query_graph.with_node(new_node)
        if predicates and multi_hop_predicates:
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


    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class")])
    def find_data(
        self,
        *,
        _from: Optional[str] = None,     # None, alias, "*" or "All"
        path: Optional[str] = None,
        _class: Optional[str] = None,
        hops: int = 3,
        filters_dict: Optional[Dict[str, Any]] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        self.cache.clear()
        g = self.query_graph

        def is_all(x: Optional[str]) -> bool:
            return isinstance(x, str) and x.strip().lower() in {"*", "all"}

        # Decide sources
        if is_all(_from):
            if not g.nodes:
                raise ValueError("find_data(from='*'): query graph has no nodes to expand from")
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
        hops: int = 3,
        filters_dict: Optional[Dict[str, Any]] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        self.cache.clear()
        g = self.query_graph

        if not g.nodes:
            g2, _ = self._add_data_node(
                g=g,
                src_id=None,
                path=None,
                _class=_class,
                alias=alias,
                hops=hops,
                filters_dict=filters_dict,
                force_one_hop=False,
            )
            return self._clone_with_graph(g2, bump_id=True)

        return self.find_data(_from="*", path=None, _class=_class, hops=hops, filters_dict=filters_dict, alias=alias)

    def dataframe(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        use_union: bool = True,
        shape: str = "wide",          # "wide" or "narrow"
        cast_value: str | None = "str",  # "float", "int", or None to keep string
    ) -> pl.DataFrame:
        """
        Fetch time series for all bound data nodes in this query result.

        Returns:
        - wide: columns ["time", <data_node_alias_1>, <data_node_alias_2>, ...]
        - narrow: columns ["time", "value", "id"]
        """
        if not getattr(self.query_graph, "data_nodes", None):
            return pl.DataFrame({"time": [], "value": [], "id": []})

        res = self.execute(use_union=use_union)
        cols: list[str] = res.get("columns", [])
        rows: list[list[Any]] = res.get("rows", [])

        # map "v<ID>" column -> ID
        col_to_id: dict[int, int] = {}
        for i, c in enumerate(cols):
            if isinstance(c, str) and c.startswith("v"):
                try:
                    nid = int(c[1:])
                    col_to_id[i] = nid
                except ValueError:
                    pass

        data_node_ids = set(self.query_graph.data_nodes.keys())
        data_col_indices = [i for i, nid in col_to_id.items() if nid in data_node_ids]
        if not data_col_indices:
            return pl.DataFrame({"time": [], "value": [], "id": []})

        # gather unique point URIs bound to data nodes
        point_uris: list[tuple[int, str]] = []
        seen = set()
        for r in rows:
            for i in data_col_indices:
                nid = col_to_id[i]
                uri = r[i]
                if uri is None:
                    continue
                uri_s = str(uri)
                key = (nid, uri_s)
                if key not in seen:
                    seen.add(key)
                    point_uris.append((nid, uri_s))

        if not point_uris:
            return pl.DataFrame({"time": [], "value": [], "id": []})

        # fetch time series for each point URI and build tall frame
        frames: list[pl.DataFrame] = []
        for nid, point_uri in point_uris:
            alias = self.query_graph.aliases_reverse.get(nid, f"v{nid}")

            df = self.client.timeseries_df(
                point_uri,
                start=start,
                end=end,
                limit=limit,
                order=order,
            )
            if df.is_empty():
                continue

            df = df.rename({"value": "value", "ts": "time","point_uri": "id"})
            frames.append(df)

        if not frames:
            return pl.DataFrame({"time": []})

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
        tall = tall.with_columns(pl.col("id").map_elements(lambda x: self._remove_prefixes(x),return_dtype=pl.Utf8).alias("id"))
        # else: keep as string
        if shape == "narrow":
            return tall.select("time", "value", "id").sort("time")

        # wide
        wide = tall.pivot(values="value", index="time", on="id", aggregate_function="first")
        # wide.columns = ["time"] + [self._remove_prefixes(c) for c in wide.columns[1:]]
        return wide.sort("time")

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


    @flex_query_rdf_inputs(specs=[FlexSpec("_class", "class")])
    def filter_data_nodes(
        self,
        *,
        predicate: str,
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

    @flex_query_rdf_inputs(specs=[FlexSpec("unit", "class")])
    def filter_by_unit(self, unit: str, *, _from: Optional[str] = None) -> "Query":
        return self.filter_data_nodes(predicate=HAS_UNIT, value=unit, _from=_from)

    @flex_query_rdf_inputs(specs=[FlexSpec("medium", "class")])
    def filter_by_medium(self, medium: str, *, _from: Optional[str] = None) -> "Query":
        return self.filter_data_nodes(predicate=HAS_MEDIUM, value=medium, _from=_from)

    @flex_query_rdf_inputs(specs=[FlexSpec("substance", "class")])
    def filter_by_substance(self, substance: str, *, _from: Optional[str] = None) -> "Query":
        return self.filter_data_nodes(predicate=OF_SUBSTANCE, value=substance, _from=_from)

    @flex_query_rdf_inputs(specs=[FlexSpec("qk", "class")])
    def filter_by_quantity_kind(self, qk: str, *, _from: Optional[str] = None) -> "Query":
        return self.filter_data_nodes(predicate=HAS_QUANTITY_KIND, value=qk, _from=_from)

    @flex_query_rdf_inputs(specs=[FlexSpec("ek", "class")])
    def filter_by_enumeration_kind(self, ek: str, *, _from: Optional[str] = None) -> "Query":
        return self.filter_data_nodes(predicate=HAS_ENUMERATION_KIND, value=ek, _from=_from)

    #TODO: Not working yet
    # def filter_by_data_source(self, data_source: str, *, _from: Optional[str] = None) -> "Query":
        # return self.filter_data_nodes(predicate=DATA_SOURCE, value=data_source, _from=_from)


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

    def _edge_pattern(self,src_var: str, tgt_var: str, edge, edge_idx: int) -> str:
        """
        Build a WHERE fragment for one edge.

        Rules:
        - If edge.predicates is present/non-empty: constrain to those predicates (union) and allow length 1..hops.
        - Else: allow any predicates, but length <= hops, via UNION of k-step chains.
        """
        hops = int(edge.hops)
        if hops < 1:
            raise ValueError(f"edge.hops must be >= 1, got {edge.hops}")

        preds = getattr(edge, "predicates", None) or []
        preds = [p for p in preds if p]  # remove falsy

        # Case A: constrained predicate set -> property path with alternation + length range
        if preds:
            seen = set()
            uniq = []
            for p in preds:
                if p not in seen:
                    seen.add(p)
                    uniq.append(p)

            

            if hops == 1:
                alt = "|".join(f"<{p}>" for p in uniq)
                path = f"({alt})"
            else:
                alt = ""
                for p in uniq:
                    for k in range(1,hops+1):
                        add = [f"<{p}>"] * k
                        alt += "/".join(add)
                        if k < hops:
                            alt += "|"
                path = f"({alt})"

            return f"{src_var} {path} {tgt_var} ."

        # Case B: unconstrained predicates -> UNION of explicit k-step chains
        union_blocks: List[str] = []
        for k in range(1, hops + 1):
            triples: List[str] = []
            prev = src_var

            # intermediate node vars for this edge/length
            mids = [f"?x_e{edge_idx}_{i}" for i in range(1, k)]  # k-1 intermediates
            # predicate vars for this edge/length
            ps = [f"?p_e{edge_idx}_{i}" for i in range(1, k + 1)]

            for step in range(k):
                pvar = ps[step]
                obj = tgt_var if step == k - 1 else mids[step]
                triples.append(f"{prev} {pvar} {obj} .")
                prev = obj

            union_blocks.append("{ " + " ".join(triples) + " }")

        return " UNION ".join(union_blocks)


    def to_sparql(self) -> str:
        # node id -> ?v{id}
        var_map = {nid: f"?v{nid}" for nid in self.query_graph.nodes}

        where_clauses: List[str] = []

        # rdf:type constraints
        for nid, node in self.query_graph.nodes.items():
            v = var_map[nid]
            if node.rdf_class:
                where_clauses.append(f"{v} a <{node.rdf_class}> .")

        # edge constraints
        for edge_idx, edge in enumerate(self.query_graph.edges):
            src_var = var_map[edge.source_id]
            tgt_var = var_map[edge.target_id]
            where_clauses.append(self._edge_pattern(src_var, tgt_var, edge, edge_idx))

        # data node constraints
        for nid, info in self.query_graph.data_nodes.items():
            v = var_map[nid]
            ext = f"?ext{nid}"
            where_clauses.append(f"{v} <{HAS_EXTERNAL_REFERENCE}> {ext} .")

            for pred, val in (info.filters or {}).items():
                if val is None:
                    continue

                # If value looks like a URI, emit <...>, otherwise emit a literal
                if isinstance(val, str) and ("://" in val or val.startswith("urn:")):
                    where_clauses.append(f"{v} <{pred}> <{val}> .")
                else:
                    # numbers and booleans become literals too
                    where_clauses.append(f'{v} <{pred}> "{val}" .')



        select_vars = " ".join(var_map.values())
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
            print(f"  {nid} [{alias}]{flags_s}  class={cls}")

        print("\nEdges:")
        for e in self.query_graph.edges:
            src = self.query_graph.aliases_reverse.get(e.source_id, str(e.source_id))
            tgt = self.query_graph.aliases_reverse.get(e.target_id, str(e.target_id))
            preds = ", ".join(e.predicates) if e.predicates else "*"
            print(f"  {src} --({preds}, hops={e.hops})--> {tgt}")

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
