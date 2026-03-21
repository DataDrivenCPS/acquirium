from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator, TYPE_CHECKING
import polars as pl
import logging

if TYPE_CHECKING:
    from acquirium.Client.query import Query
    from acquirium.Client.query_graph import QueryGraph
    from acquirium.Client.client import AcquiriumClient

logger = logging.getLogger(__name__)


def _parse_sparql_bindings(
    query: Query,
) -> tuple[
    list[tuple[int, str, str]],          # (nid, point_uri, ref_uri) — unique data bindings
    dict[tuple[int, str, str], list[dict[str, str]]],  # data_key -> list of entity context dicts
]:
    """Parse SPARQL result columns to extract data-node bindings and entity context.

    Returns:
        point_ref_uris: deduplicated list of (node_id, point_uri, ref_uri)
        entity_context: mapping from each data key to a list of dicts
            like {"entity__basin": "urn:...", "entity__process": "urn:..."} —
            one dict per SPARQL row that matched this data key.
    """
    res = query.execute(use_union=True)
    cols: list[str] = res.get("columns", [])
    rows: list[list[Any]] = res.get("rows", [])

    qg = query.query_graph
    data_node_ids = set(qg.data_nodes.keys())

    # Map column index -> node id for v<N> and ext<N> columns
    col_to_id: dict[int, int] = {}
    ext_ref_col_to_id: dict[int, int] = {}
    for i, c in enumerate(cols):
        if isinstance(c, str) and c.startswith("v"):
            try:
                col_to_id[i] = int(c[1:])
            except ValueError:
                pass
        elif isinstance(c, str) and c.startswith("ext"):
            try:
                ext_ref_col_to_id[i] = int(c[3:])
            except ValueError:
                pass

    nid_to_ext_ref_col: dict[int, int] = {v: k for k, v in ext_ref_col_to_id.items()}

    data_col_indices = [i for i, nid in col_to_id.items() if nid in data_node_ids]
    ref_col_indices = [i for i, nid in ext_ref_col_to_id.items() if nid in data_node_ids]

    # Identify entity columns (non-data v<N> columns)
    entity_col_indices: list[tuple[int, int]] = []  # (col_index, node_id)
    for i, nid in col_to_id.items():
        if nid not in data_node_ids:
            entity_col_indices.append((i, nid))

    # Collect unique data bindings and their entity contexts
    point_ref_uris: list[tuple[int, str, str]] = []
    seen: set[tuple[int, str, str]] = set()
    entity_context: dict[tuple[int, str, str], list[dict[str, str]]] = {}

    if not data_col_indices or not ref_col_indices:
        return [], {}

    for r in rows:
        # Build entity context for this row
        row_entities: dict[str, str] = {}
        for col_idx, nid in entity_col_indices:
            if col_idx < len(r) and r[col_idx] is not None:
                alias = qg.aliases_reverse.get(nid, str(nid))
                row_entities[f"entity__{alias}"] = str(r[col_idx])

        for i in data_col_indices:
            nid = col_to_id[i]
            uri = r[i]
            if uri is None:
                continue
            uri_s = str(uri)
            if nid not in nid_to_ext_ref_col:
                continue
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
                point_ref_uris.append(key)
            entity_context.setdefault(key, []).append(row_entities)

    return point_ref_uris, entity_context


def _deduplicate_contexts(contexts: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return unique context dicts, preserving order."""
    seen: set[tuple[tuple[str, str], ...]] = set()
    result: list[dict[str, str]] = []
    for ctx in contexts:
        key = tuple(sorted(ctx.items()))
        if key not in seen:
            seen.add(key)
            result.append(ctx)
    return result


@dataclass(frozen=True)
class BindingInfo:
    """Lightweight metadata for a single (nid, point_uri, ref_uri) data binding."""
    nid: int
    point_uri: str
    ref_uri: str
    alias: str
    entity_contexts: list[dict[str, str]]
    row_count: int = 0
    earliest: datetime | None = None
    latest: datetime | None = None


@dataclass
class DataObject:
    """Lazy, alias-driven structured access to sensor data.

    On construction only SPARQL metadata and per-series stats (row count,
    time range) are fetched.  The actual time-series data is materialized
    on demand when ``__getitem__``, ``dataframe()``, ``iter()``, or
    ``latest()`` are called.

    Lightweight operations such as ``aliases``, ``metadata()``,
    ``is_empty()``, ``total_rows``, ``time_range``, and ``by()`` all work
    without triggering materialization.
    """

    _bindings: list[BindingInfo]
    _entity_columns: list[str]
    _query_graph: QueryGraph

    # Deferred fetch state
    _client: AcquiriumClient | None = field(default=None, repr=False)
    _query_params: dict = field(default_factory=dict, repr=False)

    # Materialized state
    _tall: pl.DataFrame | None = field(default=None, repr=False)
    _materialized: bool = field(default=False, repr=False)
    _metadata_df: pl.DataFrame | None = field(default=None, repr=False)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def _empty(cls, qg: QueryGraph, *, cast_value: str | None = "float") -> DataObject:
        return cls(
            _bindings=[],
            _entity_columns=[],
            _query_graph=qg,
            _tall=pl.DataFrame(
                schema={
                    "data_alias": pl.Utf8,
                    "point_uri": pl.Utf8,
                    "ref_uri": pl.Utf8,
                    "time": pl.Datetime(time_zone="UTC"),
                    "value": pl.Float64 if cast_value == "float" else pl.Utf8,
                }
            ),
            _materialized=True,
        )

    @classmethod
    def _from_query(
        cls,
        query: Query,
        *,
        start=None,
        end=None,
        limit: int | None = None,
        order: str = "asc",
        use_union: bool = True,
        cast_value: str | None = "float",
    ) -> DataObject:
        qg = query.query_graph

        if not getattr(qg, "data_nodes", None):
            return cls._empty(qg, cast_value=cast_value)

        point_ref_uris, entity_context = _parse_sparql_bindings(query)

        if not point_ref_uris:
            return cls._empty(qg, cast_value=cast_value)

        # Determine all entity column names across every context dict
        all_entity_cols: set[str] = set()
        for ctx_list in entity_context.values():
            for ctx in ctx_list:
                all_entity_cols.update(ctx.keys())
        entity_columns = sorted(all_entity_cols)

        # Fetch stats for all ref_uris in one batch request
        ref_uris = list({ref_uri for _, _, ref_uri in point_ref_uris})
        stats = query.client.timeseries_info_batch(ref_uris)

        # Build BindingInfo for each binding
        bindings: list[BindingInfo] = []
        for nid, point_uri, ref_uri in point_ref_uris:
            alias = qg.aliases_reverse.get(nid, str(nid))
            key = (nid, point_uri, ref_uri)
            contexts = entity_context.get(key, [{}])
            unique_contexts = _deduplicate_contexts(contexts)
            info = stats.get(ref_uri)
            bindings.append(BindingInfo(
                nid=nid,
                point_uri=point_uri,
                ref_uri=ref_uri,
                alias=alias,
                entity_contexts=unique_contexts,
                row_count=info.row_count if info else 0,
                earliest=info.earliest if info else None,
                latest=info.latest if info else None,
            ))

        return cls(
            _bindings=bindings,
            _entity_columns=entity_columns,
            _query_graph=qg,
            _client=query.client,
            _query_params={
                "start": start,
                "end": end,
                "limit": limit,
                "order": order,
                "cast_value": cast_value,
            },
            _tall=None,
            _materialized=False,
        )

    # ------------------------------------------------------------------
    # Materialization
    # ------------------------------------------------------------------

    def _materialize(self) -> None:
        """Fetch all time-series data and populate _tall. Idempotent."""
        if self._materialized:
            return

        cast_value = self._query_params.get("cast_value", "float")

        if not self._bindings:
            self._tall = pl.DataFrame(
                schema={
                    "data_alias": pl.Utf8,
                    "point_uri": pl.Utf8,
                    "ref_uri": pl.Utf8,
                    "time": pl.Datetime(time_zone="UTC"),
                    "value": pl.Float64 if cast_value == "float" else pl.Utf8,
                }
            )
            self._materialized = True
            return

        start = self._query_params.get("start")
        end = self._query_params.get("end")
        limit = self._query_params.get("limit")
        order = self._query_params.get("order", "asc")

        frames: list[pl.DataFrame] = []
        for binding in self._bindings:
            df = self._client.timeseries_df(
                binding.ref_uri,
                start=start,
                end=end,
                limit=limit,
                order=order,
            )
            if df.is_empty():
                continue

            df = df.rename({"ts": "time", "uri": "ref_uri"})
            df = df.with_columns(
                pl.lit(binding.alias).alias("data_alias"),
                pl.lit(binding.point_uri).alias("point_uri"),
            )
            # Drop the ref_uri from timeseries (use the one from SPARQL)
            df = df.drop("ref_uri")
            df = df.with_columns(pl.lit(binding.ref_uri).alias("ref_uri"))

            # Resolve entity columns for this data key
            if len(binding.entity_contexts) <= 1:
                ctx = binding.entity_contexts[0] if binding.entity_contexts else {}
                for ec in self._entity_columns:
                    df = df.with_columns(pl.lit(ctx.get(ec)).alias(ec))
                frames.append(df)
            else:
                for ctx in binding.entity_contexts:
                    df_copy = df.clone()
                    for ec in self._entity_columns:
                        df_copy = df_copy.with_columns(pl.lit(ctx.get(ec)).alias(ec))
                    frames.append(df_copy)

        if not frames:
            schema: dict[str, Any] = {
                "data_alias": pl.Utf8,
                "point_uri": pl.Utf8,
                "ref_uri": pl.Utf8,
                "time": pl.Datetime(time_zone="UTC"),
                "value": pl.Float64 if cast_value == "float" else pl.Utf8,
            }
            for ec in self._entity_columns:
                schema[ec] = pl.Utf8
            self._tall = pl.DataFrame(schema=schema)
            self._materialized = True
            return

        tall = pl.concat(frames, how="vertical")

        # Cast value column
        if cast_value == "float":
            try:
                tall = tall.with_columns(pl.col("value").cast(pl.Float64, strict=True))
            except Exception:
                logger.warning("DataObject: casting value to float failed")
        elif cast_value == "int":
            try:
                tall = tall.with_columns(pl.col("value").cast(pl.Int64, strict=True))
            except Exception:
                logger.warning("DataObject: casting value to int failed")

        # Reorder columns for consistency
        base_cols = ["data_alias", "point_uri", "ref_uri"] + self._entity_columns + ["time", "value"]
        existing = [c for c in base_cols if c in tall.columns]
        self._tall = tall.select(existing)
        self._materialized = True

    # ------------------------------------------------------------------
    # Alias-based access (triggers materialization)
    # ------------------------------------------------------------------

    def __getitem__(self, alias: str) -> pl.DataFrame:
        """Return time-series for a data alias.

        If the alias resolves to a single ref, returns ``[time, value]``.
        If multiple refs exist, returns ``[time, value, ref_uri]`` so the
        caller can disambiguate.
        """
        self._materialize()
        subset = self._tall.filter(pl.col("data_alias") == alias)
        if subset.is_empty():
            return pl.DataFrame(schema={"time": pl.Datetime(time_zone="UTC"), "value": pl.Float64})

        n_refs = subset["ref_uri"].n_unique()
        if n_refs <= 1:
            return subset.select("time", "value").sort("time")
        return subset.select("time", "value", "ref_uri").sort("time")

    # ------------------------------------------------------------------
    # Grouping
    # ------------------------------------------------------------------

    def by(self, entity_alias: str) -> Iterator[tuple[str, DataObject]]:
        """Group by an entity alias and yield ``(entity_uri, sub_DataObject)`` pairs."""
        col = f"entity__{entity_alias}"
        if col not in self._entity_columns:
            raise KeyError(
                f"Entity alias '{entity_alias}' not found. "
                f"Available: {self.entity_aliases}"
            )

        if self._materialized:
            # Fast path: use _tall
            if self._tall is None or self._tall.is_empty():
                return
            for entity_uri in self._tall[col].drop_nulls().unique().sort().to_list():
                sub_tall = self._tall.filter(pl.col(col) == entity_uri)
                sub_bindings = [
                    b for b in self._bindings
                    if any(ctx.get(col) == entity_uri for ctx in b.entity_contexts)
                ]
                yield entity_uri, DataObject(
                    _bindings=sub_bindings,
                    _entity_columns=self._entity_columns,
                    _query_graph=self._query_graph,
                    _client=self._client,
                    _query_params=self._query_params,
                    _tall=sub_tall,
                    _materialized=True,
                )
            return

        # Lazy path: group bindings by entity context
        entity_to_bindings: dict[str, list[BindingInfo]] = {}
        for b in self._bindings:
            for ctx in b.entity_contexts:
                entity_uri = ctx.get(col)
                if entity_uri is not None:
                    entity_to_bindings.setdefault(entity_uri, []).append(b)

        for entity_uri in sorted(entity_to_bindings.keys()):
            sub_bindings = entity_to_bindings[entity_uri]
            yield entity_uri, DataObject(
                _bindings=sub_bindings,
                _entity_columns=self._entity_columns,
                _query_graph=self._query_graph,
                _client=self._client,
                _query_params=self._query_params,
                _tall=None,
                _materialized=False,
            )

    # ------------------------------------------------------------------
    # Flat DataFrame (triggers materialization)
    # ------------------------------------------------------------------

    def dataframe(self, shape: str = "wide") -> pl.DataFrame:
        """Return a flat DataFrame.

        - ``shape="narrow"``: returns the internal tall frame as-is.
        - ``shape="wide"``: pivots to ``[time, alias_0, alias_1, ...]``.
          When an alias has multiple refs, columns are suffixed ``_0``, ``_1``, etc.
        """
        self._materialize()

        if self._tall.is_empty():
            return self._tall

        if shape == "narrow":
            return self._tall.sort("time")

        # Build a pivot key that disambiguates multi-ref aliases
        tall = self._tall.clone()

        # For each alias, check if there are multiple refs
        ref_counts: dict[str, int] = {}
        for alias in tall["data_alias"].unique().to_list():
            alias_subset = tall.filter(pl.col("data_alias") == alias)
            ref_counts[alias] = alias_subset["ref_uri"].n_unique()

        # Build a pivot_key column
        def _make_pivot_key(alias: str, ref_uri: str) -> str:
            if ref_counts.get(alias, 1) <= 1:
                return alias
            # Get index of this ref within the alias
            return alias  # will be handled below

        # For multi-ref aliases, we need to create indexed column names
        has_multi_ref = any(v > 1 for v in ref_counts.values())
        if has_multi_ref:
            # Build ref index mapping
            ref_index_map: dict[tuple[str, str], int] = {}
            for alias in tall["data_alias"].unique().to_list():
                alias_subset = tall.filter(pl.col("data_alias") == alias)
                refs = alias_subset["ref_uri"].unique().sort().to_list()
                if len(refs) > 1:
                    for idx, ref in enumerate(refs):
                        ref_index_map[(alias, ref)] = idx

            # Create pivot_key column
            def pivot_key(row_alias: str, row_ref: str) -> str:
                key = (row_alias, row_ref)
                if key in ref_index_map:
                    return f"{row_alias}_{ref_index_map[key]}"
                return row_alias

            tall = tall.with_columns(
                pl.struct(["data_alias", "ref_uri"])
                .map_elements(
                    lambda s: pivot_key(s["data_alias"], s["ref_uri"]),
                    return_dtype=pl.Utf8,
                )
                .alias("_pivot_key")
            )
        else:
            tall = tall.with_columns(pl.col("data_alias").alias("_pivot_key"))

        wide = tall.pivot(
            values="value",
            index="time",
            on="_pivot_key",
            aggregate_function="first",
        )
        return wide.sort("time")

    # ------------------------------------------------------------------
    # Iteration (triggers materialization)
    # ------------------------------------------------------------------

    def iter(self, alias: str) -> Iterator[tuple[str, pl.DataFrame]]:
        """Iterate ``(point_uri, DataFrame[time, value])`` pairs for a data alias."""
        self._materialize()
        subset = self._tall.filter(pl.col("data_alias") == alias)
        for point_uri in subset["point_uri"].unique().sort().to_list():
            point_df = subset.filter(pl.col("point_uri") == point_uri)
            yield point_uri, point_df.select("time", "value").sort("time")

    # ------------------------------------------------------------------
    # Metadata & introspection (no materialization needed)
    # ------------------------------------------------------------------

    def metadata(self) -> pl.DataFrame:
        """Return a DataFrame of unique (data_alias, point_uri, ref_uri, entity__*) tuples."""
        if self._materialized and self._tall is not None:
            meta_cols = ["data_alias", "point_uri", "ref_uri"] + self._entity_columns
            existing = [c for c in meta_cols if c in self._tall.columns]
            if self._tall.is_empty():
                return pl.DataFrame(schema={c: pl.Utf8 for c in existing})
            return self._tall.select(existing).unique().sort("data_alias")

        # Build from bindings without materializing
        rows: list[dict[str, str | None]] = []
        for b in self._bindings:
            for ctx in (b.entity_contexts or [{}]):
                row: dict[str, str | None] = {
                    "data_alias": b.alias,
                    "point_uri": b.point_uri,
                    "ref_uri": b.ref_uri,
                }
                for ec in self._entity_columns:
                    row[ec] = ctx.get(ec)
                rows.append(row)
        cols = ["data_alias", "point_uri", "ref_uri"] + self._entity_columns
        if not rows:
            return pl.DataFrame(schema={c: pl.Utf8 for c in cols})
        return pl.DataFrame(rows).unique().sort("data_alias")

    @property
    def aliases(self) -> list[str]:
        """List of data aliases present in this DataObject."""
        if not self._bindings:
            return []
        return sorted(set(b.alias for b in self._bindings))

    @property
    def entity_aliases(self) -> list[str]:
        """List of entity alias names (without the ``entity__`` prefix)."""
        return [c.removeprefix("entity__") for c in self._entity_columns]

    def ref_info(self, alias: str) -> list[tuple[int, str]]:
        """Return ``[(index, ref_uri), ...]`` for a given data alias."""
        refs = sorted(set(b.ref_uri for b in self._bindings if b.alias == alias))
        return [(i, ref) for i, ref in enumerate(refs)]

    def latest(self, alias: str) -> pl.DataFrame:
        """Return the latest row(s) for a given alias."""
        self._materialize()
        subset = self._tall.filter(pl.col("data_alias") == alias)
        if subset.is_empty():
            return pl.DataFrame(schema={"time": pl.Datetime(time_zone="UTC"), "value": pl.Float64})
        return subset.sort("time", descending=True).head(1).select("time", "value")

    def is_empty(self) -> bool:
        """Check if there is any data."""
        if self._materialized:
            return self._tall.is_empty()
        return all(b.row_count == 0 for b in self._bindings)

    # ------------------------------------------------------------------
    # Stats (no materialization needed)
    # ------------------------------------------------------------------

    @property
    def total_rows(self) -> int:
        """Total row count across all bindings (from stats, no materialization)."""
        return sum(b.row_count for b in self._bindings)

    @property
    def time_range(self) -> tuple[datetime | None, datetime | None]:
        """Overall (earliest, latest) across all bindings."""
        earliests = [b.earliest for b in self._bindings if b.earliest is not None]
        latests = [b.latest for b in self._bindings if b.latest is not None]
        return (min(earliests) if earliests else None, max(latests) if latests else None)

    @property
    def bindings(self) -> list[BindingInfo]:
        """Read-only access to binding metadata."""
        return list(self._bindings)

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        aliases = self.aliases
        if self._materialized:
            n_rows = len(self._tall) if self._tall is not None else 0
            return f"DataObject({n_rows} rows, aliases={aliases}, entities={self.entity_aliases})"

        total = self.total_rows
        earliest, latest = self.time_range
        time_str = ""
        if earliest and latest:
            time_str = f", range={earliest.isoformat()} to {latest.isoformat()}"
        return f"DataObject(lazy, ~{total} rows{time_str}, aliases={aliases}, entities={self.entity_aliases})"
