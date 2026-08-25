from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Iterator, TYPE_CHECKING
import polars as pl
import logging

if TYPE_CHECKING:
    from acquirium.Client.explore.core import Query
    from acquirium.Client.query_graph import QueryGraph
    from acquirium.Client.client import AcquiriumClient

logger = logging.getLogger(__name__)


def _parse_sparql_bindings(
    query: Query,
    *,
    include_dependencies: bool,
) -> tuple[
    list[tuple[int, str, str]],          # (nid, point_uri, ref_uri) — unique data bindings
    dict[tuple[int, str, str], list[dict[str, str]]],  # data_key -> list of entity context dicts
    dict[tuple[int, str, str], str | None],  # data_key -> property unit URI
    dict[tuple[int, str, str], str | None],  # data_key -> ext ref unit URI
]:
    """Parse SPARQL result columns to extract data-node bindings, entity context, and unit metadata.

    Returns:
        point_ref_uris: deduplicated list of (node_id, point_uri, ref_uri)
        entity_context: mapping from each data key to a list of dicts
            like {"entity__basin": "urn:...", "entity__process": "urn:..."} —
            one dict per SPARQL row that matched this data key.
        property_units: mapping from data key to property unit URI (or None)
        ref_units: mapping from data key to external reference unit URI (or None)
        point_labels: mapping from data key to the point's rdfs:label (or None)
    """
    res = query.execute(include_dependencies=include_dependencies)
    cols: list[str] = res.get("columns", [])
    rows: list[list[Any]] = res.get("rows", [])

    qg = query.query_graph
    data_node_ids = set(qg.data_nodes.keys())

    # Map column index -> node id for v<N>, ext<N>, unit<N>, extunit<N>,
    # lbl<N> columns
    col_to_id: dict[int, int] = {}
    ext_ref_col_to_id: dict[int, int] = {}
    unit_col_to_id: dict[int, int] = {}
    extunit_col_to_id: dict[int, int] = {}
    lbl_col_to_id: dict[int, int] = {}
    for i, c in enumerate(cols):
        if not isinstance(c, str):
            continue
        if c.startswith("extunit"):
            try:
                extunit_col_to_id[i] = int(c[7:])
            except ValueError:
                pass
        elif c.startswith("ext"):
            try:
                ext_ref_col_to_id[i] = int(c[3:])
            except ValueError:
                pass
        elif c.startswith("unit"):
            try:
                unit_col_to_id[i] = int(c[4:])
            except ValueError:
                pass
        elif c.startswith("lbl"):
            try:
                lbl_col_to_id[i] = int(c[3:])
            except ValueError:
                pass
        elif c.startswith("v"):
            try:
                col_to_id[i] = int(c[1:])
            except ValueError:
                pass

    nid_to_ext_ref_col: dict[int, int] = {v: k for k, v in ext_ref_col_to_id.items()}
    nid_to_unit_col: dict[int, int] = {v: k for k, v in unit_col_to_id.items()}
    nid_to_extunit_col: dict[int, int] = {v: k for k, v in extunit_col_to_id.items()}
    nid_to_lbl_col: dict[int, int] = {v: k for k, v in lbl_col_to_id.items()}

    data_col_indices = [i for i, nid in col_to_id.items() if nid in data_node_ids]
    ref_col_indices = [i for i, nid in ext_ref_col_to_id.items() if nid in data_node_ids]

    # Identify entity columns (non-data v<N> columns)
    entity_col_indices: list[tuple[int, int]] = []  # (col_index, node_id)
    for i, nid in col_to_id.items():
        if nid not in data_node_ids:
            entity_col_indices.append((i, nid))

    # Collect unique data bindings, entity contexts, and unit/label info
    point_ref_uris: list[tuple[int, str, str]] = []
    seen: set[tuple[int, str, str]] = set()
    entity_context: dict[tuple[int, str, str], list[dict[str, str]]] = {}
    property_units: dict[tuple[int, str, str], str | None] = {}
    ref_units: dict[tuple[int, str, str], str | None] = {}
    point_labels: dict[tuple[int, str, str], str | None] = {}

    if not data_col_indices or not ref_col_indices:
        return [], {}, {}, {}, {}

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
                # Extract unit URIs (first non-None wins for each key)
                unit_col = nid_to_unit_col.get(nid)
                if unit_col is not None and unit_col < len(r) and r[unit_col] is not None:
                    property_units[key] = str(r[unit_col])
                else:
                    property_units[key] = None
                extunit_col = nid_to_extunit_col.get(nid)
                if extunit_col is not None and extunit_col < len(r) and r[extunit_col] is not None:
                    ref_units[key] = str(r[extunit_col])
                else:
                    ref_units[key] = None
                lbl_col = nid_to_lbl_col.get(nid)
                if lbl_col is not None and lbl_col < len(r) and r[lbl_col] is not None:
                    point_labels[key] = str(r[lbl_col])
                else:
                    point_labels[key] = None
            entity_context.setdefault(key, []).append(row_entities)

    return point_ref_uris, entity_context, property_units, ref_units, point_labels


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


def _is_numeric_dtype(dtype: pl.DataType) -> bool:
    return bool(getattr(dtype, "is_numeric", lambda: False)())

def _split_value_column(df: pl.DataFrame) -> pl.DataFrame:
    if "value" not in df.columns:
        return df
    if _is_numeric_dtype(df.schema["value"]):
        return df.with_columns([
            pl.col("value").cast(pl.Float64).alias("value_numeric"),
            pl.lit(None, dtype=pl.Utf8).alias("value_text"),
        ]).drop("value")
    return df.with_columns([
        pl.lit(None, dtype=pl.Float64).alias("value_numeric"),
        pl.col("value").cast(pl.Utf8).alias("value_text"),
    ]).drop("value")


def _restore_single_value_column(df: pl.DataFrame) -> pl.DataFrame:
    if "value_numeric" not in df.columns or "value_text" not in df.columns:
        return df
    if df.is_empty() or df["value_numeric"].null_count() < df.height:
        return df.with_columns(pl.col("value_numeric").alias("value"))
    return df.with_columns(pl.col("value_text").alias("value"))


def _as_utc_datetime(v: "datetime | str | None") -> datetime | None:
    if v is None:
        return None
    dt = v if isinstance(v, datetime) else datetime.fromisoformat(str(v))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _apply_window(
    tall: pl.DataFrame,
    start: "datetime | str | None",
    end: "datetime | str | None",
    limit: int | None,
    order: str,
) -> pl.DataFrame:
    """Restrict a tall frame to a time window and per-stream row limit.

    Mirrors the server-side fetch bounds: ``limit`` keeps the first
    ``limit`` rows *per stream* in ``order`` direction.
    """
    start = _as_utc_datetime(start)
    end = _as_utc_datetime(end)
    if start is not None:
        tall = tall.filter(pl.col("time") >= start)
    if end is not None:
        tall = tall.filter(pl.col("time") <= end)
    if limit is not None:
        tall = (
            tall.sort("time", descending=order == "desc")
            .group_by(["data_alias", "point_uri", "ref_uri"], maintain_order=True)
            .head(limit)
        )
    return tall


def _pivot_split_values(tall: pl.DataFrame, pivot_key: str) -> pl.DataFrame:
    parts: list[pl.DataFrame] = []
    numeric_rows = tall.filter(pl.col("value_numeric").is_not_null())
    if not numeric_rows.is_empty():
        parts.append(
            numeric_rows.pivot(
                values="value_numeric",
                index="time",
                on=pivot_key,
                aggregate_function="first",
            )
        )
    text_rows = tall.filter(pl.col("value_text").is_not_null())
    if not text_rows.is_empty():
        text_wide = text_rows.pivot(
            values="value_text",
            index="time",
            on=pivot_key,
            aggregate_function="first",
        )
        if parts:
            existing = set(parts[0].columns)
            text_wide = text_wide.rename({
                c: f"{c}_text" for c in text_wide.columns if c != "time" and c in existing
            })
        parts.append(text_wide)
    if not parts:
        return pl.DataFrame(schema={"time": pl.Datetime(time_zone="UTC")})
    wide = parts[0]
    for part in parts[1:]:
        wide = wide.join(part, on="time", how="full", coalesce=True)
    # deterministic column order: time first, then case-insensitive alphabetical
    value_cols = sorted((c for c in wide.columns if c != "time"), key=str.casefold)
    return wide.select(["time", *value_cols]).sort("time")


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
    property_unit: str | None = None    # unit URI from qudt:hasUnit on the property
    ref_unit: str | None = None         # unit URI from qudt:hasUnit on the ext reference
    point_label: str | None = None      # rdfs:label on the property (display name)


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

    # Pending unit conversions: list of (alias_pattern, from_unit, to_unit)
    # alias_pattern is "*" for all aliases, or a specific alias name
    _pending_conversions: list[tuple[str, str, str]] = field(default_factory=list, repr=False)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def _empty(cls, qg: QueryGraph, *, cast_value: str | None = "float",
               client: "AcquiriumClient | None" = None) -> DataObject:
        return cls(
            _bindings=[],
            _entity_columns=[],
            _query_graph=qg,
            _client=client,
            _tall=pl.DataFrame(
                schema={
                    "data_alias": pl.Utf8,
                    "point_uri": pl.Utf8,
                    "ref_uri": pl.Utf8,
                    "time": pl.Datetime(time_zone="UTC"),
                    "value_numeric": pl.Float64,
                    "value_text": pl.Utf8,
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
        include_dependencies: bool = True,
        cast_value: str | None = "float",
        value_mode: str = "default",
    ) -> DataObject:
        qg = query.query_graph

        if not getattr(qg, "data_nodes", None):
            return cls._empty(qg, cast_value=cast_value, client=query.client)

        point_ref_uris, entity_context, prop_units, ext_ref_units, point_labels = _parse_sparql_bindings(
            query,
            include_dependencies=include_dependencies,
        )

        if not point_ref_uris:
            return cls._empty(qg, cast_value=cast_value, client=query.client)

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
                property_unit=prop_units.get(key),
                ref_unit=ext_ref_units.get(key),
                point_label=point_labels.get(key),
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
                "value_mode": value_mode,
            },
            _tall=None,
            _materialized=False,
        )

    # ------------------------------------------------------------------
    # Materialization
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_conversion(df: pl.DataFrame, factors: dict) -> pl.DataFrame:
        """Apply unit conversion to a DataFrame's value column using pre-computed factors."""
        src_mult = factors["from_multiplier"]
        src_off = factors["from_offset"]
        tgt_mult = factors["to_multiplier"]
        tgt_off = factors["to_offset"]
        value_col = "value_numeric" if "value_numeric" in df.columns else "value"
        return df.with_columns(
            ((pl.col(value_col) + src_off) * src_mult / tgt_mult - tgt_off).alias(value_col)
        )

    def _point_labels(self) -> dict[str, str]:
        """``{point_uri: rdfs:label}`` for display naming.

        A label shared by several distinct points is dropped so pivot
        columns for different points can never collapse into one; those
        points fall back to their URI-based names.
        """
        pairs = {(b.point_uri, b.point_label) for b in self._bindings if b.point_label}
        counts: dict[str, int] = {}
        for _, lbl in pairs:
            counts[lbl] = counts.get(lbl, 0) + 1
        return {uri: lbl for uri, lbl in pairs if counts[lbl] == 1}

    def _resolve_effective_units(self) -> dict[str, str | None]:
        """Determine the effective unit for each alias, handling Case 3.2/3.2.1.

        Returns {alias: effective_unit_uri_or_None}.
        """
        alias_units: dict[str, str | None] = {}
        for b in self._bindings:
            if b.alias in alias_units:
                continue
            if b.property_unit:
                alias_units[b.alias] = b.property_unit
            elif b.ref_unit:
                # Case 3.2: no property unit, adopt ref_unit
                alias_units[b.alias] = b.ref_unit
            else:
                alias_units[b.alias] = None
        return alias_units

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
                    "value_numeric": pl.Float64,
                    "value_text": pl.Utf8,
                }
            )
            self._materialized = True
            return

        start = self._query_params.get("start")
        end = self._query_params.get("end")
        limit = self._query_params.get("limit")
        order = self._query_params.get("order", "asc")
        value_mode = self._query_params.get("value_mode", "default")

        # Pre-compute effective units per alias for Case 3.2/3.2.1
        effective_units = self._resolve_effective_units()

        # Cache conversion factors to avoid repeated server calls
        _factors_cache: dict[tuple[str, str], dict] = {}

        def _get_factors(from_u: str, to_u: str) -> dict:
            key = (from_u, to_u)
            if key not in _factors_cache:
                _factors_cache[key] = self._client.get_conversion_factors(from_u, to_u)
            return _factors_cache[key]

        frames: list[pl.DataFrame] = []
        for binding in self._bindings:
            df = self._client.timeseries_df(
                binding.ref_uri,
                start=start,
                end=end,
                limit=limit,
                order=order,
                value_mode=value_mode,
            )
            if df.is_empty():
                continue

            df = df.rename({"ts": "time", "uri": "ref_uri"})
            if cast_value == "float" and "value" in df.columns:
                try:
                    df = df.with_columns(pl.col("value").cast(pl.Float64, strict=True))
                except Exception:
                    logger.warning("DataObject: casting value to float failed for ref %s", binding.ref_uri)
            elif cast_value == "int" and "value" in df.columns:
                try:
                    df = df.with_columns(pl.col("value").cast(pl.Int64, strict=True))
                except Exception:
                    logger.warning("DataObject: casting value to int failed for ref %s", binding.ref_uri)
            df = df.with_columns(
                pl.lit(binding.alias).alias("data_alias"),
                pl.lit(binding.point_uri).alias("point_uri"),
            )
            # Drop the ref_uri from timeseries (use the one from SPARQL)
            df = df.drop("ref_uri")
            df = df.with_columns(pl.lit(binding.ref_uri).alias("ref_uri"))

            # --- Case 3 auto-conversion ---
            target_unit = effective_units.get(binding.alias)
            if binding.ref_unit and target_unit and binding.ref_unit != target_unit:
                # Case 3.1: ext ref unit differs from effective unit → convert
                # Need to cast value to float first for conversion
                try:
                    df = df.with_columns(pl.col("value").cast(pl.Float64))
                    factors = _get_factors(binding.ref_unit, target_unit)
                    if factors.get("compatible", False):
                        df = self._apply_conversion(df, factors)
                    else:
                        logger.warning(
                            "DataObject: incompatible units for auto-conversion "
                            "%s -> %s on ref %s, skipping",
                            binding.ref_unit, target_unit, binding.ref_uri,
                        )
                except Exception:
                    logger.warning(
                        "DataObject: auto-conversion failed for ref %s (%s -> %s)",
                        binding.ref_uri, binding.ref_unit, target_unit,
                    )

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
                "value_numeric": pl.Float64,
                "value_text": pl.Utf8,
            }
            for ec in self._entity_columns:
                schema[ec] = pl.Utf8
            self._tall = pl.DataFrame(schema=schema)
            self._materialized = True
            return

        tall = pl.concat([_split_value_column(df) for df in frames], how="vertical")

        # Apply pending conversions (from convert_to() on a lazy DataObject)
        for alias_pat, from_unit, to_unit in self._pending_conversions:
            try:
                factors = _get_factors(from_unit, to_unit)
                if not factors.get("compatible", False):
                    logger.warning(
                        "DataObject: incompatible units in pending conversion %s -> %s",
                        from_unit, to_unit,
                    )
                    continue
                if alias_pat == "*":
                    tall = self._apply_conversion(tall, factors)
                else:
                    mask = pl.col("data_alias") == alias_pat
                    converted = self._apply_conversion(tall.filter(mask), factors)
                    rest = tall.filter(~mask)
                    tall = pl.concat([rest, converted], how="vertical")
            except Exception:
                logger.warning(
                    "DataObject: pending conversion failed %s -> %s for alias '%s'",
                    from_unit, to_unit, alias_pat,
                )

        # Reorder columns for consistency
        base_cols = ["data_alias", "point_uri", "ref_uri"] + self._entity_columns + ["time", "value_numeric", "value_text"]
        existing = [c for c in base_cols if c in tall.columns]
        self._tall = tall.select(existing)
        self._materialized = True

    # ------------------------------------------------------------------
    # Alias-based access (triggers materialization)
    # ------------------------------------------------------------------

    def __getitem__(self, alias: str) -> pl.DataFrame:
        """Return time-series for a data alias.

        Multiple ref_uris that share the same point_uri are combined
        (first-wins). If the alias resolves to a single point_uri the result
        is ``[time, value]``; if several point_uris share the alias the
        result is ``[time, value, point_uri]`` so the caller can
        disambiguate.
        """
        self._materialize()
        subset = self._tall.filter(pl.col("data_alias") == alias)
        if subset.is_empty():
            return pl.DataFrame(schema={"time": pl.Datetime(time_zone="UTC"), "value": pl.Float64})

        # Combine ref_uris that share the same (point_uri, time). maintain_order
        # is what makes keep="first" mean the first row of _tall; without it
        # polars picks an arbitrary duplicate and shuffles the surviving rows.
        subset = subset.unique(subset=["point_uri", "time"], keep="first", maintain_order=True)
        subset = _restore_single_value_column(subset)
        n_points = subset["point_uri"].n_unique()
        if n_points <= 1:
            return subset.select("time", "value").sort("time")
        # (time, point_uri) is unique after the dedup above, so sorting on both
        # gives a total order — repeated access returns identical frames.
        return subset.select("time", "value", "point_uri").sort(["time", "point_uri"])

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
                _pending_conversions=list(self._pending_conversions),
            )

    # ------------------------------------------------------------------
    # Flat DataFrame (triggers materialization)
    # ------------------------------------------------------------------

    def dataframe(
        self,
        shape: str = "wide",
        *,
        start: "datetime | str | None" = None,
        end: "datetime | str | None" = None,
        limit: int | None = None,
        order: str = "asc",
        include_ref: bool = False,
        compact: bool = True,
    ) -> pl.DataFrame:
        """Return a flat DataFrame.

        The parameters and defaults mirror :meth:`Query.dataframe`, so
        ``q.data(...).dataframe(...)`` equals ``q.dataframe(...)``. Here
        ``start``/``end``/``limit``/``order`` are applied client-side on the
        already-fetched data (``limit`` keeps rows *per stream*, in
        ``order`` direction, which also orders the output); the same
        parameters on :meth:`Query.data` bound the fetch itself.

        - ``shape="wide"`` (default): pivots to ``[time, <one column per
          stream>]``. Multiple ref_uris that share the same point_uri are
          combined (first-wins). When an alias resolves to a single point
          the column is just the alias; when it resolves to several points
          the columns are disambiguated per point.
        - ``shape="narrow"``: one row per observation.

        ``compact=True`` (default) renders URIs as CURIEs and identifies
        points with a ``point_id`` column rather than the raw
        ``point_uri``/``ref_uri``; auto-aliased data nodes are named by the
        point's ``rdfs:label`` when it has one, else by their compacted
        point-local name. In this mode the narrow layout is
        ``["data_alias", "point_id", "time", "value_numeric", "value_text"]``
        and ``include_ref=True`` adds the compacted ``ref`` column.

        ``compact=False`` returns the raw layout: narrow is the internal
        tall frame (every ``(data_alias, point_uri, ref_uri, time)`` row
        preserved with entity columns), wide keeps alias/URI column names.
        """
        self._materialize()

        descending = order == "desc"
        windowed = _apply_window(self._tall, start, end, limit, order)

        if windowed.is_empty():
            return windowed

        if compact:
            return self._compact_dataframe(windowed, shape=shape, include_ref=include_ref,
                                           descending=descending)

        if shape == "narrow":
            return windowed.sort("time", descending=descending)

        tall = windowed.clone()

        # Combine ref_uris sharing the same (data_alias, point_uri, time):
        # we want one row per point_uri at each timestamp.
        tall = tall.unique(subset=["data_alias", "point_uri", "time"], keep="first")

        # Count distinct point_uris per alias from the bindings (i.e. the
        # query's metadata view, not just whichever bindings produced rows).
        # This keeps disambiguation stable even when some bindings had no
        # data within the requested window.
        points_per_alias: dict[str, set[str]] = {}
        auto_aliases: set[str] = set()
        for b in self._bindings:
            points_per_alias.setdefault(b.alias, set()).add(b.point_uri)
            if b.alias == str(b.nid):
                auto_aliases.add(b.alias)
        labels = self._point_labels()

        def _pivot_key(alias: str, point_uri: str) -> str:
            label = labels.get(point_uri)
            if alias in auto_aliases and label:
                return label
            pts = points_per_alias.get(alias, set())
            if len(pts) <= 1:
                return alias
            if label:
                return f"{alias}__{label}"
            try:
                return f"{alias}__{self._client.compact_uri(point_uri)}"
            except Exception:
                # we shouldn't come here
                return f"{alias}__{point_uri}"


        tall = tall.with_columns(
            pl.struct(["data_alias", "point_uri"])
            .map_elements(
                lambda s: _pivot_key(s["data_alias"], s["point_uri"]),
                return_dtype=pl.Utf8,
            )
            .alias("_pivot_key")
        )

        wide = _pivot_split_values(tall, "_pivot_key")
        return wide.sort("time", descending=True) if descending else wide

    def _compact_dataframe(self, tall: pl.DataFrame, *, shape: str, include_ref: bool,
                           descending: bool = False) -> pl.DataFrame:
        """Query-compatible layout: CURIE-rendered URIs and a ``point_id`` column.

        Auto-aliased data nodes (alias == ``str(nid)``) are named by the
        point's ``rdfs:label`` when it has one, else by its compacted
        point-local name; user aliases that resolve to several points are
        disambiguated as ``f"{alias}__{label_or_point_local}"``.
        """
        def _compact(uri: str) -> str:
            try:
                return self._client.compact_uri(uri)
            except Exception:
                return uri

        # Combine ref_uris sharing the same (data_alias, point_uri, time).
        tall = tall.clone().unique(
            subset=["data_alias", "point_uri", "time"], keep="first"
        )

        points_per_alias: dict[str, set[str]] = {}
        auto_aliases: set[str] = set()
        for b in self._bindings:
            points_per_alias.setdefault(b.alias, set()).add(b.point_uri)
            if b.alias == str(b.nid):
                auto_aliases.add(b.alias)

        labels = self._point_labels()

        def _label(alias: str, point_uri: str) -> str:
            if alias in auto_aliases:
                return labels.get(point_uri) or _compact(point_uri)
            if len(points_per_alias.get(alias, set())) > 1:
                return f"{alias}__{labels.get(point_uri) or _compact(point_uri)}"
            return alias

        tall = tall.with_columns(
            pl.struct(["data_alias", "point_uri"])
            .map_elements(
                lambda s: _label(s["data_alias"], s["point_uri"]),
                return_dtype=pl.Utf8,
            )
            .alias("_label")
        )

        if shape == "narrow":
            out = tall.with_columns(
                pl.col("_label").alias("data_alias"),
                pl.col("point_uri").map_elements(_compact, return_dtype=pl.Utf8).alias("point_id"),
            )
            cols = ["data_alias", "point_id", "time", "value_numeric", "value_text"]
            if include_ref:
                out = out.with_columns(
                    pl.col("ref_uri").map_elements(_compact, return_dtype=pl.Utf8).alias("ref")
                )
                cols.insert(2, "ref")
            return out.select(cols).sort("time", descending=descending)

        # wide
        wide = _pivot_split_values(tall, "_label")
        return wide.sort("time", descending=True) if descending else wide

    # ------------------------------------------------------------------
    # Iteration (triggers materialization)
    # ------------------------------------------------------------------

    def iter(self, alias: str) -> Iterator[tuple[str, pl.DataFrame]]:
        """Iterate ``(point_uri, DataFrame[time, value])`` pairs for a data alias."""
        self._materialize()
        subset = self._tall.filter(pl.col("data_alias") == alias)
        for point_uri in subset["point_uri"].unique().sort().to_list():
            point_df = subset.filter(pl.col("point_uri") == point_uri)
            point_df = _restore_single_value_column(point_df)
            yield point_uri, point_df.select("time", "value").sort("time")

    # ------------------------------------------------------------------
    # Metadata & introspection (no materialization needed)
    # ------------------------------------------------------------------

    def metadata(self, *, include_ref_uris: bool = False) -> pl.DataFrame:
        """Return a DataFrame of unique ``(data_alias, point_label,
        point_uri, entity__*)`` tuples.

        ``point_label`` is the point's ``rdfs:label`` (null when it has
        none). By default the UUID ``ref_uri`` column is hidden — multiple
        ref_uris sharing the same point are folded into one row. Pass
        ``include_ref_uris=True`` to keep the per-ref breakdown (useful for
        per-ref filtering and debugging).
        """
        ref_col = ["ref_uri"] if include_ref_uris else []
        label_map = {b.point_uri: b.point_label for b in self._bindings if b.point_label}

        if self._materialized and self._tall is not None:
            meta_cols = ["data_alias", "point_uri"] + ref_col + self._entity_columns
            existing = [c for c in meta_cols if c in self._tall.columns]
            out_cols = existing[:1] + ["point_label"] + existing[1:]
            if self._tall.is_empty():
                return pl.DataFrame(schema={c: pl.Utf8 for c in out_cols})
            return (
                self._tall.select(existing).unique()
                .with_columns(
                    pl.col("point_uri")
                    .map_elements(lambda u: label_map.get(u), return_dtype=pl.Utf8, skip_nulls=False)
                    .alias("point_label")
                )
                .select(out_cols)
                .sort("data_alias")
            )

        # Build from bindings without materializing
        rows: list[dict[str, str | None]] = []
        for b in self._bindings:
            for ctx in (b.entity_contexts or [{}]):
                row: dict[str, str | None] = {
                    "data_alias": b.alias,
                    "point_label": b.point_label,
                    "point_uri": b.point_uri,
                }
                if include_ref_uris:
                    row["ref_uri"] = b.ref_uri
                for ec in self._entity_columns:
                    row[ec] = ctx.get(ec)
                rows.append(row)
        cols = ["data_alias", "point_label", "point_uri"] + ref_col + self._entity_columns
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
        subset = _restore_single_value_column(subset)
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
    # Unit conversion
    # ------------------------------------------------------------------

    def units(self) -> dict[str, str | None]:
        """Return ``{alias: unit_uri_or_None}`` for each data alias.

        The effective unit considers property annotations and external
        reference annotations (Case 3.2: ref_unit adopted when property
        has no unit).
        """
        return self._resolve_effective_units()

    def convert_to(
        self,
        to_unit: str,
        *,
        from_unit: str | None = None,
        alias: str | None = None,
    ) -> "DataObject":
        """Return a new DataObject with values converted to the target unit.

        Args:
            to_unit: Target unit identifier (URI, label, symbol, or UCUM code).
            from_unit: Source unit identifier. If None, the property's
                ``qudt:hasUnit`` annotation is used (Case 2). If the property
                has no unit, a ValueError is raised — pass ``from_unit``
                explicitly (Case 1).
            alias: Apply conversion only to this alias. If None, all aliases
                are converted.

        Returns:
            A **new** DataObject with converted values. The original is not
            modified.

        Raises:
            ValueError: If ``from_unit`` is None and the alias has no unit
                annotation, or if the units are incompatible.
        """
        if self._client is None:
            raise ValueError("Cannot convert units without a client connection")

        effective = self._resolve_effective_units()
        affected_aliases = [alias] if alias else self.aliases

        # Resolve a convertible pair per (source, target) — the server picks,
        # among the top matches for a free-text side, the candidate that is
        # actually compatible with the other side. The from side is usually a
        # unit URI off the stream metadata, which pins it.
        raw_pairs: dict[tuple[str, str], dict] = {}

        def _factors(src: str, tgt: str) -> dict:
            key = (str(src), str(tgt))
            if key not in raw_pairs:
                try:
                    raw_pairs[key] = self._client.resolve_conversion(src, tgt)["factors"]
                except Exception as e:
                    raise ValueError(
                        f"convert_to: no convertible unit pair for {src!r} -> {tgt!r} ({e})"
                    ) from e
            return raw_pairs[key]

        # Validate and determine from_unit per alias; store resolved URIs so
        # the pending/materialize paths only ever see exact units.
        conversions: list[tuple[str, str, str]] = []  # (alias, from_uri, to_uri)
        for a in affected_aliases:
            src = from_unit
            if src is None:
                src = effective.get(a)
                if src is None:
                    raise ValueError(
                        f"No unit annotation found for alias '{a}'. "
                        f"Provide from_unit explicitly."
                    )
            f = _factors(src, to_unit)
            conversions.append((a, f["from_uri"], f["to_uri"]))

        factors_cache: dict[tuple[str, str], dict] = {
            (f["from_uri"], f["to_uri"]): f for f in raw_pairs.values()
        }

        if self._materialized and self._tall is not None:
            # Eager path: clone the tall frame and apply conversions
            tall = self._tall.clone()
            for a, fu, tu in conversions:
                f = factors_cache[(fu, tu)]
                mask = pl.col("data_alias") == a
                converted = self._apply_conversion(tall.filter(mask), f)
                rest = tall.filter(~mask)
                tall = pl.concat([rest, converted], how="vertical")

            # Update bindings with new unit
            new_bindings = []
            for b in self._bindings:
                matched = any(c[0] == b.alias for c in conversions)
                if matched:
                    # Find the to_unit URI from the factors
                    conv = next(c for c in conversions if c[0] == b.alias)
                    new_uri = factors_cache[(conv[1], conv[2])]["to_uri"]
                    new_bindings.append(replace(b, property_unit=new_uri))
                else:
                    new_bindings.append(b)

            return DataObject(
                _bindings=new_bindings,
                _entity_columns=self._entity_columns,
                _query_graph=self._query_graph,
                _client=self._client,
                _query_params=self._query_params,
                _tall=tall,
                _materialized=True,
            )

        # Lazy path: store pending conversions to apply during _materialize()
        new_bindings = []
        for b in self._bindings:
            matched = any(c[0] == b.alias for c in conversions)
            if matched:
                conv = next(c for c in conversions if c[0] == b.alias)
                new_uri = factors_cache[(conv[1], conv[2])]["to_uri"]
                new_bindings.append(replace(b, property_unit=new_uri))
            else:
                new_bindings.append(b)

        pending = list(self._pending_conversions) + conversions
        return DataObject(
            _bindings=new_bindings,
            _entity_columns=self._entity_columns,
            _query_graph=self._query_graph,
            _client=self._client,
            _query_params=self._query_params,
            _tall=None,
            _materialized=False,
            _pending_conversions=pending,
        )

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
