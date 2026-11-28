"""Shape query results into sensor data frames.

Each binding ties one query alias to a point and the stream supplying it.
Samples first take a common tall layout that preserves numeric and text
values. That layout feeds both per-alias reads and wide pivots.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Iterator, Sequence, TYPE_CHECKING
import polars as pl
import warnings

from acquirium.Client.streams import Stream, inclusive_bounds
from acquirium.ExperimentValues import timestamp as _utc
from acquirium.replacements import VIEWS
from acquirium.internals._names import accepts_old_names

if TYPE_CHECKING:
    from acquirium.Client.explore.core import Query
    from acquirium.Client.query_graph import QueryGraph
    from acquirium.Client.client import AcquiriumClient



def _parse_sparql_bindings(
    query: Query,
    *,
    include_dependencies: bool,
) -> tuple[
    list[tuple[int, str, str]],          # (nid, property_uri, stream_uri) — unique data bindings
    dict[tuple[int, str, str], list[dict[str, str]]],  # data_key -> list of entity context dicts
    dict[tuple[int, str, str], str | None],  # data_key -> property unit URI
    dict[tuple[int, str, str], str | None],  # data_key -> ext ref unit URI
]:
    """Parse SPARQL result columns to extract data-node bindings, entity context, and unit metadata.

    Returns:
        property_stream_uris: deduplicated list of (node_id, property_uri, stream_uri)
        entity_context: mapping from each data key to a list of dicts
            like {"entity__basin": "urn:...", "entity__process": "urn:..."} —
            one dict per SPARQL row that matched this data key.
        property_units: mapping from data key to property unit URI (or None)
        stream_units: mapping from data key to external reference unit URI (or None)
        property_labels: mapping from data key to the point's rdfs:label (or None)
    """
    res = query.execute(include_dependencies=include_dependencies)
    cols: list[str] = res.get("columns", [])
    rows: list[list[Any]] = res.get("rows", [])

    qg = query.query_graph
    data_node_ids = set(qg.data_nodes.keys())

    # Map column index -> node id for v<N>, ext<N>, unit<N>, extunit<N>,
    # lbl<N>, extsrc<N>, extname<N> columns
    col_to_id: dict[int, int] = {}
    ext_ref_col_to_id: dict[int, int] = {}
    unit_col_to_id: dict[int, int] = {}
    extunit_col_to_id: dict[int, int] = {}
    lbl_col_to_id: dict[int, int] = {}
    extsrc_col_to_id: dict[int, int] = {}
    extname_col_to_id: dict[int, int] = {}
    for i, c in enumerate(cols):
        if not isinstance(c, str):
            continue
        if c.startswith("extunit"):
            try:
                extunit_col_to_id[i] = int(c[7:])
            except ValueError:
                pass
        elif c.startswith("extsrc"):
            try:
                extsrc_col_to_id[i] = int(c[6:])
            except ValueError:
                pass
        elif c.startswith("extname"):
            try:
                extname_col_to_id[i] = int(c[7:])
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
    nid_to_extsrc_col: dict[int, int] = {v: k for k, v in extsrc_col_to_id.items()}
    nid_to_extname_col: dict[int, int] = {v: k for k, v in extname_col_to_id.items()}

    def _cell(r, col):
        return str(r[col]) if col is not None and col < len(r) and r[col] is not None else None

    data_col_indices = [i for i, nid in col_to_id.items() if nid in data_node_ids]
    ref_col_indices = [i for i, nid in ext_ref_col_to_id.items() if nid in data_node_ids]

    # Identify entity columns (non-data v<N> columns)
    entity_col_indices: list[tuple[int, int]] = []  # (col_index, node_id)
    for i, nid in col_to_id.items():
        if nid not in data_node_ids:
            entity_col_indices.append((i, nid))

    # Collect unique data bindings, entity contexts, and unit/label info
    property_stream_uris: list[tuple[int, str, str]] = []
    seen: set[tuple[int, str, str]] = set()
    entity_context: dict[tuple[int, str, str], list[dict[str, str]]] = {}
    property_units: dict[tuple[int, str, str], str | None] = {}
    stream_units: dict[tuple[int, str, str], str | None] = {}
    property_labels: dict[tuple[int, str, str], str | None] = {}
    stream_ids: dict[tuple[int, str, str], tuple[str | None, str | None]] = {}

    if not data_col_indices or not ref_col_indices:
        return [], {}, {}, {}, {}, {}

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
            stream_uri = r[ref_col_idx]
            if stream_uri is None:
                continue
            stream_uri_s = str(stream_uri)
            key = (nid, uri_s, stream_uri_s)
            if key not in seen:
                seen.add(key)
                property_stream_uris.append(key)
                # Extract unit URIs (first non-None wins for each key)
                unit_col = nid_to_unit_col.get(nid)
                if unit_col is not None and unit_col < len(r) and r[unit_col] is not None:
                    property_units[key] = str(r[unit_col])
                else:
                    property_units[key] = None
                extunit_col = nid_to_extunit_col.get(nid)
                if extunit_col is not None and extunit_col < len(r) and r[extunit_col] is not None:
                    stream_units[key] = str(r[extunit_col])
                else:
                    stream_units[key] = None
                lbl_col = nid_to_lbl_col.get(nid)
                if lbl_col is not None and lbl_col < len(r) and r[lbl_col] is not None:
                    property_labels[key] = str(r[lbl_col])
                else:
                    property_labels[key] = None
                stream_ids[key] = (_cell(r, nid_to_extsrc_col.get(nid)), _cell(r, nid_to_extname_col.get(nid)))
            entity_context.setdefault(key, []).append(row_entities)

    return property_stream_uris, entity_context, property_units, stream_units, property_labels, stream_ids


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

_CAST_VALUES = ("float", "int", "str", None)


def _validate_cast_value(cast_value: str | None) -> str | None:
    if cast_value not in _CAST_VALUES:
        raise ValueError(f"cast_value must be 'float', 'int', 'str' or None, got {cast_value!r}")
    return cast_value


def _split_value_column(df: pl.DataFrame, cast_value: str | None = None) -> pl.DataFrame:
    """Split the server's ``value`` column into ``value_numeric``/``value_text``.

    A numeric column goes to ``value_numeric`` and a text column to
    ``value_text``. ``cast_value`` changes that: ``"float"``/``"int"`` send
    every text value that reads as a number to ``value_numeric`` and only the
    rest stays text, so a status stream carrying ``"12.5"`` next to ``"ON"``
    keeps both; ``"str"`` sends every value to ``value_text``. Both are
    vectorized polars casts, not per-row Python.
    """
    if "value" not in df.columns:
        return df
    if cast_value == "str":
        return df.with_columns([
            pl.lit(None, dtype=pl.Float64).alias("value_numeric"),
            pl.col("value").cast(pl.Utf8).alias("value_text"),
        ]).drop("value")
    if _is_numeric_dtype(df.schema["value"]):
        return df.with_columns([
            pl.col("value").cast(pl.Float64).alias("value_numeric"),
            pl.lit(None, dtype=pl.Utf8).alias("value_text"),
        ]).drop("value")
    if cast_value in ("float", "int"):
        parsed = pl.col("value").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)
        return df.with_columns([
            parsed.alias("value_numeric"),
            pl.when(parsed.is_null())
            .then(pl.col("value").cast(pl.Utf8))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("value_text"),
        ]).drop("value")
    return df.with_columns([
        pl.lit(None, dtype=pl.Float64).alias("value_numeric"),
        pl.col("value").cast(pl.Utf8).alias("value_text"),
    ]).drop("value")


def _restore_single_value_column(df: pl.DataFrame) -> pl.DataFrame:
    """Fold ``value_numeric``/``value_text`` back into one ``value`` column.

    Numeric-only rows give a Float64 column, text-only rows a String column.
    A frame mixing both (an alias spanning a sensor and a status point) gives
    a String column with the numbers rendered as text, so no value is lost
    and the dtype shows the caller the alias is mixed.
    """
    if "value_numeric" not in df.columns or "value_text" not in df.columns:
        return df
    has_text = df["value_text"].null_count() < df.height
    has_numeric = df["value_numeric"].null_count() < df.height
    if has_text and has_numeric:
        return df.with_columns(
            pl.coalesce(pl.col("value_text"), pl.col("value_numeric").cast(pl.Utf8)).alias("value")
        )
    if has_text:
        return df.with_columns(pl.col("value_text").alias("value"))
    return df.with_columns(pl.col("value_numeric").alias("value"))


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
    start = _utc(start) if start is not None else None
    end = _utc(end) if end is not None else None
    if start is not None:
        tall = tall.filter(pl.col("time") >= start)
    if end is not None:
        tall = tall.filter(pl.col("time") <= end)
    if limit is not None:
        tall = (
            tall.sort("time", descending=order == "desc")
            .group_by(["data_alias", "property_uri", "stream_uri"], maintain_order=True)
            .head(limit)
        )
    return tall


def _with_mapped_column(tall: pl.DataFrame, keys: list[str], fn, name: str) -> pl.DataFrame:
    """Add column ``name`` = ``fn(*key values)``.

    ``fn`` runs once per distinct key, not once per row: a frame of a million
    readings over ten points costs ten calls. The result is joined back, so
    the row order of ``tall`` is kept.
    """
    distinct = tall.select(keys).unique()
    values = [fn(*row) for row in distinct.iter_rows()]
    mapping = distinct.with_columns(pl.Series(name, values, dtype=pl.Utf8))
    return tall.join(mapping, on=keys, how="left")


def _pivot_split_values(tall: pl.DataFrame, pivot_key: str) -> pl.DataFrame:
    """Pivot numeric and text readings separately to retain their dtypes.

    A key with both kinds keeps its numeric column and gets a second column
    with the ``_text`` suffix. Join both pivots on the full set of timestamps.
    """
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
    """Lightweight metadata for a single (nid, property_uri, stream_uri) data binding.

    ``row_count`` / ``earliest`` / ``latest`` are stream statistics. A
    ``DataObject`` fills them on the first read of a statistic, not when it
    is built, so read them through ``DataObject.bindings``.
    """
    nid: int
    property_uri: str
    stream_uri: str
    alias: str
    entity_contexts: list[dict[str, str]]
    row_count: int = 0
    earliest: datetime | None = None
    latest: datetime | None = None
    property_unit: str | None = None    # unit URI from qudt:hasUnit on the property
    stream_unit: str | None = None         # unit URI from qudt:hasUnit on the ext reference
    property_label: str | None = None      # rdfs:label on the property (display name)
    source_id: str | None = None        # acq:sourceId on the ext reference (the datasource)
    stream_name: str | None = None         # acq:refName on the ext reference (the driver's stream name)
    view: str = "current"


def _graph_data_aliases(qg: QueryGraph | None) -> list[str]:
    """Aliases of the query graph's data nodes, as ``_from_query`` names them."""
    data_nodes = getattr(qg, "data_nodes", None) or {}
    return sorted({qg.aliases_reverse.get(nid, str(nid)) for nid in data_nodes})


def _graph_entity_columns(qg: QueryGraph | None) -> list[str]:
    """``entity__<alias>`` for each non-data node of the query graph."""
    nodes = getattr(qg, "nodes", None) or {}
    data_nodes = getattr(qg, "data_nodes", None) or {}
    return sorted(
        f"entity__{qg.aliases_reverse.get(nid, str(nid))}"
        for nid in nodes if nid not in data_nodes
    )


@dataclass
class _StreamStats:
    """Statistics for every stream of one ``data()`` call, fetched once.

    The root ``DataObject`` owns one and hands the same instance to the
    sub-objects ``by()`` and ``convert_to()`` derive, so whichever of them
    reads a statistic first fetches the whole family's in batches per read view,
    and the rest share those results. ``exists`` is the cheaper answer
    ``is_empty()`` needs, which streams have any row, cached the same way.
    ``refresh()`` forgets both.
    """
    client: AcquiriumClient | None
    targets: list[tuple[str, str]]  # (physical/logical reference, read view)
    epoch: int | None = None
    info: dict[tuple[str, str], Any] | None = None
    exists: set[tuple[str, str]] | None = None

    def _groups(self):
        for view in sorted({view for _, view in self.targets}):
            yield view, sorted({ref for ref, mode in self.targets if mode == view})

    def get(self):
        if self.info is None:
            result = {}
            if self.client is not None:
                for view, refs in self._groups():
                    info = self.client.timeseries_info_batch(refs, view=view, epoch=self.epoch)
                    result.update({(ref, view): value for ref, value in info.items()})
            self.info = result
        return self.info

    def with_rows(self) -> set[tuple[str, str]]:
        """The stream and view pairs that have any row."""
        if self.info is not None:
            return {target for target, info in self.info.items() if info.row_count > 0}
        if self.exists is None:
            result = set()
            if self.client is not None:
                for view, refs in self._groups():
                    found = self.client.timeseries_exists_batch(refs, view=view, epoch=self.epoch)
                    result.update((ref, view) for ref in found)
            self.exists = result
        return self.exists

    def refresh(self) -> None:
        self.info = None
        self.exists = None


@accepts_old_names
@dataclass
class DataObject:
    """Lazy, alias-driven structured access to sensor data.

    Construction resolves SPARQL bindings and pins an epoch for replacement
    resolution, units and sample reads.
    Per-stream statistics (row count, time range) are fetched in batches per view the
    first time ``total_rows``, ``time_range``, or ``bindings`` needs them;
    ``is_empty()`` (before materialization) asks only which streams have a
    row, a cheaper request. Each request covers every stream of the
    ``data()`` call, and the sub-objects ``by()`` and ``convert_to()``
    return share its result; ``refresh_stats()`` forgets both. The actual
    time-series data is materialized on demand when ``__getitem__``,
    ``dataframe()``, or ``latest()`` are called.

    Lightweight operations such as ``aliases``, ``metadata()``,
    ``stream_info()``, and ``by()`` work without fetching either.
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

    # Stream statistics, shared by reference with the sub-objects by() and
    # convert_to() derive; _stats_bindings() fills them in on first use. An
    # object built without one makes its own from its bindings.
    _stats: _StreamStats | None = field(default=None, repr=False)

    # Conversions recorded by convert_to() on a lazy object, applied when the
    # values are fetched: ((alias, property_uri, stream_uri), from_uri, to_uri),
    # one per binding.
    _pending_conversions: list[tuple[tuple[str, str, str], str, str]] = field(
        default_factory=list, repr=False
    )

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def _empty(cls, qg: QueryGraph, *, cast_value: str | None = "float",
               client: "AcquiriumClient | None" = None) -> DataObject:
        # No rows to derive the entity columns from, so take them from the
        # query graph: the empty result keeps the shape of a non-empty one.
        obj = cls(
            _bindings=[],
            _entity_columns=_graph_entity_columns(qg),
            _query_graph=qg,
            _client=client,
            _materialized=True,
        )
        obj._tall = obj._empty_tall()
        return obj

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
        cast_value: str | None = None,
        value_mode: str = "default",
        view: str | None = None,
    ) -> DataObject:
        """Resolve a metadata query to stream bindings and record their engine epoch.

        Lazy sample reads request the same epoch as the plan that selected
        the stream views.
        """
        qg = query.query_graph
        _validate_cast_value(cast_value)
        if view is not None and view not in VIEWS:
            raise ValueError("view must be 'current', 'raw', or 'stored'")

        if not getattr(qg, "data_nodes", None):
            return cls._empty(qg, cast_value=cast_value, client=query.client)

        property_stream_uris, entity_context, prop_units, ext_stream_units, property_labels, stream_ids = _parse_sparql_bindings(
            query,
            include_dependencies=include_dependencies,
        )

        if not property_stream_uris:
            return cls._empty(qg, cast_value=cast_value, client=query.client)

        # Determine all entity column names across every context dict
        all_entity_cols: set[str] = set()
        for ctx_list in entity_context.values():
            for ctx in ctx_list:
                all_entity_cols.update(ctx.keys())
        entity_columns = sorted(all_entity_cols)

        # By default, app and explicit reference selectors read the stored
        # stage. Other selectors follow the replacement chain.
        bindings: list[BindingInfo] = []
        for nid, property_uri, stream_uri in property_stream_uris:
            alias = qg.aliases_reverse.get(nid, str(nid))
            key = (nid, property_uri, stream_uri)
            contexts = entity_context.get(key, [{}])
            unique_contexts = _deduplicate_contexts(contexts)
            bindings.append(BindingInfo(
                nid=nid,
                property_uri=property_uri,
                stream_uri=stream_uri,
                alias=alias,
                entity_contexts=unique_contexts,
                property_unit=prop_units.get(key),
                stream_unit=ext_stream_units.get(key),
                property_label=property_labels.get(key),
                source_id=stream_ids.get(key, (None, None))[0],
                stream_name=stream_ids.get(key, (None, None))[1],
                view=view or ("stored" if (
                    "app" in qg.data_nodes[nid].filters
                    or qg.data_nodes[nid].refs is not None
                    or any("app" in expr.attributes() for expr in qg.data_nodes[nid].exprs)
                ) else "current"),
            ))

        resolved = query.client.stream_views(sorted({b.stream_uri for b in bindings}))
        epoch, streams = resolved["epoch"], resolved["streams"]
        redirected_raw = {
            streams[b.stream_uri]["raw"] for b in bindings
            if b.view == "raw" and streams[b.stream_uri]["raw"] != b.stream_uri
        }
        raw_units = query.client._stream_units(redirected_raw) if redirected_raw else {}

        # Keep one logical measurement per node and entity context. Prefer
        # the base point's label and units when both it and a stage matched.
        logical = {}
        for binding in sorted(bindings, key=lambda b: b.stream_uri != streams[b.stream_uri]["raw"]):
            stream = streams[binding.stream_uri]
            ref = binding.stream_uri if binding.view == "stored" else stream["raw"]
            # A current read returns the final stage's values, in that stage's unit.
            unit = stream["unit"] if binding.view == "current" else None
            units = dict(property_unit=unit, stream_unit=unit) if unit else {}
            if binding.view == "raw" and ref != binding.stream_uri:
                property_unit, stream_unit = raw_units.get(ref, (None, None))
                units = dict(property_unit=property_unit, stream_unit=stream_unit)
            for context in binding.entity_contexts or [{}]:
                key = (binding.nid, ref, binding.view, tuple(sorted(context.items())))
                logical.setdefault(key, replace(binding, stream_uri=ref, entity_contexts=[context], **units))
        merged = {}
        for binding in logical.values():
            key = (binding.nid, binding.property_uri, binding.stream_uri, binding.view)
            if key in merged:
                merged[key].entity_contexts.extend(binding.entity_contexts)
            else:
                merged[key] = binding
        bindings = list(merged.values())

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
                "epoch": epoch,
            },
            _tall=None,
            _materialized=False,
            _stats=_StreamStats(query.client, sorted({(b.stream_uri, b.view) for b in bindings}), epoch),
        )

    def _stats_bindings(self) -> list[BindingInfo]:
        """The bindings with row_count / earliest / latest filled in.

        Reads the shared statistics cache, fetching it on first use: one
        batch request covering every stream of the ``data()`` call, which
        counts each stream's samples on the server. Only the statistic
        readers call this.
        """
        if not self._bindings:
            return []
        stats = self._shared_stats().get()
        filled: list[BindingInfo] = []
        for b in self._bindings:
            info = stats.get((b.stream_uri, b.view))
            filled.append(replace(
                b,
                row_count=info.row_count if info else 0,
                earliest=info.earliest if info else None,
                latest=info.latest if info else None,
            ))
        return filled

    def _shared_stats(self) -> _StreamStats:
        """The statistics cache, made from this object's bindings if it has none."""
        if self._stats is None:
            self._stats = _StreamStats(
                self._client, sorted({(b.stream_uri, b.view) for b in self._bindings}),
                self._query_params.get("epoch"),
            )
        return self._stats

    @property
    def _stats_loaded(self) -> bool:
        """Whether the statistics are in the cache (no fetch)."""
        return self._stats is not None and self._stats.info is not None

    def refresh_stats(self) -> None:
        """Forget the cached stream statistics.

        The next read of ``total_rows``, ``time_range``, ``bindings`` or
        ``is_empty()`` asks the server again at the sample epoch. Sub-objects
        of the same ``data()`` call share the refreshed cache.
        """
        if self._stats is not None:
            self._stats.refresh()

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

    def _property_labels(self) -> dict[str, str]:
        """``{property_uri: rdfs:label}`` for display naming.

        A label shared by several distinct points is dropped so pivot
        columns for different points can never collapse into one; those
        points fall back to their URI-based names.
        """
        pairs = {(b.property_uri, b.property_label) for b in self._bindings if b.property_label}
        counts: dict[str, int] = {}
        for _, lbl in pairs:
            counts[lbl] = counts.get(lbl, 0) + 1
        return {uri: lbl for uri, lbl in pairs if counts[lbl] == 1}

    @staticmethod
    def _key(b: BindingInfo) -> tuple[str, str, str]:
        return (b.alias, b.property_uri, b.stream_uri)

    @staticmethod
    def _binding_mask(key: tuple[str, str, str]) -> pl.Expr:
        alias, property_uri, stream_uri = key
        return (
            (pl.col("data_alias") == alias)
            & (pl.col("property_uri") == property_uri)
            & (pl.col("stream_uri") == stream_uri)
        )

    def _effective_unit(self, b: BindingInfo) -> str | None:
        """Unit the binding's values are (or will be) expressed in.

        The property's annotation, else the external reference's unit (Case
        3.2), else None; overlaid with any conversion recorded for this
        binding by ``convert_to()`` that has not been applied yet.
        """
        unit = b.property_unit or b.stream_unit
        key = self._key(b)
        for k, _, to_unit in self._pending_conversions:
            if k == key:
                unit = to_unit
        return unit

    def _empty_tall(self) -> pl.DataFrame:
        """A zero-row tall frame in the same column order as a fetched one."""
        schema: dict[str, Any] = {
            "data_alias": pl.Utf8,
            "property_uri": pl.Utf8,
            "stream_uri": pl.Utf8,
        }
        for ec in self._entity_columns:
            schema[ec] = pl.Utf8
        schema.update({
            "time": pl.Datetime(time_zone="UTC"),
            "value_numeric": pl.Float64,
            "value_text": pl.Utf8,
        })
        return pl.DataFrame(schema=schema)

    def _fetch_tall(
        self,
        bindings: list[BindingInfo],
        *,
        start,
        end,
        limit: int | None,
        order: str,
    ) -> pl.DataFrame:
        """Fetch ``bindings``' rows into a tall frame.

        Bindings with the same stream URI and view share one sample request,
        bounded by the given window. Values are auto-converted from the
        external reference's unit to the annotated unit, then the
        conversions recorded by ``convert_to()`` are applied.
        The frame has one row per observation and entity context, in the
        standard tall layout. Does not touch ``self``.
        """
        cast_value = self._query_params.get("cast_value")
        value_mode = self._query_params.get("value_mode", "default")

        # Cache conversion factors to avoid repeated server calls
        _factors_cache: dict[tuple[str, str], dict] = {}

        def _get_factors(from_u: str, to_u: str) -> dict:
            key = (from_u, to_u)
            if key not in _factors_cache:
                _factors_cache[key] = self._client.get_conversion_factors(from_u, to_u)
            return _factors_cache[key]

        # Read the epoch used to select the replacement and its units.
        epoch = self._query_params.get("epoch")
        if epoch is None:
            epoch = self._client.engine_snapshot()["epoch"]
        bounds = inclusive_bounds(start, end)
        # One request per stream. A stream matched by several data nodes
        # (a pump's measurement node and its connection point's, say) has one
        # binding per node; they share the fetched frame and differ only in
        # alias, point, unit and entity columns below.
        fetched: dict[tuple[str, str], pl.DataFrame] = {}
        for binding in bindings:
            key = (binding.stream_uri, binding.view)
            if key not in fetched:
                fetched[key] = self._client.timeseries_df(
                    binding.stream_uri,
                    **bounds,
                    limit=limit,
                    order=order,
                    value_mode=value_mode,
                    epoch=epoch,
                    view=binding.view,
                )

        frames: list[pl.DataFrame] = []
        for binding in bindings:
            df = fetched[(binding.stream_uri, binding.view)]
            if df.is_empty():
                continue

            df = df.rename({"ts": "time", "uri": "stream_uri"})
            df = df.with_columns(
                pl.lit(binding.alias).alias("data_alias"),
                pl.lit(binding.property_uri).alias("property_uri"),
            )
            # Drop the stream_uri from timeseries (use the one from SPARQL)
            df = df.drop("stream_uri")
            df = df.with_columns(pl.lit(binding.stream_uri).alias("stream_uri"))

            # --- Case 3 auto-conversion: raw rows are stored in the external
            # reference's unit; the point's annotation is the unit of record.
            # Conversions requested with convert_to() are applied afterwards.
            target_unit = binding.property_unit
            if binding.stream_unit and target_unit and binding.stream_unit != target_unit:
                # The user did not ask for this conversion, so a failure is a
                # warning they can see; the raw values are returned as stored.
                try:
                    factors = _get_factors(binding.stream_unit, target_unit)
                    if factors.get("compatible", False):
                        df = self._apply_conversion(
                            df.with_columns(pl.col("value").cast(pl.Float64)), factors
                        )
                    else:
                        warnings.warn(
                            f"point <{binding.property_uri}> is annotated {target_unit} but its "
                            f"stream is stored in {binding.stream_unit}, which does not convert; "
                            f"the values are returned in {binding.stream_unit}",
                            UserWarning,
                            stacklevel=4,
                        )
                except Exception as e:
                    warnings.warn(
                        f"could not convert point <{binding.property_uri}> from its stored unit "
                        f"{binding.stream_unit} to its annotation {target_unit} ({e}); the values "
                        f"are returned in {binding.stream_unit}",
                        UserWarning,
                        stacklevel=4,
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
            return self._empty_tall()

        tall = pl.concat([_split_value_column(df, cast_value) for df in frames], how="vertical")

        # Apply the conversions convert_to() recorded on a lazy object. The
        # user asked for these, so a failure is an error, not a frame in
        # the wrong unit.
        for key, from_unit, to_unit in self._pending_conversions:
            alias, property_uri, _ref = key
            try:
                factors = _get_factors(from_unit, to_unit)
            except Exception as e:
                raise ValueError(
                    f"convert_to: could not resolve {from_unit} -> {to_unit} for point "
                    f"<{property_uri}> (alias '{alias}'): {e}"
                ) from e
            if not factors.get("compatible", False):
                raise ValueError(
                    f"convert_to: {from_unit} does not convert to {to_unit} for point "
                    f"<{property_uri}> (alias '{alias}')"
                )
            mask = self._binding_mask(key)
            converted = self._apply_conversion(tall.filter(mask), factors)
            tall = pl.concat([tall.filter(~mask), converted], how="vertical")

        # Reorder columns for consistency
        base_cols = ["data_alias", "property_uri", "stream_uri"] + self._entity_columns + ["time", "value_numeric", "value_text"]
        existing = [c for c in base_cols if c in tall.columns]
        return tall.select(existing)

    def _materialize(self) -> None:
        """Fetch all time-series data and populate _tall. Idempotent."""
        if self._materialized:
            return
        if not self._bindings:
            self._tall = self._empty_tall()
            self._materialized = True
            return

        self._tall = self._fetch_tall(
            self._bindings,
            start=self._query_params.get("start"),
            end=self._query_params.get("end"),
            limit=self._query_params.get("limit"),
            order=self._query_params.get("order", "asc"),
        )
        if self._pending_conversions:
            # Values are now in the requested units; record that on the
            # bindings so units() and later convert_to() calls start there.
            targets = {key: to_unit for key, _, to_unit in self._pending_conversions}
            self._bindings = [
                replace(b, property_unit=targets[self._key(b)]) if self._key(b) in targets else b
                for b in self._bindings
            ]
        self._materialized = True

    def _cast_output(self, df: pl.DataFrame, columns: list[str] | None = None) -> pl.DataFrame:
        """Apply ``cast_value="int"`` to the numeric columns of a result frame.

        Values are kept as ``Float64`` internally; a result asked for as
        ``int`` is cast here. The cast truncates toward zero and turns
        non-finite values into nulls, and a column where that loses
        information raises a ``UserWarning`` naming the column and one of
        the affected values.
        """
        if self._query_params.get("cast_value") != "int":
            return df
        candidates = columns if columns is not None else df.columns
        for col in candidates:
            if col not in df.columns or df.schema[col] != pl.Float64:
                continue
            s = df[col]
            lossy = s.filter(s.is_not_null() & (~s.is_finite() | (s != s.round(0))))
            if lossy.len():
                warnings.warn(
                    f"cast_value='int' truncates {lossy.len()} non-integer value(s) in "
                    f"column {col!r}, such as {lossy[0]!r}; use cast_value='float' to keep them",
                    UserWarning,
                    stacklevel=3,
                )
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))
        return df

    # ------------------------------------------------------------------
    # Alias-based access (triggers materialization)
    # ------------------------------------------------------------------

    def __contains__(self, alias: object) -> bool:
        """``alias in data`` — answered from the bindings, without a fetch."""
        return alias in self.aliases

    def __iter__(self) -> Iterator[str]:
        """Iterate over the data aliases, like a dict over its keys."""
        return iter(self.aliases)

    def __len__(self) -> int:
        """Number of data aliases."""
        return len(self.aliases)

    def __getitem__(self, alias: str) -> pl.DataFrame:
        """Return time-series for a data alias.

        Multiple stream_uris that share the same property_uri are combined
        (first-wins). The result is ``[time, value, property_uri]``, whether
        the alias resolves to one point or to several. An alias that exists
        but has no rows in the window returns an empty frame with the same
        columns.

        Raises:
            KeyError: If ``alias`` is not one of :attr:`aliases`.
        """
        if alias not in self.aliases:
            raise KeyError(f"Data alias '{alias}' not found. Available: {self.aliases}")
        self._materialize()
        subset = self._tall.filter(pl.col("data_alias") == alias)
        if subset.is_empty():
            return pl.DataFrame(schema={"time": pl.Datetime(time_zone="UTC"), "value": pl.Float64,
                                        "property_uri": pl.String})

        # Combine stream_uris that share the same (property_uri, time). maintain_order
        # is what makes keep="first" mean the first row of _tall; without it
        # polars picks an arbitrary duplicate and shuffles the surviving rows.
        subset = subset.unique(subset=["property_uri", "time"], keep="first", maintain_order=True)
        subset = _restore_single_value_column(subset)
        subset = self._cast_output(subset, ["value"])
        # (time, property_uri) is unique after the dedup above, so sorting on both
        # gives a total order — repeated access returns identical frames.
        return subset.select("time", "value", "property_uri").sort(["time", "property_uri"])

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
                    _stats=self._stats,
                )
            return

        # Lazy path: group bindings by entity context. A binding whose point
        # hangs off several entities is narrowed to the contexts of the entity
        # it is grouped under, so the sub-object materializes only that
        # entity's rows.
        entity_to_bindings: dict[str, list[BindingInfo]] = {}
        for b in self._bindings:
            by_entity: dict[str, list[dict[str, str]]] = {}
            for ctx in b.entity_contexts:
                entity_uri = ctx.get(col)
                if entity_uri is not None:
                    by_entity.setdefault(entity_uri, []).append(ctx)
            for entity_uri, contexts in by_entity.items():
                entity_to_bindings.setdefault(entity_uri, []).append(
                    replace(b, entity_contexts=contexts)
                )

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
                _stats=self._stats,
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
        include_stream: bool = False,
        compact: bool = True,
        streams: "int | str | Stream | Sequence[int | str | Stream] | None" = None,
    ) -> pl.DataFrame:
        """Return a flat DataFrame.

        The parameters and defaults mirror :meth:`Query.dataframe`, so
        ``q.data(...).dataframe(...)`` equals ``q.dataframe(...)``. Here
        ``start``/``end``/``limit``/``order`` are applied client-side on the
        already-fetched data (``limit`` keeps rows *per stream*, in
        ``order`` direction, which also orders the output); the same
        parameters on :meth:`Query.data` bound the fetch itself.

        - ``shape="wide"`` (default): pivots to ``[time, <one column per
          stream>]``. Multiple stream_uris that share the same property_uri are
          combined (first-wins). A column is named ``alias__point``,
          whether the alias matched one point or several.
        - ``shape="narrow"``: one row per observation.

        ``compact=True`` (default) renders URIs as CURIEs and identifies
        points with a ``property_id`` column rather than the raw
        ``property_uri``/``stream_uri``; auto-aliased data nodes are named by the
        point's ``rdfs:label`` when it has one, else by their compacted
        point-local name. In this mode the narrow layout is
        ``["data_alias", "property_id", "time", "value_numeric", "value_text"]``
        and ``include_stream=True`` adds the compacted ``stream`` column.

        ``compact=False`` returns the raw layout: narrow is the internal
        tall frame (every ``(data_alias, property_uri, stream_uri, time)`` row
        preserved with entity columns), wide keeps alias/URI column names.

        ``streams`` keeps only the listed streams: numbers from the
        ``stream`` column of :meth:`stream_info`, stream URIs,
        :class:`~acquirium.Client.streams.Stream` handles from
        ``ac.stream(source_id, stream_name)``, or a mix. A point fed by several streams then shows just the chosen ones, so
        ``streams=[1]`` reads one driver's values for a point that has two.
        An unknown number or URI raises ``KeyError``.

        With ``cast_value="int"`` on the query the value columns come back as
        ``Int64``; a column whose values lose their fraction raises a
        ``UserWarning``.
        """
        frame = self._build_dataframe(shape, start=start, end=end, limit=limit, order=order,
                                      include_stream=include_stream, compact=compact, streams=streams)
        return self._cast_output(frame)

    def _resolve_streams(self, streams) -> set[str] | None:
        """Turn ``streams`` (numbers from :meth:`stream_info`, stream URIs,
        ``Stream`` handles, or a mix) into the set of stream URIs they name;
        ``None`` means all."""
        if streams is None:
            return None
        if isinstance(streams, (int, str, Stream)):
            streams = [streams]
        table = self.stream_info()
        by_number = dict(zip(table["stream"].to_list(), table["stream_uri"].to_list()))
        known = set(by_number.values())
        refs: set[str] = set()
        for item in streams:
            if isinstance(item, Stream):
                item = item.stream_uri
            if isinstance(item, bool) or not isinstance(item, (int, str)):
                raise TypeError(f"streams: expected a stream number, stream URI or Stream, got {item!r}")
            if isinstance(item, int):
                if item not in by_number:
                    raise KeyError(f"stream {item} not in this object; stream_info() lists 0..{len(by_number) - 1}")
                refs.add(by_number[item])
            else:
                if item not in known:
                    raise KeyError(f"stream URI {item!r} not in this object; see stream_info()")
                refs.add(item)
        return refs

    def _build_dataframe(self, shape, *, start, end, limit, order, include_stream, compact,
                         streams=None) -> pl.DataFrame:
        """Select streams and apply the read window before reshaping the sampled rows."""
        refs = self._resolve_streams(streams)
        self._materialize()

        descending = order == "desc"
        tall = self._tall if refs is None else self._tall.filter(pl.col("stream_uri").is_in(list(refs)))
        windowed = _apply_window(tall, start, end, limit, order)

        if windowed.is_empty():
            return windowed

        if compact:
            return self._compact_dataframe(windowed, shape=shape, include_stream=include_stream,
                                           descending=descending)

        if shape == "narrow":
            return windowed.sort("time", descending=descending)

        tall = windowed.clone()

        # Combine stream_uris sharing the same (data_alias, property_uri, time):
        # we want one row per property_uri at each timestamp.
        tall = tall.unique(subset=["data_alias", "property_uri", "time"], keep="first")

        # A column is named alias__point whether the alias matched one point
        # or several, so the schema does not depend on how many matched.
        auto_aliases = {b.alias for b in self._bindings if b.alias == str(b.nid)}
        labels = self._property_labels()

        def _pivot_key(alias: str, property_uri: str) -> str:
            label = labels.get(property_uri)
            if alias in auto_aliases and label:
                return label
            if label:
                return f"{alias}__{label}"
            try:
                return f"{alias}__{self._client.compact_uri(property_uri)}"
            except Exception:
                # we shouldn't come here
                return f"{alias}__{property_uri}"

        tall = _with_mapped_column(tall, ["data_alias", "property_uri"], _pivot_key, "_pivot_key")

        wide = _pivot_split_values(tall, "_pivot_key")
        return wide.sort("time", descending=True) if descending else wide

    def _compact_dataframe(self, tall: pl.DataFrame, *, shape: str, include_stream: bool,
                           descending: bool = False) -> pl.DataFrame:
        """Query-compatible layout: CURIE-rendered URIs and a ``property_id`` column.

        Auto-aliased data nodes (alias == ``str(nid)``) are named by the
        point's ``rdfs:label`` when it has one, else by its compacted
        point-local name; under a user alias every point is named
        ``f"{alias}__{label_or_point_local}"``, also when the alias matched
        a single point, so the schema does not depend on how many matched.
        """
        def _compact(uri: str) -> str:
            try:
                return self._client.compact_uri(uri)
            except Exception:
                return uri

        # One row per observation. A point fed by several refs keeps every
        # ref's row; ``include_stream`` only decides whether the ref is shown.
        tall = tall.clone().unique(
            subset=["data_alias", "property_uri", "stream_uri", "time"], keep="first"
        )

        auto_aliases = {b.alias for b in self._bindings if b.alias == str(b.nid)}
        labels = self._property_labels()

        def _label(alias: str, property_uri: str) -> str:
            if alias in auto_aliases:
                return labels.get(property_uri) or _compact(property_uri)
            return f"{alias}__{labels.get(property_uri) or _compact(property_uri)}"

        tall = _with_mapped_column(tall, ["data_alias", "property_uri"], _label, "_label")

        if shape == "narrow":
            out = _with_mapped_column(tall, ["property_uri"], _compact, "property_id")
            out = out.with_columns(pl.col("_label").alias("data_alias"))
            cols = ["data_alias", "property_id", "time", "value_numeric", "value_text"]
            if include_stream:
                out = _with_mapped_column(out, ["stream_uri"], _compact, "stream")
                cols.insert(2, "stream")
            return out.select(cols).sort("time", descending=descending)

        # wide
        wide = _pivot_split_values(tall, "_label")
        return wide.sort("time", descending=True) if descending else wide

    # ------------------------------------------------------------------
    # Metadata & introspection (no materialization needed)
    # ------------------------------------------------------------------

    def metadata(self, *, include_stream_uris: bool = False) -> pl.DataFrame:
        """Return a DataFrame of unique ``(data_alias, property_label,
        property_uri, unit, entity__*)`` tuples.

        Built from the query's bindings, so the answer is the same before
        and after the values are fetched and lists every matched point,
        including those with no rows in the requested window.
        ``property_label`` is the point's ``rdfs:label`` (null when it has
        none); ``unit`` is the unit the point's values are expressed in
        (null when unannotated). By default the UUID ``stream_uri`` column is hidden — multiple
        stream_uris sharing the same point are folded into one row. Pass
        ``include_stream_uris=True`` to keep the per-ref breakdown (useful for
        per-ref filtering and debugging).
        """
        ref_col = ["stream_uri", "source_id", "stream_name"] if include_stream_uris else []
        cols = ["data_alias", "property_label", "property_uri", "unit"] + ref_col + self._entity_columns
        rows: list[dict[str, str | None]] = []
        for b in self._bindings:
            for ctx in (b.entity_contexts or [{}]):
                row: dict[str, str | None] = {
                    "data_alias": b.alias,
                    "property_label": b.property_label,
                    "property_uri": b.property_uri,
                    "unit": self._effective_unit(b),
                }
                if include_stream_uris:
                    row["stream_uri"] = b.stream_uri
                    row["source_id"] = b.source_id
                    row["stream_name"] = b.stream_name
                for ec in self._entity_columns:
                    row[ec] = ctx.get(ec)
                rows.append(row)
        schema = {c: pl.Utf8 for c in cols}
        if not rows:
            return pl.DataFrame(schema=schema)
        return pl.DataFrame(rows, schema=schema).unique().sort(cols)

    @property
    def aliases(self) -> list[str]:
        """List of data aliases present in this DataObject.

        An empty result has no bindings, so its aliases come from the query
        graph's data nodes.
        """
        if self._bindings:
            return sorted(set(b.alias for b in self._bindings))
        return _graph_data_aliases(self._query_graph)

    @property
    def entity_aliases(self) -> list[str]:
        """List of entity alias names (without the ``entity__`` prefix)."""
        return [c.removeprefix("entity__") for c in self._entity_columns]

    def stream_info(self, alias: str | None = None) -> pl.DataFrame:
        """The streams behind the object, one row each.

        Columns: ``stream`` (a number to pass as ``streams=`` to
        :meth:`dataframe` or :meth:`latest`, valid for this object), ``alias``, ``property_uri``, ``stream_uri``, ``source_id``
        and ``stream_name``, the last two being the datasource and stream name
        the driver registered. With ``alias`` only that alias's streams are
        listed, numbered as in the full table.
        """
        if alias is not None and alias not in self.aliases:
            raise KeyError(f"Data alias '{alias}' not found. Available: {self.aliases}")
        rows = sorted(
            {(b.alias, b.property_uri, b.stream_uri, b.source_id, b.stream_name) for b in self._bindings}
        )
        table = pl.DataFrame(
            rows, orient="row",
            schema={"alias": pl.Utf8, "property_uri": pl.Utf8, "stream_uri": pl.Utf8,
                    "source_id": pl.Utf8, "stream_name": pl.Utf8},
        ).with_row_index("stream")
        if alias is not None:
            table = table.filter(pl.col("alias") == alias)
        return table

    def latest(self, alias: str | None = None, *,
               streams: "int | str | Stream | Sequence[int | str | Stream] | None" = None) -> pl.DataFrame:
        """Return the newest reading of each point.

        Answers from its own fetch (one row per stream, newest first), so
        the result does not depend on the ``limit``/``order`` the object was
        built with, and the full window is never materialized. The object's
        ``start``/``end`` bounds still apply: the newest reading inside them.
        A point fed by several refs reports its newest row across them.

        With ``alias`` the frame is ``[time, value, property_uri]``, as with
        ``__getitem__``. Without it every binding is reported as
        ``[data_alias, property_uri, time, value]``. ``streams`` restricts the
        fetch to the listed streams, as in :meth:`dataframe`.
        """
        refs = self._resolve_streams(streams)
        if alias is not None:
            if alias not in self.aliases:
                raise KeyError(f"Data alias '{alias}' not found. Available: {self.aliases}")
            bindings = [b for b in self._bindings if b.alias == alias]
        else:
            bindings = list(self._bindings)
        if refs is not None:
            bindings = [b for b in bindings if b.stream_uri in refs]

        if bindings and self._client is not None:
            tall = self._fetch_tall(
                bindings,
                start=self._query_params.get("start"),
                end=self._query_params.get("end"),
                limit=1,
                order="desc",
            )
        else:
            tall = self._empty_tall()

        newest = (
            tall.sort("time", descending=True)
            .unique(subset=["data_alias", "property_uri"], keep="first", maintain_order=True)
        )
        newest = self._cast_output(_restore_single_value_column(newest), ["value"])

        if alias is None:
            return newest.select("data_alias", "property_uri", "time", "value").sort("data_alias", "property_uri")
        return newest.select("time", "value", "property_uri").sort("property_uri")

    def is_empty(self) -> bool:
        """Whether any rows exist.

        Before the values are fetched this answers whether the matched
        streams hold any rows at all, regardless of the requested window. It
        reads the stream statistics if they are loaded; otherwise it asks the
        server only which streams have a row, one request shared with the
        sub-objects of the same ``data()`` call, without counting them.
        After a fetch it reports the fetched window.
        """
        if self._materialized:
            return self._tall.is_empty()
        if not self._bindings:
            return True
        with_rows = self._shared_stats().with_rows()
        return not any((b.stream_uri, b.view) in with_rows for b in self._bindings)

    # ------------------------------------------------------------------
    # Stats (fetched on first use, no materialization needed)
    # ------------------------------------------------------------------

    @property
    def total_rows(self) -> int:
        """Total row count across all bindings (from stats, no materialization)."""
        return sum(b.row_count for b in self._stats_bindings())

    @property
    def time_range(self) -> tuple[datetime | None, datetime | None]:
        """Overall (earliest, latest) across all bindings (from stats)."""
        bindings = self._stats_bindings()
        earliests = [b.earliest for b in bindings if b.earliest is not None]
        latests = [b.latest for b in bindings if b.latest is not None]
        return (min(earliests) if earliests else None, max(latests) if latests else None)

    @property
    def bindings(self) -> list[BindingInfo]:
        """Read-only access to binding metadata, stream statistics included."""
        return self._stats_bindings()

    # ------------------------------------------------------------------
    # Unit conversion
    # ------------------------------------------------------------------

    def units(self) -> dict[str, list[str | None]]:
        """Return ``{alias: [unit_uri, ...]}``: the distinct units of each
        alias's points, in binding order.

        An alias whose points all share one unit gives a one-element list.
        ``None`` stands for a point with no unit annotation. The per-point
        view is the ``unit`` column of :meth:`metadata`.
        """
        out: dict[str, list[str | None]] = {}
        for b in self._bindings:
            units = out.setdefault(b.alias, [])
            unit = self._effective_unit(b)
            if unit not in units:
                units.append(unit)
        return out

    def _check_values_in_annotated_unit(self, b: BindingInfo) -> None:
        """Raise if ``b``'s values could not be brought to its annotated unit.

        A stream stored in a unit that does not convert to the point's
        annotation is read as stored, with a warning. Its values are then
        not in the unit ``units()`` reports, so ``convert_to()`` must not
        use that unit as the source. Registration rejects such a pair; this
        guards a model edited afterwards.
        """
        stored, annotated = b.stream_unit, b.property_unit
        if not stored or not annotated or stored == annotated:
            return
        if not self._client._units_compatible(stored, annotated):
            raise ValueError(
                f"convert_to: point <{b.property_uri}> (alias '{b.alias}') is annotated "
                f"{annotated} but its stream is stored in {stored}, which does not convert, "
                f"so its values are still in {stored}. Correct the unit in the model, or "
                f"pass from_unit to state the unit of the values."
            )

    def convert_to(
        self,
        to_unit: str,
        *,
        from_unit: str | None = None,
        alias: str | None = None,
    ) -> "DataObject":
        """Return a new DataObject with values converted to the target unit.

        Each point converts from its own unit, so an alias whose points are
        annotated differently still ends up in one unit.

        Args:
            to_unit: Target unit identifier (URI, label, symbol, or UCUM code).
            from_unit: Source unit identifier for every affected point. If
                None, each point's own unit is used (Case 2). A point with
                no unit annotation then raises — pass ``from_unit``
                explicitly (Case 1).
            alias: Apply conversion only to this alias. If None, all aliases
                are converted.

        Returns:
            A **new** DataObject with converted values. The original is not
            modified.

        Raises:
            KeyError: If ``alias`` is not one of :attr:`aliases`.
            ValueError: If ``from_unit`` is None and a point has no unit
                annotation, or its stream is stored in a unit that does not
                convert to the annotation; or if the units are incompatible.
        """
        if self._query_params.get("cast_value") == "str":
            raise ValueError("convert_to() needs numeric values; this object was built with cast_value='str'")
        if self._client is None:
            raise ValueError("Cannot convert units without a client connection")
        if alias is not None and alias not in self.aliases:
            raise KeyError(f"Data alias '{alias}' not found. Available: {self.aliases}")
        affected = [b for b in self._bindings if alias is None or b.alias == alias]

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

        # One conversion per binding, with resolved URIs so the
        # pending/materialize paths only ever see exact units.
        conversions: list[tuple[tuple[str, str, str], str, str]] = []
        for b in affected:
            if from_unit is None:
                self._check_values_in_annotated_unit(b)
            src = from_unit if from_unit is not None else self._effective_unit(b)
            if src is None:
                raise ValueError(
                    f"No unit annotation found for point <{b.property_uri}> "
                    f"(alias '{b.alias}'). Provide from_unit explicitly."
                )
            f = _factors(src, to_unit)
            conversions.append((self._key(b), f["from_uri"], f["to_uri"]))

        factors_cache: dict[tuple[str, str], dict] = {
            (f["from_uri"], f["to_uri"]): f for f in raw_pairs.values()
        }

        if self._materialized and self._tall is not None:
            # Eager path: convert the fetched rows of each binding.
            tall = self._tall.clone()
            for key, fu, tu in conversions:
                mask = self._binding_mask(key)
                converted = self._apply_conversion(tall.filter(mask), factors_cache[(fu, tu)])
                tall = pl.concat([tall.filter(~mask), converted], how="vertical")
            targets = {key: tu for key, _, tu in conversions}
            new_bindings = [
                replace(b, property_unit=targets[self._key(b)]) if self._key(b) in targets else b
                for b in self._bindings
            ]
            return DataObject(
                _bindings=new_bindings,
                _entity_columns=self._entity_columns,
                _query_graph=self._query_graph,
                _client=self._client,
                _query_params=self._query_params,
                _tall=tall,
                _materialized=True,
                _stats=self._stats,
            )

        # Lazy path: record the conversions for _materialize(). The bindings
        # keep their annotated units until the values are actually converted;
        # _effective_unit() overlays the pending targets meanwhile.
        pending = list(self._pending_conversions) + conversions
        return DataObject(
            _bindings=list(self._bindings),
            _entity_columns=self._entity_columns,
            _query_graph=self._query_graph,
            _client=self._client,
            _query_params=self._query_params,
            _tall=None,
            _materialized=False,
            _pending_conversions=pending,
            _stats=self._stats,
        )

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        aliases = self.aliases
        if self._materialized:
            # The tall frame holds one copy of a reading per entity context;
            # count readings, as the lazy estimate and dataframe() do.
            n_rows = 0
            if self._tall is not None and not self._tall.is_empty():
                n_rows = self._tall.select(["data_alias", "property_uri", "stream_uri", "time"]).n_unique()
            return f"DataObject({n_rows} rows, aliases={aliases}, entities={self.entity_aliases})"

        if not self._stats_loaded:
            # The repr never fetches: say how many streams, not how many rows.
            n_streams = len({b.stream_uri for b in self._bindings})
            return (f"DataObject(lazy, {n_streams} streams, aliases={aliases}, "
                    f"entities={self.entity_aliases})")
        total = self.total_rows
        earliest, latest = self.time_range
        time_str = ""
        if earliest and latest:
            time_str = f", range={earliest.isoformat()} to {latest.isoformat()}"
        return f"DataObject(lazy, ~{total} rows{time_str}, aliases={aliases}, entities={self.entity_aliases})"
