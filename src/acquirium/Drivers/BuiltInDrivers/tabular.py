"""Wide/narrow reshaping shared by the built-in tabular drivers.

These are plain functions, not a base class: a driver reads its own file format
however it likes, then calls :func:`to_observations` to get canonical
``(ts, ref_name, value)`` rows. Nothing here knows about drivers, config, or
the graph.
"""

from __future__ import annotations

import logging

import polars as pl

from acquirium.Drivers.Driver import safe_stream_name

logger = logging.getLogger("acquirium.tabular")

#: Tried in order when a timestamp column is text and ISO parsing finds nulls.
FALLBACK_DATE_FORMATS = (
    "%m/%d/%Y",   # US: 1/15/2025
    "%d/%m/%Y",   # European: 15/1/2025
    "%Y/%m/%d",   # ISO with slashes
    "%m-%d-%Y",   # US with dashes
    "%d-%m-%Y",   # European with dashes
    "%m/%d/%y",   # US 2-digit year
)


def parse_timestamps(col: pl.Series, date_format: str | None = None) -> pl.Series:
    """Return *col* as a UTC datetime series, whatever it started as.

    Naive datetimes are read as UTC. Text is parsed with ``date_format`` when
    given, else ISO, else the fallback formats — whichever yields the fewest
    unparseable rows. Values that never parse come back null for the caller to
    drop.
    """
    if col.dtype == pl.Date:
        return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")

    if col.dtype in (pl.String, pl.Utf8):
        if col.drop_nulls().len() == 0:
            return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        best, fewest_nulls = None, col.len() + 1
        for fmt in ([date_format] if date_format else []) + [None, *FALLBACK_DATE_FORMATS]:
            try:
                parsed = col.str.to_datetime(format=fmt, strict=False)
            except Exception:
                continue
            if parsed.null_count() < fewest_nulls:
                best, fewest_nulls = parsed, parsed.null_count()
            if fewest_nulls == 0:
                break
        col = best if best is not None else col.str.to_datetime(strict=False)

    tz = getattr(col.dtype, "time_zone", None)
    if tz is None:
        return col.dt.replace_time_zone("UTC")
    return col if tz == "UTC" else col.dt.convert_time_zone("UTC")


def to_observations(
    df: pl.DataFrame,
    *,
    time_col: str = "time",
    id_col: str = "id",
    value_col: str = "value",
    layout: str = "auto",
    date_format: str | None = None,
) -> pl.DataFrame:
    """Reshape a wide or narrow tabular frame into ``(ts, ref_name, value)``.

    ``layout`` is ``"wide"`` (one column per stream), ``"narrow"``
    (``time``/``id``/``value`` triples), or ``"auto"`` to pick narrow when both
    ``id_col`` and ``value_col`` are present. Stream names are normalised with
    :func:`safe_stream_name`, and rows with an unparseable timestamp or a null
    value are dropped.
    """
    if layout == "auto":
        layout = "narrow" if {id_col, value_col} <= set(df.columns) else "wide"
    if time_col not in df.columns:
        raise ValueError(f"time column '{time_col}' not found in {df.columns}")

    df = df.with_columns(parse_timestamps(df[time_col], date_format).alias("__ts"))
    unparsed = df["__ts"].null_count()
    if unparsed:
        logger.warning(
            'tabular: dropped %d row(s) with unparseable timestamps '
            '(hint: set date_format, e.g. "%%m/%%d/%%Y")', unparsed,
        )
        df = df.drop_nulls("__ts")

    if layout == "narrow":
        for col in (id_col, value_col):
            if col not in df.columns:
                raise ValueError(f"column '{col}' not found in {df.columns}")
        names = {
            str(raw): safe_stream_name(str(raw))
            for raw in df[id_col].drop_nulls().unique().to_list()
        }
        observations = df.select(
            pl.col("__ts").alias("ts"),
            pl.col(id_col).cast(pl.Utf8).replace_strict(names).alias("ref_name"),
            _as_text(df, value_col).alias("value"),
        ).drop_nulls("ref_name")
    else:
        streams = {c: safe_stream_name(c) for c in df.columns if c not in (time_col, "__ts")}
        observations = (
            df.select(
                pl.col("__ts").alias("ts"),
                *[_as_text(df, raw).alias(name) for raw, name in streams.items()],
            )
            .unpivot(index="ts", variable_name="ref_name", value_name="value")
        )
    return observations.drop_nulls("value")


def _as_text(df: pl.DataFrame, column: str) -> pl.Expr:
    """Cast a value column to text, treating float NaN as absent."""
    if df[column].dtype in (pl.Float32, pl.Float64):
        return pl.col(column).fill_nan(None).cast(pl.Utf8)
    return pl.col(column).cast(pl.Utf8, strict=False)
