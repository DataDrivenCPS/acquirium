"""Reusable timestamp conversion and wide/narrow observation reshaping.

These are plain functions, not a base class: a driver reads its own file format
however it likes, then calls :func:`to_observations` to get canonical
``(ts, ref_name, value)`` rows. Nothing here knows about drivers, config, or
the graph.
"""

from __future__ import annotations

import logging
import re

import polars as pl

logger = logging.getLogger("acquirium.tabular")


def to_timestamp(
    date_or_timestamp: pl.Series,
    time: pl.Series | None = None,
    *,
    date_format: str | None = None,
    timezone: str = "UTC",
    day_first: bool = False,
) -> pl.Series:
    """Convert one timestamp column, or separate date/time columns, to UTC.

    Native Date/Datetime values, ISO/RFC3339 text, common delimited date/time
    forms, and integer Unix epochs are accepted. ``date_format`` overrides the
    string heuristics. Naive values are interpreted in ``timezone`` and the
    result is always UTC. Set ``day_first`` to prefer day/month ordering when a
    delimited date is genuinely ambiguous.

    Passing ``time`` joins a common split date/clock representation before
    parsing. This is deliberately a plain data conversion helper: it does not
    know about driver configuration or stream registration.
    """
    col = date_or_timestamp
    if time is not None:
        col = pl.DataFrame({
            "__date": date_or_timestamp,
            "__time": time,
        }).select(
            pl.concat_str(
                pl.col("__date").cast(pl.Utf8),
                pl.col("__time").cast(pl.Utf8),
                separator=" ",
                ignore_nulls=False,
            ).alias(date_or_timestamp.name)
        ).to_series()

    if col.dtype == pl.Date:
        col = col.cast(pl.Datetime("us"))

    integer_types = (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    )
    if col.dtype in integer_types:
        non_null = col.drop_nulls()
        if non_null.is_empty():
            col = col.cast(pl.Datetime("us"))
        else:
            magnitude = abs(int(non_null.median()))
            unit = "s" if magnitude < 100_000_000_000 else "ms"
            if magnitude >= 100_000_000_000_000:
                unit = "us"
            if magnitude >= 100_000_000_000_000_000:
                unit = "ns"
            col = pl.from_epoch(col, time_unit=unit)

    if col.dtype in (pl.String, pl.Utf8):
        if col.drop_nulls().len() == 0:
            col = col.cast(pl.Datetime("us"))
        else:
            col = _parse_timestamp_text(col, date_format=date_format, day_first=day_first)

    if not isinstance(col.dtype, pl.Datetime):
        raise TypeError(f"unsupported timestamp dtype {col.dtype}")

    tz = getattr(col.dtype, "time_zone", None)
    if tz is None:
        return col.dt.replace_time_zone(
            timezone, ambiguous="null", non_existent="null"
        ).dt.convert_time_zone("UTC")
    return col if tz == "UTC" else col.dt.convert_time_zone("UTC")


def to_observations(
    df: pl.DataFrame,
    *,
    time_col: str | None = None,
    date_col: str | None = None,
    clock_col: str | None = None,
    id_col: str = "id",
    value_col: str = "value",
    layout: str,
    date_format: str | None = None,
    timezone: str = "UTC",
    day_first: bool = False,
) -> pl.DataFrame:
    """Reshape a wide or narrow tabular frame into ``(ts, ref_name, value)``.

    ``layout`` is ``"wide"`` (one column per stream) or ``"narrow"``
    (``time``/``id``/``value`` triples). Stream names are preserved exactly;
    source-specific mapping belongs in the driver. Rows with an unparseable
    timestamp or a null value are dropped.
    """
    if layout not in {"wide", "narrow"}:
        raise ValueError("layout must be explicitly set to 'wide' or 'narrow'")
    timestamp_columns = _resolve_timestamp_columns(
        df, time_col=time_col, date_col=date_col, clock_col=clock_col
    )
    timestamp = to_timestamp(
        df[timestamp_columns[0]],
        df[timestamp_columns[1]] if len(timestamp_columns) == 2 else None,
        date_format=date_format,
        timezone=timezone,
        day_first=day_first,
    )

    df = df.with_columns(timestamp.alias("__ts"))
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
        observations = df.select(
            pl.col("__ts").alias("ts"),
            pl.col(id_col).cast(pl.Utf8).alias("ref_name"),
            _as_text(df, value_col).alias("value"),
        ).drop_nulls("ref_name")
    else:
        streams = [c for c in df.columns if c not in (*timestamp_columns, "__ts")]
        observations = (
            df.select(
                pl.col("__ts").alias("ts"),
                *[_as_text(df, name).alias(name) for name in streams],
            )
            .unpivot(index="ts", variable_name="ref_name", value_name="value")
        )
    return observations.drop_nulls("value")


def _resolve_timestamp_columns(
    df: pl.DataFrame,
    *,
    time_col: str | None,
    date_col: str | None,
    clock_col: str | None,
) -> tuple[str, ...]:
    """Resolve explicit timestamp columns or a conservative common-name heuristic."""
    if time_col is not None and (date_col is not None or clock_col is not None):
        raise ValueError("set time_col or date_col/clock_col, not both")
    if clock_col is not None and date_col is None:
        raise ValueError("clock_col requires date_col")

    if time_col is not None:
        _require_columns(df, time_col)
        return (time_col,)
    if date_col is not None:
        columns = (date_col,) if clock_col is None else (date_col, clock_col)
        _require_columns(df, *columns)
        return columns

    normalized: dict[str, list[str]] = {}
    for name in df.columns:
        key = re.sub(r"[^a-z0-9]+", "", name.casefold())
        normalized.setdefault(key, []).append(name)

    # A Date + Time pair is more informative than treating Time as a complete
    # timestamp. Case and punctuation do not matter.
    if len(normalized.get("date", [])) == 1 and len(normalized.get("time", [])) == 1:
        return (normalized["date"][0], normalized["time"][0])

    date_prefixes = {
        key[:-4]: names[0]
        for key, names in normalized.items()
        if key.endswith("date") and len(names) == 1
    }
    time_prefixes = {
        key[:-4]: names[0]
        for key, names in normalized.items()
        if key.endswith("time") and len(names) == 1
    }
    pairs = sorted(set(date_prefixes) & set(time_prefixes))
    if len(pairs) == 1:
        prefix = pairs[0]
        return (date_prefixes[prefix], time_prefixes[prefix])
    if len(pairs) > 1:
        raise ValueError(
            f"multiple date/time column pairs found in {df.columns}; "
            "set date_col and clock_col explicitly"
        )

    for candidate in ("timestamp", "datetime", "ts", "time", "date"):
        matches = normalized.get(candidate, [])
        if len(matches) == 1:
            return (matches[0],)
    raise ValueError(
        f"could not identify timestamp columns in {df.columns}; set time_col, "
        "or set date_col and clock_col for split timestamps"
    )


def _require_columns(df: pl.DataFrame, *columns: str) -> None:
    missing = [name for name in columns if name not in df.columns]
    if missing:
        raise ValueError(f"timestamp column(s) {missing} not found in {df.columns}")


def _parse_timestamp_text(
    col: pl.Series,
    *,
    date_format: str | None,
    day_first: bool,
) -> pl.Series:
    if date_format is not None:
        return col.str.to_datetime(format=date_format, strict=False)

    # Polars' automatic parser is fast and handles ISO/RFC3339, including
    # offsets. It raises when it cannot infer a format, at which point we try
    # common export formats below.
    try:
        parsed = col.str.to_datetime(strict=False)
        if parsed.drop_nulls().len():
            return parsed
    except pl.exceptions.ComputeError:
        pass

    month_first = [
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %I:%M:%S %p",
    ]
    day_first_formats = [
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%Y %I:%M:%S %p",
    ]
    other = [
        "%Y-%m-%d %I:%M %p",
        "%Y-%m-%d %I:%M:%S %p",
        "%Y/%m/%d",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %I:%M %p",
        "%Y/%m/%d %I:%M:%S %p",
        "%d-%b-%Y",
        "%d-%b-%Y %H:%M",
        "%d-%b-%Y %H:%M:%S",
    ]
    formats = (
        day_first_formats + month_first + other
        if day_first else month_first + day_first_formats + other
    )
    best: pl.Series | None = None
    best_count = 0
    for date_format_candidate in formats:
        candidate = col.str.to_datetime(format=date_format_candidate, strict=False)
        count = candidate.drop_nulls().len()
        if count > best_count:
            best, best_count = candidate, count
    if best is None:
        return pl.Series(col.name, [None] * len(col), dtype=pl.Datetime("us"))
    return best


def _as_text(df: pl.DataFrame, column: str) -> pl.Expr:
    """Cast a value column to text, treating float NaN as absent."""
    if df[column].dtype in (pl.Float32, pl.Float64):
        return pl.col(column).fill_nan(None).cast(pl.Utf8)
    return pl.col(column).cast(pl.Utf8, strict=False)
