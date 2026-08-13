from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
import logging
import math
from numbers import Real
from typing import Any, Iterable, Literal, cast

import polars as pl

from acquirium.internals._log import timed_debug

logger = logging.getLogger(__name__)

ValueMode = Literal["default", "coalesce", "numeric", "text"]
VALUE_MODES = {"default", "coalesce", "numeric", "text"}


def normalize_value_kind(value_kind: Any = None) -> Literal["numeric", "text"]:
    if value_kind is None:
        return "text"
    value = str(value_kind).strip().lower()
    if value in {"", "unknown"}:
        return "text"
    if value in {"numeric", "number", "float", "int", "integer"}:
        return "numeric"
    if value in {"text", "string", "str", "status", "state"}:
        return "text"
    raise ValueError(f"value_kind must be 'numeric' or 'text', got {value_kind!r}")


def normalize_value_mode(value_mode: Any = None) -> ValueMode:
    if value_mode is None:
        return "default"
    value = str(value_mode).strip().lower()
    if value in VALUE_MODES:
        return cast(ValueMode, value)
    raise ValueError(
        "value_mode must be 'default', 'coalesce', 'numeric', or 'text', "
        f"got {value_mode!r}"
    )


def _is_numeric_value(value: Any, *, parse_numeric_strings: bool = True) -> bool:
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, (Real, Decimal)):
        return True
    if not parse_numeric_strings or not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    try:
        float(text)
    except ValueError:
        return False
    return True


def assign_stream_value_kind(
    values: Iterable[Any],
    *,
    parse_numeric_strings: bool = True,
) -> Literal["numeric", "text"]:
    """Assign a stream-level ``value_kind`` from observed values.

    This is the helper drivers should use when they infer stream metadata from
    data. ``value_kind`` is the preferred/default storage column for the stream:
    a stream with any observed numeric value is assigned ``"numeric"``, while
    streams with only text, blank, or null values are assigned ``"text"``.
    Unparseable rows in numeric streams are still stored in ``text_value`` by
    the storage fallback path.
    """
    return "numeric" if any(
        _is_numeric_value(value, parse_numeric_strings=parse_numeric_strings)
        for value in values
    ) else "text"


def split_value(value: Any, value_kind: str | None = None) -> tuple[float | None, str | None]:
    kind = normalize_value_kind(value_kind)
    if value is None:
        return None, None
    if kind == "numeric":
        if isinstance(value, str) and not value.strip():
            return None, None
        if isinstance(value, bool):
            return None, str(value)
        try:
            numeric_value = float(value)
        except (TypeError, ValueError, OverflowError):
            return None, str(value)
        if not math.isfinite(numeric_value):
            return None, None
        return numeric_value, None
    return None, str(value)


def typed_value_series(values: list[Any]) -> pl.Series:
    """Build a ``value`` column with a real dtype so the split can vectorize.

    Decides by an exact scan of the Python types rather than polars inference,
    which silently coerces bools mixed into ints (True -> 1) where
    ``split_value`` stores text "True", and rejects a type mismatch that falls
    outside its inference window.

    Only homogeneous batches get a native dtype. A batch mixing scalar types —
    e.g. one manager call spanning numeric and text streams — is stringified
    rather than sent to Object: ``str()`` is exactly what ``split_value``
    stores under text kind, and floats round-trip through repr under numeric
    kind. Non-scalar types, and any batch holding an int too large for Int64,
    take the Object (row-wise) path where their semantics are preserved
    exactly.
    """
    types = {type(v) for v in values}
    types.discard(type(None))
    if int in types and any(
        type(v) is int and not -(2**63) <= v < 2**63 for v in values
    ):
        # An int past Int64 has no native column to live in. Stringifying it
        # would be lossy: one past float range parses back to inf, and the
        # split drops non-finite values, where split_value stores the digits
        # as text. The row-wise path keeps that behaviour.
        return pl.Series("value", values, dtype=pl.Object)
    if types == {int}:
        # Int64, not Float64: under text kind the cast must render "7", not "7.0".
        return pl.Series("value", values, dtype=pl.Int64)
    if types == {float}:
        return pl.Series("value", values, dtype=pl.Float64)
    if types <= {str}:  # also all-None/empty: String nulls split to (None, None)
        return pl.Series("value", values, dtype=pl.String)
    if types == {bool}:
        return pl.Series("value", values, dtype=pl.Boolean)
    if types <= {str, float, int, bool}:
        return pl.Series(
            "value",
            [None if v is None else str(v) for v in values],
            dtype=pl.String,
        )
    return pl.Series("value", values, dtype=pl.Object)


def prepare_value_columns(df: pl.DataFrame) -> pl.DataFrame:
    if {"numeric_value", "text_value"}.issubset(df.columns):
        logger.debug("prepare_value_columns: already split (%d rows)", len(df))
        return df.select(["ref_uri", "ts", "numeric_value", "text_value"])

    if "value" not in df.columns:
        raise ValueError("timeseries dataframe must include value or numeric_value/text_value columns")

    vectorized = _prepare_value_columns_vectorized(df)
    if vectorized is not None:
        return vectorized
    return _prepare_value_columns_rowwise(df)


def _prepare_value_columns_vectorized(df: pl.DataFrame) -> pl.DataFrame | None:
    """Expression-based split for typed value columns; None when unsupported.

    Matches ``split_value`` row for row, with two accepted divergences on the
    string path: Python's ``float()`` accepts underscores ("1_000") where the
    cast does not (such values land in text_value), and float→string formatting
    for numeric input under text kind is polars' rather than ``str()``'s.
    """
    dtype = df.schema["value"]
    value = pl.col("value")
    kind = pl.col("_kind")

    if dtype == pl.Null:
        numeric = pl.lit(None, dtype=pl.Float64)
        text = pl.lit(None, dtype=pl.String)
    elif dtype == pl.Boolean:
        # Bools are never numeric; both kinds store str(value) ("True"/"False").
        numeric = pl.lit(None, dtype=pl.Float64)
        text = value.cast(pl.String).str.to_titlecase()
    elif dtype.is_numeric() or isinstance(dtype, pl.Decimal):
        parsed = value.cast(pl.Float64)
        numeric = pl.when(kind.eq("numeric") & parsed.is_finite()).then(parsed)
        # str(float('nan')) is "nan"; polars renders NaN as "NaN".
        as_text = (
            pl.when(parsed.is_nan()).then(pl.lit("nan")).otherwise(value.cast(pl.String))
            if dtype.is_float()
            else value.cast(pl.String)
        )
        text = pl.when(kind.eq("text")).then(as_text)
    elif dtype == pl.String:
        stripped = value.str.strip_chars()
        parsed = stripped.cast(pl.Float64, strict=False)
        numeric = pl.when(kind.eq("numeric") & parsed.is_finite()).then(parsed)
        # Numeric-kind fallback: non-blank unparseable strings keep their
        # original text. Parseable-but-non-finite ("inf", "nan") stays
        # (NULL, NULL), and blanks stay (NULL, NULL), matching split_value.
        text = pl.when(
            kind.eq("text")
            | (kind.eq("numeric") & parsed.is_null() & stripped.str.len_chars().gt(0))
        ).then(value)
    else:
        return None  # Object or exotic dtypes take the row-wise path

    has_value_kind = "value_kind" in df.columns
    with timed_debug(
        logger, "prepare_value_columns vectorized rows=%d dtype=%s", len(df), dtype
    ):
        if has_value_kind:
            # Normalise/validate the handful of distinct kinds, not n rows.
            mapping = {u: normalize_value_kind(u) for u in df["value_kind"].unique().to_list()}
            kind_expr = pl.col("value_kind").replace_strict(mapping, return_dtype=pl.String)
        else:
            kind_expr = pl.lit("text", dtype=pl.String)
        tmp = df.with_columns(kind_expr.alias("_kind")).with_columns(
            numeric.cast(pl.Float64).alias("numeric_value"),
            text.cast(pl.String).alias("text_value"),
        )
        fallbacks = (
            tmp.filter(kind.eq("numeric") & pl.col("text_value").is_not_null())
            .group_by("ref_uri")
            .len()
        )
    for ref_uri, count in fallbacks.iter_rows():
        logger.warning(
            "timeseries: stored %d unparseable numeric value(s) as text for %s",
            count,
            ref_uri,
        )
    return tmp.select(["ref_uri", "ts", "numeric_value", "text_value"])


def _prepare_value_columns_rowwise(df: pl.DataFrame) -> pl.DataFrame:
    """Per-row split via ``split_value`` — the fallback for Object-dtype frames."""
    has_value_kind = "value_kind" in df.columns
    selected = ["ref_uri", "ts", "value"] + (["value_kind"] if has_value_kind else [])
    rows = []
    fallback_counts: defaultdict[str, int] = defaultdict(int)
    with timed_debug(logger, "prepare_value_columns split rows=%d has_value_kind=%s", len(df), has_value_kind):
        for row in df.select(selected).iter_rows():
            if has_value_kind:
                ref_uri, ts, value, value_kind = row
                kind = normalize_value_kind(value_kind)
                numeric_value, text_value = split_value(value, kind)
                if kind == "numeric" and text_value is not None:
                    fallback_counts[str(ref_uri)] += 1
            else:
                ref_uri, ts, value = row
                numeric_value, text_value = split_value(value)
            rows.append((ref_uri, ts, numeric_value, text_value))

    for ref_uri, count in fallback_counts.items():
        logger.warning(
            "timeseries: stored %d unparseable numeric value(s) as text for %s",
            count,
            ref_uri,
        )

    return pl.DataFrame(
        rows,
        schema={
            "ref_uri": pl.Utf8,
            "ts": df["ts"].dtype,
            "numeric_value": pl.Float64,
            "text_value": pl.Utf8,
        },
        orient="row",
    )
