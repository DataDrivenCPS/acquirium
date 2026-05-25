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


def prepare_value_columns(df: pl.DataFrame) -> pl.DataFrame:
    if {"numeric_value", "text_value"}.issubset(df.columns):
        logger.debug("prepare_value_columns: already split (%d rows)", len(df))
        return df.select(["ref_uri", "ts", "numeric_value", "text_value"])

    if "value" not in df.columns:
        raise ValueError("timeseries dataframe must include value or numeric_value/text_value columns")

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
