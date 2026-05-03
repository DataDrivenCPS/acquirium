from __future__ import annotations

from decimal import Decimal
from numbers import Real
from typing import Any, Iterable, Literal

import polars as pl

ValueKind = Literal["unknown", "numeric", "text"]


def normalize_value_kind(value_kind: Any = None) -> Literal["numeric", "text"]:
    if value_kind is None:
        return "numeric"
    value = str(value_kind).strip().lower()
    if value in {"", "unknown", "numeric", "number", "float", "int", "integer"}:
        return "numeric"
    if value in {"text", "string", "str", "status", "state"}:
        return "text"
    raise ValueError(f"value_kind must be 'numeric' or 'text', got {value_kind!r}")


def classify_value(value: Any, *, parse_numeric_strings: bool = False) -> ValueKind:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "text"
    if isinstance(value, (Real, Decimal)):
        return "numeric"
    if parse_numeric_strings and isinstance(value, str):
        text = value.strip()
        if not text:
            return "unknown"
        try:
            float(text)
            return "numeric"
        except ValueError:
            return "text"
    return "text"


def infer_value_kind(
    values: Iterable[Any],
    *,
    parse_numeric_strings: bool = False,
    unknown_default: Literal["numeric", "text", "unknown"] = "unknown",
) -> ValueKind:
    """Infer a stream-level value kind from observed values.

    A stream has a single storage type. If any non-null value is non-numeric,
    the stream is treated as text so every row for that ref_uri lands in the
    same value column. Set ``parse_numeric_strings`` for CSV/XLSX-style sources
    where numeric values may arrive as strings. ``unknown_default`` is used when
    every observed value is null or blank.
    """
    observed_numeric = False
    for value in values:
        kind = classify_value(value, parse_numeric_strings=parse_numeric_strings)
        if kind == "text":
            return "text"
        if kind == "numeric":
            observed_numeric = True
    return "numeric" if observed_numeric else unknown_default


def split_value(value: Any, value_kind: str | None = None) -> tuple[float | None, str | None]:
    kind = normalize_value_kind(value_kind)
    if value is None:
        return None, None
    if kind == "numeric":
        if isinstance(value, str) and not value.strip():
            return None, None
        return float(value), None
    return None, str(value)


def prepare_value_columns(df: pl.DataFrame) -> pl.DataFrame:
    if {"numeric_value", "text_value"}.issubset(df.columns):
        return df.select(["ref_uri", "ts", "numeric_value", "text_value"])

    if "value" not in df.columns:
        raise ValueError("timeseries dataframe must include value or numeric_value/text_value columns")

    has_value_kind = "value_kind" in df.columns
    selected = ["ref_uri", "ts", "value"] + (["value_kind"] if has_value_kind else [])
    rows = []
    for row in df.select(selected).iter_rows():
        if has_value_kind:
            ref_uri, ts, value, value_kind = row
            numeric_value, text_value = split_value(value, value_kind)
        else:
            ref_uri, ts, value = row
            numeric_value, text_value = split_value(value)
        rows.append((ref_uri, ts, numeric_value, text_value))

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
