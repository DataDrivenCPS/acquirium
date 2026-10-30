"""Stream value kinds and the number/text split of observations.

Escapement stores each reading as a finite number or text. A stream's
``value_kind`` is Acquirium graph metadata: it decides how a written value is
stored and which readings a default read returns. The engine never sees it.
"""

from __future__ import annotations

import logging
import math
from decimal import Decimal
from numbers import Real
from typing import Any, Iterable, Literal, cast

import polars as pl

logger = logging.getLogger(__name__)

ValueKind = Literal["numeric", "text"]
ValueMode = Literal["default", "coalesce", "numeric", "text"]
VALUE_MODES = {"default", "coalesce", "numeric", "text"}


def normalize_value_kind(value_kind: Any = None) -> ValueKind:
    if value_kind is None:
        return "numeric"
    value = str(value_kind).strip().lower()
    if value in {"", "unknown", "numeric", "number", "float", "int", "integer"}:
        return "numeric"
    if value in {"text", "string", "str", "status", "state"}:
        return "text"
    raise ValueError(f"value_kind must be 'numeric' or 'text', got {value_kind!r}")


def normalize_value_mode(value_mode: Any = None) -> ValueMode:
    value = "default" if value_mode is None else str(value_mode).strip().lower()
    if value in VALUE_MODES:
        return cast(ValueMode, value)
    raise ValueError(
        f"value_mode must be 'default', 'coalesce', 'numeric', or 'text', got {value_mode!r}"
    )


def _is_numeric_value(value: Any, *, parse_numeric_strings: bool = True) -> bool:
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, (Real, Decimal)):
        return True
    if not parse_numeric_strings or not isinstance(value, str) or not value.strip():
        return False
    try:
        float(value)
    except ValueError:
        return False
    return True


def assign_stream_value_kind(
    values: Iterable[Any], *, parse_numeric_strings: bool = True
) -> ValueKind:
    """A stream's ``value_kind`` from observed values: ``"numeric"`` if any
    value is a number (or a numeric string), otherwise ``"text"``."""
    return "numeric" if any(
        _is_numeric_value(value, parse_numeric_strings=parse_numeric_strings)
        for value in values
    ) else "text"


def _as_text(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return None
    return str(value)


def split_values(frame: pl.DataFrame) -> pl.DataFrame:
    """Replace ``value`` and ``value_kind`` with the ``number`` and ``text``
    columns Escapement stores, dropping rows that hold no reading (null,
    blank, NaN or infinite).

    On a numeric stream a value that parses as a number is stored as one
    (``"12.5"`` counts) and anything else as text, so a fault code is kept.
    On a text stream every value is text. Booleans are always text.
    """
    if frame.schema["value"] == pl.Object:
        frame = frame.with_columns(
            pl.Series("value", [_as_text(v) for v in frame["value"]], dtype=pl.String)
        )
    dtype = frame.schema["value"]
    value, numeric = pl.col("value"), pl.col("value_kind").eq("numeric")
    if dtype == pl.Null:
        number, text = pl.lit(None, pl.Float64), pl.lit(None, pl.String)
    elif dtype == pl.Boolean:
        number = pl.lit(None, pl.Float64)
        text = value.cast(pl.String).str.to_titlecase()
    elif dtype.is_numeric() or isinstance(dtype, pl.Decimal):
        parsed = value.cast(pl.Float64)
        number = pl.when(numeric & parsed.is_finite()).then(parsed)
        text = pl.when(~numeric & parsed.is_finite()).then(value.cast(pl.String))
    elif dtype == pl.String:
        stripped = value.str.strip_chars()
        parsed = stripped.cast(pl.Float64, strict=False)
        number = pl.when(numeric & parsed.is_finite()).then(parsed)
        # Unparseable values on a numeric stream are kept as text; blank
        # values are no reading on either kind.
        text = pl.when(
            stripped.str.len_chars().gt(0) & (~numeric | parsed.is_null())
        ).then(value)
    else:
        raise ValueError(f"unsupported value column type {dtype}")
    result = frame.with_columns(
        number.cast(pl.Float64).alias("number"), text.cast(pl.String).alias("text")
    )
    fallbacks = result.filter(numeric & pl.col("text").is_not_null()).group_by("ref_uri").len()
    for ref_uri, count in fallbacks.iter_rows():
        logger.warning("stored %d non-numeric value(s) as text for %s", count, ref_uri)
    return result.filter(
        pl.col("number").is_not_null() | pl.col("text").is_not_null()
    ).drop("value", "value_kind")
