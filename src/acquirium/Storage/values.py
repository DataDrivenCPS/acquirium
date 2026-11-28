"""Stream value kinds and the number/text split of observations.

Escapement stores each reading as a finite number or text. A stream's
``value_kind`` is Acquirium graph metadata: it decides how a written value is
stored and which readings a default read returns. The engine never sees it.
"""

from __future__ import annotations

import logging
import math
from decimal import Decimal
from typing import Any, Literal, cast

import polars as pl

logger = logging.getLogger(__name__)

ValueKind = Literal["numeric", "text"]
ValueMode = Literal["default", "coalesce", "numeric", "text"]
VALUE_MODES = {"default", "coalesce", "numeric", "text"}
_INEXACT_INTEGER = "integer reading cannot be represented exactly as Float64"
_MIXED_VALUE = pl.Struct({"rendered": pl.String, "integer": pl.Int64})


# Kind of a stream registered without a ``value_kind``. It reads as text and
# becomes numeric when its first numeric value is inserted.
UNDECLARED_VALUE_KIND = "undeclared"


def normalize_value_kind(value_kind: Any = None) -> Literal["numeric", "text"]:
    """Resolve declaration aliases to the effective kind used for values."""
    if value_kind is None:
        return "text"
    value = str(value_kind).strip().lower()
    if value in {"", "unknown", UNDECLARED_VALUE_KIND}:
        return "text"
    if value in {"numeric", "number", "float", "int", "integer"}:
        return "numeric"
    if value in {"text", "string", "str", "status", "state"}:
        return "text"
    raise ValueError(f"value_kind must be 'numeric' or 'text', got {value_kind!r}")


def stored_value_kind(value_kind: Any = None) -> str:
    """The kind kept for a stream: the declared one, or ``"undeclared"``."""
    if value_kind is None or str(value_kind).strip().lower() in {"", "unknown", UNDECLARED_VALUE_KIND}:
        return UNDECLARED_VALUE_KIND
    return normalize_value_kind(value_kind)


def normalize_value_mode(value_mode: Any = None) -> ValueMode:
    """Validate a read mode, using ``default`` when the caller omits it."""
    value = "default" if value_mode is None else str(value_mode).strip().lower()
    if value in VALUE_MODES:
        return cast(ValueMode, value)
    raise ValueError(
        f"value_mode must be 'default', 'coalesce', 'numeric', or 'text', got {value_mode!r}"
    )



def _split_scalar(value: Any, *, numeric: bool) -> tuple[float | None, str | None]:
    """Normalize a value from a mixed batch with a Python object column."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, None
    if numeric:
        if isinstance(value, int):
            try:
                number = float(value)
            except OverflowError as error:
                raise ValueError(_INEXACT_INTEGER) from error
            if number != value:
                raise ValueError(_INEXACT_INTEGER)
            return number, None
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None, str(value)
        return (number, None) if math.isfinite(number) else (None, None)
    if isinstance(value, (float, Decimal)) and not math.isfinite(value):
        return None, None
    return None, str(value)


def streams_with_numeric_values(df: pl.DataFrame) -> set[str]:
    """The ``stream_uri`` values of *df* that have at least one numeric ``value``.

    A value is numeric when a numeric stream would store it as a number:
    finite numbers and strings that parse as one.
    """
    if df.is_empty():
        return set()
    dtype = df.schema["value"]
    value = pl.col("value")
    if dtype == _MIXED_VALUE:
        value, dtype = value.struct.field("rendered"), pl.String
    if dtype == pl.String:
        numeric = value.str.strip_chars().cast(pl.Float64, strict=False).is_finite()
    elif dtype.is_numeric() or isinstance(dtype, pl.Decimal):
        numeric = value.cast(pl.Float64).is_finite()
    elif dtype == pl.Boolean:
        numeric = value.is_not_null()
    elif dtype == pl.Null:
        return set()
    else:
        return {
            uri
            for uri, item in zip(df["stream_uri"].to_list(), df["value"].to_list())
            if isinstance(item, int) or _split_scalar(item, numeric=True)[0] is not None
        }
    return set(df.filter(numeric)["stream_uri"].unique().to_list())


def typed_value_series(values: list[Any]) -> pl.Series:
    """Build a ``value`` column with a real dtype so the split can vectorize.

    Decides by an exact scan of the Python types rather than polars inference,
    which silently coerces bools mixed into ints (True -> 1) where
    ``_split_scalar`` stores text "True" under text kind, and rejects a type
    mismatch that falls outside its inference window.

    Mixed strings and floats can share a String column since floats round-trip
    through their string representation. In a mixed scalar batch, retain the
    original integers alongside their string representations for validation.
    Booleans mixed with other types and integers outside Int64 use Object.
    """
    types = {type(v) for v in values}
    types.discard(type(None))
    if int in types and any(type(v) is int and not -(2**63) <= v < 2**63 for v in values):
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
    if types <= {str, float}:
        return pl.Series(
            "value",
            [None if v is None else str(v) for v in values],
            dtype=pl.String,
        )
    if types <= {str, float, int}:
        return pl.DataFrame({
            "rendered": pl.Series([None if v is None else str(v) for v in values], dtype=pl.String),
            "integer": pl.Series([v if type(v) is int else None for v in values], dtype=pl.Int64),
        }).to_struct("value")
    return pl.Series("value", values, dtype=pl.Object)


def _validate_integers(frame: pl.DataFrame, integer: pl.Expr, numeric: pl.Expr) -> None:
    """Raise when a numeric-stream integer changes on conversion to Float64."""
    # Compare using Int128 because rounding may exceed the original dtype.
    roundtrip = integer.cast(pl.Float64).cast(pl.Int128, strict=False)
    if frame.select((numeric & integer.cast(pl.Int128).ne_missing(roundtrip)).any()).item():
        raise ValueError(_INEXACT_INTEGER)


def split_values(frame: pl.DataFrame) -> pl.DataFrame:
    """Replace ``value`` and ``value_kind`` with the ``number`` and ``text``
    columns Escapement stores, dropping rows that hold no reading (null,
    blank, NaN or infinite).

    On a numeric stream a value that parses as a number is stored as one
    (``"12.5"`` counts) and anything else as text, so a fault code is kept.
    On a text stream booleans retain their Python spelling. On a
    numeric stream they are 1.0 and 0.0. An integer reading for a
    numeric stream must round-trip through Float64 or this raises ValueError.
    """
    dtype = frame.schema["value"]
    value, numeric = pl.col("value"), pl.col("value_kind").eq("numeric")
    integer = None
    if dtype == _MIXED_VALUE:
        integer = value.struct.field("integer")
        _validate_integers(frame, integer, numeric)
        value, dtype = value.struct.field("rendered"), pl.String
    if dtype == pl.Object:
        cells = [
            _split_scalar(item, numeric=kind == "numeric")
            for item, kind in frame.select("value", "value_kind").iter_rows()
        ]
        number = pl.Series("number", [item[0] for item in cells], dtype=pl.Float64)
        text = pl.Series("text", [item[1] for item in cells], dtype=pl.String)
    elif dtype == pl.Null:
        number, text = pl.lit(None, pl.Float64), pl.lit(None, pl.String)
    elif dtype == pl.Boolean:
        number = pl.when(numeric).then(value.cast(pl.Float64))
        text = pl.when(~numeric).then(value.cast(pl.String).str.to_titlecase())
    elif dtype.is_numeric() or isinstance(dtype, pl.Decimal):
        parsed = value.cast(pl.Float64)
        if dtype.is_integer() and dtype not in (pl.Int8, pl.UInt8, pl.Int16, pl.UInt16, pl.Int32, pl.UInt32):
            _validate_integers(frame, value, numeric)
        number = pl.when(numeric & parsed.is_finite()).then(parsed)
        text = pl.when(~numeric & parsed.is_finite()).then(value.cast(pl.String))
    elif dtype == pl.String:
        stripped = value.str.strip_chars()
        parsed = stripped.cast(pl.Float64, strict=False)
        if integer is not None:
            parsed = pl.coalesce(integer.cast(pl.Float64), parsed)
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
    fallbacks = result.filter(numeric & pl.col("text").is_not_null()).group_by("stream_uri").len()
    for stream_uri, count in fallbacks.iter_rows():
        logger.warning("stored %d non-numeric value(s) as text for %s", count, stream_uri)
    return result.filter(
        pl.col("number").is_not_null() | pl.col("text").is_not_null()
    ).drop("value", "value_kind")
