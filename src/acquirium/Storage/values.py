"""Numeric stream declarations for the Escapement-backed runtime."""

from __future__ import annotations

import math
from typing import Any, Iterable


def normalize_value_kind(value_kind: Any = None) -> str:
    if value_kind is None or str(value_kind).strip().lower() in {
        "numeric",
        "number",
        "float",
        "int",
        "integer",
    }:
        return "numeric"
    raise ValueError("only numeric streams are supported")


def assign_stream_value_kind(
    values: Iterable[Any], *, parse_numeric_strings: bool = True
) -> str:
    for value in values:
        if (
            value is None
            or isinstance(value, bool)
            or (isinstance(value, str) and not parse_numeric_strings)
        ):
            raise ValueError("observations must be finite non-null numbers")
        try:
            number = float(value)
        except (ValueError, TypeError, OverflowError) as error:
            raise ValueError("observations must be finite non-null numbers") from error
        if not math.isfinite(number):
            raise ValueError("observations must be finite non-null numbers")
    return "numeric"
