"""Typed cells for query results.

The server's row endpoint returns every cell as text and, next to the rows,
a ``datatypes`` matrix of the same shape: the XSD datatype IRI of a literal,
``"iri"`` for a node, ``None`` for a plain string or an unbound cell.
:func:`parse_cell` turns a text cell back into the Python value its datatype
names, and :func:`column_dtype` picks the polars dtype a column of such
values can hold. ``Query.metadata()`` uses both so a value written as an
integer, a date or a boolean comes back as one, not as its spelling.

When a column mixes datatypes, integer and float widen to ``Float64``;
anything else widens to ``String`` here. ``include(..., type=...)`` and
``schema()`` (later steps) make a mixed column explicit instead of silent.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, List, Optional, Tuple

import polars as pl

XSD = "http://www.w3.org/2001/XMLSchema#"
IRI = "iri"

_INTEGER_TYPES = frozenset(
    XSD + t for t in (
        "integer", "int", "long", "short", "byte", "nonNegativeInteger",
        "positiveInteger", "nonPositiveInteger", "negativeInteger",
        "unsignedLong", "unsignedInt", "unsignedShort", "unsignedByte",
    )
)
_FLOAT_TYPES = frozenset(XSD + t for t in ("decimal", "double", "float"))


def parse_cell(value: Any, datatype: Optional[str]) -> Any:
    """The Python value of one result cell, by its datatype.

    ``None`` stays ``None``. An IRI and a plain or unknown-typed literal
    stay strings. A value that does not parse as its datatype says (a bad
    ``xsd:integer`` in someone's Turtle) stays a string rather than failing
    the whole frame.
    """
    if value is None or datatype is None or datatype == IRI:
        return value
    try:
        if datatype in _INTEGER_TYPES:
            return int(value)
        if datatype in _FLOAT_TYPES:
            return float(value)
        if datatype == XSD + "boolean":
            return str(value).strip().lower() in ("true", "1")
        if datatype == XSD + "date":
            return date.fromisoformat(str(value))
        if datatype == XSD + "dateTime":
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return value
    return value


def column_dtype(values: Iterable[Any]) -> pl.DataType:
    """The polars dtype for a column of parsed cells.

    One Python type gives its dtype; ``int`` and ``float`` together give
    ``Float64``; any other mixture, or no values at all, gives ``String``.
    """
    kinds = set()
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            kinds.add(bool)
        elif isinstance(v, int):
            kinds.add(int)
        elif isinstance(v, float):
            kinds.add(float)
        elif isinstance(v, datetime):
            kinds.add(datetime)
        elif isinstance(v, date):
            kinds.add(date)
        else:
            kinds.add(str)
    if kinds == {int}:
        return pl.Int64
    if kinds <= {int, float} and kinds:
        return pl.Float64
    if kinds == {bool}:
        return pl.Boolean
    if kinds == {date}:
        return pl.Date
    if kinds == {datetime}:
        return pl.Datetime("us", "UTC") if all(
            getattr(v, "tzinfo", None) is not None for v in values if isinstance(v, datetime)
        ) else pl.Datetime("us")
    return pl.String


def coerce(values: List[Any], dtype: pl.DataType) -> List[Any]:
    """Make every value of a column fit ``dtype`` (``str()`` for String,
    ``float()`` for a widened numeric column); ``None`` stays."""
    if dtype == pl.String:
        return [None if v is None else (v if isinstance(v, str) else _text(v)) for v in values]
    if dtype == pl.Float64:
        return [None if v is None else float(v) for v in values]
    return values


def _text(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return str(v)


def typed_column(values: List[Any], datatypes: List[Optional[str]]) -> Tuple[List[Any], pl.DataType]:
    """Parse one column's cells and return ``(values, dtype)`` ready for polars."""
    parsed = [parse_cell(v, d) for v, d in zip(values, datatypes)]
    dtype = column_dtype(parsed)
    return coerce(parsed, dtype), dtype
