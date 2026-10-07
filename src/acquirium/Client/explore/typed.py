"""Typed cells for query results.

The server's row endpoint returns every cell as text and, next to the rows,
a ``datatypes`` matrix of the same shape: the XSD datatype IRI of a literal,
``"iri"`` for a node, ``None`` for a plain string or an unbound cell.
:func:`parse_cell` turns a text cell back into the Python value its datatype
names, and :func:`column_dtype` picks the polars dtype a column of such
values can hold. ``Query.metadata()`` uses both so a value written as an
integer, a date or a boolean comes back as one, not as its spelling.

When a column mixes kinds, integer and float widen to ``Float64``; any
other mixture is a ``pl.Object`` column whose cells keep the Python values
as written (``2019`` here, ``"2019"`` there, a ``dict`` next to a ``str``).
Nothing is dropped or re-spelled; ``include(..., type=...)`` casts such a
column to one dtype on request (:func:`cast_values`), and ``schema()``
reports the mixture before a query runs.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Callable, Iterable, List, Optional, Tuple

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
    ``Float64``; no values at all gives ``String``; any other mixture gives
    ``Object``.
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
        elif isinstance(v, str):
            kinds.add(str)
        else:
            kinds.add(object)
    if not kinds:
        return pl.String
    if kinds == {str}:
        return pl.String
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
    return pl.Object


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


# ---- casts asked for with include(type=...) -------------------------------

_TYPE_NAMES = {
    "string": "string", "str": "string", str: "string", pl.String: "string", pl.Utf8: "string",
    "int": "int", "integer": "int", int: "int", pl.Int64: "int",
    "float": "float", "double": "float", float: "float", pl.Float64: "float",
    "bool": "bool", "boolean": "bool", bool: "bool", pl.Boolean: "bool",
    "date": "date", date: "date", pl.Date: "date",
    "datetime": "datetime", datetime: "datetime",
    "struct": "struct", dict: "struct",
    "object": "object", object: "object", pl.Object: "object",
}
_TARGET_DTYPE = {"string": pl.String, "int": pl.Int64, "float": pl.Float64, "bool": pl.Boolean,
                 "date": pl.Date, "datetime": pl.Datetime("us"), "object": pl.Object}


class CastError(ValueError):
    """A value could not be cast to the type ``include(type=...)`` asked for."""


def normalize_type(type_: Any) -> str:
    """``include(type=...)`` accepts a name (``"int"``), a Python type or a
    polars dtype; returns the canonical name."""
    if isinstance(type_, pl.Datetime):
        return "datetime"
    try:
        return _TYPE_NAMES[type_]
    except (KeyError, TypeError):
        raise ValueError(
            f"unknown type {type_!r}; use one of string, int, float, bool, date, datetime, "
            "struct, object (or the Python type / polars dtype)") from None


def _cast_scalar(v: Any, target: str) -> Any:
    if v is None:
        return None
    if target == "string":
        if isinstance(v, (dict, list)):
            return json.dumps(v, default=_text, sort_keys=True)
        return _text(v) if not isinstance(v, str) else v
    if target == "int":
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            if v.is_integer():
                return int(v)
            raise ValueError(v)
        if isinstance(v, str):
            return int(v.strip())
        raise ValueError(v)
    if target == "float":
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        if isinstance(v, bool):
            return float(v)
        if isinstance(v, str):
            return float(v.strip())
        raise ValueError(v)
    if target == "bool":
        if isinstance(v, bool):
            return v
        if isinstance(v, int) and v in (0, 1):
            return bool(v)
        if isinstance(v, str) and v.strip().lower() in ("true", "false", "1", "0"):
            return v.strip().lower() in ("true", "1")
        raise ValueError(v)
    if target == "date":
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            return date.fromisoformat(v.strip())
        raise ValueError(v)
    if target == "datetime":
        if isinstance(v, datetime):
            return v
        if isinstance(v, date):
            return datetime(v.year, v.month, v.day)
        if isinstance(v, str):
            return datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
        raise ValueError(v)
    if target == "struct":
        return v if isinstance(v, dict) else None   # a plain value under a dict key: null
    return v  # object


def cast_values(values: List[Any], type_: Any, *, strict: bool = True,
                labels: Optional[List[Any]] = None, column: str = "") -> Tuple[List[Any], str]:
    """Cast a column's Python cells to ``type_``; returns ``(values, target)``.

    A list cell is cast element by element. With ``strict=True`` (the
    default, as in polars' ``cast``) a value that does not convert raises
    :class:`CastError` naming the value and the node; with ``strict=False``
    it becomes ``null``. ``labels`` are the node identifiers per row for
    the message.
    """
    target = normalize_type(type_)

    def one(v: Any, i: int) -> Any:
        try:
            return _cast_scalar(v, target)
        except (ValueError, TypeError, AttributeError):
            if not strict:
                return None
            where = f" on {labels[i]}" if labels and i < len(labels) else ""
            raise CastError(
                f"include({column!r}, type={target!r}): cannot cast {v!r}{where}; "
                "pass strict=False to get null instead") from None

    out: List[Any] = []
    for i, v in enumerate(values):
        if isinstance(v, list) and target not in ("string", "object"):
            out.append([one(e, i) for e in v])
        else:
            out.append(one(v, i))
    return out, target
