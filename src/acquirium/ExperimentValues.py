"""Turn recorded Python values into JSON, and check them against a kind.

`to_json()` converts values on the client before they are sent. Lists, dicts,
strings, numbers, booleans, and None pass through; other types go through a
handler looked up by type, so a project can teach recording about its own
values with `register_value_handler()`. `check_kind()` is shared by the client
and the server: the client raises early, the server is authoritative.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime, time, timezone
import math
from typing import Any

import numpy as np

# A handler returns something closer to JSON; its result is converted again,
# so it may return containers that still hold numpy or date values.
_HANDLERS: dict[type, Callable[[Any], Any]] = {}

# Kinds whose value travels in the observation itself. Files and tables are
# stored as artifacts and time series as stream references instead.
VALUE_KINDS = {"json", "text", "scalar", "log"}
ARTIFACT_KINDS = {"file", "table"}
KINDS = VALUE_KINDS | ARTIFACT_KINDS | {"timeseries"}
TABLE_MEDIA_TYPE = "application/vnd.apache.parquet"


def register_value_handler(cls: type, handler: Callable[[Any], Any]) -> None:
    """Record instances of `cls` (and its subclasses) as `handler(value)`."""
    if not isinstance(cls, type):
        raise TypeError("register_value_handler() takes a type")
    _HANDLERS[cls] = handler


def _handler(value: Any) -> Callable[[Any], Any] | None:
    for cls in type(value).__mro__:
        if cls in _HANDLERS:
            return _HANDLERS[cls]
    return None


def to_json(value: Any, *, path: str = "value") -> Any:
    """Return `value` as JSON-compatible data, or raise naming the bad part."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} is {value}; recorded values cannot be NaN or infinite")
        return float(value)  # numpy.float64 is a float subclass
    if isinstance(value, Mapping):
        result = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} has key {key!r}; recorded dict keys must be strings")
            result[key] = to_json(child, path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [to_json(child, path=f"{path}[{index}]") for index, child in enumerate(value)]
    handler = _handler(value)
    if handler is None:
        raise TypeError(
            f"{path} is a {type(value).__name__}, which cannot be recorded as JSON; "
            "convert it first or register a handler with acquirium.register_value_handler()"
        )
    return to_json(handler(value), path=path)


def _datetime(value: datetime) -> str:
    # Match the experiment API: a naive datetime is UTC.
    zone = value.tzinfo or timezone.utc
    return value.replace(tzinfo=zone).astimezone(timezone.utc).isoformat()


register_value_handler(np.generic, lambda value: value.item())
register_value_handler(np.ndarray, lambda value: value.tolist())
register_value_handler(datetime, _datetime)
register_value_handler(date, date.isoformat)
register_value_handler(time, time.isoformat)


def check_kind(kind: str, value: Any, *, label: str = "variable") -> None:
    """Raise unless `value`, already JSON, fits a variable of `kind`."""
    if kind == "scalar":
        if not isinstance(value, (int, float)) or (isinstance(value, float) and not math.isfinite(value)):
            raise TypeError(f"{label} is a scalar and records a finite number or boolean, not {value!r}")
    elif kind == "text":
        if not isinstance(value, str):
            raise TypeError(f"{label} is text and records a string, not {type(value).__name__}")
    elif kind not in VALUE_KINDS:
        raise TypeError(f"{label} is a {kind} variable; record it with record(), not a JSON value")
