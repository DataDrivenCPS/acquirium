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
import numbers
import io
import base64
import polars as pl
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


def timestamp(value: datetime | str) -> datetime:
    """Parse an ISO timestamp, treating a timezone-less value as UTC."""
    from dateutil import parser
    parsed = parser.isoparse(value) if isinstance(value, str) else value
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parquet(value: Any) -> bytes:
    """Serialize a Polars, pandas, or Arrow table as Parquet bytes."""
    if isinstance(value, pl.DataFrame):
        frame = value
    elif type(value).__module__.startswith("pyarrow") and hasattr(value, "schema"):
        frame = pl.from_arrow(value)
    elif type(value).__module__.startswith("pandas") and hasattr(value, "columns"):
        frame = pl.from_pandas(value)
    else:
        raise TypeError(f"a table variable records a Polars, pandas, or Arrow table, not {type(value).__name__}")
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()



def sample_rows(rows: Any) -> list[tuple[datetime, Any]]:
    """Read (time, value) pairs, or a table with time and value columns.

    Tables may use the names stream reads return: `time` or `ts`, and
    `value` or `value_numeric`. A wide query frame with a time column and one
    stream column works too.
    """
    if type(rows).__module__.startswith("pyarrow") and hasattr(rows, "schema"):
        rows = pl.from_arrow(rows)
    if isinstance(rows, pl.DataFrame):
        time = next((name for name in ("time", "ts") if name in rows.columns), None)
        value = next((name for name in ("value", "value_numeric") if name in rows.columns), None)
        if time is not None and value is None and len(rows.columns) == 2:
            value = next(name for name in rows.columns if name != time)
        if time is None or value is None:
            raise ValueError("time-series tables need a time (or ts) column and a value (or value_numeric) column")
        for column in ("data_alias", "point_id", "ref", "uri"):
            if column in rows.columns and rows[column].n_unique() > 1:
                raise ValueError("these rows come from several streams; record one stream at a time")
        rows = zip(rows[time].to_list(), rows[value].to_list())
    pairs = [(timestamp(when), to_json(value)) for when, value in rows]
    if any(not isinstance(value, numbers.Real) for _, value in pairs):
        raise ValueError("time-series rows need a numeric value in every row")
    return pairs


def json_value(kind, value, *, label="variable"):
    value = to_json(value)
    check_kind(kind, value, label=label)
    return value


def sampled_values(component, value):
    """Validate instantaneous numeric model values, including their keys."""
    from acquirium.ExperimentDeclarations import stream_key
    if not component.keys:
        return json_value("scalar", value)
    if not isinstance(value, Mapping):
        raise TypeError("a keyed time-series reader must return a key-to-number mapping")
    return {stream_key(key, component.keys): json_value("scalar", item) for key, item in value.items()}


def value_entry(variable_id, kind, label, value, *, occurred_at=None):
    return {"form": "value", "variable_id": variable_id, "value": json_value(kind, value, label=repr(label)),
            "occurred_at": timestamp(occurred_at).isoformat() if occurred_at else None}


def artifact_entry(variable_id, data, *, filename, media_type=None):
    return {"form": "artifact", "variable_id": variable_id, "artifact": {
        "filename": filename, "media_type": media_type, "data": base64.b64encode(data).decode("ascii")}}


def recording_entries(variable, value, *, at=None):
    """Prepare ordinary or model values for one recording request.

    Model samples supply ``at`` and numeric reader values; ordinary samples
    supply timestamped rows. No network operations occur during preparation.
    """
    from acquirium.Client.streams import StreamInterval
    from acquirium.ExperimentDeclarations import stream_key
    if variable.kind == "table":
        return [artifact_entry(variable.variable_id, parquet(value),
            filename=f"{variable.label}.parquet", media_type=TABLE_MEDIA_TYPE)]
    if variable.kind != "timeseries":
        return [value_entry(variable.variable_id, variable.kind, variable.label, value)]
    if at is None and isinstance(value, StreamInterval):
        return [{"form": "reference", "variable_id": variable.variable_id, "ref_uri": value.ref_uri,
                 "interval": [value.start.isoformat(), value.end.isoformat()]}]
    keys = variable.keys
    if at is not None:
        value = sampled_values(variable, value)
    if not keys and not isinstance(value, Mapping):
        value = {(): value}
    if not isinstance(value, Mapping):
        raise TypeError("keyed time series record a mapping from key tuples to rows")
    entries = []
    for key, rows in value.items():
        key = stream_key(key, keys) if at is None else key
        rows = sample_rows(rows) if at is None else [(at, rows)]
        if rows:
            entries.append({"form": "samples", "variable_id": variable.variable_id,
                "key": list(key) if keys else None,
                "samples": [[when.isoformat(), value] for when, value in rows]})
    return entries
