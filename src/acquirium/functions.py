"""Helpers for app calculation files.

A Python function returns one frame per output port. ``emit`` and ``empty``
build those frames for a function called once per window, and ``emit_windows``
for a batch function. A sequential function reads its earlier results with
``previous_value``, ``previous_outputs`` and ``previous_rows``, and
``same_period`` says whether a period boundary lies between the previous window
and this one. ``SCHEMA`` gives the columns of every output frame.

Frame timestamps use ``time: Datetime('us', 'UTC')``. Context timestamps are
timezone-aware Python datetimes, and durations are timedeltas. Batch frames
also carry ``output_time`` to identify each row's result window.
"""

from datetime import datetime, timedelta, timezone
from functools import cache, wraps
from pathlib import Path
from types import ModuleType

from escapement import artifact_digest
import polars as pl

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_TIME = pl.Datetime("us", "UTC")
SCHEMA = {
    "time": _TIME,
    "value": pl.Float64,
    "text": pl.String,
}


def empty() -> pl.DataFrame:
    """An empty output frame; removes the previous result for this window.

    >>> empty().shape
    (0, 3)
    """
    return pl.DataFrame(schema=SCHEMA)


def emit(context, value) -> pl.DataFrame:
    """Construct a one-row dataframe at ``context['output_time']``.

    A None value produces an empty dataframe. The function returns this frame
    under its declared output name; Acquirium stores the returned results.
    The ``time`` column has type ``Datetime('us', 'UTC')``.

    >>> at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    >>> result = emit({"output_time": at}, 12.0)
    >>> result.shape
    (1, 3)
    >>> result["time"].item() == at
    True
    """
    if value is None:
        return empty()
    _check_datetime(context["output_time"])
    text = value if isinstance(value, str) else None
    return pl.DataFrame({
        "time": [context["output_time"]],
        "value": [None if text is not None else float(value)],
        "text": [text],
    }, schema=SCHEMA)


def emit_windows(frame: pl.DataFrame) -> pl.DataFrame:
    """Batch results from ``output_time`` and ``value`` or ``text`` columns.

    Each input row produces one output row, with ``output_time`` renamed to
    ``time``.

    >>> at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    >>> rows = pl.DataFrame({"output_time": [at, at + timedelta(minutes=1)], "value": [2., 3.]})
    >>> emit_windows(rows).select("time", "value").shape
    (2, 2)
    """
    _check_time_column(frame["output_time"])
    return frame.select(
        pl.col("output_time").alias("time"),
        pl.col("value") if "value" in frame.columns else pl.lit(None).alias("value"),
        pl.col("text") if "text" in frame.columns else pl.lit(None).alias("text"),
    ).cast(SCHEMA)


def previous_outputs(context, port) -> pl.DataFrame:
    """The previous window's output rows on ``port``, with the output schema."""
    previous = context.get("previous")
    rows = previous["outputs"].get(port, []) if previous else []
    return pl.DataFrame(rows, schema=SCHEMA) if rows else empty()


def previous_value(context, port, default=None):
    """The previous window's last value on ``port``, or ``default``."""
    previous = context.get("previous")
    rows = previous["outputs"].get(port, []) if previous else []
    if not rows:
        return default
    row = rows[-1]
    return row["text"] if row["value"] is None else row["value"]


def previous_rows(context) -> pl.DataFrame:
    """The previous window's input readings, with a UTC datetime ``time`` column."""
    previous = context.get("previous")
    rows = previous["rows"] if previous else []
    schema = {"role": pl.String, "stream": pl.String, **SCHEMA}
    return pl.DataFrame(rows, schema=schema)


def same_period(context, width: timedelta, origin: datetime = _EPOCH) -> bool:
    """Whether this and the previous output fall in the same fixed period.

    ``width`` is a positive timedelta. ``origin`` is a timezone-aware datetime,
    defaulting to the Unix epoch. The first window returns False.
    """
    if not isinstance(width, timedelta) or width <= timedelta(0):
        raise ValueError("width must be a positive timedelta")
    _check_datetime(origin)
    previous = context.get("previous")
    return previous is not None and (
        (previous["output_time"] - origin) // width
        == (context["output_time"] - origin) // width
    )


def _check_datetime(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise TypeError("function timestamps must be timezone-aware datetimes")
    if getattr(value, "nanosecond", 0):
        raise ValueError("soft sensor timestamps must have microsecond precision")


def _check_time_column(series):
    if not isinstance(series.dtype, pl.Datetime) or series.dtype.time_zone is None:
        raise TypeError(f"{series.name!r} must be a timezone-aware datetime column")
    if series.dtype.time_unit == "ns" and (series.cast(pl.Int64) % 1000 != 0).any():
        raise ValueError("soft sensor timestamps must have microsecond precision")


def _datetime(ns, *, boundary=False):
    # Exclusive nanosecond bounds become exclusive microsecond bounds. With
    # microsecond-aligned readings, both select exactly the same samples.
    if not boundary and ns % 1000:
        raise ValueError("soft sensor timestamps must have microsecond precision")
    us = -(-ns // 1000) if boundary else ns // 1000
    return _EPOCH + timedelta(microseconds=us)


def _public_frame(frame):
    if "sample_key" in frame.columns and (frame["sample_key"] != 0).any():
        raise ValueError("Acquirium supports one reading per stream and timestamp")
    names = {"event_time_ns": "time", "window_ns": "output_time"}
    expressions = []
    for name, target in names.items():
        if name in frame.columns:
            if (frame[name] % 1000 != 0).any():
                raise ValueError("soft sensor timestamps must have microsecond precision")
            expressions.append((pl.col(name) // 1000).cast(_TIME).alias(name))
    return frame.with_columns(expressions).drop("sample_key", "quality", strict=False).rename({
        name: target for name, target in names.items() if name in frame.columns
    })


def _public_record(row):
    result = dict(row)
    if result.pop("sample_key", 0) != 0:
        raise ValueError("Acquirium supports one reading per stream and timestamp")
    result.pop("quality", None)
    result["time"] = _datetime(result.pop("event_time_ns"))
    return result


def _public_context(context):
    result = dict(context)
    for old, new in (("window_ns", "output_time"),
                     ("window_start_ns", "window_start"),
                     ("window_end_ns", "window_end")):
        if old in result:
            result[new] = _datetime(result.pop(old), boundary=new != "output_time")
    if "inputs" in result:
        result["inputs"] = [_public_input(item) for item in result["inputs"]]
    if "windows" in result:
        result["windows"] = [_public_context(window) for window in result["windows"]]
    if "previous" in result:
        previous = dict(result["previous"])
        previous["output_time"] = _datetime(previous.pop("window_ns"))
        previous["rows"] = [_public_record(row) for row in previous["rows"]]
        previous["outputs"] = {
            port: [_public_record(row) for row in rows]
            for port, rows in previous["outputs"].items()
        }
        result["previous"] = previous
    if "parameters" in result:
        result["parameters"] = dict(result["parameters"])
        if "group" in result["parameters"]:
            result["parameters"]["group"] = tuple(result["parameters"]["group"])
    if "operator" in result:
        operator = dict(result["operator"])
        window = dict(operator["window"])
        for name in list(window):
            if name.endswith("_ns"):
                value = window.pop(name)
                if value % 1000:
                    raise ValueError("soft sensor windows must have microsecond precision")
                window[name[:-3]] = (_datetime(value) if name == "origin_ns"
                                    else timedelta(microseconds=value // 1000))
        operator["window"] = window
        operator["inputs"] = [_public_input(item) for item in operator["inputs"]]
        if "parameters" in operator:
            parameters = dict(operator["parameters"])
            if "group" in parameters:
                parameters["group"] = tuple(parameters["group"])
            operator["parameters"] = parameters
        if "as_of" in operator:
            if any(spec["max_age_ns"] % 1000 for spec in operator["as_of"].values()):
                raise ValueError("soft sensor as-of ages must have microsecond precision")
            operator["as_of"] = {
                role: timedelta(microseconds=spec["max_age_ns"] // 1000)
                for role, spec in operator["as_of"].items()
            }
        result["operator"] = operator
    return result


def _public_input(item):
    result = dict(item)
    for name in ("valid_from", "valid_to"):
        if name in result:
            ns = result[name]
            result[name] = None if ns is None or ns == -(2**63) else _datetime(ns, boundary=True)
    for old, new in (("span_start_ns", "span_start"), ("span_end_ns", "span_end")):
        if old in result:
            result[new] = _datetime(result.pop(old), boundary=True)
    return result


def _engine_frame(frame):
    unexpected = set(frame.columns) - SCHEMA.keys()
    if unexpected:
        raise ValueError(f"unexpected soft sensor output columns: {sorted(unexpected)}")
    _check_time_column(frame["time"])
    us = frame["time"].dt.epoch("us")
    if ((us < -(2**63 // 1000)) | (us > (2**63 - 1) // 1000)).any():
        raise ValueError("output timestamp is outside the engine's supported range")
    return frame.with_columns(
        (pl.col("time").dt.epoch("us") * 1000).alias("event_time_ns"),
        # Required by the engine wire schema; never exposed to calculations.
        pl.lit(0, pl.Int64).alias("sample_key"),
        pl.lit(0, pl.UInt32).alias("quality"),
    ).drop("time")


def _datetime_function(function):
    """Adapt an Acquirium calculation to the engine's private wire schema."""
    @wraps(function)
    def call(frame, context):
        outputs = function(_public_frame(frame), _public_context(context))
        return {port: _engine_frame(output) for port, output in outputs.items()}

    return call


def _function_module(path, digest):
    """Load one immutable module, shared by every entrypoint in a warm worker."""
    def source():
        code = Path(path).read_bytes()
        if artifact_digest(code) != digest:
            raise ValueError("Python artifact digest mismatch")
        return code

    module = ModuleType("escapement_function")
    module.__file__ = path
    exec(compile(source(), path, "exec"), module.__dict__)
    @cache
    def entrypoint(name):
        function = getattr(module, name)
        calculate = _datetime_function(function)

        @wraps(function)
        def call(frame, context):
            # Check the original artifact even when a worker reuses the module.
            source()
            return calculate(frame, context)

        return call

    return entrypoint


def _group_function(path, digest, entrypoint):
    # Existing manifests may refer to this private loader from earlier builds.
    return _function_module(path, digest)(entrypoint)


__all__ = [
    "SCHEMA",
    "emit",
    "emit_windows",
    "empty",
    "previous_outputs",
    "previous_rows",
    "previous_value",
    "same_period",
]
