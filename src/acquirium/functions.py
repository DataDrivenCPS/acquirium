"""Helpers for app calculation files.

A Python function returns a mapping from output ports to values or frames.
Per-window values use the window's output time. ``emit_windows`` builds frames
for a batch function. A sequential function reads its earlier results with
``previous_value`` and ``previous_outputs``, and ``same_period`` says whether
a period boundary lies between the previous window and this one. ``SCHEMA`` gives the Polars output schema.

Annotate the first parameter with ``pandas.DataFrame`` for Arrow-backed pandas
or ``polars.DataFrame`` for Polars. Unannotated functions receive Polars.
Helpers follow the selected library while a calculation runs.

Frame timestamps use UTC microseconds. Context timestamps are
timezone-aware Python datetimes, and durations are timedeltas. Batch frames
also carry ``output_time`` to identify each row's result window.
"""

from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from functools import cache
import inspect
import json
from numbers import Real
from typing import get_type_hints

import polars as pl

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_TIME = pl.Datetime("us", "UTC")
_FRAME_LIBRARY = ContextVar("acquirium_frame_library", default="polars")
SCHEMA = {
    "time": _TIME,
    "value": pl.Float64,
    "text": pl.String,
}


@cache
def _pandas():
    # Polars calculations do not need to import pandas into their worker.
    import pandas as pd
    return pd


def _as_library(frame, library=None):
    if (library or _FRAME_LIBRARY.get()) == "pandas":
        # Keep Arrow buffers and nullable types; do not materialize NumPy arrays.
        return frame.to_arrow().to_pandas(types_mapper=_pandas().ArrowDtype)
    return frame


def _as_polars(frame):
    if isinstance(frame, pl.DataFrame):
        return frame
    if isinstance(frame, _pandas().DataFrame):
        return pl.from_pandas(frame, include_index=False)
    raise TypeError("soft sensor outputs must be pandas or Polars DataFrames")


def _frame_library(function):
    """Resolve only the frame annotation, once when an entrypoint is loaded."""
    target = inspect.unwrap(function)
    parameters = list(inspect.signature(target).parameters.values())
    annotation = parameters[0].annotation if parameters else inspect.Signature.empty
    if annotation is inspect.Signature.empty:
        return "polars"

    # A small annotation holder resolves aliases and postponed annotations
    # without evaluating unrelated context or return annotations.
    def hint():
        pass
    hint.__annotations__ = {"frame": annotation}
    try:
        annotation = get_type_hints(hint, globalns=getattr(target, "__globals__", {}))["frame"]
    except (NameError, TypeError, SyntaxError) as error:
        raise TypeError(f"cannot resolve frame annotation for {function.__name__}: {error}") from error
    if annotation is pl.DataFrame:
        return "polars"
    if annotation is _pandas().DataFrame:
        return "pandas"
    raise TypeError("annotate frame with pandas.DataFrame or polars.DataFrame, or leave it unannotated")


def empty():
    """An empty output frame; removes the previous result for this window.

    >>> empty().shape
    (0, 3)
    """
    return _as_library(pl.DataFrame(schema=SCHEMA))


def emit(context, value):
    """Construct a one-row dataframe at ``context['output_time']``.

    A None value produces an empty dataframe. The function returns this frame
    under its declared output name; Acquirium stores the returned results.
    During a calculation the frame uses its selected library, with UTC
    microsecond timestamps. pandas missing values also produce an empty frame.

    >>> at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    >>> result = emit({"output_time": at}, 12.0)
    >>> result.shape
    (1, 3)
    >>> result["time"].item() == at
    True
    """
    return _as_library(_scalar_output(context["output_time"], value))


def _scalar_output(output_time, value):
    if value is None or (_FRAME_LIBRARY.get() == "pandas" and _pandas().isna(value)):
        return pl.DataFrame(schema=SCHEMA)
    _check_datetime(output_time)
    text = value if isinstance(value, str) else None
    return pl.DataFrame({
        "time": [output_time],
        "value": [None if text is not None else float(value)],
        "text": [text],
    }, schema=SCHEMA)


def emit_windows(frame):
    """Batch results from ``output_time`` and ``value`` or ``text`` columns.

    Each input row produces one output row, with ``output_time`` renamed to
    ``time``.

    >>> at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    >>> rows = pl.DataFrame({"output_time": [at, at + timedelta(minutes=1)], "value": [2., 3.]})
    >>> emit_windows(rows).select("time", "value").shape
    (2, 2)
    """
    library = "polars" if isinstance(frame, pl.DataFrame) else "pandas"
    frame = _as_polars(frame)
    _check_time_column(frame["output_time"])
    return _as_library(frame.select(
        pl.col("output_time").alias("time"),
        pl.col("value") if "value" in frame.columns else pl.lit(None).alias("value"),
        pl.col("text") if "text" in frame.columns else pl.lit(None).alias("text"),
    ).cast(SCHEMA), library)


def previous_outputs(context, port):
    """The previous window's output rows on ``port``, with the output schema."""
    previous = context.get("previous")
    rows = previous["outputs"].get(port, []) if previous else []
    return _as_library(pl.DataFrame(rows, schema=SCHEMA)) if rows else empty()


def previous_value(context, port, default=None):
    """The previous window's last value on ``port``, or ``default``."""
    previous = context.get("previous")
    rows = previous["outputs"].get(port, []) if previous else []
    if not rows:
        return default
    row = rows[-1]
    return row["text"] if row["value"] is None else row["value"]


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


def _public_parameters(parameters):
    """An instance's parameters, with its group key as a tuple."""
    result = dict(parameters)
    if "group" in result:
        result["group"] = tuple(result["group"])
    return result


def _public_window(window):
    """A window spec, with datetimes and timedeltas for its nanosecond fields."""
    result = {}
    for name, value in window.items():
        if not name.endswith("_ns"):
            result[name] = value
            continue
        if value % 1000:
            raise ValueError("soft sensor windows must have microsecond precision")
        result[name[:-3]] = (_datetime(value) if name == "origin_ns"
                             else timedelta(microseconds=value // 1000))
    return result


def _split_own_ports(frame, context):
    """Split a window's rows into the instance's own earlier outputs, which
    the worker binds to ports, and its input readings."""
    own = {bound["stream"]: bound["port"] for bound in context["inputs"] if "port" in bound}
    mine = pl.col("stream").is_in(list(own))
    return own, frame.filter(mine), frame.filter(~mine)


def _epoch_input(frame, context, window, ports):
    """Build the calculation's public frame and context for a single window.

    Inputs carry their binding context. Earlier outputs are grouped by
    their window time and exposed through the previous-output helpers.
    """
    from acquirium.apps import _operator_id, output_reference

    own_ports, state, frame = _split_own_ports(frame, context)
    earlier = state.select("stream", "time", "value", "text").iter_rows(named=True)
    _check_time_column(frame["time"])
    inputs = []
    for bound in context["inputs"]:
        if "port" in bound:
            continue
        reading = {"input": bound["role"], "stream": bound["stream"], "context": bound["context"]}
        for name, limit in zip(("span_start", "span_end"), bound["reads_ns"]):
            if limit is not None:
                reading[name] = _datetime(limit, boundary=True)
        inputs.append(reading)
    # Each row carries the context of the binding that selected it; of two
    # bindings of one stream to one input, the later.
    contexts = pl.DataFrame(
        [{"role": item["input"], "stream": item["stream"], "context": item["context"]} for item in inputs],
        schema={"role": pl.String, "stream": pl.String, "context": pl.String},
    ).unique(subset=["role", "stream"], keep="last", maintain_order=True)
    public = frame.join(contexts, on=["role", "stream"], how="left", maintain_order="left").with_columns(
        pl.col("time").cast(_TIME)
    ).rename({"role": "input"})
    for item in inputs:
        item["context"] = json.loads(item["context"])
    name, group = context["template"], context["instance"]
    start, end = context["body_ns"]
    if window["kind"] == "rolling":
        start = max(-(2**63), start - window["lookback_ns"])
        end = min(2**63 - 1, end + window.get("lookahead_ns", 0))
    parameters = _public_parameters(context["parameters"])
    public_context = {
        "output_time": _datetime(context["anchor_ns"]),
        "window_start": _datetime(start, boundary=True),
        "window_end": _datetime(end, boundary=True),
        "inputs": inputs,
        "parameters": parameters,
        "operator": {
            "id": _operator_id(name, group),
            "window": _public_window(window),
            "inputs": [dict(item) for item in inputs],
            "outputs": {port: output_reference(name, group, port) for port in ports},
            "parameters": dict(parameters),
        },
    }
    by_time = {}
    for row in earlier:
        port = own_ports[row.pop("stream")]
        time = row["time"]
        if window["kind"] == "fixed":
            # An earlier output can fall anywhere in its bucket. Use the
            # bucket anchor to collect ports from the same prior window.
            width = timedelta(microseconds=window["width_ns"] // 1000)
            origin = _datetime(window["origin_ns"])
            time -= (time - origin) % width
        by_time.setdefault(time, {})[port] = [row]
    if by_time:
        latest = max(by_time)
        public_context["previous"] = {"output_time": latest, "outputs": by_time[latest]}
    return public, public_context


def _epoch_outputs(outputs, ports, replaced, anchors, *, scalar_time=None):
    """Validate calculation results and prepare one engine frame per port."""
    results = {}
    for port, output in outputs.items():
        if scalar_time is not None and (output is None or isinstance(output, (Real, str)) or (
            _FRAME_LIBRARY.get() == "pandas" and output is _pandas().NA
        )):
            out = _scalar_output(scalar_time, output)
        else:
            out = _as_polars(output)
        if "time" not in out:
            raise ValueError("return a 'time' column; use reset_index() on a pandas time index")
        unexpected = set(out.columns) - SCHEMA.keys()
        if unexpected:
            raise ValueError(f"unexpected soft sensor output columns: {sorted(unexpected)}")
        _check_time_column(out["time"])
        us = out["time"].dt.epoch("us")
        if ((us < -((2**63) // 1000)) | (us > (2**63 - 1) // 1000)).any():
            raise ValueError("output timestamp is outside the engine's supported range")
        out = out.with_columns(pl.col("time").cast(_TIME))
        if port in replaced:
            # Replacement ports update the input keys that triggered this run.
            out = out.filter(pl.col("time").is_in(pl.Series(anchors, dtype=_TIME).implode()))
        if "value" not in out:
            out = out.with_columns(pl.lit(None, pl.Float64).alias("value"))
        results[port] = out.with_columns(pl.col("value").cast(pl.Float64))
    for port in ports:
        # The worker expects a result for every declared port.
        results.setdefault(port, pl.DataFrame(schema=SCHEMA))
    return results


def _epoch_function(function, window, ports, replaced):
    """Present the Acquirium per-window interface to the epoch worker."""
    library = _frame_library(function)

    def call(frame, context):
        public, public_context = _epoch_input(frame, context, window, ports)
        token = _FRAME_LIBRARY.set(library)
        try:
            outputs = function(_as_library(public, library), public_context)
            output_time = public_context["output_time"]
            return _epoch_outputs(outputs, ports, replaced, [output_time], scalar_time=output_time)
        finally:
            _FRAME_LIBRARY.reset(token)

    return call


def _epoch_batch_function(function, window, ports, replaced):
    """Adapt the worker's vectorized run to one batch per Acquirium group."""
    library = _frame_library(function)

    def call(frame, context):
        windows = context["windows"].cast({key: pl.Int64 for key in ("anchor", "body_start", "body_end")})
        inputs = context["inputs"].cast({
            key: pl.Int64 for key in ("anchor", "reads_start", "reads_end")
        })
        frame = frame.with_columns(pl.col("anchor").cast(pl.Int64))
        # Split rows and bindings by window once, not once per window.
        window_rows = frame.partition_by("instance", "anchor", as_dict=True, include_key=False)
        window_inputs = inputs.partition_by("instance", "anchor", as_dict=True)
        no_rows, no_inputs = frame.drop("instance", "anchor").clear(), inputs.clear()
        result = {port: [] for port in ports}
        token = _FRAME_LIBRARY.set(library)
        try:
            for (instance,), group in windows.group_by("instance", maintain_order=True):
                chunks, public_windows = [], []
                for spec in group.iter_rows(named=True):
                    anchor = spec["anchor"]
                    bound = window_inputs.get((instance, anchor), no_inputs)
                    view, window_context = _epoch_input(
                        window_rows.get((instance, anchor), no_rows),
                        {
                            "template": context["template"], "instance": instance,
                            "anchor_ns": anchor, "body_ns": (spec["body_start"], spec["body_end"]),
                            "parameters": context["instances"][instance]["parameters"],
                            "inputs": [
                                {**item, "reads_ns": (item["reads_start"], item["reads_end"])}
                                for item in bound.iter_rows(named=True)
                            ],
                        },
                        window, ports,
                    )
                    chunks.append(view.with_columns(
                        pl.lit(window_context["output_time"], dtype=_TIME).alias("output_time")
                    ))
                    public_windows.append(window_context)
                shared = {**public_windows[0], "windows": public_windows}
                outputs = function(_as_library(pl.concat(chunks), library), shared)
                anchors = [item["output_time"] for item in public_windows]
                for port, out in _epoch_outputs(outputs, ports, replaced, anchors).items():
                    result.setdefault(port, []).append(out.select(
                        "time", "value",
                        pl.col("text") if "text" in out else pl.lit(None, pl.String).alias("text"),
                        pl.lit(instance).alias("instance"),
                    ))
        finally:
            _FRAME_LIBRARY.reset(token)
        return {port: pl.concat(frames) for port, frames in result.items()}

    return call


__all__ = [
    "SCHEMA",
    "emit",
    "emit_windows",
    "empty",
    "previous_outputs",
    "previous_value",
    "same_period",
]
