"""Turbidity soft-sensor calculations; this self-contained file is pinned by its digest.

Each function receives one window's input readings as a polars frame and
returns one output row stamped at ``context["window_ns"]``, or an empty frame.
"""

import json

import polars as pl
from escapement import emit, empty

MINUTE_NS = 60 * 1_000_000_000

# High-turbidity thresholds in NTU per location.
THRESHOLDS_NTU = {
    "urn:bulk/loc-0": 5.0,
    "urn:bulk/loc-1": 5.0,
    "urn:bulk/loc-2": 0.5,
    "urn:bulk/loc-3": 0.3,
}

# Quality flag on gap-filled values.
INTERPOLATED = 1


def snap_to_minute(frame, context):
    """Average the raw readings in one 1-minute bucket."""
    if frame.is_empty():
        return {"turbidity": empty()}
    return {"turbidity": emit(context, frame["value"].mean())}


def fill_gaps(frame, context):
    """Keep the snapped value at this minute, or interpolate across a gap
    shorter than the app's rolling window looks back."""
    t = context["window_ns"]
    max_gap = context["operator"]["window"]["lookback_ns"]
    readings = frame.sort("event_time_ns")

    exact = readings.filter(pl.col("event_time_ns") == t)
    if not exact.is_empty():
        return {"turbidity": emit(context, exact["value"][0])}

    before = readings.filter(pl.col("event_time_ns") < t).tail(1)
    after = readings.filter(pl.col("event_time_ns") > t).head(1)
    if before.is_empty() or after.is_empty():
        return {"turbidity": empty()}

    t0, v0 = before["event_time_ns"][0], before["value"][0]
    t1, v1 = after["event_time_ns"][0], after["value"][0]
    if t1 - t0 >= max_gap:
        return {"turbidity": empty()}

    value = v0 + (v1 - v0) * (t - t0) / (t1 - t0)
    return {"turbidity": emit(context, value, quality=INTERPOLATED)}


def plant_turbidity(frame, context):
    """Combine every location's filled turbidity at the same minute."""
    if frame.is_empty():
        return {"mean": empty(), "max": empty(), "reporting": empty()}
    return {
        "mean": emit(context, frame["value"].mean()),
        "max": emit(context, frame["value"].max()),
        "reporting": emit(context, frame.height),
    }


def high_turbidity(frame, context):
    """1 when turbidity is above this location's threshold, otherwise 0."""
    if frame.is_empty():
        return {"alarm": empty()}
    # The input's context holds the selected upstream group: the location URI.
    location = json.loads(frame["context"][0])["group"]
    threshold = THRESHOLDS_NTU.get(location)
    if threshold is None:
        return {"alarm": empty()}
    return {"alarm": emit(context, 1 if frame["value"][0] > threshold else 0)}


def sustained_high_turbidity(frame, context):
    """1 when the high alarm was on for every minute of the last `sustained_minutes`."""
    t = context["window_ns"]
    # Only report at minutes that have an alarm value, so output stops with the data.
    if frame.filter(pl.col("event_time_ns") == t).is_empty():
        return {"alarm": empty()}
    # Set per location by the app's `parameters`.
    minutes = context["parameters"]["sustained_minutes"]
    span = frame.filter(pl.col("event_time_ns") >= t - minutes * MINUTE_NS)
    # Both ends are included, so N minutes holds N + 1 samples.
    every_minute_high = span.height == minutes + 1 and span["value"].min() == 1
    return {"alarm": emit(context, 1 if every_minute_high else 0)}
