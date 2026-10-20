"""Batch versions of transforms.py: each call gets every affected window at once.

The frame has a ``window_ns`` column naming the window (its timestamp) for
each row; a row read by several overlapping windows appears once per window.
Functions return rows for all windows; a window left out is retracted.
"""

import json

import polars as pl
from escapement import emit_windows, empty

MINUTE_NS = 60 * 1_000_000_000
THRESHOLDS_NTU = {
    "urn:bulk/loc-0": 5.0,
    "urn:bulk/loc-1": 5.0,
    "urn:bulk/loc-2": 0.5,
    "urn:bulk/loc-3": 0.3,
}
INTERPOLATED = 1


def snap_to_minute(frame, context):
    """Average the raw readings in each 1-minute bucket."""
    means = frame.group_by("window_ns").agg(pl.col("value").mean())
    return {"turbidity": emit_windows(means)}


def fill_gaps(frame, context):
    """Each window's snapped value, or interpolation across a gap shorter than
    the app's rolling window looks back."""
    max_gap = context["operator"]["window"]["lookback_ns"]
    t, time, value = pl.col("window_ns"), pl.col("event_time_ns"), pl.col("value")
    # Sorted by time within each window, the nearest readings before and after
    # the window's minute are the last before it and the first after it.
    windows = frame.sort("window_ns", "event_time_ns").group_by("window_ns").agg(
        value.filter(time == t).first().alias("exact"),
        time.filter(time < t).last().alias("t0"),
        value.filter(time < t).last().alias("v0"),
        time.filter(time > t).first().alias("t1"),
        value.filter(time > t).first().alias("v1"),
    )
    interpolated = pl.col("v0") + (pl.col("v1") - pl.col("v0")) * (t - pl.col("t0")) / (
        pl.col("t1") - pl.col("t0")
    )
    short_gap = (pl.col("t1") - pl.col("t0")) < max_gap
    result = windows.select(
        "window_ns",
        pl.when(pl.col("exact").is_not_null())
        .then(pl.col("exact"))
        .when(short_gap)
        .then(interpolated)
        .alias("value"),
        pl.when(pl.col("exact").is_not_null()).then(0).otherwise(INTERPOLATED).alias("quality"),
    ).drop_nulls("value")
    return {"turbidity": emit_windows(result)}


def plant_turbidity(frame, context):
    """Combine every location's filled turbidity at each minute."""
    stats = frame.group_by("window_ns").agg(
        pl.col("value").mean().alias("mean"),
        pl.col("value").max().alias("max"),
        pl.len().cast(pl.Float64).alias("reporting"),
    )
    return {
        port: emit_windows(stats.select("window_ns", pl.col(port).alias("value")))
        for port in ("mean", "max", "reporting")
    }


def high_turbidity(frame, context):
    """1 when turbidity is above this location's threshold, otherwise 0."""
    if frame.is_empty():
        return {"alarm": empty()}
    # Every row of this operator is for the same location.
    location = json.loads(frame["context"][0])["group"]
    threshold = THRESHOLDS_NTU.get(location)
    if threshold is None:
        return {"alarm": empty()}
    alarms = frame.select("window_ns", (pl.col("value") > threshold).cast(pl.Float64).alias("value"))
    return {"alarm": emit_windows(alarms)}


def sustained_high_turbidity(frame, context):
    """1 when the high alarm was on for every minute of the last `sustained_minutes`."""
    if frame.is_empty():
        return {"alarm": empty()}
    minutes = context["parameters"]["sustained_minutes"]
    t, time = pl.col("window_ns"), pl.col("event_time_ns")
    span = time >= t - minutes * MINUTE_NS
    windows = frame.group_by("window_ns").agg(
        (time == t).any().alias("has_value"),
        span.sum().alias("samples"),
        pl.col("value").filter(span).min().alias("lowest"),
    )
    result = windows.filter("has_value").select(
        "window_ns",
        ((pl.col("samples") == minutes + 1) & (pl.col("lowest") == 1)).cast(pl.Float64).alias("value"),
    )
    return {"alarm": emit_windows(result)}


def plant_alarm_count(frame, context):
    """Count how many locations are in sustained alarm at each minute."""
    counts = frame.group_by("window_ns").agg((pl.col("value") == 1).sum().cast(pl.Float64).alias("value"))
    return {"locations": emit_windows(counts)}
