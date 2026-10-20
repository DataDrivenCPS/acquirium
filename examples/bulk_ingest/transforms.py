"""Turbidity soft-sensor calculations; this self-contained file is pinned by its digest.

Each function receives one window's input readings as a polars frame and
returns one output row stamped at ``context["group"]``, or an empty frame.
"""

import json

import polars as pl

MINUTE_NS = 60 * 1_000_000_000

# Gaps shorter than this are filled by linear interpolation. The fill app's
# rolling window must look at least this far back and ahead.
MAX_GAP_NS = 120 * MINUTE_NS

# High-turbidity thresholds in NTU per location.
THRESHOLDS_NTU = {
    "urn:bulk/loc-0": 5.0,
    "urn:bulk/loc-1": 5.0,
    "urn:bulk/loc-2": 0.5,
    "urn:bulk/loc-3": 0.3,
}

# Quality flag on gap-filled values.
INTERPOLATED = 1

SCHEMA = {
    "event_time_ns": pl.Int64,
    "sample_key": pl.Int64,
    "value": pl.Float64,
    "quality": pl.UInt32,
}


def output(context, value, quality=0):
    return pl.DataFrame(
        {
            "event_time_ns": [context["group"]],
            "sample_key": [0],
            "value": [float(value)],
            "quality": [quality],
        },
        schema=SCHEMA,
    )


def no_output():
    return pl.DataFrame(schema=SCHEMA)


def snap_to_minute(frame, context):
    """Average the raw readings in one 1-minute bucket."""
    if frame.is_empty():
        return {"turbidity": no_output()}
    return {"turbidity": output(context, frame["value"].mean())}


def fill_gaps(frame, context):
    """Keep the snapped value at this minute, or interpolate across a short gap."""
    t = context["group"]
    readings = frame.sort("event_time_ns")

    exact = readings.filter(pl.col("event_time_ns") == t)
    if not exact.is_empty():
        return {"turbidity": output(context, exact["value"][0])}

    before = readings.filter(pl.col("event_time_ns") < t).tail(1)
    after = readings.filter(pl.col("event_time_ns") > t).head(1)
    if before.is_empty() or after.is_empty():
        return {"turbidity": no_output()}

    t0, v0 = before["event_time_ns"][0], before["value"][0]
    t1, v1 = after["event_time_ns"][0], after["value"][0]
    if t1 - t0 >= MAX_GAP_NS:
        return {"turbidity": no_output()}

    value = v0 + (v1 - v0) * (t - t0) / (t1 - t0)
    return {"turbidity": output(context, value, quality=INTERPOLATED)}


def plant_turbidity(frame, context):
    """Combine every location's filled turbidity at the same minute."""
    if frame.is_empty():
        return {"mean": no_output(), "max": no_output(), "reporting": no_output()}
    return {
        "mean": output(context, frame["value"].mean()),
        "max": output(context, frame["value"].max()),
        "reporting": output(context, frame.height),
    }


def high_turbidity(frame, context):
    """1 when turbidity is above this location's threshold, otherwise 0."""
    if frame.is_empty():
        return {"alarm": no_output()}
    # The input's context holds the selected upstream group: the location URI.
    location = json.loads(frame["context"][0])["group"]
    threshold = THRESHOLDS_NTU.get(location)
    if threshold is None:
        return {"alarm": no_output()}
    return {"alarm": output(context, 1 if frame["value"][0] > threshold else 0)}


def sustained_high_turbidity(frame, context):
    """1 when the high alarm was on for every minute of the last `sustained_minutes`."""
    t = context["group"]
    # Only report at minutes that have an alarm value, so output stops with the data.
    if frame.filter(pl.col("event_time_ns") == t).is_empty():
        return {"alarm": no_output()}
    # Set per location by the app's `parameters`; arrives in context as a string.
    minutes = int(json.loads(frame["context"][0])["sustained_minutes"])
    span = frame.filter(pl.col("event_time_ns") >= t - minutes * MINUTE_NS)
    # Both ends are included, so N minutes holds N + 1 samples.
    every_minute_high = span.height == minutes + 1 and span["value"].min() == 1
    return {"alarm": output(context, 1 if every_minute_high else 0)}


def plant_alarm_count(frame, context):
    """Count how many locations are in sustained alarm at this minute."""
    if frame.is_empty():
        return {"locations": no_output()}
    return {"locations": output(context, (frame["value"] == 1).sum())}
