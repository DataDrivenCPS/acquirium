"""Turbidity cleaning and alarm calculations for the turbidity tutorial."""

from datetime import timedelta

import pandas as pd
import polars as pl

from acquirium.functions import emit

# A reading this far from the median of the readings around it is a spike.
SPIKE_NTU = 1.0


def clean(frame: pd.DataFrame, context):
    """One clean turbidity value per minute, from the raw readings around it."""
    t = context["output_time"]

    # Fault codes arrive as text, with no numeric value.
    readings = frame.dropna(subset=["value"])
    # A spike, such as a bubble past the sensor, sits far from the median.
    median = readings["value"].median()
    readings = readings[(readings["value"] - median).abs() <= SPIKE_NTU]
    if readings.empty:
        return {"turbidity": emit(context, None)}

    # Average the remaining readings within each minute.
    minutes = readings.resample("1min", on="time")["value"].mean()
    # Fill gaps between valid minutes; keep the edges unfilled.
    filled = minutes.interpolate(limit_area="inside")
    # get() returns None for a minute outside the available range.
    return {"turbidity": emit(context, filled.get(t))}


def alarm(frame: pl.DataFrame, context):
    """1 when clean turbidity stayed above the limit for the whole duration."""
    t = context["output_time"]
    # Report only at minutes that have a clean value.
    if frame.filter(pl.col("time") == t).is_empty():
        return {"alarm": emit(context, None)}

    limit = context["parameters"]["limit_ntu"]
    minutes = context["parameters"]["sustained_minutes"]
    recent = frame.filter(pl.col("time") > t - timedelta(minutes=minutes))
    sustained = recent.height == minutes and recent["value"].min() > limit
    return {"alarm": emit(context, 1 if sustained else 0)}
