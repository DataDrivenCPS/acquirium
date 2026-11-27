"""Turbidity cleaning and alarm calculations for the turbidity tutorial."""

import pandas as pd
import polars as pl

from acquirium.functions import emit


def clean(frame: pd.DataFrame, context):
    """One clean turbidity value per minute, from the raw readings around it."""
    # Fault codes arrive as text, with no numeric value.
    readings = frame.dropna(subset=["value"])
    # A spike, such as a bubble past the sensor, sits far from the median.
    threshold = context["parameters"]["threshold"]
    median = readings["value"].median()
    readings = readings[(readings["value"] - median).abs() <= threshold]
    if readings.empty:
        return {"turbidity": readings[["time", "value"]]}

    # Average the remaining readings within each minute.
    minutes = readings.resample("1min", on="time")["value"].mean()
    # Fill gaps between valid minutes; keep the edges unfilled.
    filled = minutes.interpolate(limit_area="inside")
    # Return the calculated series. The app maintains its one-minute grid.
    return {"turbidity": filled.dropna().rename("value").rename_axis("time").reset_index()}


def alarm(frame: pl.DataFrame, context):
    """1 when clean turbidity stayed above the limit for the whole duration."""
    t = pd.Timestamp(context["output_time"])
    # Report only at minutes that have a clean value.
    if frame.filter(pl.col("time") == t).is_empty():
        return {"alarm": emit(context, None)}

    limit = context["parameters"]["limit_ntu"]
    minutes = context["parameters"]["sustained_minutes"]
    recent = frame.filter(pl.col("time") > t + pd.Timedelta(f"-{minutes}m"))
    sustained = recent.height == minutes and recent["value"].min() > limit
    return {"alarm": emit(context, 1 if sustained else 0)}
