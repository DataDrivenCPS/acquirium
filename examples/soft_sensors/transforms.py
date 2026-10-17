"""Pure calculations; this self-contained file is pinned by its content digest."""

import json

import polars as pl

SCHEMA = {
    "event_time_ns": pl.Int64,
    "sample_key": pl.Int64,
    "value": pl.Float64,
    "quality": pl.UInt32,
}


def result(value, context):
    if value is None:
        return pl.DataFrame(schema=SCHEMA)
    return pl.DataFrame(
        {
            "event_time_ns": [context["group"]],
            "sample_key": [0],
            "value": [float(value)],
            "quality": [0],
        },
        schema=SCHEMA,
    )


def tank_mean(frame, context):
    """Sample-weighted mean of all selected observations in one UTC bucket."""

    def factor(key):
        return pl.col("context").str.json_path_match(f"$.{key}").cast(pl.Float64)

    values = frame.select(
        (
            (pl.col("value") + factor("from_offset"))
            * factor("from_multiplier")
            / factor("to_multiplier")
            - factor("to_offset")
        ).alias("celsius")
    )
    return {"mean": result(values["celsius"].mean(), context)}


def hottest_tank(frame, context):
    """Maximum of tank means at the same bucket timestamp."""
    value = frame["value"].max()
    return {"hottest": result(value, context)}


def high_temperature(frame, context):
    """Emit the hottest temperature above the frozen threshold; otherwise clear."""
    if frame.height > 1:
        raise ValueError("alarm expects one hottest-tank value per timestamp")
    value = None
    if frame.height:
        threshold = float(json.loads(frame["context"][0])["threshold"])
        if frame["value"][0] > threshold:
            value = frame["value"][0]
    return {"alarm": result(value, context)}
