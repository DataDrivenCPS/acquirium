"""Pure calculations; this self-contained file is pinned by its content digest."""

import polars as pl
from escapement import emit


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
    return {"mean": emit(context, values["celsius"].mean())}


def hottest_tank(frame, context):
    """Maximum of tank means at the same bucket timestamp."""
    value = frame["value"].max()
    return {"hottest": emit(context, value)}


def high_temperature(frame, context):
    """Emit the hottest temperature above the plant's threshold parameter; otherwise clear."""
    if frame.height > 1:
        raise ValueError("alarm expects one hottest-tank value per timestamp")
    value = None
    if frame.height:
        if frame["value"][0] > context["parameters"]["threshold"]:
            value = frame["value"][0]
    return {"alarm": emit(context, value)}
