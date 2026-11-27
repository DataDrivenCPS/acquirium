"""Pure calculations; this self-contained file is pinned by its content digest."""

from datetime import timedelta

import polars as pl

from acquirium.functions import previous_value, same_period


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
    return {"mean": values["celsius"].mean()}


def hottest_tank(frame, context):
    """Maximum of tank means at the same bucket timestamp."""
    return {"hottest": frame["value"].max()}


def high_temperature(frame, context):
    """Emit the hottest temperature above the plant's threshold parameter; otherwise clear."""
    values = frame["value"].drop_nulls()
    if len(values) > 1:
        raise ValueError("alarm expects one hottest-tank value per timestamp")
    value = values.first()
    return {"alarm": value if value is not None and value > context["parameters"]["threshold"] else None}


def daily_volume(frame, context):
    """Integrate hourly flow rates, carrying the previous rate and daily volume."""
    rates = frame["value"].drop_nulls()
    if rates.is_empty():
        return {}
    rate = rates.item()
    earlier_rate = previous_value(context, "rate")
    volume = 0.0
    if earlier_rate is not None:
        elapsed = context["output_time"] - context["previous"]["output_time"]
        if same_period(context, timedelta(days=1)):
            volume = previous_value(context, "volume")
        volume += (earlier_rate + rate) / 2 * (elapsed / timedelta(hours=1))
    return {"rate": rate, "volume": volume}
