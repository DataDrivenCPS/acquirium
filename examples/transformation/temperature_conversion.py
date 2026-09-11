"""Convert Fahrenheit water-temperature readings to Celsius."""
from __future__ import annotations

import polars as pl

import acquirium as aq


class FahrenheitToCelsius(aq.App):
    """Publish a separate Celsius stream for each Fahrenheit input stream."""

    name = "fahrenheit-to-celsius"
    grouping = "per_match"
    backfill = True
    outputs = {
        "celsius": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
        ),
    }

    def build_query(self, plant):
        return plant.query().measurement(alias="temperature", unit="DEG_F")

    def transform(self, inputs, output, context):
        fahrenheit = inputs["temperature"].df()
        output["celsius"] = fahrenheit.select(
            "time", ((pl.col("value") - 32.0) * 5.0 / 9.0).alias("value")
        )
