"""An app that converts an input temperature stream to Fahrenheit."""
from __future__ import annotations

import polars as pl

import acquirium as aq


INPUT_SOURCE = "temperature-example-input"
OUTPUT_POINT = "urn:example:temperature:fahrenheit"


class CelsiusToFahrenheit(aq.App):
    """Publish a Fahrenheit stream for every Celsius input sample."""

    name = "celsius-to-fahrenheit"
    backfill = True
    outputs = {
        "fahrenheit": aq.output.named(
            "fahrenheit",
            value_kind="numeric",
            point_uri=OUTPUT_POINT,
            unit="http://qudt.org/vocab/unit/DEG_F",
            data_source="temperature-example-output",
        ),
    }

    def build_query(self, plant):
        return plant.query().measurement(alias="temperature", data_source=INPUT_SOURCE)

    def transform(self, inputs, output, context):
        celsius = inputs["temperature"].df()
        if celsius.is_empty():
            return
        output["fahrenheit"] = celsius.select(
            "time", (pl.col("value") * 9.0 / 5.0 + 32.0).alias("value")
        )
