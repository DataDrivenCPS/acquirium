"""Normalize each water-temperature stream to degrees Celsius.

Check the app without contacting a server:

    acquirium app check scripts/apps/temperature_normalization.py:TemperatureNormalization

The query matches every registered temperature measurement. Each match gets a
separate derived stream, so the app can normalize many tanks independently.
"""

from __future__ import annotations

import polars as pl

import acquirium as aq


class TemperatureNormalization(aq.App):
    """Publish a Celsius stream for each water-temperature input stream."""

    name = "temperature-normalization"
    # Run once for each matched tank or other water-temperature stream.
    grouping = "per_match"
    # Also process readings that were stored before this app was deployed.
    backfill = True
    # A stream output gets a stable derived identity for each input match.
    outputs = {
        "temperature": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
            quantity_kind="http://qudt.org/vocab/quantitykind/Temperature",
        ),
    }

    def build_query(self, plant):
        # Free-text metadata is resolved against the plant model at deployment.
        return plant.query().measurement(
            alias="temperature", quantity_kind="temperature"
        )

    def transform(self, inputs, output, context):
        # Convert compatible units before copying the result to the output port.
        temperature = inputs["temperature"].in_unit(
            "http://qudt.org/vocab/unit/DEG_C"
        ).df()
        # The runtime publishes only the time/value columns assigned here.
        output["temperature"] = temperature.select("time", "value")
