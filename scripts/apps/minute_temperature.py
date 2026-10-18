"""Average each water-temperature stream into complete minutes, lazily.

Check the app without contacting a server:

    acquirium app check scripts/apps/minute_temperature.py:MinuteTemperature

The transform assigns a polars LazyFrame instead of a dataframe. The runtime
collects every lazy output of a dependency layer in one call after all its
transforms ran, so an app that buckets the same stream the same way, say a
minute maximum next to this minute mean, shares the resample with it instead
of computing it again. The eager form of the same app is in the how-to on
derived streams; both publish the same rows.
"""

from __future__ import annotations

import acquirium as aq


class MinuteTemperature(aq.App):
    """Publish a one-minute mean Celsius stream for each water-temperature input."""

    name = "minute-temperature"
    grouping = "per_match"
    # Complete UTC minutes: the runtime reads whole buckets, and align() uses
    # the same size, so a result never depends on where a batch was cut.
    every = "1m"
    backfill = True
    outputs = {
        "temperature": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
            quantity_kind="http://qudt.org/vocab/quantitykind/Temperature",
        ),
    }

    def build_query(self, plant):
        return plant.query().measurement(
            alias="temperature", quantity_kind="temperature"
        )

    def transform(self, inputs, output, context):
        converted = {
            "temperature": inputs["temperature"].in_unit(
                "http://qudt.org/vocab/unit/DEG_C"
            )
        }
        # A LazyFrame: nothing runs here. The runtime collects it with the
        # rest of the layer and validates the result under this port's name.
        output["temperature"] = aq.align(converted, lazy=True).rename(
            {"temperature": "value"}
        )
