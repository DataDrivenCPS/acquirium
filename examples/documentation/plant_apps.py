"""The app of the tutorial page "Declaring and deploying Apps"."""

import polars as pl

import acquirium as aq


class PumpPowerKW(aq.App):
    name = "pump-power-kw"
    grouping = "per_match"      # one output stream per pump
    backfill = True             # also calculate for the data already stored
    outputs = {
        "power": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/KiloW",
        )
    }

    def build_query(self, plant):
        return plant.query().entity("pump").measurement(alias="power", quantity_kind="power")

    def transform(self, inputs, output, context):
        df = inputs["power"].df()
        output["power"] = df.select("time", pl.col("value") / 1000)
