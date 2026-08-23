"""Class-based transformations for the synthetic two-zone building."""

import polars as pl

from acquirium import Transformation, outputs
from acquirium.Materialization.impact import pointwise
from acquirium.internals.models import compute_ref_uri


SOURCE_ID = "batch-example-building"
ZONE_A_C = str(compute_ref_uri(SOURCE_ID, "zone_a_temperature_c"))
ZONE_B_C = str(compute_ref_uri(SOURCE_ID, "zone_b_temperature_c"))
AVERAGE_C = "urn:batch-example:average-temperature-c"
TEMPERATURE_KIND = "http://qudt.org/vocab/quantitykind/Temperature"
CELSIUS =  "http://qudt.org/vocab/unit/DEG_C"


class AverageTemperature(Transformation):
    """Average all temperature streams returned by one query."""

    name = "batch-example-average-temperature"
    outputs = {
        "average": outputs.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
            ref_uri=AVERAGE_C,
        )
    }
    impact = pointwise()

    def build_query(self, aq):
        return aq.query().measurement(alias="temperature", quantity_kind=TEMPERATURE_KIND)

    def transform(self, inputs, context):
        frame = inputs.values
        zone_a = frame.filter(pl.col("ref_uri") == ZONE_A_C).select(
            "time", pl.col("value").alias("zone_a")
        )
        zone_b = frame.filter(pl.col("ref_uri") == ZONE_B_C).select(
            "time", pl.col("value").alias("zone_b")
        )
        average = (
            zone_a.join(zone_b, on="time", how="inner")
            .select("time", ((pl.col("zone_a") + pl.col("zone_b")) / 2).alias("value"))
        )
        context.outputs.declare("average", ref_uri=AVERAGE_C).write(average)


class FahrenheitPerTemperature(Transformation):
    """One query, one invocation per matched stream row."""

    name = "batch-example-fahrenheit"
    invocation = "per_row"
    outputs = {"fahrenheit": outputs.stream(
        value_kind="numeric",
        unit="http://qudt.org/vocab/unit/DEG_F",
        prefix="urn:batch-example:fahrenheit",
    )}

    def build_query(self, aq):
        return aq.query().measurement(alias="temperature", quantity_kind=TEMPERATURE_KIND, unit=CELSIUS)

    def transform(self, stream, context):
        fahrenheit = stream.values.select(
            "time", (pl.col("value") * 9 / 5 + 32).alias("value")
        )
        context.outputs.declare("fahrenheit", for_input=stream).write(fahrenheit)


APPS = (AverageTemperature, FahrenheitPerTemperature)
