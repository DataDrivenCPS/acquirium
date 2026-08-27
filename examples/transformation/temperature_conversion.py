"""A transformation that converts an input temperature stream to Fahrenheit."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

import acquirium as aq


INPUT_SOURCE = "temperature-example-input"
OUTPUT_POINT = "urn:example:temperature:fahrenheit"


class CelsiusToFahrenheit(aq.Transformation):
    """Publish a Fahrenheit stream for every Celsius input sample."""

    name = "celsius-to-fahrenheit"
    start = aq.AllAvailable()
    outputs = {
        "fahrenheit": aq.outputs.stream(
            value_kind="numeric",
            point_uri=OUTPUT_POINT,
            unit="http://qudt.org/vocab/unit/DEG_F",
            data_source="temperature-example-output",
        ),
    }

    def build_query(self, aq):
        return aq.query().measurement(alias="temperature", data_source=INPUT_SOURCE)

    def transform(self, inputs, output, context):
        celsius = inputs["temperature"].collect()
        if not celsius.num_rows:
            return
        fahrenheit = pc.add(pc.multiply(celsius["value"].cast(pa.float64()), 9.0 / 5.0), 32.0)
        output["fahrenheit"] = pa.table({"time": celsius["time"], "value": fahrenheit})
