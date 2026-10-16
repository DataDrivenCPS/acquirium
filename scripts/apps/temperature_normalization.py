"""Normalize declared temperature measurements using frozen unit factors."""
from pathlib import Path
import polars as pl
import acquirium as aq

CELSIUS = "http://qudt.org/vocab/unit/DEG_C"
NAME = "temperature-normalization"


def normalize(frame, context):
    factor = lambda key: pl.col("context").str.json_path_match(f"$.{key}").cast(pl.Float64)
    return {"temperature": frame.select("event_time_ns", "sample_key",
        (((pl.col("value") + factor("from_offset")) * factor("from_multiplier")
          / factor("to_multiplier")) - factor("to_offset")).alias("value"), "quality")}


def temperature_normalization(client, parameters):
    query = client.query().measurement(alias="temperature", quantity_kind="temperature")
    return aq.App(name=NAME,
        inputs=[aq.Selection.from_query(query, alias="temperature", target_unit=CELSIUS, exclude_app=NAME)],
        outputs={"temperature": aq.Output(unit=CELSIUS,
            quantity_kind="http://qudt.org/vocab/quantitykind/Temperature")},
        function=aq.PythonFunction.from_file(Path(__file__), "normalize"))
