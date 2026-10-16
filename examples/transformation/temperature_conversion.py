"""A native pointwise function selected through Acquirium's semantic query API."""
from pathlib import Path
import polars as pl
import acquirium as aq


def convert(frame, context):
    return {"celsius": frame.select("event_time_ns", "sample_key",
        ((pl.col("value") - 32.0) * 5.0 / 9.0).alias("value"), "quality")}


def fahrenheit_to_celsius(client, parameters):
    query = client.query().measurement(alias="temperature", unit="http://qudt.org/vocab/unit/DEG_F")
    return aq.App(name="fahrenheit-to-celsius",
        inputs=[aq.Selection.from_query(query, alias="temperature")],
        outputs={"celsius": aq.Output(unit="http://qudt.org/vocab/unit/DEG_C")},
        function=aq.PythonFunction.from_file(Path(__file__), "convert"),
        window=aq.Pointwise())
