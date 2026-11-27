"""Convert each selected Fahrenheit reading to Celsius."""
from pathlib import Path

import acquirium as aq


def convert(frame, context):
    if frame.is_empty():
        return {"celsius": None}
    fahrenheit = frame["value"][0]
    return {"celsius": (fahrenheit - 32) * 5 / 9}


def fahrenheit_to_celsius(client, parameters):
    query = client.query().measurement(
        alias="temperature", unit="http://qudt.org/vocab/unit/DEG_F"
    )
    return aq.App(
        name="fahrenheit-to-celsius",
        inputs={"temperature": aq.Selection.from_query(query, measurement="temperature")},
        outputs={"celsius": aq.Output(unit="http://qudt.org/vocab/unit/DEG_C")},
        function=aq.PythonFunction.from_file(Path(__file__), "convert"),
        window=aq.Pointwise(),
    )
