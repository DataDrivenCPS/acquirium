"""The turbidity tutorial's two apps: a cleaner and a sustained alarm."""

from pathlib import Path

import pandas as pd

import acquirium as aq

def declarations(client):
    calculations = Path(__file__).with_name("calculations.py")
    turbidity = (
        client.query()
        .measurement(
            alias="turbidity",
            quantity_kind="http://qudt.org/vocab/quantitykind/Turbidity",
        )
        .context(alias="location")
    )
    clean = aq.App(
        name="turbidity-clean",
        inputs={
            "raw": aq.Selection.from_query(
                turbidity,
                measurement="turbidity",
                group="location",
                # A reading this far from the local median counts as a spike.
                parameters={
                    "urn:turbidity:filter-1": {"threshold": 1.0},
                    "urn:turbidity:filter-2": {"threshold": 1.0},
                    "urn:turbidity:filter-3": {"threshold": 1.0},
                    "urn:turbidity:combined": {"threshold": 0.5},
                },
            ),
        },
        # The clean stream replaces the analyser readings it is calculated from.
        outputs={"turbidity": aq.ReplacementOutput()},
        function=aq.PythonFunction.from_file(calculations, "clean"),
        window=aq.Rolling(lookback=pd.Timedelta("5m"), step=pd.Timedelta("1m"), lookahead=pd.Timedelta("5m")),
    )
    # The same query. The clean stream is now each measurement's final stage,
    # so the alarm reads it rather than the analyser readings.
    alarm = aq.App(
        name="turbidity-alarm",
        inputs={
            "turbidity": aq.Selection.from_query(
                turbidity,
                measurement="turbidity",
                group="location",
                parameters={
                    "urn:turbidity:filter-1": {"limit_ntu": 0.5, "sustained_minutes": 15},
                    "urn:turbidity:filter-2": {"limit_ntu": 0.5, "sustained_minutes": 15},
                    "urn:turbidity:filter-3": {"limit_ntu": 0.5, "sustained_minutes": 15},
                    "urn:turbidity:combined": {"limit_ntu": 0.3, "sustained_minutes": 5},
                },
            ),
        },
        outputs={"alarm": aq.Output(on_group=True)},
        function=aq.PythonFunction.from_file(calculations, "alarm"),
        window=aq.Rolling(lookback=pd.Timedelta("15m"), step=pd.Timedelta("1m")),
    )
    return [clean, alarm]
