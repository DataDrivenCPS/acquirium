"""Fit membrane permeability in an actor and apply it in a separate app."""

from pathlib import Path

import pandas as pd
import polars as pl

import acquirium as aq
from acquirium.functions import emit, empty


class MembraneFit:
    def initial_state(self):
        # Count successful fits to show how the actor carries state across ticks.
        return {"fits": 0}

    def on_timer(self, tick: aq.TimerFired) -> aq.Transition:
        # Both inputs come from the same published snapshot and training interval.
        end = pd.Timestamp(tick.scheduled_at)
        start = end - tick.interval
        pressure = tick.snapshot.read("pressure", start=start, end=end)
        flux = tick.snapshot.read("flux", start=start, end=end)
        pairs = pressure.select("time", pl.col("value").alias("pressure")).join(
            flux.select("time", pl.col("value").alias("flux")), on="time"
        ).drop_nulls()
        if pairs.is_empty():
            return aq.Transition(self.state)

        # Least squares for flux = permeability × pressure, with zero intercept.
        pressure_values = pairs["pressure"]
        pressure_squared = pressure_values.dot(pressure_values)
        if pressure_squared == 0:
            return aq.Transition(self.state)
        permeability = pressure_values.dot(pairs["flux"]) / pressure_squared
        # The timestamp decides which readings are run against this model.
        # Stamp it when the fit finishes so it applies to later readings, or use
        # tick.scheduled_at to also recalculate readings that arrived during the fit.
        effective_at = pd.Timestamp.now(tz="UTC")
        return aq.Transition(
            {"fits": self.state["fits"] + 1},
            {"permeability": [(effective_at, permeability)]},
        )


fit = aq.Actor(
    name="membrane-fit",
    handler=MembraneFit,
    inputs={
        "pressure": aq.Stream.from_source("membrane", "pressure"),
        "flux": aq.Stream.from_source("membrane", "flux"),
    },
    outputs={"permeability": "numeric"},
    timers={"train": pd.Timedelta("4s")},
)


def predict(frame, context):
    pressure = frame.filter(pl.col("input") == "pressure")["value"].drop_nulls()
    permeability = frame.filter(pl.col("input") == "permeability")["value"].drop_nulls()
    if pressure.is_empty() or permeability.is_empty():
        return {"flux": empty()}
    return {"flux": emit(context, pressure.item() * permeability.item())}


prediction = aq.App(
    name="predict-membrane-flux",
    inputs={
        "pressure": aq.Selection.from_stream(aq.Stream.from_source("membrane", "pressure")),
        # Read the latest coefficient available at each pressure sample's time.
        "permeability": aq.Selection.from_stream(
            aq.Stream.from_source("actor:membrane-fit", "permeability"),
            as_of=pd.Timedelta("2h"),
        ),
    },
    outputs={"flux": aq.Output()},
    function=aq.PythonFunction.from_file(Path(__file__), "predict"),
    window=aq.Pointwise(),
)
