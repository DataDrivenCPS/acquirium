"""Views for the engine sanity runs. Importable by the planner (module path
``experiments.engine_sanity.apps``)."""
from __future__ import annotations

from datetime import timedelta

import pyarrow as pa
import pyarrow.compute as pc

from acquirium.Materialization.models import App, output

TEMPERATURE = "http://qudt.org/vocab/quantitykind/Temperature"


def _empty() -> pa.Table:
    return pa.table({"time": pa.array([], pa.timestamp("us", tz="UTC")), "value": pa.array([], pa.float64())})


def _inside(table: pa.Table, window) -> pa.Table:
    return table.filter(pc.and_(pc.greater_equal(table["time"], pa.scalar(window.start)),
                                pc.less_equal(table["time"], pa.scalar(window.end))))


class Double(App):
    name = "double"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        table = _inside(inputs["t"].collect(), context.output_window)
        output["out"] = pa.table({"time": table["time"], "value": pc.multiply(table["value"], 2.0)})


class RollingMean(App):
    """Mean over the last 10 minutes, at every sample."""
    name = "rolling-mean"
    grouping = "per_match"
    lookback = "10m"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        import polars as pl
        frame = inputs["t"].df().sort("time")
        if frame.is_empty():
            output["out"] = _empty()
            return
        rolled = frame.rolling("time", period="10m").agg(pl.col("value").mean().alias("value"))
        window = context.output_window
        rolled = rolled.filter((pl.col("time") >= window.start) & (pl.col("time") <= window.end))
        output["out"] = rolled.select("time", "value").to_arrow()


class PlantMean(App):
    """Mean of every doubled stream at each time: selects generated outputs."""
    name = "plant-mean"
    grouping = "all_matches"
    outputs = {"mean": output.named("plant-mean", value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="d", app="double")
    def transform(self, inputs, output, context):
        import polars as pl
        frame = inputs["d"].df()
        if frame.is_empty():
            output["mean"] = _empty()
            return
        window = context.output_window
        mean = (frame.filter((pl.col("time") >= window.start) & (pl.col("time") <= window.end))
                .group_by("time").agg(pl.col("value").mean()).sort("time"))
        output["mean"] = mean.to_arrow()


class Ewma(App):
    """y(t) = a x(t) + (1 - a) y(t - 1 min) on the plant mean: a loop on its
    own named output, read with a one-minute delay."""
    name = "ewma"
    grouping = "all_matches"
    named_inputs = {"x": "plant-mean", "previous": "plant-ewma"}
    windows = {"x": ("0s", "0s"), "previous": ("-1m", "-1m")}
    outputs = {"y": output.named("plant-ewma", value_kind="numeric")}
    def transform(self, inputs, output, context):
        x = inputs["x"].collect()
        prev = inputs["previous"].collect()
        previous = dict(zip(prev["time"].to_pylist(), prev["value"].to_pylist()))
        rows = []
        for t, value in sorted(zip(x["time"].to_pylist(), x["value"].to_pylist())):
            if context.output_window.start <= t <= context.output_window.end:
                y = 0.3 * value + 0.7 * previous.get(t - timedelta(minutes=1), value)
                previous[t] = y
                rows.append((t, y))
        output["y"] = pa.table({"time": pa.array([r[0] for r in rows], pa.timestamp("us", tz="UTC")),
                                "value": pa.array([r[1] for r in rows], pa.float64())})
