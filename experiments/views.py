"""The view library used by every experiment.

Six views over the Benicia model, chosen to cover the runtime's shapes:
per-match bucketing (V1), a rolling window over a derived stream (V2), a
text alarm (V3), an aggregate over alarms with a named output (V4), an
aggregate over raw streams (V5), and day-long buckets (V6). V1 to V4 form a
four-deep chain. Root views select by quantity kind, so a graph edit that
adds, decommissions or swaps a measurement point changes their bindings.

Each view also carries ``oracle``, a from-scratch Polars computation over
the same inputs, used by the correctness check and the recompute baselines.
"""
from __future__ import annotations

from datetime import timedelta

import polars as pl

import acquirium as aq

S223 = "http://data.ashrae.org/standard223#"
QK = "http://qudt.org/vocab/quantitykind/"
CONCENTRATION = f"{QK}Concentration"
FLOW = f"{QK}VolumeFlowRate"
ACIDITY = f"{QK}Acidity"

EMPTY = pl.DataFrame({"time": pl.Series([], dtype=pl.Datetime("us", "UTC")),
                      "value": pl.Series([], dtype=pl.Float64)})


def _bucket(frame: pl.DataFrame, every: str, agg: pl.Expr) -> pl.DataFrame:
    if frame.is_empty():
        return EMPTY
    return (frame.sort("time").group_by_dynamic("time", every=every)
            .agg(agg.alias("value")).select("time", "value"))


class ConcentrationFiveMinute(aq.App):
    """V1: five-minute mean of every concentration measurement."""
    name = "conc-5m"
    grouping = "per_match"
    every = "5m"
    backfill = True
    # No quantity kind on outputs: a root view selecting by quantity kind must
    # not match its own or a downstream view's derived streams.
    outputs = {"mean": aq.output.stream(value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="x", quantity_kind=CONCENTRATION)

    def transform(self, inputs, output, context):
        output["mean"] = aq.align(inputs).rename({"x": "value"}).select("time", "value")

    @staticmethod
    def oracle(frame: pl.DataFrame) -> pl.DataFrame:
        return _bucket(frame, "5m", pl.col("value").mean())


class ConcentrationRollingHour(aq.App):
    """V2: one-hour rolling mean over V1's output."""
    name = "conc-1h-rolling"
    grouping = "per_match"
    lookback = "1h"
    backfill = True
    outputs = {"rolling": aq.output.stream(value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="x", app="conc-5m")

    def transform(self, inputs, output, context):
        frame = inputs["x"].df().sort("time")
        output["rolling"] = frame.select("time", pl.col("value").rolling_mean_by("time", window_size="1h"))

    @staticmethod
    def oracle(frame: pl.DataFrame) -> pl.DataFrame:
        if frame.is_empty():
            return EMPTY
        return frame.sort("time").select("time", pl.col("value").rolling_mean_by("time", window_size="1h"))


class ConcentrationHigh(aq.App):
    """V3: a text alarm wherever V2's rolling mean exceeds a threshold."""
    name = "conc-high"
    grouping = "per_match"
    backfill = True
    outputs = {"alarm": aq.output.stream(value_kind="text")}

    def __init__(self, threshold: float = 90.0):
        self.threshold = float(threshold)

    def build_query(self, plant):
        return plant.query().measurement(alias="x", app="conc-1h-rolling")

    def transform(self, inputs, output, context):
        frame = inputs["x"].df()
        output["alarm"] = frame.filter(pl.col("value") > self.threshold).select(
            "time", pl.lit("high").alias("value"))

    @staticmethod
    def oracle(frame: pl.DataFrame, threshold: float = 90.0) -> pl.DataFrame:
        if frame.is_empty():
            return pl.DataFrame({"time": pl.Series([], dtype=pl.Datetime("us", "UTC")),
                                 "value": pl.Series([], dtype=pl.Utf8)})
        return frame.filter(pl.col("value") > threshold).select("time", pl.lit("high").alias("value"))


class AlarmCountHour(aq.App):
    """V4: alarms per hour across every V3 stream, one named output."""
    name = "alarm-count-1h"
    grouping = "all_matches"
    every = "1h"
    backfill = True
    outputs = {"count": aq.output.named("alarm-count-1h", value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="alarms", app="conc-high")

    def transform(self, inputs, output, context):
        wide = aq.align(inputs, aggregate="count")
        output["count"] = wide.select(
            "time", pl.sum_horizontal(pl.exclude("time")).cast(pl.Float64).alias("value"))

    @staticmethod
    def oracle(frames: list[pl.DataFrame]) -> pl.DataFrame:
        rows = [frame for frame in frames if not frame.is_empty()]
        if not rows:
            return EMPTY
        return _bucket(pl.concat(rows).select("time", pl.lit(1.0).alias("value")), "1h",
                       pl.col("value").count().cast(pl.Float64))


class FlowTotalFiveMinute(aq.App):
    """V5: total of every flow-rate measurement per five minutes, named."""
    name = "flow-total-5m"
    grouping = "all_matches"
    every = "5m"
    backfill = True
    outputs = {"total": aq.output.named("flow-total-5m", value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="flow", quantity_kind=FLOW)

    def transform(self, inputs, output, context):
        wide = aq.align(inputs)
        output["total"] = wide.select("time", pl.sum_horizontal(pl.exclude("time")).alias("value"))

    @staticmethod
    def oracle(frames: list[pl.DataFrame]) -> pl.DataFrame:
        means = [_bucket(frame, "5m", pl.col("value").mean()) for frame in frames if not frame.is_empty()]
        if not means:
            return EMPTY
        return (pl.concat(means).group_by("time").agg(pl.col("value").sum()).sort("time"))


class AcidityDailyRange(aq.App):
    """V6: daily max minus min of every pH measurement."""
    name = "ph-daily-range"
    grouping = "per_match"
    every = "1d"
    backfill = True
    outputs = {"range": aq.output.stream(value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="x", quantity_kind=ACIDITY)

    def transform(self, inputs, output, context):
        high = aq.align(inputs, aggregate="max").rename({"x": "high"})
        low = aq.align(inputs, aggregate="min").rename({"x": "low"})
        output["range"] = high.join(low, on="time").select(
            "time", (pl.col("high") - pl.col("low")).alias("value"))

    @staticmethod
    def oracle(frame: pl.DataFrame) -> pl.DataFrame:
        return _bucket(frame, "1d", pl.col("value").max() - pl.col("value").min())


VIEWS = [ConcentrationFiveMinute, ConcentrationRollingHour, ConcentrationHigh,
         AlarmCountHour, FlowTotalFiveMinute, AcidityDailyRange]
CHAIN = [ConcentrationFiveMinute, ConcentrationRollingHour, ConcentrationHigh, AlarmCountHour]
BY_NAME = {view.name: view for view in VIEWS}


def deploy(client, views=VIEWS, *, parameters: dict[str, dict] | None = None,
           retries: int = 10) -> None:
    """Deploy views in order, waiting out the replan each deployment triggers.

    Publishing a view's lineage advances the graph version, so a deployment
    issued while that publication is still being folded into the query
    cache is rejected as compiled against a changing graph. Wait for the
    cache to be current and retry that one rejection.
    """
    import time
    for view in views:
        for attempt in range(retries):
            while not client.graph_status().get("is_current", False):
                time.sleep(0.1)
            try:
                client.deploy_app(view, parameters=(parameters or {}).get(view.name))
                break
            except Exception as error:  # requests.HTTPError carries the server message
                if "graph changed" not in str(error) or attempt == retries - 1:
                    raise
                time.sleep(0.5)
