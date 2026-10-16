"""Lazy transforms: inputs as LazyFrames, outputs as plans, one collection per layer, shared prefixes.

A transform may read ``inputs[alias].lazy()`` (or ``lazyframe()``) and assign
a LazyFrame to an output. The scheduler collects every lazy output of a layer
in one call after all its transforms ran. ``align(..., lazy=True)`` registers
each stream's resample with the layer's plan memo, so two views aligning the
same stream the same way share one plan object, which polars computes once.
"""
from datetime import datetime, timedelta, timezone

import polars as pl
import pyarrow as pa
import pytest

from acquirium.Materialization import align, output
from acquirium.Materialization.models import OutputBuilder, OutputPort, PlanMemo, StreamDescriptor, StreamSet, TimeWindow
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import NOW
from tests.unit.test_materialization_effective_time import T, frame, rows
from tests.unit.test_materialization_readiness import CopyEach, FleetGraph


def _table(ref, points):
    return pa.table({"ref_uri": pa.array([ref] * len(points), pa.string()),
                     "time": pa.array([t for t, _ in points], pa.timestamp("us", tz="UTC")),
                     "value": pa.array([v for _, v in points], pa.float64())})


def _stream_set(alias, ref, points, memo=None, every=None):
    window = TimeWindow(points[0][0], points[-1][0])
    return StreamSet(alias, window, (StreamDescriptor(ref),), _table(ref, points), memo=memo, every=every)


def test_lazy_and_lazyframe_give_the_loaded_rows_as_a_plan():
    stream_set = _stream_set("x", "urn:a", [(T[0], 1.0), (T[1], 2.0)])
    assert isinstance(stream_set.lazy(), pl.LazyFrame) and isinstance(stream_set.lazyframe(), pl.LazyFrame)
    assert stream_set.lazy().collect().equals(stream_set.df())
    assert stream_set.lazyframe().select("value").collect()["value"].to_list() == [1.0, 2.0]


def test_a_lazy_output_is_deferred_until_collected():
    port = OutputPort("urn:out", "out", "urn:point", output.stream(value_kind="numeric"))
    builder = OutputBuilder({"out": port})
    builder["out"] = pl.LazyFrame({"time": [T[0], T[1]], "value": [1.0, 2.0]}).with_columns(pl.col("time").dt.replace_time_zone("UTC"))
    assert set(builder.deferred) == {"out"}
    assert builder.values["out"].num_rows == 2   # values collects what is still deferred
    assert not builder.deferred


def test_a_lazy_output_with_a_bad_schema_fails_at_collection_under_its_port_name():
    port = OutputPort("urn:out", "out", "urn:point", output.stream(value_kind="numeric"))
    builder = OutputBuilder({"out": port})
    builder["out"] = pl.LazyFrame({"time": [T[0]], "value": ["text"]}).with_columns(pl.col("time").dt.replace_time_zone("UTC"))
    with pytest.raises(TypeError, match="output 'out'"):
        builder.values


def test_lazy_align_matches_eager_align_and_keeps_every_streams_column():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sets = {
        "temperature": _stream_set("temperature", "urn:temp", [(start + timedelta(seconds=10), 20.0), (start + timedelta(seconds=40), 22.0), (start + timedelta(seconds=70), 24.0)]),
        "flow": _stream_set("flow", "urn:flow", [(start + timedelta(seconds=5), 1.0)]),
    }
    eager = align(sets, "1m")
    lazy = align(sets, "1m", lazy=True)
    assert isinstance(lazy, pl.LazyFrame)
    assert lazy.collect().equals(eager)


def test_lazy_align_shares_one_plan_per_stream_across_bindings():
    memo = PlanMemo()
    points = [(T[i], float(i)) for i in range(6)]
    table = _table("urn:a", points)
    shared = pl.from_arrow(table)
    # Two bindings of one layer reading the same rows get the same frame object.
    first = StreamSet("x", TimeWindow(T[0], T[5]), (StreamDescriptor("urn:a"),), table, memo=memo, _frame=shared)
    second = StreamSet("y", TimeWindow(T[0], T[5]), (StreamDescriptor("urn:a"),), table, memo=memo, _frame=shared)
    a = align({"x": first}, "2h", lazy=True)
    b = align({"y": second}, "2h", lazy=True)
    assert len(memo) == 1 and memo.hits == 1
    # Different aliases name the column differently but compute the same thing.
    assert a.collect()["x"].to_list() == b.collect()["y"].to_list() == [0.5, 2.5, 4.5]
    # A different bucket or aggregate is another plan.
    align({"x": first}, "2h", aggregate="max", lazy=True)
    assert len(memo) == 2


class LazyCopy(CopyEach):
    name = "lazy-copy"

    def transform(self, inputs, output, context):
        output["out"] = inputs["input"].lazy().select("time", "value")


class LazyTwoHourMean(CopyEach):
    name = "lazy-two-hour-mean"
    every = "2h"

    def transform(self, inputs, output, context):
        output["out"] = align(inputs, "2h", lazy=True).rename({"input": "value"})


class LazyTwoHourScaled(CopyEach):
    name = "lazy-two-hour-scaled"
    every = "2h"

    def transform(self, inputs, output, context):
        output["out"] = align(inputs, "2h", lazy=True).select("time", (pl.col("input") * 10).alias("value"))


class LazyBroken(CopyEach):
    name = "lazy-broken"

    def transform(self, inputs, output, context):
        output["out"] = inputs["input"].lazy().select("time", pl.col("missing").alias("value"))


def _runtime(tmp_path, refs):
    store = DuckDBStore(tmp_path / "lazy.duckdb")
    graph = FleetGraph(refs)
    runtime = Materializer(store, graph)
    for ref in refs:
        store.upsert_rows(ref, [(t, float(i)) for i, t in enumerate(T)], value_kind="numeric")
    return store, graph, runtime


def test_a_lazy_transform_publishes_what_the_eager_one_would(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        runtime.deploy(Deployment.from_class(LazyCopy))
        while runtime.run_once():
            pass
        outputs = {b.inputs["input"][0].ref_uri: b.outputs["out"].ref_uri for b in runtime._dag.bindings}
        assert rows(store, outputs["urn:a"]) == T
        assert frame(store, outputs["urn:b"])["value"].to_list() == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        # A later reading reaches the derived stream through the same path.
        store.upsert_rows("urn:a", [(T[5] + timedelta(hours=1), 9.0)], value_kind="numeric")
        while runtime.run_once():
            pass
        assert frame(store, outputs["urn:a"])["value"].to_list()[-1] == 9.0
        assert "collect" in runtime.timings
    finally:
        runtime.close(); store.close()


def test_two_lazy_views_over_the_same_streams_share_the_resample(tmp_path, monkeypatch):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    memos = []
    original = PlanMemo.share

    def counting(self, key, build):
        if self not in memos:
            memos.append(self)
        return original(self, key, build)
    monkeypatch.setattr(PlanMemo, "share", counting)
    try:
        runtime.deploy(Deployment.from_class(LazyTwoHourMean))
        runtime.deploy(Deployment.from_class(LazyTwoHourScaled))
        while runtime.run_once():
            pass
        by_app = {(b.application_name, b.inputs["input"][0].ref_uri): b.outputs["out"].ref_uri for b in runtime._dag.bindings}
        assert frame(store, by_app[("lazy-two-hour-mean", "urn:a")])["value"].to_list() == [0.5, 2.5, 4.5]
        assert frame(store, by_app[("lazy-two-hour-scaled", "urn:a")])["value"].to_list() == [5.0, 25.0, 45.0]
        # Both views are one layer: per stream one resample plan, asked for twice.
        layer = [memo for memo in memos if len(memo) == 2]
        assert layer and layer[-1].hits == 2
    finally:
        runtime.close(); store.close()


def test_a_failing_lazy_plan_fails_only_its_binding(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a"])
    try:
        runtime.deploy(Deployment.from_class(LazyCopy))
        runtime.deploy(Deployment.from_class(LazyBroken))
        while runtime.run_once():
            pass
        good = next(b for b in runtime._dag.bindings if b.application_name == "lazy-copy").outputs["out"].ref_uri
        assert rows(store, good) == T
        assert any("lazy-broken" in key for key in runtime.failures())
    finally:
        runtime.close(); store.close()
