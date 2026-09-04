from datetime import datetime, timedelta, timezone
from threading import Lock
from time import sleep

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from acquirium.Materialization import (
    App, ApplicationGraph, Binding, InProcessExecutor, InputBatch, RevisionStore, Scheduler,
    StreamDescriptor, StreamSet, TimeWindow, align, output,
)
from acquirium.Materialization.planner import BindingPlanner, Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.models import compute_ref_uri

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _context(row, result):
    """An InputBatch carrying just the match information under test."""
    return InputBatch("sig", 1, 0, 1, TimeWindow(NOW, NOW), TimeWindow(NOW, NOW), row, result)


class Mean(App):
    backfill = True
    lookback = "1m"
    outputs = {"mean": output.per_row(value_kind="numeric")}

    def transform(self, inputs, output, context):
        assert inputs["left"].window == inputs["right"].window == context.read_window
        source = inputs["left"].collect()
        output["mean"] = pa.table({"time": source["time"], "value": pa.array([3.0])})


class LineageCopy(App):
    name = "lineage-copy"
    outputs = {"out": output.per_row(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class MixedFanOut(App):
    name = "mixed-fan-out"
    outputs = {
        "each": output.per_row(value_kind="numeric"),
        "total": output.named("kpi", value_kind="numeric"),
    }
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class NamedTotal(App):
    name = "named-total"
    outputs = {"total": output.named("plant-total", value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class Copy(App):
    backfill = True
    outputs = {"out": output.per_row(value_kind="numeric")}

    def transform(self, inputs, output, context):
        source = inputs["source"].collect()
        output["out"] = pa.table({"time": source["time"], "value": source["value"]})


class ConfiguredLookback(App):
    outputs = {"out": output.per_row(value_kind="numeric")}

    def __init__(self, window="5m"):
        self.lookback = window


class WholeStream(App):
    lookback = "all"
    backfill = True
    min_interval = "5m"
    outputs = {"out": output.per_row(value_kind="numeric")}


class ConcurrentProbeExecutor(InProcessExecutor):
    def __init__(self):
        self.active = self.peak = 0
        self.lock = Lock()

    def execute(self, application, batch, ports):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            sleep(0.05)
            return super().execute(application, batch, ports)
        finally:
            with self.lock:
                self.active -= 1


class DeferredProbeExecutor:
    """A fake Ray executor that records whether a wave was fully submitted."""
    def __init__(self):
        self.pending = []
        self.submitted_before_first_resolution = 0

    def submit(self, application, batch, ports):
        self.pending.append((application, batch, ports))
        return len(self.pending) - 1

    def resolve(self, ticket):
        if self.submitted_before_first_resolution == 0:
            self.submitted_before_first_resolution = len(self.pending)
        application, batch, ports = self.pending[ticket]
        return InProcessExecutor().execute(application, batch, ports)


class LineageGraph:
    def __init__(self): self.published = []
    def graph_status(self): return {"published_version": 1}
    def sparql_query(self, query, **kwargs):
        return {"columns": ["v0", "ext0", "unit0", "extunit0"], "rows": [["urn:point", "urn:input", None, None]]}
    def insert_graph(self, graph, **kwargs): self.published.append(graph)


class MultiStreamLineageGraph(LineageGraph):
    def sparql_query(self, query, **kwargs):
        return {
            "columns": ["v0", "ext0", "unit0", "extunit0"],
            "rows": [
                ["urn:point-a", "urn:input-a", None, None],
                ["urn:point-b", "urn:input-b", None, None],
            ],
        }


class PairedRowGraph(LineageGraph):
    """Two RO units, each row pairing a flow and a pressure stream."""
    def sparql_query(self, query, **kwargs):
        return {
            "columns": ["v0", "v1", "ext1", "unit1", "extunit1", "lbl1",
                        "v2", "ext2", "unit2", "extunit2", "lbl2"],
            "rows": [[f"urn:ro-{n}", f"urn:p-f{n}", f"urn:ref-f{n}", None, None, f"flow {n}",
                      f"urn:p-p{n}", f"urn:ref-p{n}", None, None, f"pressure {n}"] for n in (1, 2)],
        }


class PairedApp(App):
    name = "paired"
    outputs = {"ratio": output.per_row(value_kind="numeric")}
    def build_query(self, plant):
        # Two aliases the query genuinely separates, by quantity kind.
        return (plant.query().entity("urn:ReverseOsmosis", alias="ro")
                .measurement(frm="ro", alias="flow", quantity_kind="urn:qk/VolumeFlowRate")
                .measurement(frm="ro", alias="pressure", quantity_kind="urn:qk/Pressure"))
    def transform(self, inputs, output, context): pass


def planner_for(graph, app):
    return BindingPlanner(graph).compile((Deployment.from_class(app),), graph_revision=1)


class LinearFakeConverter:
    """Celsius to Fahrenheit without a QUDT graph."""
    def convert(self, value, from_unit, to_unit):
        assert from_unit == "urn:unit:DEG_C" and to_unit == "urn:unit:DEG_F"
        return value * 9.0 / 5.0 + 32.0


def test_revision_frontier_commits_coherent_output_and_converges(tmp_path):
    store = DuckDBStore(tmp_path / "timeseries.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:left", [(timestamp, 2.0)], value_kind="numeric")
    store.upsert_rows("urn:right", [(timestamp, 4.0)], value_kind="numeric")
    inputs = {"left": (StreamDescriptor("urn:left"),), "right": (StreamDescriptor("urn:right"),)}
    binding = Binding("mean", "digest", inputs, {
        "mean": (Binding.derive_output_uri("mean", "mean", inputs), Mean.outputs["mean"]),
    }, timedelta(minutes=1))
    scheduler = Scheduler(RevisionStore(store), InProcessExecutor())

    assert scheduler.run_once(binding, Mean())
    assert not scheduler.run_once(binding, Mean())
    assert RevisionStore(store).current_revision() == 3
    output = list(store.timeseries(binding.outputs["mean"][0], value_mode="numeric"))[0]
    assert output.column("value").to_pylist() == [3.0]
    with store._own_conn() as conn:
        assert conn.execute("SELECT source_id FROM streams WHERE ref_uri=?", [binding.outputs["mean"][0]]).fetchone()[0] == "derived:mean"


def test_graph_rejects_cycles_and_duplicate_output_ownership():
    input_a = {"source": (StreamDescriptor("urn:b"),)}
    input_b = {"source": (StreamDescriptor("urn:a"),)}
    spec = output.per_row(value_kind="numeric")
    a = Binding("a", "a", input_a, {"out": ("urn:a", spec)})
    b = Binding("b", "b", input_b, {"out": ("urn:b", spec)})
    try:
        ApplicationGraph((a, b))
    except ValueError as error:
        assert "cycle" in str(error)
    else:
        raise AssertionError("cycle was accepted")


def test_derived_output_uri_uses_the_managed_reference_identity():
    inputs = {"source": (StreamDescriptor("urn:input"),)}
    ref_name = Binding.derive_output_ref_name("out", inputs)

    assert Binding.derive_output_uri("derived-app", "out", inputs) == str(
        compute_ref_uri("derived:derived-app", ref_name)
    )


def test_named_output_keeps_its_exact_reference_name():
    inputs = {"source": (StreamDescriptor("urn:input"),)}
    spec = output.named("plant-total", value_kind="numeric")

    assert Binding.derive_output_ref_name("out", inputs, spec) == "plant-total"
    assert Binding.derive_output_uri("kpi-app", "out", inputs, spec) == str(
        compute_ref_uri("derived:kpi-app", "plant-total")
    )


def test_named_output_cannot_ride_along_with_fan_out():
    planner = BindingPlanner(MultiStreamLineageGraph())

    with pytest.raises(ValueError, match="named output"):
        planner.compile((Deployment.from_class(MixedFanOut),), graph_revision=1)


def test_mixed_outputs_are_allowed_for_a_single_input_group():
    planner = BindingPlanner(LineageGraph())

    application_graph, _ = planner.compile((Deployment.from_class(MixedFanOut),), graph_revision=1)

    (binding,) = application_graph.bindings
    assert binding.output_ref_name("total") == "kpi"
    assert binding.output_ref_name("each").startswith("each:")


def test_deployment_persists_constructor_parameters_and_policy():
    deployment = Deployment.from_class(ConfiguredLookback, parameters={"window": "15m"})
    restored = Deployment.from_json(deployment.to_json())

    assert restored.parameters == {"window": "15m"}
    assert restored.lookback == timedelta(minutes=15)


def test_durations_accept_every_suffix_and_reject_the_rest():
    from acquirium.Materialization.incremental import _duration

    assert _duration("250ms") == timedelta(milliseconds=250)
    assert _duration("30s") == timedelta(seconds=30)
    assert _duration("5m") == timedelta(minutes=5)
    assert _duration("2h") == timedelta(hours=2)
    # A week of context is a normal lookback for gap filling; days spell it.
    assert _duration("7d") == timedelta(days=7)
    assert _duration("1.5d") == timedelta(days=1, hours=12)
    assert _duration(timedelta(days=7)) == timedelta(days=7)

    for bad in ("7w", "7", "-1d"):
        with pytest.raises(ValueError):
            _duration(bad)


def test_a_lookback_in_days_round_trips_through_a_deployment():
    deployment = Deployment.from_class(ConfiguredLookback, parameters={"window": "7d"})

    assert Deployment.from_json(deployment.to_json()).lookback == timedelta(days=7)


def test_whole_stream_attributes_round_trip():
    deployment = Deployment.from_class(WholeStream)
    restored = Deployment.from_json(deployment.to_json())

    assert restored.lookback is None          # "all"
    assert restored.backfill is True
    assert restored.min_interval == timedelta(minutes=5)


def test_named_outputs_aggregate_the_complete_query_result():
    planner = BindingPlanner(MultiStreamLineageGraph())

    application_graph, _ = planner.compile((Deployment.from_class(NamedTotal),), graph_revision=1)

    (binding,) = application_graph.bindings
    assert binding.output_ref_name("total") == "plant-total"
    assert {item.ref_uri for item in binding.inputs["input"]} == {
        "urn:input-a", "urn:input-b",
    }


def test_per_row_outputs_bind_one_group_per_query_row():
    graph = MultiStreamLineageGraph()
    planner = BindingPlanner(graph)

    application_graph, _ = planner.compile((Deployment.from_class(LineageCopy),), graph_revision=1)

    assert len(application_graph.bindings) == 2
    assert {binding.inputs["input"][0].ref_uri for binding in application_graph.bindings} == {
        "urn:input-a", "urn:input-b",
    }


def test_progress_survives_code_and_parameter_edits(tmp_path):
    store = DuckDBStore(tmp_path / "progress.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:left", [(timestamp, 1.0)], value_kind="numeric")
    inputs = {"source": (StreamDescriptor("urn:left"),)}
    ports = {"out": ("urn:out:left", Copy.outputs["out"])}
    before = Binding("copy", "digest-before-edit", inputs, ports)
    after = Binding("copy", "digest-after-edit", inputs, ports, parameters={"tweak": 1})
    scheduler = Scheduler(RevisionStore(store), InProcessExecutor())

    assert before.signature != after.signature
    assert before.progress_key == after.progress_key
    assert scheduler.run_once(before, Copy())
    # The edited deployment reads the same inputs and writes the same outputs,
    # so it resumes the frontier instead of resetting or skipping anything.
    assert not scheduler.run_once(after, Copy())


def test_in_unit_converts_every_accessor_of_the_stream_set():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    window = TimeWindow(start, start + timedelta(minutes=1))
    table = pa.table({
        "ref_uri": pa.array(["urn:temp", "urn:temp"], pa.string()),
        "time": pa.array([start, start + timedelta(seconds=30)], pa.timestamp("us", tz="UTC")),
        "value": pa.array([0.0, 100.0], pa.float64()),
    })
    stream_set = StreamSet("temperature", window, (StreamDescriptor("urn:temp", unit="urn:unit:DEG_C"),),
                           table, table.slice(1), converter=LinearFakeConverter())

    fahrenheit = stream_set.in_unit("urn:unit:DEG_F")

    assert fahrenheit.collect()["value"].to_pylist() == pytest.approx([32.0, 212.0])
    assert fahrenheit.changes["value"].to_pylist() == pytest.approx([212.0])
    assert fahrenheit.df()["value"].to_list() == pytest.approx([32.0, 212.0])
    assert fahrenheit.streams[0].unit == "urn:unit:DEG_F"


def test_in_unit_requires_a_converter_and_recorded_units():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    window = TimeWindow(start, start)
    unitless = StreamSet("temperature", window, (StreamDescriptor("urn:temp"),))
    with pytest.raises(RuntimeError, match="unit converter"):
        unitless.in_unit("urn:unit:DEG_F")
    with pytest.raises(ValueError, match="no recorded unit"):
        StreamSet("temperature", window, (StreamDescriptor("urn:temp"),),
                  converter=LinearFakeConverter()).in_unit("urn:unit:DEG_F")


def test_named_output_gets_every_matched_row_for_grouping():
    planner = BindingPlanner(MultiStreamLineageGraph())

    application_graph, _ = planner.compile((Deployment.from_class(NamedTotal),), graph_revision=1)

    (binding,) = application_graph.bindings
    # An aggregate sees the whole table, so it can group on the match itself.
    assert binding.row is None
    result = _context(None, binding.result).result
    assert result.height == 2
    assert sorted(result["input_ref"].to_list()) == ["urn:input-a", "urn:input-b"]


def test_every_per_row_call_sees_the_whole_query_result():
    planner = BindingPlanner(MultiStreamLineageGraph())

    application_graph, _ = planner.compile((Deployment.from_class(LineageCopy),), graph_revision=1)

    assert len(application_graph.bindings) == 2
    for binding in application_graph.bindings:
        context = _context(binding.row, binding.result)
        # Its own row, but the whole fleet it belongs to.
        assert context.row["input_ref"] == binding.inputs["input"][0].ref_uri
        assert context.result.height == 2


def test_row_is_the_per_row_accessor_and_refuses_to_guess():
    one = _context({"hx": "urn:hx-1"}, ({"hx": "urn:hx-1"}, {"hx": "urn:hx-2"}))
    aggregate = _context(None, ({"hx": "urn:hx-1"}, {"hx": "urn:hx-2"}))

    assert one.row == {"hx": "urn:hx-1"}
    assert one.result.height == 2          # result is the whole query either way
    with pytest.raises(ValueError, match="named output"):
        aggregate.row
    assert aggregate.result.height == 2


def test_stream_names_the_single_bound_stream_and_refuses_to_guess():
    window = TimeWindow(datetime(2026, 1, 1, tzinfo=timezone.utc),
                        datetime(2026, 1, 1, tzinfo=timezone.utc))
    one = StreamSet("temperature", window, (StreamDescriptor("urn:a", label="Basin 1"),))
    several = StreamSet("temperature", window,
                        (StreamDescriptor("urn:a"), StreamDescriptor("urn:b")))

    # A per_row call binds one stream, so this is how it asks which.
    assert one.stream.ref_uri == "urn:a" and one.stream.label == "Basin 1"
    with pytest.raises(ValueError, match="named output sees every match"):
        several.stream
    assert [d.ref_uri for d in several.streams] == ["urn:a", "urn:b"]


def test_per_row_binds_one_stream_per_alias_even_for_paired_rows():
    # The invariant behind .stream: a per_row call binds exactly one stream
    # under every alias, so a flow/pressure row gives one of each.
    graph = PairedRowGraph()

    application_graph, _ = planner_for(graph, PairedApp)

    assert len(application_graph.bindings) == 2
    for binding in application_graph.bindings:
        assert sorted(binding.inputs) == ["flow", "pressure"]
        assert all(len(streams) == 1 for streams in binding.inputs.values())


def test_context_carries_the_match_and_not_the_data(tmp_path):
    store = DuckDBStore(tmp_path / "context.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:left", [(timestamp, 1.0)], value_kind="numeric")
    inputs = {"source": (StreamDescriptor("urn:left"),)}
    binding = Binding("copy", "digest", inputs, {"out": ("urn:out:left", Copy.outputs["out"])},
                      row={"hx": "urn:plant/hx-1"}, result=({"hx": "urn:plant/hx-1"},))
    revisions = RevisionStore(store)
    revisions.initialise(binding, True)

    batch = revisions.preview_batch(binding)

    # The data arrives beside the context, never inside it.
    assert not hasattr(batch.context, "inputs")
    assert batch.context.row == {"hx": "urn:plant/hx-1"}
    assert batch.context.result["hx"].to_list() == ["urn:plant/hx-1"]
    assert batch.inputs["source"].stream.ref_uri == "urn:left"


def test_output_schema_violations_name_the_port():
    from acquirium.Materialization import OutputBuilder
    builder = OutputBuilder({"celsius": ("urn:out", output.per_row(value_kind="numeric"))})
    bad = pa.table({"time": pa.array([datetime(2026, 1, 1, tzinfo=timezone.utc)], pa.timestamp("us", tz="UTC")),
                    "value": pa.array([1.0]), "ref_uri": pa.array(["urn:x"])})
    with pytest.raises(TypeError, match="output 'celsius'"):
        builder["celsius"] = bad
    with pytest.raises(KeyError, match="not declared in this app's outputs"):
        builder["fahrenheit"] = bad


def test_a_text_output_accepts_a_polars_frame():
    """Polars renders strings as Arrow large_string; a text port takes both."""
    import polars as pl

    from acquirium.Materialization import OutputBuilder
    builder = OutputBuilder({"alarm": ("urn:out", output.per_row(value_kind="text"))})

    builder["alarm"] = pl.DataFrame({
        "time": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
        "value": ["turbidity over limit"],
    })

    stored = builder.values["alarm"]
    assert stored["value"].type == pa.string()
    assert stored["value"].to_pylist() == ["turbidity over limit"]


def test_align_resamples_every_stream_onto_one_clock():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    window = TimeWindow(start, start + timedelta(minutes=2))
    def table(ref, points):
        return pa.table({
            "ref_uri": pa.array([ref] * len(points), pa.string()),
            "time": pa.array([start + offset for offset, _ in points], pa.timestamp("us", tz="UTC")),
            "value": pa.array([value for _, value in points], pa.float64()),
        })
    inputs = {
        "temperature": StreamSet("temperature", window, (StreamDescriptor("urn:temp"),),
            table("urn:temp", [(timedelta(seconds=10), 20.0), (timedelta(seconds=40), 22.0), (timedelta(seconds=70), 24.0)])),
        "flow": StreamSet("flow", window, (StreamDescriptor("urn:flow"),),
            table("urn:flow", [(timedelta(seconds=5), 1.0)])),
    }

    frame = align(inputs, "1m")

    assert frame.columns == ["time", "flow", "temperature"]
    assert frame["temperature"].to_list() == [21.0, 24.0]
    assert frame["flow"].to_list()[0] == 1.0 and frame["flow"].to_list()[1] is None


def test_scheduler_runs_independent_topological_wave_concurrently(tmp_path):
    store = DuckDBStore(tmp_path / "parallel.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:left", [(timestamp, 1.0)], value_kind="numeric")
    store.upsert_rows("urn:right", [(timestamp, 2.0)], value_kind="numeric")
    spec = Copy.outputs["out"]
    left = Binding("left-copy", "copy", {"source": (StreamDescriptor("urn:left"),)}, {"out": ("urn:out:left", spec)})
    right = Binding("right-copy", "copy", {"source": (StreamDescriptor("urn:right"),)}, {"out": ("urn:out:right", spec)})
    graph = ApplicationGraph((left, right))
    assert len(graph.layers()) == 1
    assert {binding.signature for binding in graph.layers()[0]} == {left.signature, right.signature}
    executor = ConcurrentProbeExecutor()
    scheduler = Scheduler(RevisionStore(store), executor)

    assert scheduler.run_graph_once(graph, {left.signature: Copy(), right.signature: Copy()}, max_workers=2)
    assert executor.peak == 2


def test_scheduler_submits_an_entire_async_wave_before_resolving(tmp_path):
    store = DuckDBStore(tmp_path / "async-wave.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:left", [(timestamp, 1.0)], value_kind="numeric")
    store.upsert_rows("urn:right", [(timestamp, 2.0)], value_kind="numeric")
    spec = Copy.outputs["out"]
    left = Binding("left-copy", "copy", {"source": (StreamDescriptor("urn:left"),)}, {"out": ("urn:out:left", spec)})
    right = Binding("right-copy", "copy", {"source": (StreamDescriptor("urn:right"),)}, {"out": ("urn:out:right", spec)})
    executor = DeferredProbeExecutor()
    scheduler = Scheduler(RevisionStore(store), executor)

    assert scheduler.run_graph_once(ApplicationGraph((left, right)), {left.signature: Copy(), right.signature: Copy()})
    assert executor.submitted_before_first_resolution == 2
    assert RevisionStore(store).current_revision() == 3


class EntityRowGraph(LineageGraph):
    """One entity node (0) related to one data node (1), twice over."""
    def sparql_query(self, query, **kwargs):
        return {
            "columns": ["v0", "v1", "ext1", "unit1", "extunit1", "lbl1"],
            "rows": [
                ["urn:ro-1", "urn:point-1", "urn:input-1", None, None, "Feed pressure 1"],
                ["urn:ro-2", "urn:point-2", "urn:input-2", None, None, "Feed pressure 2"],
            ],
        }


class PerRowEntity(App):
    name = "per-row-entity"
    outputs = {"out": output.per_row(value_kind="numeric")}
    def build_query(self, plant):
        return (plant.query().entity("urn:ReverseOsmosis", alias="ro")
                .measurement(frm="ro", alias="input"))
    def transform(self, inputs, output, context): pass


def test_context_carries_the_bound_rows_entities_and_labels():
    planner = BindingPlanner(EntityRowGraph())

    application_graph, _ = planner.compile((Deployment.from_class(PerRowEntity),), graph_revision=1)

    by_ref = {binding.inputs["input"][0].ref_uri: binding for binding in application_graph.bindings}
    first = by_ref["urn:input-1"].row
    assert first["ro"] == "urn:ro-1"
    assert first["input_ref"] == "urn:input-1" and first["input.label"] == "Feed pressure 1"
    assert by_ref["urn:input-2"].row["ro"] == "urn:ro-2"
    assert by_ref["urn:input-1"].inputs["input"][0].label == "Feed pressure 1"


class CheckDouble(App):
    name = "check-double"
    outputs = {"doubled": output.per_row(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context):
        source = inputs["input"].collect()
        output["doubled"] = pa.table({
            "time": source["time"], "value": pc.multiply(source["value"], 2.0),
        })


class CheckBroken(CheckDouble):
    name = "check-broken"
    def transform(self, inputs, output, context):
        raise ValueError("sensor calibration missing")


class CheckUndeclared(CheckDouble):
    name = "check-undeclared"
    def transform(self, inputs, output, context):
        output["dubbled"] = inputs["input"].collect()


def test_check_returns_computed_rows_without_writing_anything(tmp_path):
    store = DuckDBStore(tmp_path / "check.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:input", [(timestamp, 2.0), (timestamp + timedelta(minutes=1), 3.0)],
                      value_kind="numeric")
    materializer = Materializer(store, LineageGraph())
    revision_before = materializer._revisions.current_revision()

    result = materializer.check(Deployment.from_class(CheckDouble))

    (binding,) = result["bindings"]
    assert binding["error"] is None
    assert binding["inputs"]["input"][0]["ref_uri"] == "urn:input"
    assert binding["input_rows"] == {"input": 2}
    doubled = binding["outputs"]["doubled"]
    assert [row["value"] for row in doubled["values"]] == [4.0, 6.0]
    assert doubled["rows"] == 2 and doubled["truncated"] is False

    # A check is a dry run: no derived stream, no progress, no new revision.
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM streams WHERE source_id LIKE 'derived:%'").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM binding_progress").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM materialization_deployments").fetchone()[0] == 0
    assert materializer._revisions.current_revision() == revision_before


def test_check_limit_heads_the_output_and_says_so(tmp_path):
    store = DuckDBStore(tmp_path / "check-limit.duckdb")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:input", [(start + timedelta(minutes=i), float(i)) for i in range(10)],
                      value_kind="numeric")
    materializer = Materializer(store, LineageGraph())

    full = materializer.check(Deployment.from_class(CheckDouble))
    headed = materializer.check(Deployment.from_class(CheckDouble), limit=3)

    assert len(full["bindings"][0]["outputs"]["doubled"]["values"]) == 10
    assert full["bindings"][0]["outputs"]["doubled"]["truncated"] is False
    headed_output = headed["bindings"][0]["outputs"]["doubled"]
    assert [row["value"] for row in headed_output["values"]] == [0.0, 2.0, 4.0]
    assert headed_output["rows"] == 10 and headed_output["truncated"] is True


def test_check_reports_a_failing_transform_as_a_result(tmp_path):
    store = DuckDBStore(tmp_path / "check-broken.duckdb")
    store.upsert_rows("urn:input", [(datetime(2026, 1, 1, tzinfo=timezone.utc), 2.0)], value_kind="numeric")
    materializer = Materializer(store, LineageGraph())

    result = materializer.check(Deployment.from_class(CheckBroken))

    assert "sensor calibration missing" in result["bindings"][0]["error"]


def test_check_flags_an_assignment_to_an_undeclared_output(tmp_path):
    store = DuckDBStore(tmp_path / "check-undeclared.duckdb")
    store.upsert_rows("urn:input", [(datetime(2026, 1, 1, tzinfo=timezone.utc), 2.0)], value_kind="numeric")
    materializer = Materializer(store, LineageGraph())

    result = materializer.check(Deployment.from_class(CheckUndeclared))

    error = result["bindings"][0]["error"]
    assert "'dubbled'" in error and "not declared" in error and "'doubled'" in error


def test_preview_batch_leaves_the_deployed_apps_progress_alone(tmp_path):
    store = DuckDBStore(tmp_path / "preview.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:left", [(timestamp, 1.0)], value_kind="numeric")
    inputs = {"source": (StreamDescriptor("urn:left"),)}
    binding = Binding("copy", "digest", inputs, {"out": ("urn:out:left", Copy.outputs["out"])})
    revisions = RevisionStore(store)
    scheduler = Scheduler(revisions, InProcessExecutor())
    assert scheduler.run_once(binding, Copy())

    # The frontier is caught up, so a real batch is None while a preview still
    # reads every stored row.
    assert revisions.next_batch(binding) is None
    preview = revisions.preview_batch(binding)

    assert preview is not None and preview.inputs["source"].collect().num_rows == 1
    with store._own_conn() as conn:
        consumed = conn.execute("SELECT consumed_revision FROM binding_progress WHERE progress_key=?",
                                [binding.progress_key]).fetchone()[0]
    assert consumed == revisions.current_revision()


def test_output_declarations_are_validated_before_deployment():
    class BadSpec(App):
        name = "bad-spec"
        outputs = {"out": "numeric"}
        def build_query(self, plant): return plant.query().measurement(alias="input")

    class Colliding(App):
        name = "colliding"
        outputs = {"a": output.named("total", value_kind="numeric"),
                   "b": output.named("total", value_kind="numeric")}
        def build_query(self, plant): return plant.query().measurement(alias="input")

    with pytest.raises(TypeError, match="aq.output.per_row"):
        Deployment.from_class(BadSpec)
    with pytest.raises(ValueError, match="claim the stream name"):
        Deployment.from_class(Colliding)


def test_remove_forgets_the_apps_durable_progress(tmp_path):
    store = DuckDBStore(tmp_path / "remove.duckdb")
    materializer = Materializer(store, LineageGraph())
    materializer.deploy(Deployment.from_class(LineageCopy))
    materializer.refresh()
    with store._own_conn() as conn:
        (progress_key,) = conn.execute("SELECT DISTINCT progress_key FROM materialization_lineage").fetchone()
    with store._lock, store._write_conn() as conn:
        conn.execute("INSERT INTO binding_progress VALUES (?, ?)", [progress_key, 7])

    materializer.remove("lineage-copy")

    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM binding_progress").fetchone()[0] == 0


def test_compiled_binding_publishes_structural_lineage(tmp_path):
    graph = LineageGraph()
    materializer = Materializer(DuckDBStore(tmp_path / "lineage.duckdb"), graph)
    materializer.deploy(Deployment.from_class(LineageCopy))
    materializer.refresh()
    triples = list(graph.published[0])
    assert any(str(subject).startswith("urn:acquirium:binding:") for subject, _, _ in triples)
