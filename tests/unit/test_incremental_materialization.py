from datetime import datetime, timedelta, timezone
from threading import Lock
from time import sleep

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from rdflib import URIRef

from acquirium.Materialization import (
    App, ApplicationGraph, Binding, InputBatch, OutputPort,
    StreamDescriptor, StreamSet, TimeWindow, align, output,
)
from acquirium.Materialization.planner import BindingPlanner, Deployment, output_port


def _port(name="out", ref="urn:out", spec=None):
    """A resolved output port, for tests that build a Binding by hand."""
    return OutputPort(ref, name, f"urn:point:{name}", spec or output.stream(value_kind="numeric"))
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.models import compute_ref_uri

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _context(row, result):
    """An InputBatch carrying just the match information under test."""
    return InputBatch("sig", 1, 0, 1, TimeWindow(NOW, NOW), TimeWindow(NOW, NOW), row, result,
                      output_window=TimeWindow(NOW, NOW))


class Mean(App):
    grouping = "per_match"
    lookback = "1m"
    outputs = {"mean": output.stream(value_kind="numeric")}

    def transform(self, inputs, output, context):
        assert inputs["left"].window == inputs["right"].window == context.read_window
        source = inputs["left"].collect()
        output["mean"] = pa.table({"time": source["time"], "value": pa.array([3.0])})


class LineageCopy(App):
    name = "lineage-copy"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class BorrowedPoint(App):
    name = "borrowed-point"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric", point_uri="urn:plant/T-101")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class TaggedOutput(App):
    name = "tagged-output"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric", data_source="normalized")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class MixedFanOut(App):
    name = "mixed-fan-out"
    grouping = "per_match"
    outputs = {
        "each": output.stream(value_kind="numeric"),
        "total": output.named("kpi", value_kind="numeric"),
    }
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class NamedTotal(App):
    grouping = "all_matches"
    name = "named-total"
    outputs = {"total": output.named("plant-total", value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class Copy(App):
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}

    def transform(self, inputs, output, context):
        source = inputs["source"].collect()
        output["out"] = pa.table({"time": source["time"], "value": source["value"]})


class ConfiguredLookback(App):
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}

    def __init__(self, window="5m"):
        self.lookback = window


class WholeStream(App):
    grouping = "per_match"
    lookback = "all"
    windows = {"source": ("-1h", "-1m")}
    named_inputs = {"previous": "whole-stream-out"}
    outputs = {"out": output.stream(value_kind="numeric")}


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
    grouping = "per_match"
    outputs = {"ratio": output.stream(value_kind="numeric")}
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




def test_graph_rejects_cycles_and_duplicate_output_ownership():
    input_a = {"source": (StreamDescriptor("urn:b"),)}
    input_b = {"source": (StreamDescriptor("urn:a"),)}
    spec = output.stream(value_kind="numeric")
    a = Binding("a", "a", input_a, {"out": _port(ref="urn:a", spec=spec)})
    b = Binding("b", "b", input_b, {"out": _port(ref="urn:b", spec=spec)})
    try:
        ApplicationGraph((a, b))
    except ValueError as error:
        assert "cycle" in str(error)
    else:
        raise AssertionError("cycle was accepted")


def test_derived_output_uri_uses_the_managed_reference_identity():
    inputs = {"source": (StreamDescriptor("urn:input"),)}
    port = output_port("derived-app", "out", inputs, output.stream(value_kind="numeric"))

    # The name carries the app, the port and the bound inputs; the URI is the
    # ordinary reference identity of that (source, name) pair; and the point
    # is named after the stream, so neither moves when the app is edited.
    assert port.ref_name.startswith("derived-app:out:")
    assert port.ref_uri == str(compute_ref_uri("derived:derived-app", port.ref_name))
    assert port.point_uri == f"urn:acquirium:derived-point:{port.ref_uri.split('#')[-1]}"


def test_named_output_keeps_its_exact_reference_name():
    inputs = {"source": (StreamDescriptor("urn:input"),)}
    spec = output.named("plant-total", value_kind="numeric")

    port = output_port("kpi-app", "out", inputs, spec)

    # A named output is called exactly what its author called it — no app
    # prefix, since the source id already scopes it.
    assert port.ref_name == "plant-total"
    assert port.ref_uri == str(compute_ref_uri("derived:kpi-app", "plant-total"))


def test_named_output_cannot_ride_along_with_fan_out():
    planner = BindingPlanner(MultiStreamLineageGraph())

    with pytest.raises(ValueError, match="named output"):
        planner.compile((Deployment.from_class(MixedFanOut),), graph_revision=1)


def test_mixed_outputs_are_allowed_for_a_single_input_group():
    planner = BindingPlanner(LineageGraph())

    application_graph, _ = planner.compile((Deployment.from_class(MixedFanOut),), graph_revision=1)

    (binding,) = application_graph.bindings
    assert binding.outputs["total"].ref_name == "kpi"
    assert binding.outputs["each"].ref_name.startswith("mixed-fan-out:each:")


def test_deployment_persists_constructor_parameters_and_policy():
    deployment = Deployment.from_class(ConfiguredLookback, parameters={"window": "15m"})
    restored = Deployment.from_json(deployment.to_json())

    assert restored.parameters == {"window": "15m"}
    assert restored.lookback == timedelta(minutes=15)


def test_durations_accept_every_suffix_and_reject_the_rest():
    from acquirium.Materialization.models import _duration

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
    assert restored.windows == {"source": (timedelta(hours=-1), timedelta(minutes=-1))}
    assert restored.named_inputs == {"previous": "whole-stream-out"}
    assert restored.window("source") == (3_600_000_000, -60_000_000)
    assert restored == deployment


def test_named_outputs_aggregate_the_complete_query_result():
    planner = BindingPlanner(MultiStreamLineageGraph())

    application_graph, _ = planner.compile((Deployment.from_class(NamedTotal),), graph_revision=1)

    (binding,) = application_graph.bindings
    assert binding.outputs["total"].ref_name == "plant-total"
    assert {item.ref_uri for item in binding.inputs["input"]} == {
        "urn:input-a", "urn:input-b",
    }


def test_per_match_outputs_bind_one_group_per_query_row():
    graph = MultiStreamLineageGraph()
    planner = BindingPlanner(graph)

    application_graph, _ = planner.compile((Deployment.from_class(LineageCopy),), graph_revision=1)

    assert len(application_graph.bindings) == 2
    assert {binding.inputs["input"][0].ref_uri for binding in application_graph.bindings} == {
        "urn:input-a", "urn:input-b",
    }




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


def test_every_per_match_call_sees_the_whole_query_result():
    planner = BindingPlanner(MultiStreamLineageGraph())

    application_graph, _ = planner.compile((Deployment.from_class(LineageCopy),), graph_revision=1)

    assert len(application_graph.bindings) == 2
    for binding in application_graph.bindings:
        context = _context(binding.row, binding.result)
        # Its own row, but the whole fleet it belongs to.
        assert context.row["input_ref"] == binding.inputs["input"][0].ref_uri
        assert context.result.height == 2


def test_row_is_the_per_match_accessor_and_refuses_to_guess():
    one = _context({"hx": "urn:hx-1"}, ({"hx": "urn:hx-1"}, {"hx": "urn:hx-2"}))
    aggregate = _context(None, ({"hx": "urn:hx-1"}, {"hx": "urn:hx-2"}))

    assert one.row == {"hx": "urn:hx-1"}
    assert one.result.height == 2          # result is the whole query either way
    with pytest.raises(ValueError, match="all_matches grouping"):
        aggregate.row
    assert aggregate.result.height == 2


def test_stream_names_the_single_bound_stream_and_refuses_to_guess():
    window = TimeWindow(datetime(2026, 1, 1, tzinfo=timezone.utc),
                        datetime(2026, 1, 1, tzinfo=timezone.utc))
    one = StreamSet("temperature", window, (StreamDescriptor("urn:a", label="Basin 1"),))
    several = StreamSet("temperature", window,
                        (StreamDescriptor("urn:a"), StreamDescriptor("urn:b")))

    # A per_match call binds one stream, so this is how it asks which.
    assert one.stream.ref_uri == "urn:a" and one.stream.label == "Basin 1"
    with pytest.raises(ValueError, match="all_matches grouping sees every match"):
        several.stream
    assert [d.ref_uri for d in several.streams] == ["urn:a", "urn:b"]


def test_per_match_binds_one_stream_per_alias_even_for_paired_rows():
    # The invariant behind .stream: a per_match call binds exactly one stream
    # under every alias, so a flow/pressure row gives one of each.
    graph = PairedRowGraph()

    application_graph, _ = planner_for(graph, PairedApp)

    assert len(application_graph.bindings) == 2
    for binding in application_graph.bindings:
        assert sorted(binding.inputs) == ["flow", "pressure"]
        assert all(len(streams) == 1 for streams in binding.inputs.values())




def test_output_schema_violations_name_the_port():
    from acquirium.Materialization import OutputBuilder
    builder = OutputBuilder({"celsius": _port("celsius")})
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
    builder = OutputBuilder({"alarm": _port("alarm", spec=output.stream(value_kind="text"))})

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


class PerMatchEntity(App):
    name = "per-row-entity"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant):
        return (plant.query().entity("urn:ReverseOsmosis", alias="ro")
                .measurement(frm="ro", alias="input"))
    def transform(self, inputs, output, context): pass


def test_context_carries_the_bound_rows_entities_and_labels():
    planner = BindingPlanner(EntityRowGraph())

    application_graph, _ = planner.compile((Deployment.from_class(PerMatchEntity),), graph_revision=1)

    by_ref = {binding.inputs["input"][0].ref_uri: binding for binding in application_graph.bindings}
    first = by_ref["urn:input-1"].row
    assert first["ro"] == "urn:ro-1"
    assert first["input_ref"] == "urn:input-1" and first["input.label"] == "Feed pressure 1"
    assert by_ref["urn:input-2"].row["ro"] == "urn:ro-2"
    assert by_ref["urn:input-1"].inputs["input"][0].label == "Feed pressure 1"


class CheckDouble(App):
    name = "check-double"
    grouping = "per_match"
    outputs = {"doubled": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context):
        source = inputs["input"].collect()
        output["doubled"] = pa.table({
            "time": source["time"], "value": pc.multiply(source["value"], 2.0),
        })


class CheckBroken(CheckDouble):
    name = "check-broken"
    grouping = "per_match"
    def transform(self, inputs, output, context):
        raise ValueError("sensor calibration missing")


class CheckUndeclared(CheckDouble):
    name = "check-undeclared"
    grouping = "per_match"
    def transform(self, inputs, output, context):
        output["dubbled"] = inputs["input"].collect()












def test_output_declarations_are_validated_before_deployment():
    class BadSpec(App):
        name = "bad-spec"
        grouping = "per_match"
        outputs = {"out": "numeric"}
        def build_query(self, plant): return plant.query().measurement(alias="input")

    class Colliding(App):
        name = "colliding"
        grouping = "per_match"
        outputs = {"a": output.named("total", value_kind="numeric"),
                   "b": output.named("total", value_kind="numeric")}
        def build_query(self, plant): return plant.query().measurement(alias="input")

    class MappingSpec(App):
        grouping = "per_match"
        outputs = {"out": {"value_kind": "numeric"}}

    with pytest.raises(TypeError, match="aq.output.stream"):
        Deployment.from_class(BadSpec)
    with pytest.raises(TypeError, match="aq.output.stream"):
        Deployment.from_class(MappingSpec)
    with pytest.raises(ValueError, match="claim the stream name"):
        Deployment.from_class(Colliding)




def test_a_derived_point_uri_survives_a_code_or_metadata_edit(tmp_path):
    """Change 3: the point is keyed by the stream, not by the binding signature."""
    spec = output.stream(value_kind="numeric")
    sensor = StreamDescriptor("urn:ref:raw", point_uri="urn:plant/T-101", label="Basin 1 inlet")
    inputs = {"t": (sensor,)}
    ports = {"celsius": output_port("normalize", "celsius", inputs, spec)}

    edited_code = Binding("normalize", "digest-after-edit", inputs, ports)
    relabelled = Binding("normalize", "digest-before-edit",
                         {"t": (StreamDescriptor("urn:ref:raw", point_uri="urn:plant/T-101",
                                                 label="Basin 1 inlet temperature"),)}, ports)
    original = Binding("normalize", "digest-before-edit", inputs, ports)

    # The signature moves with the code and with input metadata — that is what
    # it is for — but nothing a query or a dashboard is pinned to moves with it.
    assert original.signature != edited_code.signature != relabelled.signature
    for binding in (original, edited_code, relabelled):
        assert binding.outputs["celsius"].point_uri == ports["celsius"].point_uri
        assert binding.outputs["celsius"].ref_uri == ports["celsius"].ref_uri
        assert binding.progress_key == original.progress_key


def test_a_generated_label_names_the_subject_and_its_app():
    from acquirium.Materialization.runtime import _default_label

    spec = output.stream(value_kind="numeric")
    sensor = StreamDescriptor("urn:ref:raw", label="Basin 1 inlet temperature")
    ports = {"celsius": output_port("normalize-temperatures", "celsius", {"t": (sensor,)}, spec)}
    one = Binding("normalize-temperatures", "d", {"t": (sensor,)}, ports)

    assert _default_label(one, "celsius", spec) == (
        "Basin 1 inlet temperature (normalize-temperatures[celsius])")

    # A second app reading that stream keeps the subject and swaps the tag,
    # instead of accumulating one parenthetical per hop.
    derived = StreamDescriptor("urn:ref:celsius",
                               label="Basin 1 inlet temperature (normalize-temperatures[celsius])")
    two = Binding("fill-gaps", "d", {"t": (derived,)}, ports)
    assert _default_label(two, "filled", spec) == "Basin 1 inlet temperature (fill-gaps[filled])"

    # No single subject, or none with a name: the app and port stand alone.
    many = Binding("plant-average-flow", "d",
                   {"t": (StreamDescriptor("urn:a", label="A"), StreamDescriptor("urn:b", label="B"))},
                   ports)
    assert _default_label(many, "average", spec) == "plant-average-flow[average]"
    unlabelled = Binding("fill-gaps", "d", {"t": (StreamDescriptor("urn:c"),)}, ports)
    assert _default_label(unlabelled, "filled", spec) == "fill-gaps[filled]"

    # A named output already has the human name its author chose.
    named = output.named("plant-average-flow", value_kind="numeric")
    assert _default_label(many, "average", named) == "plant-average-flow"








