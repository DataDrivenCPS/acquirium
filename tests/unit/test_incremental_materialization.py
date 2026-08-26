from datetime import datetime, timedelta, timezone
from threading import Lock
from time import sleep

import pyarrow as pa

from acquirium.Materialization import (
    AllAvailable, ApplicationGraph, AroundChange, Binding, InProcessExecutor,
    RevisionStore, RowWiseTransformation, Scheduler, StreamDescriptor, Transformation, outputs,
)
from acquirium.Materialization.planner import BindingPlanner, Deployment, _resolved_output
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.models import compute_ref_uri


class Mean(Transformation):
    start = AllAvailable()
    window = AroundChange(before="1m")
    outputs = {"mean": outputs.stream(value_kind="numeric")}

    def transform(self, inputs, output, context):
        assert inputs["left"].window == inputs["right"].window == context.read_window
        source = inputs["left"].collect()
        output["mean"] = pa.table({"time": source["time"], "value": pa.array([3.0])})


class LineageCopy(Transformation):
    name = "lineage-copy"
    outputs = {"out": outputs.stream(value_kind="numeric")}
    def build_query(self, aq): return aq.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class RowWiseLineageCopy(RowWiseTransformation):
    name = "row-wise-lineage-copy"
    outputs = {"out": outputs.stream(value_kind="numeric")}
    def build_query(self, aq): return aq.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


class Copy(Transformation):
    start = AllAvailable()
    outputs = {"out": outputs.stream(value_kind="numeric")}

    def transform(self, inputs, output, context):
        source = inputs["source"].collect()
        output["out"] = pa.table({"time": source["time"], "value": source["value"]})


class ConfiguredWindow(Transformation):
    outputs = {"out": outputs.stream(value_kind="numeric")}

    def __init__(self, window="5m"):
        self.window = AroundChange(before=window)


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


def test_revision_frontier_commits_coherent_output_and_converges(tmp_path):
    store = DuckDBStore(tmp_path / "timeseries.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:left", [(timestamp, 2.0)], value_kind="numeric")
    store.upsert_rows("urn:right", [(timestamp, 4.0)], value_kind="numeric")
    inputs = {"left": (StreamDescriptor("urn:left"),), "right": (StreamDescriptor("urn:right"),)}
    binding = Binding("mean", "digest", inputs, {
        "mean": (Binding.derive_output_uri("mean", "mean", inputs), Mean.outputs["mean"]),
    }, Mean.window)
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
    spec = outputs.stream(value_kind="numeric")
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


def test_deployment_persists_constructor_parameters_and_policy():
    deployment = Deployment.from_class(ConfiguredWindow, parameters={"window": "15m"})
    restored = Deployment.from_json(deployment.to_json())

    assert restored.parameters == {"window": "15m"}
    assert restored.window.before == timedelta(minutes=15)


def test_planner_aggregates_full_query_bindings_by_default():
    planner = BindingPlanner(MultiStreamLineageGraph())

    application_graph, _ = planner.compile((Deployment.from_class(LineageCopy),), graph_revision=1)

    assert len(application_graph.bindings) == 1
    assert {item.ref_uri for item in application_graph.bindings[0].inputs["input"]} == {
        "urn:input-a", "urn:input-b",
    }


def test_row_wise_transformation_creates_one_binding_per_matching_stream():
    graph = MultiStreamLineageGraph()
    planner = BindingPlanner(graph)

    application_graph, _ = planner.compile((Deployment.from_class(RowWiseLineageCopy),), graph_revision=1)

    assert len(application_graph.bindings) == 2
    assert {binding.inputs["input"][0].ref_uri for binding in application_graph.bindings} == {
        "urn:input-a", "urn:input-b",
    }


def test_scheduler_runs_independent_topological_wave_concurrently(tmp_path):
    store = DuckDBStore(tmp_path / "parallel.duckdb")
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:left", [(timestamp, 1.0)], value_kind="numeric")
    store.upsert_rows("urn:right", [(timestamp, 2.0)], value_kind="numeric")
    spec = Copy.outputs["out"]
    left = Binding("left-copy", "copy", {"source": (StreamDescriptor("urn:left"),)}, {"out": ("urn:out:left", spec)})
    right = Binding("right-copy", "copy", {"source": (StreamDescriptor("urn:right"),)}, {"out": ("urn:out:right", spec)})
    graph = ApplicationGraph((left, right))
    assert graph.layers() == ((left, right),)
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


def test_output_inheritance_only_copies_common_semantic_metadata():
    inputs = {"input": (
        StreamDescriptor("urn:a", unit="urn:unit:C", medium="urn:air", properties={"urn:p": ("urn:x",)}),
        StreamDescriptor("urn:b", unit="urn:unit:C", medium="urn:water", properties={"urn:p": ("urn:x", "urn:y")}),
    )}
    resolved = _resolved_output(outputs.stream(inherit=True, inherit_properties=("urn:p",)), inputs)
    assert resolved.unit == "urn:unit:C"
    assert resolved.medium is None
    assert resolved.properties == {"urn:p": ("urn:x",)}


def test_compiled_binding_publishes_structural_lineage(tmp_path):
    graph = LineageGraph()
    materializer = Materializer(DuckDBStore(tmp_path / "lineage.duckdb"), graph)
    materializer.deploy(Deployment.from_class(LineageCopy))
    materializer.refresh()
    triples = list(graph.published[0])
    assert any(str(subject).startswith("urn:acquirium:binding:") for subject, _, _ in triples)
