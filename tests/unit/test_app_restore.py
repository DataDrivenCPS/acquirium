"""Round-trip tests for restoring registered apps after a server restart.

``app_spec_graph`` writes an app's registration triples;
``restore_app_specs`` reads them back into AppSpecs. These tests check the
two stay inverses, using an rdflib graph as a stand-in for the oxigraph
store (same {"rows": [...]} result shape).
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from acquirium.Apps.supervisor import restore_app_specs
from acquirium.internals.app_utils import app_spec_graph
from acquirium.internals.internals_namespaces import OUTPUT_KIND
from acquirium.internals.models import AppSpec, AppOutputSpec, EnvSpec


class FakeGraphStore:
    def __init__(self, graph):
        self.graph = graph

    def sparql_query(self, query: str, include_dependencies: bool = False) -> dict:
        return {"rows": [list(row) for row in self.graph.query(query)]}


def make_spec(name: str = "restore_test_app") -> AppSpec:
    return AppSpec(
        name=name,
        version="1.2",
        app_type="threshold",
        app_class="MyApp",
        source_code="class MyApp: pass",
        entry_file="app.py",
        queries={"default": {"select": ["urn:test:in1"]}},
        outputs=[
            AppOutputSpec(
                kind="timeseries",
                point_uri="urn:test:out1",
                quantity_kind="quantitykind:Temperature",
                unit="unit:DEG_C",
                data_source="urn:test:source",
                storage_backend="timescale",
            ),
            AppOutputSpec(kind="event", point_uri="urn:test:out2"),
        ],
    )


def make_manager(spec: AppSpec, tmp_path, graph=None):
    graph = graph if graph is not None else app_spec_graph(spec)
    (tmp_path / spec.name).mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(graph_store=FakeGraphStore(graph), app_storage_root=tmp_path)


def test_round_trip(tmp_path):
    spec = make_spec()
    restored, = restore_app_specs(make_manager(spec, tmp_path))

    assert restored.name == spec.name
    assert restored.version == spec.version
    assert restored.app_type == spec.app_type
    assert restored.queries == spec.queries
    # Source and load metadata live on disk (app.json), not in the graph.
    assert restored.source_code is None and restored.app_class is None

    by_point = {o.point_uri: o for o in restored.outputs}
    assert set(by_point) == {"urn:test:out1", "urn:test:out2"}
    out1 = by_point["urn:test:out1"]
    assert (out1.kind, out1.quantity_kind, out1.unit, out1.storage_backend) == (
        "timeseries", "quantitykind:Temperature", "unit:DEG_C", "timescale",
    )
    assert by_point["urn:test:out2"].kind == "event"


def test_trigger_outputs_round_trip(tmp_path):
    spec = make_spec()
    spec.outputs = [AppOutputSpec(kind="trigger", point_uri="urn:test:out3")]
    restored, = restore_app_specs(make_manager(spec, tmp_path))
    assert restored.outputs[0].kind == "trigger"


def test_legacy_graph_without_output_kind_falls_back_to_event(tmp_path):
    # Graphs written before acq:outputKind encode event and trigger outputs
    # identically (both EventStream) — the stream type cannot distinguish
    # them, so such a trigger restores as event.
    spec = make_spec()
    spec.outputs = [AppOutputSpec(kind="trigger", point_uri="urn:test:out3")]
    graph = app_spec_graph(spec)
    for s, o in list(graph.subject_objects(OUTPUT_KIND)):
        graph.remove((s, OUTPUT_KIND, o))
    restored, = restore_app_specs(make_manager(spec, tmp_path, graph=graph))
    assert restored.outputs[0].kind == "event"


def test_new_spec_fields_round_trip(tmp_path):
    spec = make_spec("full_fields_app")
    spec.kind = "task"
    spec.run_mode = "on_change"
    spec.interval = 42.5
    spec.env = EnvSpec(
        pip=["paho-mqtt>=2.1.0"],
        env_vars={"IDAES_DIR": "/opt/idaes"},
        setup_commands=["idaes get-extensions"],
        py_modules=[],
    )
    restored, = restore_app_specs(make_manager(spec, tmp_path))
    assert restored.kind == "task"
    assert restored.run_mode == "on_change"
    assert restored.interval == 42.5
    assert restored.env == spec.env
    # And the pre-existing fields still ride along unchanged.
    assert restored.app_type == spec.app_type
    assert restored.queries == spec.queries


def test_field_defaults_round_trip(tmp_path):
    # A spec with default kind/run_mode/interval/env restores to the same
    # defaults (nothing invented by the graph round-trip).
    restored, = restore_app_specs(make_manager(make_spec(), tmp_path))
    assert restored.kind == "app"
    assert restored.run_mode == "manual"
    assert restored.interval is None
    assert restored.env is None


def test_missing_source_dir_is_skipped(tmp_path):
    spec = make_spec()
    manager = make_manager(spec, tmp_path)
    manager.app_storage_root = tmp_path / "elsewhere"  # no app dir here
    assert restore_app_specs(manager) == []


def test_minimal_spec_defaults(tmp_path):
    # An app registered with defaults (no version/queries/outputs/deps)
    # restores to the same defaults.
    spec = AppSpec(name="bare_app", app_type="soft_sensor")
    spec.version = "0.0"
    restored, = restore_app_specs(make_manager(spec, tmp_path))
    assert (restored.name, restored.version, restored.app_type) == ("bare_app", "0.0", "soft_sensor")
    assert restored.queries == {} and restored.outputs == []


def test_task_kind_round_trips(tmp_path):
    from acquirium.internals.models import TaskSpec

    task = TaskSpec(
        name="tds_task", query={"nodes": [], "edges": []},
        fn_name="f", fn_source="def f(ctx): return []",
        outputs=[AppOutputSpec(kind="trigger", point_uri="urn:t")],
        run_mode="interval", interval=5.0,
    )
    restored, = restore_app_specs(make_manager(task.to_app_spec(), tmp_path))
    assert restored.kind == "task"
    assert restored.app_type == "task"
    assert restored.queries == {"default": {"nodes": [], "edges": []}}
    assert restored.outputs[0].kind == "trigger"
    assert (restored.run_mode, restored.interval) == ("interval", 5.0)
