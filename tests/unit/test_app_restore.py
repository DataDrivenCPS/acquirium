"""Round-trip tests for restoring registered apps after a server restart.

``AppRunner._app_spec_graph`` writes an app's registration triples;
``restore_app_specs`` reads them back into AppSpecs. These tests check the
two stay inverses, using an rdflib graph as a stand-in for the oxigraph
store (same {"rows": [...]} result shape).
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from acquirium.Server.ray_backend import AppRunner, restore_app_specs
from acquirium.internals.models import AppSpec, AppOutputSpec

AppRunnerCls = AppRunner.__ray_actor_class__


class FakeGraphStore:
    def __init__(self, graph):
        self.graph = graph

    def sparql_query(self, query: str, use_union: bool = False) -> dict:
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
        depends_on=["urn:test:in1", "urn:test:in2"],
    )


def make_manager(spec: AppSpec, tmp_path):
    graph = AppRunnerCls._app_spec_graph(None, spec)
    (tmp_path / spec.name).mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(graph_store=FakeGraphStore(graph), app_storage_root=tmp_path)


def test_round_trip(tmp_path):
    spec = make_spec()
    restored, = restore_app_specs(make_manager(spec, tmp_path))

    assert restored.name == spec.name
    assert restored.version == spec.version
    assert restored.app_type == spec.app_type
    assert restored.queries == spec.queries
    assert restored.depends_on == sorted(spec.depends_on)
    # Source and load metadata live on disk (app.json), not in the graph.
    assert restored.source_code is None and restored.app_class is None

    by_point = {o.point_uri: o for o in restored.outputs}
    assert set(by_point) == {"urn:test:out1", "urn:test:out2"}
    out1 = by_point["urn:test:out1"]
    assert (out1.kind, out1.quantity_kind, out1.unit, out1.storage_backend) == (
        "timeseries", "quantitykind:Temperature", "unit:DEG_C", "timescale",
    )
    assert by_point["urn:test:out2"].kind == "event"


def test_trigger_outputs_restore_as_event(tmp_path):
    # Registration writes event and trigger outputs identically, so the
    # distinction is lost on restore — behavior-neutral (kind is only used
    # when writing the registration graph, which the restore path skips).
    spec = make_spec()
    spec.outputs = [AppOutputSpec(kind="trigger", point_uri="urn:test:out3")]
    restored, = restore_app_specs(make_manager(spec, tmp_path))
    assert restored.outputs[0].kind == "event"


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
    assert restored.queries == {} and restored.outputs == [] and restored.depends_on == []
