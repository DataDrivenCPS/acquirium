"""Round-trip tests for restoring registered apps after a server restart.

``AppRunner._app_spec_graph`` writes an app's registration triples;
``restore_app_specs`` reads them back into AppSpecs. These tests check the
two stay inverses, using an rdflib graph as a stand-in for the oxigraph
store (same {"rows": [...]} result shape).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from acquirium.Apps.runner import AppRunner
from acquirium.Apps.supervisor import AppSupervisor, restore_app_specs
from acquirium.internals.models import AppSpec, AppOutputSpec

AppRunnerCls = AppRunner.__ray_actor_class__


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
                ref_name="derived/out1",
                value_kind="numeric",
                quantity_kind="quantitykind:Temperature",
                unit="unit:DEG_C",
                data_source="urn:test:source",
                storage_backend="timescale",
                depends_on=["urn:test:in1"],
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
    assert out1.ref_name == "derived/out1"
    assert out1.value_kind == "numeric"
    assert out1.depends_on == ["urn:test:in1"]
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


def test_restore_preserves_source_spec_from_load_metadata(tmp_path):
    spec = make_spec()
    spec.source_spec = "/work/apps/my_app.py:MyApp"
    manager = make_manager(spec, tmp_path)
    app_dir = tmp_path / spec.name
    (app_dir / "app.json").write_text(json.dumps({
        "entry_file": "my_app.py",
        "app_class": "MyApp",
        "source_spec": spec.source_spec,
    }))

    restored, = restore_app_specs(manager)

    assert restored.source_spec == spec.source_spec
    assert restored.entry_file == "my_app.py"
    assert restored.app_class == "MyApp"


def test_restore_loads_active_keep_alive_run_state(tmp_path):
    spec = make_spec()
    manager = make_manager(spec, tmp_path)
    app_dir = tmp_path / spec.name
    (app_dir / "run.json").write_text(json.dumps({
        "keep_alive": True,
        "interval": 45,
        "start": "2026-08-01T00:00:00Z",
        "end": None,
        "params": {"window": 7},
    }))

    restored, = restore_app_specs(manager)

    assert restored.resume_keep_alive is True
    assert restored.run_interval == 45
    assert restored.run_start.isoformat() == "2026-08-01T00:00:00+00:00"
    assert restored.run_end is None
    assert restored.run_params == {"window": 7}


def test_list_apps_includes_copy_pasteable_source_spec(tmp_path):
    supervisor = AppSupervisor(tmp_path, "localhost", 8000)
    spec = AppSpec(
        name="average",
        source_spec="/work/apps/average.py:Average",
    )
    supervisor._apps[spec.name] = {
        "name": spec.name,
        "spec": spec,
        "running": False,
        "started_at": None,
        "stopped_at": None,
    }

    assert supervisor.list_apps()[0]["spec"] == spec.source_spec


def test_runner_persists_and_clears_keep_alive_marker(tmp_path):
    runner = AppRunnerCls(AppSpec(name="average"), tmp_path, MagicMock())
    runner._persist_run_state(
        active=True,
        interval=30,
        start="2026-08-01T00:00:00Z",
        params={"window": 5},
    )

    run_path = tmp_path / "average" / "run.json"
    assert json.loads(run_path.read_text()) == {
        "end": None,
        "interval": 30.0,
        "keep_alive": True,
        "params": {"window": 5},
        "start": "2026-08-01T00:00:00Z",
    }

    runner.stop()
    assert json.loads(run_path.read_text())["keep_alive"] is False


def test_minimal_spec_defaults(tmp_path):
    # An app registered with defaults (no version/queries/outputs/deps)
    # restores to the same defaults.
    spec = AppSpec(name="bare_app", app_type="soft_sensor")
    spec.version = "0.0"
    restored, = restore_app_specs(make_manager(spec, tmp_path))
    assert (restored.name, restored.version, restored.app_type) == ("bare_app", "0.0", "soft_sensor")
    assert restored.queries == {} and restored.outputs == [] and restored.depends_on == []
