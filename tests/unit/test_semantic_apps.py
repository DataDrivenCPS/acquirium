from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from tests.epoch_helpers import epoch_server, value

from acquirium import App, Selection, Output
from acquirium.apps import Apps, output_reference
from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_stream_uri


def test_list_settles_pending_removal_before_selecting_app_names(tmp_path):
    # In-memory declarations still list the app. The published plan has removed it.
    core, graph = Mock(), Mock()
    core.engine.snapshot.return_value.epoch = 2
    core.engine.snapshot.return_value.plan.return_value = {"templates": [], "instances": [], "bindings": []}
    core.engine.epochs_of.return_value = [2]
    graph.project_app_sources.return_value = (1, 2)
    apps = Apps(core, graph, tmp_path)
    apps.state["apps"] = {"removed": {}}
    apps._pending = (object(), 1)

    assert apps.list() == []
    core.engine.snapshot.assert_called_once_with()
    core.wait.assert_not_called()


@pytest.fixture(scope="module")
def system(tmp_path_factory):
    with epoch_server(tmp_path_factory.mktemp("semantic-core")) as server:
        yield server


def register(system, source):
    """Register a numeric temperature probe and return its stream URI."""
    _, _, client = system
    client.register_streams(
        [
            dict(
                source_id=source,
                stream_name="temperature",
                value_kind="numeric",
                property_uri=f"urn:{source}:point",
                unit="http://qudt.org/vocab/unit/DEG_F",
            )
        ]
    )
    return str(compute_stream_uri(source, "temperature"))


def mean(name, stream, group="urn:tank"):
    """Declare a native mean over one stream in a fixed group."""
    return App(
        name,
        {
            "input": Selection(
                f"SELECT ?stream ?group WHERE {{ BIND(<{stream}> AS ?stream) BIND(<{group}> AS ?group) }}"
            )
        },
        {"mean": Output()},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )


def test_python_chain_repairs_history_and_recovers_without_source_files(tmp_path):
    from dataclasses import replace
    from pathlib import Path

    from acquirium import PythonFunction
    from examples.transformation import temperature_conversion

    data = tmp_path / "server"
    module = tmp_path / "temperature_conversion.py"
    module.write_bytes(Path(temperature_conversion.__file__).read_bytes())
    downstream = tmp_path / "downstream.py"
    downstream.write_text(
        'import json\n'
        'from acquirium.functions import emit\n'
        'def read(frame, context):\n'
        '    binding = json.loads(frame["context"][0])\n'
        '    assert binding["group"] == context["parameters"]["group"][0]\n'
        '    assert binding["stream"] == frame["stream"][0]\n'
        '    return {"double": emit(context, 2 * frame["value"][0])}\n',
        encoding="utf-8",
    )

    def write(value):
        """Publish a reading at the test timestamp."""
        receipt, = client.insert_timeseries("conversion", "temperature", [(1000, value)])["receipts"]
        return client.wait(receipt)["epoch"]

    def read(uri, *, epoch=None):
        """Read one worker output at the current or a pinned epoch."""
        return client.stream(uri).dataframe(epoch=epoch)["value"].to_list()

    with epoch_server(data) as system:
        manager, _, client = system
        ref = register(system, "conversion")
        original = temperature_conversion.fahrenheit_to_celsius(client, {})
        declaration = replace(original, function=PythonFunction.from_file(module, "convert"))
        output = output_reference(declaration.name, ref, "celsius")
        assert client.deploy_app(declaration, dry_run=True)["app"]["within_limits"]
        assert not list((data / "app-functions").iterdir())

        # The upstream app is needed for a group to match. Compile the
        # downstream SPARQL against the candidate output in the same deploy.
        chained = App(
            "double-celsius",
            {"celsius": replace(Selection.outputs_of(declaration, "celsius"), upstream=None, upstream_port=None)},
            {"double": Output()},
            PythonFunction.from_file(downstream, "read"),
        )
        chained_output = output_reference(chained.name, ref, "double")
        assert client.deploy_app(chained)["app"]["matches"] == {"celsius": 0}

        assert client.deploy_app(declaration)["app"]["status"] == "active"
        original_epoch = write(68)
        assert read(output) == [20]
        assert read(chained_output) == [40]
        write(86)
        assert read(output) == [30]
        assert read(chained_output) == [60]

        # Deleting the probe reading retracts both current outputs and keeps the earlier epoch.
        client.wait(client.delete_timeseries("conversion", "temperature", [1000]))
        assert read(output) == read(chained_output) == []
        assert read(output, epoch=original_epoch) == [20]
        published = client.engine_snapshot()

        # Only the active engine plan should be needed to rebuild this graph
        # and run the captured Python module after restarting.
        assert not (data / "apps.json").exists()
        module.rename(module.with_suffix(".removed"))
        downstream.rename(downstream.with_suffix(".removed"))
        for artifact in (data / "app-functions").glob("*.py"):
            artifact.rename(artifact.with_suffix(".removed"))
        manager.graph_store.clear_graphs([manager.graph_store.source_graph_uri(f"derived:{declaration.name}")])
    with epoch_server(data) as (manager, _, client):
        recovered = client.engine_snapshot()
        assert (recovered["epoch"], recovered["plan"]) == (published["epoch"], published["plan"])
        assert manager.graph_store.data_graph(f"derived:{declaration.name}")
        assert client.inspect_app(declaration.name)["app"]["status"] == "active"
        assert read(output) == read(chained_output) == []
        assert read(output, epoch=original_epoch) == [20]
        write(104)
        assert read(output) == [40]
        assert read(chained_output) == [80]
        manager.remove_app(chained.name)
        assert read(chained_output) == []
        assert read(output) == [40]


def test_reading_graph_status_delivers_experiment_metadata_and_refreshes_apps(system):
    manager, _, _ = system
    experiments = manager.experiments
    # Declare a Study output and start a run. The app looks for streams owned by that run.
    study = experiments.define("app-membership")
    variable = experiments.declare(study["template_id"], "level", "output", "timeseries", {})
    run = experiments.start(study["template_id"], {})["run_id"]
    name = "experiment-membership"
    declaration = App(
        name,
        {"input": Selection(
            f'SELECT ?stream WHERE {{ ?stream <urn:acquirium#sourceId> "experiment/{run}" . }}',
            group=None,
        )},
        {"mean": Output()},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )
    manager.deploy_app(declaration)
    assert manager.apps.inspect(name)["operators"] == []
    # Recording the first sample creates a stream. Deliver its metadata so the app can bind it.
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    experiments.record_batch(run, [{
        "form": "samples", "variable_id": variable["variable_id"], "samples": [(when, 12.0)],
    }])
    manager.graph_status()
    from acquirium.core import time_ns

    output = output_reference(name, name, "mean")
    assert len(manager.apps.inspect(name)["operators"]) == 1
    assert value(manager.core.engine.snapshot(), output, time_ns(when)) == 12.0

    # Deleting the run removes the binding and retracts its calculated value.
    experiments.finish(run, "succeeded")
    experiments.delete_runs([run])
    assert manager.apps.inspect(name)["operators"] == []
    assert value(manager.core.engine.snapshot(), output, time_ns(when)) is None
    manager.remove_app(name)


def test_experiment_metadata_retries_app_refresh_after_delivery(system, monkeypatch):
    manager, _, _ = system
    experiments = manager.experiments
    manager.deploy_app(mean("retry-experiment-app", "urn:never-recorded"))
    experiments.define("retry-app-refresh")
    install = manager.apps._install
    with monkeypatch.context() as patch:
        def fail_install(*args):
            raise ValueError("temporary app compilation failure")

        patch.setattr(manager.apps, "_install", fail_install)
        with pytest.raises(ValueError, match="temporary app compilation failure"):
            experiments.sync_metadata_graph()
    # Delivery was already acknowledged, but the next sync still retries apps.
    with manager.ledger._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] == 0
    calls = []
    monkeypatch.setattr(manager.apps, "_install", lambda declarations: (calls.append(True), install(declarations)))
    experiments.sync_metadata_graph()
    assert calls == [True]
    manager.remove_app("retry-experiment-app")


def test_app_declaration_rejects_a_legacy_lookback_field():
    definition = mean("invalid-app", "urn:invalid:temperature").to_dict()
    definition["lookback"] = "all"
    with pytest.raises(ValueError, match="app fields must be"):
        App.from_dict(definition)


def test_python_redeployment_uses_the_declared_source_bytes(system, tmp_path):
    from acquirium import PythonFunction

    manager, _, _ = system
    ref = register(system, "content-hash")
    module = tmp_path / "calculation.py"
    source = ('def calculate(frame, context):\n'
              '    return {"mean": frame.select("time", (frame["value"] * 2).alias("value"))}\n')
    module.write_text(source)
    base = mean("content-hash-app", ref)

    def app(path=module):
        return App(base.name, base.inputs, base.outputs, PythonFunction.from_file(path, "calculate"))

    def write(value, request_id):
        receipt = manager.insert_timeseries(source_id="content-hash", stream_name="temperature",
            rows=[(1000, value)], request_id=request_id).receipt
        return manager.core.wait(receipt)

    manager.deploy_app(app())
    original = manager.apps.inspect(base.name)["operators"][0]
    output = output_reference(base.name, "urn:tank", "mean")
    assert value(write(10, "initial"), output, 1000) == 20
    declaration = app()
    new_source = source.replace("* 2", "* 3")
    module.write_text(new_source)
    # Editing a file cannot change the active deployment.
    assert value(write(20, "old-code"), output, 1000) == 40
    edited = app()
    # A new descriptor requires the bytes it named until they are retained.
    module.write_text("# edited after declaration\n")
    with pytest.raises(ValueError, match="changed since"):
        manager.deploy_app(edited)
    module.unlink()
    with pytest.raises(ValueError, match="cannot read"):
        manager.deploy_app(edited)
    assert manager.apps.inspect(base.name)["operators"][0]["version"] == original["version"]
    module.write_text(new_source)
    manager.deploy_app(edited)
    updated = manager.apps.inspect(base.name)["operators"][0]
    assert updated["version"] != original["version"]
    assert value(manager.core.engine.snapshot(), output, 1000) == 60
    same_content = tmp_path / "same.py"
    same_content.write_bytes(module.read_bytes())
    manager.deploy_app(app(same_content))
    assert manager.apps.inspect(base.name)["operators"][0]["version"] == updated["version"]
    # An older descriptor still identifies its stored bytes, even after editing.
    manager.deploy_app(declaration)
    assert value(manager.core.engine.snapshot(), output, 1000) == 40
    module.unlink()
    same_content.unlink()
    assert value(write(30, "removed-file"), output, 1000) == 60
    manager.remove_app(base.name)


def test_invalid_cycle_keeps_the_previous_plan_and_projection(system):
    manager, http, client = system
    name, group = "invalid-cycle", "urn:tank"
    reference = output_reference(name, group, "mean")
    previous = manager.core.engine.snapshot().plan()
    declaration = mean(name, reference, group)
    response = http.put(f"/apps/{name}", json=declaration.to_dict())
    assert response.status_code == 400
    assert manager.core.engine.snapshot().plan() == previous
    assert not list(manager.graph_store.data_graph(f"derived:{name}"))


def test_activation_failure_and_later_writes_can_be_retried_across_restart(tmp_path, caplog):
    import logging
    import time

    from acquirium import PythonFunction

    flag = tmp_path / "fail"
    module = tmp_path / "calculation.py"
    module.write_text(
        "from pathlib import Path\n"
        "def calculate(frame, context):\n"
        f"    if Path({str(flag)!r}).exists():\n"
        '        raise RuntimeError("sensor offline")\n'
        '    return {"mean": frame.select("time", "value")}\n'
    )

    def retry(manager, request_id):
        """Retry without another reading and wait for the repaired app to become active."""
        manager.core.wait(manager.core.engine.retry(request_id))
        deadline = time.monotonic() + 10
        while manager.apps.inspect(declaration.name)["status"] != "active":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        return manager.core.engine.snapshot()

    with caplog.at_level(logging.ERROR, logger="acquirium.manager"):
        with epoch_server(tmp_path / "data") as system:
            manager, _, client = system
            ref = register(system, "halted")
            template = mean("halted-app", ref)
            declaration = App(template.name, template.inputs, template.outputs, PythonFunction.from_file(module, "calculate"))
            output = output_reference(declaration.name, "urn:tank", "mean")

            # Existing history makes the worker fail during activation. The app is
            # still installed and an unchanged refresh does not resubmit its plan.
            client.insert_timeseries("halted", "temperature", [(1000, 10)])
            flag.touch()
            failed = client.deploy_app(declaration)["app"]
            assert failed["status"] == "failed" and "sensor offline" in failed["error"]
            assert any(row["annotations"]["app"] == declaration.to_dict() for row in manager.core.plan_at()["templates"])
            accepted = manager.core.engine.status()["accepted"]
            manager.apps.refresh()
            assert manager.core.engine.status()["accepted"] == accepted
            flag.unlink()
            assert value(retry(manager, "retry-activation"), output, 1000) == 10

            # Once active, another failure leaves the earlier output intact and
            # still allows both new source readings to publish.
            flag.touch()
            client.insert_timeseries("halted", "temperature", [(2000, 20)])
            receipt, = client.insert_timeseries("halted", "temperature", [(3000, 30)])["receipts"]
            assert receipt["published"]
            snapshot = manager.core.engine.snapshot()
            assert value(snapshot, output, 1000) == 10
            assert value(snapshot, output, 2000) is None
            assert value(snapshot, ref, 3000) == 30
            assert manager.apps.inspect(declaration.name)["status"] == "failed"
            status = manager.core.engine.status()
            assert status["fault"] is None and status["pending"] == 0
            del snapshot
            caplog.clear()
        assert not [r for r in caplog.records if r.name == "acquirium.manager" and r.levelno >= logging.ERROR]

    # The saved status survives restart. Retry still uses the captured source.
    flag.unlink()
    module.unlink()
    with epoch_server(tmp_path / "data") as (manager, _, _):
        assert manager.apps.inspect(declaration.name)["status"] == "failed"
        snapshot = retry(manager, "retry-after-restart")
        assert [value(snapshot, output, time) for time in (1000, 2000, 3000)] == [10, 20, 30]
        assert manager.core.engine.status()["pending"] == 0
        del snapshot


def test_closing_reports_work_left_by_a_drain_timeout(caplog):
    import logging
    from types import SimpleNamespace

    closed = []
    manager = Manager.__new__(Manager)
    manager.drain_timeout = 0.5
    manager.ledger = SimpleNamespace(close=lambda: closed.append("ledger"))
    manager.graph_store = SimpleNamespace(close=lambda: closed.append("graph"))
    manager.core = SimpleNamespace(
        close=lambda timeout: closed.append(("engine", timeout)) and False,
        pending_at_close=3,
    )
    with caplog.at_level(logging.WARNING, logger="acquirium.manager"):
        manager.close()
    assert closed == ["ledger", ("engine", 0.5), "graph"]
    assert [r.getMessage() for r in caplog.records] == [
        "Manager.close: Escapement did not drain within 0.5s; leaving 3 "
        "pending request(s) for the next start"
    ]


def test_frozen_unit_factors_execute_in_native_python_worker(system):
    from pathlib import Path
    from acquirium import PythonFunction

    manager, http, client = system
    ref = register(system, "units")
    selection = Selection(
        f"SELECT ?stream ?group ?unit WHERE {{ BIND(<{ref}> AS ?stream) BIND(<urn:tank> AS ?group) "
        "BIND(<http://qudt.org/vocab/unit/DEG_F> AS ?unit) }",
        target_unit="http://qudt.org/vocab/unit/DEG_C",
    )
    module = (
        Path(__file__).resolve().parents[2]
        / "scripts/apps/temperature_normalization.py"
    )
    declaration = App(
        "units-app",
        {"temperature": selection},
        {"temperature": Output(unit="http://qudt.org/vocab/unit/DEG_C")},
        PythonFunction.from_file(module, "normalize"),
    )
    manager.deploy_app(declaration)
    snapshot = manager.core.wait(
        manager.insert_timeseries(
            source_id="units",
            stream_name="temperature",
            rows=[(2222000, 68)],
            request_id="one",
        ).receipt
    )
    assert value(snapshot,
        output_reference(declaration.name, "urn:tank", "temperature"), 2222000
    ) == pytest.approx(20)


def test_group_output_links_are_owned_and_removed_with_membership(system):
    from acquirium.internals.internals_namespaces import S223
    from rdflib import URIRef

    manager, _, client = system
    ref = register(system, "attached")
    manager.insert_graph(
        f"<{ref}> <urn:group> <urn:attached:tank> .", source_id="attached-layout"
    )
    declaration = App(
        "attached-mean",
        {
            "input": Selection(
                f"SELECT ?stream ?group WHERE {{ VALUES ?stream {{ <{ref}> }} "
                "?stream <urn:group> ?group }"
            )
        },
        {"mean": Output(on_group=True)},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )
    client.deploy_app(declaration)
    output = output_reference(declaration.name, "urn:attached:tank", "mean")
    graph = manager.graph_store.data_graph("derived:attached-mean")
    assert (
        URIRef("urn:attached:tank"),
        S223.hasProperty,
        URIRef(f"{output}__point"),
    ) in graph
    client.insert_timeseries("attached", "temperature", [(1234, 32)])
    assert client.stream(output).dataframe()["value"].to_list() == [32]

    # Moving the probe moves both the derived property's link and its reading.
    moved = "urn:attached:new-tank"
    new_output = output_reference(declaration.name, moved, "mean")
    client.insert_graph(f"<{ref}> <urn:group> <{moved}> .", source_id="attached-layout", replace=True)
    graph = manager.graph_store.data_graph("derived:attached-mean")
    assert list(graph.subject_objects(S223.hasProperty)) == [(URIRef(moved), URIRef(f"{new_output}__point"))]
    assert client.stream(output).dataframe().is_empty()
    assert client.stream(new_output).dataframe()["value"].to_list() == [32]

    client.insert_graph("", source_id="attached-layout", replace=True)
    assert not list(manager.graph_store.data_graph("derived:attached-mean"))
    assert client.inspect_app(declaration.name)["app"]["operators"] == []
    assert client.stream(new_output).dataframe().is_empty()
    # The app owns only its derived links, not the stream registration graph.
    assert ref in str(
        client.query().measurement(unit="http://qudt.org/vocab/unit/DEG_F").execute()
    )
    client.remove_app(declaration.name)


def test_output_attachment_rejects_literal_groups_without_changing_active_plan(system):
    manager, http, client = system
    ref = register(system, "literal-group")
    declaration = App(
        "invalid-attachment",
        {
            "input": Selection(
                f'SELECT ?stream ?group WHERE {{ BIND(<{ref}> AS ?stream) BIND("tank" AS ?group) }}'
            )
        },
        {"mean": Output(on_group=True)},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )
    previous = manager.core.engine.snapshot().plan()
    response = http.put(f"/apps/{declaration.name}", json=declaration.to_dict())
    assert response.status_code == 400
    assert "group URI" in response.text
    assert manager.core.engine.snapshot().plan() == previous
    assert not list(manager.graph_store.data_graph(f"derived:{declaration.name}"))


def test_output_attachment_requires_a_boolean():
    with pytest.raises(ValueError, match="on_group must be a boolean"):
        App(
            "invalid-output",
            {},
            {"value": Output(on_group="yes")},
            {"kind": "mean", "port": "value", "per_stream": False},
        ).to_dict()


def test_outputs_of_chains_by_port_combines_groups_and_guards_dependencies(system):
    from acquirium.internals.internals_namespaces import OUTPUT_PORT
    from rdflib import Literal, URIRef
    from rdflib.namespace import XSD

    manager, http, client = system
    ref = register(system, "ports")
    first = mean("ports-first", ref)
    with pytest.raises(ValueError, match="no output port"):
        Selection.outputs_of(first, "missing")

    def downstream(name, **options):
        return App(
            name,
            {"mean": Selection.outputs_of(first, "mean", **options)},
            {"mean": Output()},
            {"kind": "mean", "port": "mean", "per_stream": False},
        )

    response = http.put("/apps/ports-early", json=downstream("ports-early").to_dict())
    assert response.status_code == 400
    assert "not deployed" in response.text

    client.deploy_app(first)
    point = URIRef(f"{output_reference(first.name, 'urn:tank', 'mean')}__point")
    graph = manager.graph_store.data_graph(f"derived:{first.name}")
    assert (point, OUTPUT_PORT, Literal("mean", datatype=XSD.string)) in graph

    same = client.deploy_app(downstream("ports-same"))["app"]
    combined = client.deploy_app(downstream("ports-combined", combine=True))["app"]
    assert same["matches"] == {"mean": 1} and not same["warnings"]
    snapshot = manager.core.wait(
        manager.insert_timeseries(
            source_id="ports", stream_name="temperature", rows=[(5555, 7)], request_id="one"
        ).receipt
    )
    # The default keeps the upstream group; combine uses one group for the app.
    assert value(snapshot, output_reference("ports-same", "urn:tank", "mean"), 5555) == 7
    assert (
        value(snapshot, output_reference("ports-combined", "ports-combined", "mean"), 5555)
        == 7
    )
    assert combined["matches"] == {"mean": 1}

    with pytest.raises(ValueError, match="not deployed"):
        manager.remove_app(first.name)
    assert manager.apps.inspect(first.name)["status"] == "active"
    manager.remove_app("ports-same")
    manager.remove_app("ports-combined")
    manager.remove_app(first.name)

    empty = mean("ports-empty", "urn:ports:no-such-stream")
    empty = App(
        empty.name,
        {"input": Selection("SELECT ?stream ?group WHERE { FILTER(false) }")},
        empty.outputs,
        empty.function,
    )
    state = client.deploy_app(empty)["app"]
    assert state["warnings"] == ["input 'input' matches no streams"]
    manager.remove_app(empty.name)


def test_selection_parameters_become_operator_parameters_for_listed_groups(system):
    manager, http, client = system
    ref = register(system, "parameters")
    first = mean("parameters-first", ref)
    client.deploy_app(first)

    def thresholds(name, parameters):
        return App(
            name,
            {"mean": Selection.outputs_of(first, "mean", parameters=parameters)},
            {"mean": Output()},
            {"kind": "mean", "port": "mean", "per_stream": False},
        )

    state = client.deploy_app(thresholds("parameters-listed", {"urn:tank": {"threshold": 3.5}}))["app"]
    operator, = state["operators"]
    assert operator["parameters"] == {"group": ["urn:tank"], "threshold": 3.5}
    assert not state["warnings"]
    context, = [item["context"] for item in operator["inputs"]]
    assert "threshold" not in context
    # A parameter is part of the operator's definition, so a new value is a new version.
    state = client.deploy_app(thresholds("parameters-listed", {"urn:tank": {"threshold": 4}}))["app"]
    changed, = state["operators"]
    assert changed["parameters"] == {"group": ["urn:tank"], "threshold": 4}
    assert changed["version"] != operator["version"]

    state = client.deploy_app(thresholds("parameters-other", {"urn:elsewhere": {"threshold": 1}}))["app"]
    assert state["operators"] == []
    assert state["warnings"] == [
        "input 'mean' matches no streams",
        "input 'mean' skips groups without parameters: urn:tank",
    ]
    with pytest.raises(ValueError, match="must be JSON"):
        thresholds("parameters-invalid", {"urn:tank": {"threshold": float("nan")}}).to_dict()
    for name in ("parameters-listed", "parameters-other", first.name):
        manager.remove_app(name)


def test_python_functions_read_typed_parameters_and_the_window(system, tmp_path):
    from acquirium import PythonFunction

    manager, http, client = system
    ref = register(system, "typed-parameters")
    manager.core.wait(
        manager.insert_timeseries(
            source_id="typed-parameters",
            stream_name="temperature",
            rows=[(5000, 12)],
            request_id="typed-parameters",
        ).receipt
    )
    module = tmp_path / "scaled.py"
    module.write_text(
        "from acquirium.functions import emit\n"
        "def scaled(frame, context):\n"
        "    assert context['operator']['window'] == {'kind': 'pointwise'}\n"
        "    assert context['parameters']['group'] == ('urn:tank',)\n"
        "    return {'mean': emit(context, frame['value'][0] * context['parameters']['scale'])}\n"
    )
    template = mean("typed-parameters", ref)
    selection = Selection(template.inputs["input"].query, parameters={"urn:tank": {"scale": 2.5}})
    declaration = App(
        template.name, {"input": selection}, template.outputs, PythonFunction.from_file(module, "scaled")
    )
    state = client.deploy_app(declaration)["app"]
    assert state["error"] is None, state
    snapshot = manager.core.engine.snapshot()
    assert value(snapshot, output_reference(declaration.name, "urn:tank", "mean"), 5000) == 30
    manager.remove_app(declaration.name)


@pytest.mark.parametrize(("mode", "grouping"), [
    pytest.param("default", "tank", id="pointwise-entity"),
    pytest.param("batch", "stream", id="batch-stream"),
    pytest.param("sequential", None, id="sequential-combined"),
])
def test_python_context_includes_group_key_without_configured_parameters(system, tmp_path, mode, grouping):
    from acquirium import PythonFunction

    manager, _, client = system
    source = f"group-context-{mode}-{grouping or 'combined'}"
    ref = register(system, source)
    manager.core.wait(manager.insert_timeseries(
        source_id=source, stream_name="temperature", rows=[(1000, 20), (2000, 22)],
        request_id=source,
    ).receipt)
    module = tmp_path / "identity.py"
    module.write_text(
        "import polars as pl\n"
        "from acquirium.functions import emit, emit_windows\n"
        "def identify(frame, context):\n"
        "    key = context['parameters']['group']\n"
        "    assert isinstance(key, tuple)\n"
        "    group, = key\n"
        "    assert {key: group}[key] == group\n"
        "    if 'windows' in context:\n"
        "        times = [window['output_time'] for window in context['windows']]\n"
        "        return {'identity': emit_windows(pl.DataFrame({'output_time': times, 'text': [group] * len(times)}))}\n"
        "    return {'identity': emit(context, group)}\n"
    )
    selection = Selection(
        f"SELECT ?stream ?tank WHERE {{ BIND(<{ref}> AS ?stream) BIND(<urn:tank> AS ?tank) }}",
        group=grouping,
    )
    declaration = App(
        source, {"input": selection}, {"identity": Output(value_kind="text")},
        PythonFunction.from_file(module, "identify", batch=mode == "batch", sequential=mode == "sequential"),
    )
    state = client.deploy_app(declaration)["app"]
    assert state["error"] is None, state
    expected = {"tank": "urn:tank", "stream": ref, None: source}[grouping]
    output = output_reference(source, expected, "identity")
    snapshot = manager.core.engine.snapshot()
    assert value(snapshot, output, 1000) == expected
    assert value(snapshot, output, 2000) == expected
    manager.remove_app(source)


def test_group_parameter_cannot_override_selected_group(system):
    manager, _, client = system
    ref = register(system, "reserved-group-parameter")
    declaration = mean("reserved-group-parameter", ref)
    query = declaration.inputs["input"].query
    declaration.inputs["input"] = Selection(query, parameters={"urn:tank": {"group": "urn:other"}})
    with pytest.raises(ValueError, match="parameter 'group' is reserved"):
        manager.deploy_app(declaration)


@pytest.mark.parametrize("mode", ["default", "batch"])
def test_arrow_pandas_and_polars_functions_share_a_pipeline(system, tmp_path, mode):
    import acquirium as aq

    manager, _, client = system
    source = f"pandas-{mode}"
    ref = register(system, source)
    manager.core.wait(manager.insert_timeseries(
        source_id=source, stream_name="temperature", rows=[(1000, 10), (2000, 20)],
        request_id="initial",
    ).receipt)
    module = tmp_path / "calculations.py"
    module.write_text(
        "from __future__ import annotations\n"
        "import pandas as pd\n"
        "import polars as pl\n"
        "from acquirium.functions import emit, empty, emit_windows\n"
        "def pandas_sum(frame: pd.DataFrame, context):\n"
        "    assert isinstance(frame, pd.DataFrame)\n"
        "    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in frame.dtypes)\n"
        "    if frame.empty:\n"
        "        return {'mean': empty()}\n"
        "    if 'windows' in context:\n"
        "        means = frame.groupby('output_time', as_index=False)['value'].sum()\n"
        "        result = emit_windows(means)\n"
        "    else:\n"
        "        result = emit(context, frame['value'].sum())\n"
        "    assert isinstance(result, pd.DataFrame)\n"
        "    return {'mean': result}\n"
        "def polars_double(frame: pl.DataFrame, context):\n"
        "    assert isinstance(frame, pl.DataFrame)\n"
        "    result = emit(context, frame['value'][0] * 2 if frame.height else None)\n"
        "    assert isinstance(result, pl.DataFrame)\n"
        "    return {'double': result}\n"
    )
    first = App(source, mean(source, ref).inputs, {"mean": Output()},
                aq.PythonFunction.from_file(module, "pandas_sum", batch=mode == "batch"))
    second = App(f"{source}-double", {"clean": Selection.outputs_of(first, "mean")},
                 {"double": Output()}, aq.PythonFunction.from_file(module, "polars_double"))
    try:
        assert client.deploy_app(first)["app"]["status"] == "active"
        assert client.deploy_app(second)["app"]["status"] == "active"
        out = output_reference(second.name, "urn:tank", "double")
        snapshot = manager.core.engine.snapshot()
        assert [value(snapshot, out, t) for t in (1000, 2000)] == [20, 40]
        receipt = manager.insert_timeseries(source_id=source, stream_name="temperature",
            rows=[(1000, 5)], request_id="corrected").receipt
        snapshot = manager.core.wait(receipt)
        assert [value(snapshot, out, t) for t in (1000, 2000)] == [10, 40]
        snapshot = manager.core.wait(manager.core.clear(source, "deleted", [ref]))
        assert value(snapshot, out, 1000) is None
        assert value(snapshot, out, 2000) is None
        assert client.inspect_app(first.name)["app"]["status"] == "active"
        assert client.inspect_app(second.name)["app"]["status"] == "active"
    finally:
        if second.name in manager.apps.state["apps"]:
            manager.remove_app(second.name)
        if first.name in manager.apps.state["apps"]:
            manager.remove_app(first.name)


def test_as_of_selections_read_the_latest_reading_of_a_slower_stream(system, tmp_path):
    from acquirium import PythonFunction

    manager, http, client = system
    reading = register(system, "as-of-reading")
    gain = register(system, "as-of-gain")
    module = tmp_path / "calibrated.py"
    module.write_text(
        "import polars as pl\n"
        "from acquirium.functions import emit, empty\n"
        "def calibrated(frame, context):\n"
        "    x = frame.filter(pl.col('input') == 'x')['value']\n"
        "    gain = frame.filter(pl.col('input') == 'gain')['value']\n"
        "    if x.is_empty() or gain.is_empty():\n"
        "        return {'mean': empty()}\n"
        "    return {'mean': emit(context, x[0] * gain[0])}\n"
    )
    template = mean("as-of", reading)
    def selected(stream, **options):
        return Selection(
            f"SELECT ?stream ?group WHERE {{ BIND(<{stream}> AS ?stream) "
            "BIND(<urn:tank> AS ?group) }",
            **options,
        )

    def declaration(age):
        return App(
            template.name,
            {"x": selected(reading), "gain": selected(gain, as_of=age)},
            template.outputs,
            PythonFunction.from_file(module, "calibrated"),
        )

    assert "as_of" not in declaration(None).to_dict()["inputs"][1]
    with pytest.raises(ValueError, match="positive"):
        declaration(0).to_dict()
    with pytest.raises(ValueError, match="different ages"):
        App(template.name, {"gain": [selected(gain, as_of=5), selected(reading)]},
            template.outputs, PythonFunction.from_file(module, "calibrated")).to_dict()

    state = client.deploy_app(declaration(3000))["app"]
    assert state["error"] is None, state
    for source, rows in (("as-of-reading", [(t, 1.0) for t in range(1000, 9000, 1000)]),
                         ("as-of-gain", [(1000, 2.0), (5000, 3.0)])):
        manager.core.wait(manager.insert_timeseries(
            source_id=source, stream_name="temperature", rows=rows, request_id=source).receipt)
    reference = output_reference(template.name, "urn:tank", "mean")
    def values():
        snapshot = manager.core.engine.snapshot()
        return [value(snapshot, reference, t) for t in range(1000, 9000, 1000)]
    def operator():
        return manager.apps.inspect(template.name)["operators"][0]

    # Each reading takes the latest gain at most 3000 ns old.
    assert values() == [2, 2, 2, 2, 3, 3, 3, 3]
    version = operator()["version"]
    # A shorter maximum age is a new definition, which rebuilds the history.
    client.deploy_app(declaration(2000))
    assert values() == [2, 2, 2, None, 3, 3, 3, None]
    assert operator()["as_of"] == {"gain": {"max_age_ns": 2000}}
    assert operator()["version"] != version
    manager.remove_app(template.name)


def test_native_function_arguments_bind_to_each_groups_parameters(system):
    from acquirium import Native, Parameter

    manager, http, client = system
    ref = register(system, "native-parameters")
    name = "native-parameters"
    query = client.query().measurement(uri="urn:native-parameters:point", alias="temperature")

    def alarm(parameters):
        return App(
            name,
            {"input": Selection.from_query(query, measurement="temperature", parameters=parameters)},
            {"alarm": Output()},
            Native.threshold("alarm", above=Parameter("limit")),
        )

    declaration = alarm({ref: {"limit": 10}})
    assert declaration.to_dict()["function"] == {
        "kind": "threshold", "port": "alarm", "above": {"parameter": "limit"}
    }
    state = client.deploy_app(declaration)["app"]
    operator, = state["operators"]
    assert operator["function"] == {"kind": "threshold", "port": "alarm", "above": 10}
    # The native app reads the selected probe and exposes its thresholded result.
    client.insert_timeseries("native-parameters", "temperature", [(6000, 12), (7000, 8)])
    result = client.query().measurement(app=name, alias="alarm").data()
    assert [binding.stream_uri for binding in result.bindings] == [output_reference(name, ref, "alarm")]
    assert result.dataframe(shape="narrow")["value_numeric"].to_list() == [12]

    response = http.put(
        f"/apps/{name}", json=alarm({ref: {"other": 1}}).to_dict()
    )
    assert response.status_code == 400
    assert "no parameter 'limit'" in response.text
    manager.remove_app(name)


def test_a_flow_totalizer_repairs_every_later_volume(system):
    from acquirium import FixedBucket, Native

    manager, http, client = system
    ref = register(system, "flow-totalizer")
    template = mean("flow-totalizer", ref)
    hour = 3_600_000_000_000
    client.deploy_app(App(
        template.name,
        template.inputs,
        {"volume": Output()},
        Native.integral("volume", per_ns=hour, reset=FixedBucket(24 * hour)),
    ))

    def insert(rows, request_id):
        manager.core.wait(manager.insert_timeseries(
            source_id="flow-totalizer", stream_name="temperature", rows=rows, request_id=request_id,
        ).receipt)

    def volumes(*hours):
        snapshot = manager.core.engine.snapshot()
        return [value(snapshot, reference, int(h * hour)) for h in hours]

    reference = output_reference(template.name, "urn:tank", "volume")
    # A flow of 10 per hour, read hourly from 22:00 into the next day.
    insert([(h * hour, 10.0) for h in range(22, 27)], "flow")
    assert volumes(22, 23, 24, 25, 26) == [0, 10, 10, 20, 30]
    # A late reading of 30 at 22:30 adds 10 to every volume of that day.
    insert([(int(22.5 * hour), 30.0)], "late")
    assert volumes(22, 22.5, 23, 24, 25) == [0, 10, 20, 10, 20]
    manager.remove_app(template.name)


def test_dry_run_reports_the_work_of_a_deploy_without_making_it(system):
    from acquirium import Native

    manager, http, client = system
    ref = register(system, "dry-run")
    manager.core.wait(
        manager.insert_timeseries(
            source_id="dry-run",
            stream_name="temperature",
            rows=[(8000, 12), (9000, 14)],
            request_id="dry-run",
        ).receipt
    )
    declaration = mean("dry-run-mean", ref)
    preview = client.deploy_app(declaration, dry_run=True)["app"]
    assert preview["dry_run"] and preview["within_limits"]
    added, = [row for row in preview["instances"] if row["template"] == declaration.name]
    assert (added["windows"], added["input_rows"], added["output_rows"]) == (2, 2, 0)
    assert preview["matches"] == {"input": 1} and preview["warnings"] == []
    # Nothing was deployed, published or activated.
    assert "dry-run-mean" not in {item["name"] for item in client.list_apps()["apps"]}
    assert not list(manager.graph_store.data_graph("derived:dry-run-mean"))
    assert preview["active_plan"] == manager.apps.preview(declaration)["active_plan"]

    client.deploy_app(declaration)
    redefined = App(declaration.name, declaration.inputs, declaration.outputs, Native.sum("mean"))
    preview = client.deploy_app(redefined, dry_run=True)["app"]
    changed, = [row for row in preview["instances"] if row["template"] == declaration.name]
    assert (changed["windows"], changed["input_rows"], changed["output_rows"]) == (2, 2, 2)
    unchanged = client.deploy_app(declaration, dry_run=True)["app"]
    same, = [row for row in unchanged["instances"] if row["template"] == declaration.name]
    assert same["windows"] == same["output_rows"] == 0

    dependent = App(
        "dry-run-dependent",
        {"mean": Selection.outputs_of(App("dry-run-missing", {}, {"mean": Output()}, Native.sum("mean")),
                                      "mean")},
        {"mean": Output()},
        Native.mean("mean"),
    )
    response = http.put(
        f"/apps/{dependent.name}", params={"dry_run": "true"}, json=dependent.to_dict()
    )
    assert response.status_code == 400 and "not deployed" in response.text
    manager.remove_app(declaration.name)


def test_conflicting_parameters_for_one_group_are_rejected(system):
    manager, http, client = system
    ref = register(system, "conflict")
    template = mean("parameters-conflict", ref)
    query = template.inputs["input"].query
    declaration = App(
        template.name,
        {
            "a": Selection(query, parameters={"urn:tank": {"limit": 1}}),
            "b": Selection(query, parameters={"urn:tank": {"limit": 2}}),
        },
        template.outputs,
        template.function,
    )
    response = http.put(f"/apps/{declaration.name}", json=declaration.to_dict())
    assert response.status_code == 400
    assert "different values" in response.text


def test_engine_snapshot_waits_for_the_plan_to_catch_up_with_the_graph(system):
    manager, http, _ = system
    ref = register(system, "catch-up")
    query = f"SELECT ?stream ?group WHERE {{ VALUES ?stream {{ <{ref}> }} ?stream <urn:catch-up> ?group }}"
    declaration = App(
        "catch-up-mean", {"input": Selection(query)}, {"mean": Output()},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )
    manager.insert_graph(f"<{ref}> <urn:catch-up> <urn:room:A> .", source_id="catch-up-groups")
    manager.deploy_app(declaration)
    manager.core.wait(manager.insert_timeseries(
        source_id="catch-up", stream_name="temperature", rows=[(4242, 7)], request_id="one",
    ).receipt)
    moved = output_reference(declaration.name, "urn:room:B", "mean")
    # The graph moves the stream, as any graph write does before the apps
    # that read it install a plan; nothing has refreshed them yet.
    manager.graph_store.insert_graph(
        f"<{ref}> <urn:catch-up> <urn:room:B> .", replace=True,
        graph_uri=manager.graph_store.source_graph_uri("catch-up-groups"),
    )
    assert value(manager.core.engine.snapshot(), moved, 4242) is None
    response = http.get("/engine/snapshot")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["plan_current"] is True
    outputs = {
        stream for op in body["plan"]["instances"] if op["template"] == "catch-up-mean"
        for stream in op["outputs"].values()
    }
    assert outputs == {moved}
    assert value(manager.core.engine.snapshot(), moved, 4242) == 7
    manager.remove_app(declaration.name)


def test_startup_refuses_an_engine_plan_without_acquirium_annotations(tmp_path):
    from acquirium.apps import Apps
    from acquirium.core import Core
    from tests.epoch_helpers import plan

    module = tmp_path / "calculation.py"
    module.write_text("def calculate(frame, context): return {}\n")
    core = Core(tmp_path / "engine.duckdb", module_paths=[str(tmp_path)])
    try:
        core.wait(core.activate("deploy", plan(module)))
        apps = Apps(core, graph=None, directory=tmp_path)
        with pytest.raises(ValueError, match="no Acquirium declaration"):
            apps.recover()
        assert len(core.plan_at()["templates"]) == 1
    finally:
        core.close()


def test_conversions_resolve_each_unit_once():
    from types import SimpleNamespace
    from acquirium.apps import Apps

    resolved = []

    class Converter:
        def resolve_unit(self, unit):
            resolved.append(unit)
            return SimpleNamespace(multiplier=2.0 if unit == "degF" else 1.0, offset=0.0, kind=unit[:3])

        @staticmethod
        def check_compatible(source, target):
            return source.kind == target.kind

    apps = Apps.__new__(Apps)
    apps.converter = Converter
    convert = apps._conversions("degC")
    assert resolved == []  # nothing resolves until an input needs it
    for _ in range(3):
        assert convert("degF") == dict(from_multiplier="2.0", from_offset="0.0",
                                       to_multiplier="1.0", to_offset="0.0")
    assert sorted(resolved) == ["degC", "degF"]
    with pytest.raises(ValueError, match="incompatible"):
        convert("psi")
