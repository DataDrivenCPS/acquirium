from datetime import datetime, timezone

from fastapi.testclient import TestClient
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from acquirium import Acquirium, App, Selection, Output
from acquirium.apps import output_reference, totals_per_app
from acquirium.Server.manager import Manager
from acquirium.Server.app import app as api
from acquirium.internals.models import compute_ref_uri


@pytest.fixture(scope="module")
def system(tmp_path_factory):
    directory = tmp_path_factory.mktemp("semantic-core")
    manager = Manager(data_dir=directory, exact_only=True)
    manager.recover_apps()
    api.state.manager = manager
    api.state.read_batch_size = 101
    http = TestClient(api)
    client = Acquirium(health_timeout=None)
    client.client._http = http
    try:
        yield manager, http, client
    finally:
        http.close()
        manager.close()


def register(system, source):
    manager, http, client = system
    client.register_streams(
        [
            dict(
                source_id=source,
                ref_name="temperature",
                value_kind="numeric",
                point_uri=f"urn:{source}:point",
                unit="http://qudt.org/vocab/unit/DEG_F",
            )
        ]
    )
    return str(compute_ref_uri(source, "temperature"))


def mean(name, stream, group="urn:tank"):
    return App(
        name,
        [
            Selection(
                f"SELECT ?stream ?group WHERE {{ BIND(<{stream}> AS ?stream) BIND(<{group}> AS ?group) }}"
            )
        ],
        {"mean": Output()},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )


def test_semantic_query_native_plan_http_ingestion_and_arrow_read(system):
    manager, http, client = system
    ref = register(system, "semantic")
    query = client.query().measurement(
        alias="temperature", unit="http://qudt.org/vocab/unit/DEG_F"
    )
    selection = Selection.from_query(query, alias="temperature")
    declaration = App(
        "semantic-mean",
        [selection],
        {"mean": Output()},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )
    response = http.put(f"/apps/{declaration.name}", json=declaration.to_dict())
    assert response.status_code == 200, response.text
    response = http.post(
        "/insert_timeseries",
        json=[
            dict(
                source_id="semantic",
                ref_name="temperature",
                request_id="initial",
                values=[[1001, 21.5], [2002, 25]],
            )
        ],
    )
    assert response.status_code == 200, response.text
    receipt = response.json()["receipts"][0]
    assert receipt["published"]
    output = output_reference("semantic-mean", ref, "mean")
    response = http.get("/timeseries", params={"uri": output})
    assert response.status_code == 200
    data = ipc.open_stream(response.content).read_all()
    assert data["ts"].type == pa.timestamp("ns", tz="UTC")
    assert data["ts"].cast(pa.int64()).to_pylist() == [1001, 2002]
    assert data["value"].to_pylist() == [21.5, 25]
    matched = client.query().measurement(app="semantic-mean").execute()
    assert output in str(matched)
    assert not (manager.data_dir / "timeseries.duckdb").exists()
    assert (manager.data_dir / "escapement.duckdb").exists()


def test_two_apps_retain_each_other_and_form_a_chain(system):
    manager, http, client = system
    ref = register(system, "chain")
    first = mean("chain-first", ref)
    response = http.put(f"/apps/{first.name}", json=first.to_dict())
    assert response.status_code == 200, response.text
    output = output_reference(first.name, "urn:tank", "mean")
    second = mean("chain-second", output)
    response = http.put(f"/apps/{second.name}", json=second.to_dict())
    assert response.status_code == 200, response.text
    receipt = manager.insert_timeseries(
        source_id="chain", ref_name="temperature", rows=[(7777, 9)], request_id="one"
    )
    snapshot = manager.core.wait(receipt)
    assert snapshot.value(output_reference(second.name, "urn:tank", "mean"), 7777) == 9
    assert (
        len([op for op in snapshot.plan["operators"] if op["id"].startswith("chain-")])
        == 2
    )
    manager.remove_app(second.name)
    snapshot = manager.core.engine.snapshot()
    assert snapshot.value(output, 7777) == 9
    assert (
        snapshot.value(output_reference(second.name, "urn:tank", "mean"), 7777) is None
    )


def test_metadata_move_keeps_group_outputs_stable_and_repairs_history(system):
    manager, http, client = system
    ref = register(system, "moving")
    query = f"SELECT ?stream ?group WHERE {{ VALUES ?stream {{ <{ref}> }} ?stream <urn:group> ?group }}"
    declaration = App(
        "moving-mean",
        [Selection(query)],
        {"mean": Output()},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )
    manager.insert_graph(
        f"<{ref}> <urn:group> <urn:room:A> .", source_id="membership", replace=True
    )
    response = http.put(f"/apps/{declaration.name}", json=declaration.to_dict())
    assert response.status_code == 200, response.text
    manager.core.wait(
        manager.insert_timeseries(
            source_id="moving",
            ref_name="temperature",
            rows=[(9999, 42)],
            request_id="one",
        )
    )
    manager.insert_graph(
        f"<{ref}> <urn:group> <urn:room:B> .", source_id="membership", replace=True
    )
    snapshot = manager.core.engine.snapshot()
    assert (
        snapshot.value(output_reference(declaration.name, "urn:room:A", "mean"), 9999)
        is None
    )
    assert (
        snapshot.value(output_reference(declaration.name, "urn:room:B", "mean"), 9999)
        == 42
    )
    manager.insert_graph("", source_id="membership", replace=True)
    assert (
        manager.core.engine.snapshot().value(
            output_reference(declaration.name, "urn:room:B", "mean"), 9999
        )
        is None
    )


def test_atomic_request_retry_and_explicit_delete_over_http(system):
    manager, http, client = system
    ref = register(system, "retry")
    payload = [
        dict(
            source_id="retry",
            ref_name="temperature",
            request_id="one",
            values=[[1234, 10]],
        )
    ]
    first = http.post("/insert_timeseries", json=payload)
    second = http.post("/insert_timeseries", json=payload)
    assert first.status_code == 200, first.text
    assert (
        first.json()["receipts"][0]["sequence"]
        == second.json()["receipts"][0]["sequence"]
    )
    epoch = manager.core.engine.snapshot().epoch
    response = http.post(
        "/delete_timeseries",
        json=dict(
            source_id="retry",
            ref_name="temperature",
            timestamps=[1234],
            request_id="delete",
        ),
    )
    assert response.json()["published"]
    assert manager.core.engine.snapshot().value(ref, 1234) is None
    data = ipc.open_stream(
        http.get("/timeseries", params=dict(uri=ref, epoch=epoch)).content
    ).read_all()
    assert data["value"].to_pylist() == [10]


def test_text_on_a_numeric_stream_is_kept_and_legacy_declarations_are_rejected(system):
    manager, http, client = system
    ref = register(system, "invalid")
    response = http.post(
        "/insert_timeseries",
        json=[
            dict(
                source_id="invalid",
                ref_name="temperature",
                request_id="fault",
                values=[[1, "Bad"]],
            )
        ],
    )
    assert response.status_code == 200
    assert response.json()["receipts"][0]["published"]
    assert manager.core.engine.snapshot().value(ref, 1) == "Bad"
    definition = mean("invalid-app", ref).to_dict()
    definition["lookback"] = "all"
    response = http.put("/apps/invalid-app", json=definition)
    assert response.status_code == 400


def test_failed_activation_remains_durable_and_can_be_retried(system, tmp_path):
    from escapement import PythonFunction

    manager, http, client = system
    ref = register(system, "artifact")
    manager.core.wait(
        manager.insert_timeseries(
            source_id="artifact",
            ref_name="temperature",
            rows=[(4444, 12)],
            request_id="one",
        )
    )
    module = tmp_path / "calculation.py"
    source = 'def calculate(frame, context):\n    return {"mean": frame.select("event_time_ns", "sample_key", "value", "quality")}\n'
    module.write_text(source)
    declaration = mean("artifact-app", ref)
    declaration = App(
        declaration.name,
        declaration.inputs,
        declaration.outputs,
        PythonFunction.from_file(module, "calculate"),
    )
    module.unlink()
    response = http.put(f"/apps/{declaration.name}", json=declaration.to_dict())
    assert response.status_code == 200, response.text
    assert manager.core.processor.status()["error"]
    saved = __import__("json").loads((manager.data_dir / "apps.json").read_text())
    assert declaration.name in saved["apps"]
    assert saved["plan"]["id"] != manager.core.engine.snapshot().plan["id"]
    accepted = manager.core.engine.status()["accepted"]
    manager.apps.refresh()
    assert manager.core.engine.status()["accepted"] == accepted
    module.write_text(source)
    manager.core.processor.retry()
    receipt = manager.core.activate(saved["activation_id"], saved["plan"])
    snapshot = manager.core.wait(receipt)
    assert (
        snapshot.value(output_reference(declaration.name, "urn:tank", "mean"), 4444)
        == 12
    )
    assert manager.apps.inspect(declaration.name)["error"] is None
    manager.remove_app(declaration.name)


def test_invalid_cycle_keeps_the_previous_plan_and_manifest(system):
    import json

    manager, http, client = system
    name, group = "invalid-cycle", "urn:tank"
    reference = output_reference(name, group, "mean")
    previous = manager.core.engine.snapshot().plan
    declaration = mean(name, reference, group)
    response = http.put(f"/apps/{name}", json=declaration.to_dict())
    assert response.status_code == 400
    assert manager.core.engine.snapshot().plan == previous
    assert name not in json.loads((manager.data_dir / "apps.json").read_text())["apps"]
    assert not list(manager.graph_store.data_graph(f"derived:{name}"))


def test_restart_reconciles_desired_plan_and_retains_history(tmp_path):
    import json
    from acquirium.internals.models import compute_ref_uri

    ref = str(compute_ref_uri("restart", "temperature"))
    manager = Manager(data_dir=tmp_path, exact_only=True)
    try:
        manager.insert_graph(
            f'<{ref}> <urn:acquirium#sourceId> "restart" ; <urn:acquirium#refName> "temperature" .',
            source_id="restart",
        )
        manager.deploy_app(mean("restart-app", ref))
        manager.core.wait(
            manager.insert_timeseries(
                source_id="restart",
                ref_name="temperature",
                rows=[(1111, 33)],
                request_id="original",
            )
        )
        plan = manager.core.engine.snapshot().plan
        epoch = manager.core.engine.snapshot().epoch
    finally:
        manager.close()
    manager = Manager(data_dir=tmp_path, exact_only=True)
    try:
        manager.recover_apps()
        snapshot = manager.core.engine.snapshot()
        assert snapshot.plan == plan
        assert (
            snapshot.epoch == epoch
        )  # Recovery does not manufacture a new activation.
        assert (
            snapshot.value(output_reference("restart-app", "urn:tank", "mean"), 1111)
            == 33
        )
        receipt = manager.insert_timeseries(
            source_id="restart",
            ref_name="temperature",
            rows=[(1111, 55)],
            request_id="correction",
        )
        assert (
            manager.core.wait(receipt).value(
                output_reference("restart-app", "urn:tank", "mean"), 1111
            )
            == 55
        )
    finally:
        manager.close()


def test_frozen_unit_factors_execute_in_native_python_worker(system):
    from pathlib import Path
    from escapement import PythonFunction

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
        [selection],
        {"temperature": Output(unit="http://qudt.org/vocab/unit/DEG_C")},
        PythonFunction.from_file(module, "normalize"),
    )
    manager.deploy_app(declaration)
    snapshot = manager.core.wait(
        manager.insert_timeseries(
            source_id="units",
            ref_name="temperature",
            rows=[(2222, 68)],
            request_id="one",
        )
    )
    assert snapshot.value(
        output_reference(declaration.name, "urn:tank", "temperature"), 2222
    ) == pytest.approx(20)


def test_http_range_strings_preserve_nanosecond_boundaries(system):
    manager, http, client = system
    ref = register(system, "precision")
    manager.core.wait(
        manager.insert_timeseries(
            source_id="precision",
            ref_name="temperature",
            rows=[(1001, 1), (1002, 2), (1003, 3)],
            request_id="boundary",
        )
    )
    response = http.get(
        "/timeseries",
        params=dict(
            uri=ref,
            start="1970-01-01T00:00:00.000001002Z",
            end="1970-01-01T00:00:00.000001003Z",
        ),
    )
    assert response.status_code == 200
    result = ipc.open_stream(response.content).read_all()
    assert result["ts"].cast(pa.int64()).to_pylist() == [1002]
    assert result["value"].to_pylist() == [2.0]


def test_group_output_links_are_owned_and_removed_with_membership(system):
    from acquirium.internals.internals_namespaces import S223
    from rdflib import URIRef

    manager, http, client = system
    ref = register(system, "attached")
    manager.insert_graph(
        f"<{ref}> <urn:group> <urn:attached:tank> .", source_id="attached-layout"
    )
    declaration = App(
        "attached-mean",
        [
            Selection(
                f"SELECT ?stream ?group WHERE {{ VALUES ?stream {{ <{ref}> }} "
                "?stream <urn:group> ?group }"
            )
        ],
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
    manager.core.wait(
        manager.insert_timeseries(
            source_id="attached",
            ref_name="temperature",
            rows=[(1234, 32)],
            request_id="one",
        )
    )
    client.insert_graph("", source_id="attached-layout", replace=True)
    assert not list(manager.graph_store.data_graph("derived:attached-mean"))
    assert client.inspect_app(declaration.name)["app"]["operators"] == []
    assert manager.core.engine.snapshot().value(output, 1234) is None
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
        [
            Selection(
                f'SELECT ?stream ?group WHERE {{ BIND(<{ref}> AS ?stream) BIND("tank" AS ?group) }}'
            )
        ],
        {"mean": Output(on_group=True)},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )
    previous = manager.core.engine.snapshot().plan
    response = http.put(f"/apps/{declaration.name}", json=declaration.to_dict())
    assert response.status_code == 400
    assert "group URI" in response.text
    assert manager.core.engine.snapshot().plan == previous
    assert not list(manager.graph_store.data_graph(f"derived:{declaration.name}"))


def test_output_attachment_requires_a_boolean():
    with pytest.raises(ValueError, match="on_group must be a boolean"):
        App(
            "invalid-output",
            [],
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
        Selection.outputs_of(first, "missing", role="mean")

    def downstream(name, **options):
        return App(
            name,
            [Selection.outputs_of(first, "mean", role="mean", **options)],
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
            source_id="ports", ref_name="temperature", rows=[(5555, 7)], request_id="one"
        )
    )
    # The default keeps the upstream group; combine uses one group for the app.
    assert snapshot.value(output_reference("ports-same", "urn:tank", "mean"), 5555) == 7
    assert (
        snapshot.value(output_reference("ports-combined", "ports-combined", "mean"), 5555)
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
        [Selection("SELECT ?stream ?group WHERE { FILTER(false) }")],
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
            [Selection.outputs_of(first, "mean", role="mean", parameters=parameters)],
            {"mean": Output()},
            {"kind": "mean", "port": "mean", "per_stream": False},
        )

    state = client.deploy_app(thresholds("parameters-listed", {"urn:tank": {"threshold": 3.5}}))["app"]
    operator, = state["operators"]
    assert operator["parameters"] == {"threshold": 3.5} and not state["warnings"]
    context, = [item["context"] for item in operator["inputs"]]
    assert "threshold" not in context
    # A parameter is part of the operator's definition, so a new value is a new version.
    state = client.deploy_app(thresholds("parameters-listed", {"urn:tank": {"threshold": 4}}))["app"]
    changed, = state["operators"]
    assert changed["parameters"] == {"threshold": 4}
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
    from escapement import PythonFunction

    manager, http, client = system
    ref = register(system, "typed-parameters")
    manager.core.wait(
        manager.insert_timeseries(
            source_id="typed-parameters",
            ref_name="temperature",
            rows=[(5000, 12)],
            request_id="typed-parameters",
        )
    )
    module = tmp_path / "scaled.py"
    module.write_text(
        "from escapement import emit\n"
        "def scaled(frame, context):\n"
        "    assert context['operator']['window'] == {'kind': 'pointwise'}\n"
        "    return {'mean': emit(context, frame['value'][0] * context['parameters']['scale'])}\n"
    )
    template = mean("typed-parameters", ref)
    selection = Selection(template.inputs[0].query, parameters={"urn:tank": {"scale": 2.5}})
    declaration = App(
        template.name, [selection], template.outputs, PythonFunction.from_file(module, "scaled")
    )
    state = client.deploy_app(declaration)["app"]
    assert state["error"] is None, state
    snapshot = manager.core.engine.snapshot()
    assert snapshot.value(output_reference(declaration.name, "urn:tank", "mean"), 5000) == 30
    manager.remove_app(declaration.name)


def test_native_function_arguments_bind_to_each_groups_parameters(system):
    from acquirium import Native, Parameter

    manager, http, client = system
    ref = register(system, "native-parameters")
    template = mean("native-parameters", ref)
    query = template.inputs[0].query

    def alarm(parameters):
        return App(
            template.name,
            [Selection(query, parameters=parameters)],
            {"alarm": Output()},
            Native.threshold("alarm", above=Parameter("limit")),
        )

    declaration = alarm({"urn:tank": {"limit": 10}})
    assert declaration.to_dict()["function"] == {
        "kind": "threshold", "port": "alarm", "above": {"parameter": "limit"}
    }
    state = client.deploy_app(declaration)["app"]
    operator, = state["operators"]
    assert operator["function"] == {"kind": "threshold", "port": "alarm", "above": 10}
    manager.core.wait(
        manager.insert_timeseries(
            source_id="native-parameters",
            ref_name="temperature",
            rows=[(6000, 12), (7000, 8)],
            request_id="native-parameters",
        )
    )
    reference = output_reference(template.name, "urn:tank", "alarm")
    snapshot = manager.core.engine.snapshot()
    assert snapshot.value(reference, 6000) == 12 and snapshot.value(reference, 7000) is None

    response = http.put(
        f"/apps/{template.name}", json=alarm({"urn:tank": {"other": 1}}).to_dict()
    )
    assert response.status_code == 400
    assert "no parameter 'limit'" in response.text
    manager.remove_app(template.name)


def test_dry_run_reports_the_work_of_a_deploy_without_making_it(system):
    from acquirium import Native

    manager, http, client = system
    ref = register(system, "dry-run")
    manager.core.wait(
        manager.insert_timeseries(
            source_id="dry-run",
            ref_name="temperature",
            rows=[(8000, 12), (9000, 14)],
            request_id="dry-run",
        )
    )
    declaration = mean("dry-run-mean", ref)
    preview = client.deploy_app(declaration, dry_run=True)["app"]
    assert preview["dry_run"] and preview["within_limits"]
    assert preview["apps"]["dry-run-mean"] == {
        "operators": 1, "changes": {"new": 1}, "groups": 2, "input_rows": 2,
        "cleared_rows": 0, "downstream": False,
    }
    assert preview["metadata_changes"] == ["dry-run-mean"]
    assert preview["matches"] == {"input": 1} and preview["warnings"] == []
    # Nothing was deployed, published or activated.
    assert "dry-run-mean" not in {item["name"] for item in client.list_apps()["apps"]}
    assert not list(manager.graph_store.data_graph("derived:dry-run-mean"))
    assert preview["active_plan"] == manager.core.engine.snapshot().plan["id"]

    client.deploy_app(declaration)
    redefined = App(declaration.name, declaration.inputs, declaration.outputs, Native.sum("mean"))
    preview = client.deploy_app(redefined, dry_run=True)["app"]
    assert preview["apps"]["dry-run-mean"] == {
        "operators": 1, "changes": {"redefined": 1}, "groups": 2, "input_rows": 2,
        "cleared_rows": 2, "downstream": False,
    }
    assert preview["metadata_changes"] == []
    assert client.deploy_app(declaration, dry_run=True)["app"]["apps"]["dry-run-mean"][
        "changes"
    ] == {"unchanged": 1}

    dependent = App(
        "dry-run-dependent",
        [Selection.outputs_of(App("dry-run-missing", [], {"mean": Output()}, Native.sum("mean")),
                              "mean", role="mean")],
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
    query = template.inputs[0].query
    declaration = App(
        template.name,
        [
            Selection(query, role="a", parameters={"urn:tank": {"limit": 1}}),
            Selection(query, role="b", parameters={"urn:tank": {"limit": 2}}),
        ],
        template.outputs,
        template.function,
    )
    response = http.put(f"/apps/{declaration.name}", json=declaration.to_dict())
    assert response.status_code == 400
    assert "different values" in response.text


def test_engine_status_rolls_operator_totals_up_per_app(system):
    manager, http, client = system
    ref = register(system, "totals")
    declaration = mean("totals-mean", ref)
    client.deploy_app(declaration)
    manager.core.wait(
        manager.insert_timeseries(
            source_id="totals", ref_name="temperature", rows=[(3333, 1), (3334, 2)], request_id="one"
        )
    )
    status = http.get("/engine/status").json()
    operators = {
        key: value
        for key, value in status["stats"]["operators"].items()
        if key.startswith("totals-mean:")
    }
    rollup = status["apps"]["totals-mean"]
    assert rollup["operators"] == len(operators) == 1
    totals, = operators.values()
    assert totals["groups"] == rollup["groups"] >= 2
    assert totals["input_rows"] == rollup["input_rows"] >= 2
    assert rollup["changed_rows"] >= 2
    manager.remove_app(declaration.name)


def test_text_streams_store_and_read_text(system):
    manager, http, client = system
    client.register_streams(
        [dict(source_id="pumps", ref_name="mode", value_kind="text", point_uri="urn:pumps:mode")]
    )
    numeric = register(system, "pumps")
    ref = str(compute_ref_uri("pumps", "mode"))
    response = http.post(
        "/insert_timeseries",
        json=[
            dict(source_id="pumps", ref_name="mode", request_id="modes",
                 values=[[1, "AUTO"], [2, 3]]),
            dict(source_id="pumps", ref_name="temperature", request_id="modes",
                 values=[[1, 70.5], [2, "Bad"]]),
        ],
    )
    assert response.status_code == 200

    def read(uri, value_mode="default"):
        response = http.get("/timeseries", params={"uri": uri, "value_mode": value_mode})
        assert response.status_code == 200
        return ipc.open_stream(response.content).read_all()["value"].to_pylist()

    # A text stream keeps numbers as text; a numeric stream keeps fault codes.
    assert read(ref) == ["AUTO", "3"]
    assert read(numeric) == [70.5]
    assert read(numeric, "text") == ["Bad"]
    assert read(numeric, "coalesce") == ["70.5", "Bad"]


def test_startup_refuses_an_engine_plan_that_apps_json_does_not_record(tmp_path):
    from escapement import Input, Native, Operator, Plan, Pointwise
    from acquirium.apps import Apps
    from acquirium.core import Core

    core = Core(tmp_path / "engine.duckdb")
    try:
        plan = Plan("deployed", [Operator(
            "app:group", [Input("urn:sensor", "x")], {"out": "urn:derived"},
            Pointwise(), Native.mean("out"),
        )])
        core.wait(core.activate("deploy", plan))
        # A data directory without apps.json, next to an engine that kept its plan.
        apps = Apps(core, graph=None, directory=tmp_path)
        with pytest.raises(RuntimeError, match="records no apps"):
            apps.recover()
        assert core.engine.snapshot().plan["id"] == "deployed"
    finally:
        core.close()


def test_engine_failures_are_retryable_and_bad_input_is_not(system, monkeypatch):
    from escapement import EngineError

    manager, http, client = system
    register(system, "errors")
    body = [dict(source_id="errors", ref_name="temperature", request_id="r", values=[[1, 2.0]])]

    def fail(*args, **kwargs):
        raise EngineError("database connection lost")

    monkeypatch.setattr(manager, "insert_timeseries_batch", fail)
    response = http.post("/insert_timeseries", json=body)
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"
    monkeypatch.undo()
    body[0]["ref_name"] = "unregistered"
    assert http.post("/insert_timeseries", json=body).status_code == 400


def test_totals_per_app_counts_operators_and_sums_counters_by_app():
    operators = [("a:1", {"rows": 2, "ms": 0.5}), ("b:1", {"rows": 1, "ms": 1.0}),
                 ("a:2", {"rows": 3, "ms": 0.25})]
    assert totals_per_app(operators, ("rows", "ms")) == {
        "a": {"operators": 2, "rows": 5, "ms": 0.75},
        "b": {"operators": 1, "rows": 1, "ms": 1.0},
    }
