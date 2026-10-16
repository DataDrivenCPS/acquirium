from datetime import datetime, timezone

from fastapi.testclient import TestClient
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from acquirium import Acquirium, App, Selection, Output
from acquirium.apps import output_reference
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


def test_unsupported_text_and_legacy_declarations_are_rejected(system):
    manager, http, client = system
    ref = register(system, "invalid")
    before = manager.core.engine.status()["accepted"]
    response = http.post(
        "/insert_timeseries",
        json=[
            dict(
                source_id="invalid",
                ref_name="temperature",
                request_id="invalid",
                values=[[1, "text"]],
            )
        ],
    )
    assert response.status_code == 400
    assert manager.core.engine.status()["accepted"] == before
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
