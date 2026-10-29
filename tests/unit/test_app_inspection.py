"""Deployment inspection must remain useful without executing application code."""
from dataclasses import replace
from types import SimpleNamespace
import json

import pytest
import requests
from fastapi import HTTPException
from typer.testing import CliRunner

from acquirium.cli import app as cli
from acquirium.Materialization.models import output
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Server.app import app as server, inspect_app as inspect_app_endpoint, list_apps as list_apps_endpoint
from tests.unit.test_time_aware_engine import Double, NEG_INF, at, point, world  # noqa: F401  (the world fixture)

import pyarrow as pa
from acquirium.Materialization.models import App


class Failing(Double):
    name = "failing"
    def transform(self, inputs, output, context):
        raise ValueError("bad reading")


@pytest.fixture
def runtime(world):
    world.model(point(1))
    return world.views


def test_inspection_describes_deployments_and_unknown_apps(runtime):
    assert runtime.list_apps() == []
    definition = Deployment.from_class(Double)
    runtime.deploy(definition, start=NEG_INF)
    item = runtime.inspect_app(definition.name)
    assert item["status"] == "idle" and item["plan_current"]
    assert item["definition"] == json.loads(definition.to_json())
    assert item["deployed"] == [["-infinity", "infinity"]]
    (binding,) = item["bindings"]
    assert binding["inputs"] == {"t": ["urn:ex/r1"]}
    assert binding["present"] == [["-infinity", "infinity"]]
    assert item["output_schemas"]["out"]["value"] == {"type": "float64", "nullable": False}
    with pytest.raises(KeyError):
        runtime.inspect_app("absent")


def test_no_matches_and_planning_failure_are_visible(runtime, monkeypatch):
    runtime._graph.sparql_update(f"DELETE DATA {{ {point(1)} }}", graph_uri=__import__("rdflib").URIRef(
        "urn:acquirium#MainGraph"), effective_from="-infinity", effective_until="infinity")
    runtime.model_changed()
    definition = Deployment.from_class(Double)
    runtime.deploy(definition, start=NEG_INF)
    assert runtime.list_apps()[0]["status"] == "no_matches"
    assert runtime.list_apps()[0]["binding_count"] == 0

    def fail(*args, **kwargs):
        raise ValueError("query cannot be planned")
    monkeypatch.setattr(runtime._planner, "temporal_plan", fail)
    runtime._plan = None
    item = runtime.inspect_app(definition.name)
    assert item["status"] == "failed"
    assert "query cannot be planned" in item["error"]
    assert item["definition"]["outputs"]


def test_execution_failures_are_reported_and_retried(world, runtime, monkeypatch):
    world.write("urn:ex/r1", [(at(0), 1.0)])
    runtime.deploy(Deployment.from_class(Failing), start=NEG_INF)
    runtime.step()
    item = runtime.inspect_app("failing")
    assert item["status"] == "failed"
    assert item["binding_statuses"] == {"failed": 1}
    assert "bad reading" in item["bindings"][0]["error"]
    # the failed window stays queued; once the code works it is computed
    monkeypatch.setattr(Failing, "transform", Double.transform)
    runtime.step()
    assert runtime.inspect_app("failing")["status"] == "idle"
    (values,) = world.outputs("failing").values()
    assert values == [(at(0), 2.0)]


def test_text_schema_and_sorted_deployment_list(runtime):
    definition = Deployment.from_class(Double)
    runtime.deploy(replace(definition, name="zeta", outputs={"message": output.stream(value_kind="text")}))
    runtime.deploy(replace(definition, name="alpha"))
    assert [item["name"] for item in runtime.list_apps()] == ["alpha", "zeta"]
    assert runtime.inspect_app("zeta")["output_schemas"]["message"]["value"]["type"] == "string"


def test_http_inspection(runtime, monkeypatch):
    monkeypatch.setattr(server.state, "manager", SimpleNamespace(materializer=runtime), raising=False)
    routes = {(route.path, frozenset(route.methods or ())) for route in server.routes}
    assert ("/apps", frozenset({"GET"})) in routes
    assert ("/apps/{name}", frozenset({"GET"})) in routes
    assert ("/materialization/step", frozenset({"POST"})) in routes
    assert list_apps_endpoint() == {"ok": True, "apps": []}
    with pytest.raises(HTTPException, match="404"):
        inspect_app_endpoint("missing")
    definition = Deployment.from_class(Double)
    runtime.deploy(definition)
    response = inspect_app_endpoint(definition.name)
    assert response["app"]["definition"]["name"] == definition.name


def mock_get(monkeypatch, payload, status=200):
    calls = []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        response = requests.Response()
        response.status_code = status
        response._content = json.dumps(payload).encode()
        response.url = url
        return response
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(requests.Session, "get", lambda self, url, **kwargs: get(url, **kwargs))
    return calls


def test_cli_list_and_inspect(runtime, monkeypatch):
    definition = Deployment.from_class(Double)
    runtime.deploy(definition, start=NEG_INF)
    runner = CliRunner()
    calls = mock_get(monkeypatch, {"ok": True, "apps": runtime.list_apps()})
    result = runner.invoke(cli, ["app", "list", "--server-port", "8123"])
    assert result.exit_code == 0, result.output
    assert definition.name in result.output
    assert "bindings=1" in result.output
    assert calls[0][0].endswith(":8123/apps")
    assert calls[0][1] == {"timeout": 30}
    payload = {"ok": True, "app": runtime.inspect_app(definition.name)}
    mock_get(monkeypatch, payload)
    result = runner.invoke(cli, ["app", "inspect", definition.name])
    assert result.exit_code == 0, result.output
    for text in ("Declared outputs:", "float64", "timestamp[us, tz=UTC]", "urn:ex/r1", "lookback:",
                 "deployed: -infinity to infinity", "present:"):
        assert text in result.output
    result = runner.invoke(cli, ["app", "inspect", definition.name, "--json"])
    assert result.exit_code == 0
    assert json.loads(result.output) == payload


def test_cli_empty_list_json_and_errors(monkeypatch):
    runner = CliRunner()
    payload = {"ok": True, "apps": []}
    mock_get(monkeypatch, payload)
    assert "No apps deployed" in runner.invoke(cli, ["app", "list"]).output
    assert json.loads(runner.invoke(cli, ["app", "list", "--json"]).output) == payload
    mock_get(monkeypatch, {}, status=404)
    result = runner.invoke(cli, ["app", "inspect", "absent"])
    assert result.exit_code == 1
    assert "Unknown app 'absent'" in result.output
    mock_get(monkeypatch, {}, status=500)
    assert runner.invoke(cli, ["app", "list"]).exit_code == 1
    def fail(*args, **kwargs):
        raise requests.ConnectionError("connection refused")
    monkeypatch.setattr(requests, "get", fail)
    result = runner.invoke(cli, ["app", "list"])
    assert result.exit_code == 1
    assert "connection refused" in result.output


def test_python_client_inspection(monkeypatch):
    from acquirium.Client.acquirium import Acquirium
    client = Acquirium(server_url="localhost", server_port=8123, health_timeout=0)
    payload = {"ok": True, "apps": []}
    calls = mock_get(monkeypatch, payload)
    assert client.list_apps() == payload
    assert calls[-1] == ("http://localhost:8123/apps", {"timeout": 30})
    payload = {"ok": True, "app": {"name": "test app"}}
    calls = mock_get(monkeypatch, payload)
    assert client.inspect_app("test app") == payload
    assert calls[-1][0] == "http://localhost:8123/apps/test%20app"


def test_a_restart_lists_and_plans_persisted_apps(runtime):
    definition = Deployment.from_class(Double)
    runtime.deploy(definition, start=NEG_INF)
    restarted = Materializer(runtime._store, runtime._graph)
    item = restarted.inspect_app(definition.name)
    assert item["status"] == "idle"
    assert item["binding_count"] == 1
