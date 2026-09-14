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
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import LineageCopy, NOW
from tests.unit.test_materialization_regressions import FleetGraph


@pytest.fixture
def runtime(tmp_path):
    store = DuckDBStore(tmp_path / "inspect.duckdb")
    materializer = Materializer(store, FleetGraph(["urn:input"]))
    yield materializer
    materializer.close()
    store.close()


def test_inspection_before_planning_and_unknown_app(runtime, monkeypatch):
    assert runtime.list_apps() == []
    definition = Deployment.from_class(LineageCopy)
    runtime.deploy(definition)
    def forbid_refresh():
        pytest.fail("inspection must not refresh the plan")
    monkeypatch.setattr(runtime, "refresh", forbid_refresh)
    item = runtime.inspect_app(definition.name)
    assert item["status"] == "planning"
    assert not item["plan_current"]
    assert item["definition"] == json.loads(definition.to_json())
    assert item["bindings"] == []
    assert item["output_schemas"]["out"]["value"] == {"type": "float64", "nullable": False}
    with pytest.raises(KeyError):
        runtime.inspect_app("absent")


def test_no_matches_and_planning_failure_are_visible(runtime, monkeypatch):
    definition = Deployment.from_class(LineageCopy)
    runtime._graph.refs = []
    runtime.deploy(definition)
    runtime.refresh()
    assert runtime.list_apps()[0]["status"] == "no_matches"
    assert runtime.list_apps()[0]["binding_count"] == 0

    def fail(*args, **kwargs):
        raise ValueError("query cannot be planned")
    monkeypatch.setattr(runtime._planner, "compile", fail)
    runtime._graph_revision = -1
    runtime.refresh()
    item = runtime.inspect_app(definition.name)
    assert item["status"] == "failed"
    assert "query cannot be planned" in item["error"]
    assert item["definition"]["outputs"]


def test_inspection_reports_progress_and_execution_failures_without_writes(runtime):
    definition = Deployment.from_class(LineageCopy)
    runtime.deploy(definition)
    runtime.run_once()
    runtime._store.upsert_rows("urn:input", [(NOW, 5.)], value_kind="numeric")
    runtime.run_once()
    revision = runtime._revisions.current_revision()
    progress = runtime._revisions.progress_snapshot()
    item = runtime.inspect_app(definition.name)
    binding = item["bindings"][0]
    assert binding["inputs"] == {"input": ["urn:input"]}
    assert binding["outputs"]["out"]
    assert binding["last_success"]
    assert binding["consumed_revision"] is not None
    assert runtime._revisions.current_revision() == revision
    assert runtime._revisions.progress_snapshot() == progress
    runtime._scheduler.errors[binding["binding_signature"]] = "bad reading"
    item = runtime.inspect_app(definition.name)
    assert item["status"] == "failed"
    assert item["binding_statuses"] == {"failed": 1}
    assert item["bindings"][0]["error"] == "bad reading"


def test_text_schema_and_sorted_deployment_list(runtime):
    definition = Deployment.from_class(LineageCopy)
    runtime.deploy(replace(definition, name="zeta", outputs={"message": output.stream(value_kind="text")}))
    runtime.deploy(replace(definition, name="alpha"))
    assert [item["name"] for item in runtime.list_apps()] == ["alpha", "zeta"]
    assert runtime.inspect_app("zeta")["output_schemas"]["message"]["value"]["type"] == "string"


def test_http_inspection(runtime, monkeypatch):
    monkeypatch.setattr(server.state, "manager", SimpleNamespace(materializer=runtime), raising=False)
    routes = {(route.path, frozenset(route.methods or ())) for route in server.routes}
    assert ("/apps", frozenset({"GET"})) in routes
    assert ("/apps/{name}", frozenset({"GET"})) in routes
    assert list_apps_endpoint() == {"ok": True, "apps": []}
    with pytest.raises(HTTPException, match="404"):
        inspect_app_endpoint("missing")
    definition = Deployment.from_class(LineageCopy)
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
    definition = Deployment.from_class(LineageCopy)
    runtime.deploy(definition)
    runtime.refresh()
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
    for text in ("Declared outputs:", "float64", "timestamp[us, tz=UTC]", "urn:input", "lookback:", "revision:"):
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


def test_restart_lists_persisted_apps_before_planning(runtime):
    definition = Deployment.from_class(LineageCopy)
    runtime.deploy(definition)
    runtime.refresh()
    restarted = Materializer(runtime._store, runtime._graph)
    try:
        item = restarted.inspect_app(definition.name)
        assert item["status"] == "planning"
        assert item["definition"]["outputs"]
    finally:
        restarted.close()
