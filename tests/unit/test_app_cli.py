from __future__ import annotations

import json
from types import SimpleNamespace

from typer.testing import CliRunner

from acquirium import App, Output
from acquirium.cli import app


class CliApp(App):
    name = "cli-app"

    def build_query(self, aq):
        return {}

    def run(self, ctx) -> list[Output]:
        return []


class PreviewResult:
    def to_dict(self):
        return {"app_id": "cli-app", "effects": []}


class FakeAcquirium:
    instance = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.preview_calls = []
        self.register_calls = []
        self.run_calls = []
        self.list_calls = []
        self.delete_calls = []
        self.debug_calls = []
        self.mapping_calls = []
        type(self).instance = self

    def preview_app(self, app, **kwargs):
        self.preview_calls.append((app, kwargs))
        return PreviewResult()

    def register_app(self, app, **kwargs):
        self.register_calls.append((app, kwargs))
        return {"ok": True, "name": app.name}

    def run_app(self, app_id, **kwargs):
        self.run_calls.append((app_id, kwargs))
        return {"ok": True, "run_id": "cli-app-1"}

    def list_app_runs(self, **kwargs):
        self.list_calls.append(kwargs)
        if kwargs.get("app_id"):
            return {
                "ok": True,
                "name": kwargs["app_id"],
                "mappings": [{
                    "input_point_uri": "urn:test:registered-input",
                    "input_ref_uri": None,
                    "output_point_uri": "urn:test:registered-output",
                    "output_ref_name": "average/registered",
                }],
            }
        return {"ok": True, "apps": [{"name": "cli-app"}]}

    def delete_app(self, app_id):
        self.delete_calls.append(app_id)
        return {"ok": True, "name": app_id}

    def prepare_app_debug(self, app, **kwargs):
        self.debug_calls.append((app, kwargs))
        return SimpleNamespace(
            app=app,
            streams=["first", "second"],
            namespace=lambda: {"stream": "first", "transform": object()},
        )

    def app_mappings(self, app):
        self.mapping_calls.append(app)
        return [SimpleNamespace(to_dict=lambda: {
            "input_point_uri": "urn:test:input",
            "input_ref_uri": "urn:test:input-ref",
            "output_point_uri": "urn:test:output",
            "output_ref_name": "average/123",
        })]


def install_fakes(monkeypatch):
    monkeypatch.setattr("acquirium.cli._import_app_class", lambda spec, base_dir=None: CliApp)
    monkeypatch.setattr("acquirium.Client.acquirium.Acquirium", FakeAcquirium)


def test_app_run_dry_run_only_previews(monkeypatch):
    install_fakes(monkeypatch)
    result = CliRunner().invoke(app, [
        "app", "run", "fake.py:CliApp",
        "--dry-run",
        "--params", '{"window": 5}',
    ])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["app_id"] == "cli-app"
    aq = FakeAcquirium.instance
    assert aq.preview_calls[0][1]["params"] == {"window": 5}
    assert aq.register_calls == []
    assert aq.run_calls == []


def test_app_run_registers_and_runs_without_dry_run(monkeypatch):
    install_fakes(monkeypatch)
    result = CliRunner().invoke(app, [
        "app", "run", "fake.py:CliApp",
        "--replace",
        "--keep-alive",
        "--interval", "30",
        "--build-params", '{"training": 10}',
        "--params", '{"window": 5}',
    ])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["registration"]["name"] == "cli-app"
    aq = FakeAcquirium.instance
    assert aq.register_calls[0][1] == {
        "params": {"training": 10},
        "replace": True,
    }
    assert aq.run_calls == [(
        "cli-app",
        {
            "start": None,
            "end": None,
            "params": {"window": 5},
            "keep_alive": True,
            "interval": 30.0,
        },
    )]


def test_app_list_and_deregister(monkeypatch):
    install_fakes(monkeypatch)
    listed = CliRunner().invoke(app, ["app", "list", "--name", "cli-app"])

    assert listed.exit_code == 0, listed.output
    assert "cli-app" in listed.stdout
    assert FakeAcquirium.instance.list_calls == [{"app_id": "cli-app"}]

    removed = CliRunner().invoke(app, ["app", "deregister", "cli-app"])

    assert removed.exit_code == 0, removed.output
    assert "Deregistered app 'cli-app'" in removed.stdout
    assert FakeAcquirium.instance.delete_calls == ["cli-app"]


def test_app_mappings_supports_json_output(monkeypatch):
    install_fakes(monkeypatch)
    result = CliRunner().invoke(app, [
        "app", "mappings", "fake.py:CliApp", "--json",
    ])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload == [{
        "input_point_uri": "urn:test:input",
        "input_ref_uri": "urn:test:input-ref",
        "output_point_uri": "urn:test:output",
        "output_ref_name": "average/123",
    }]
    assert FakeAcquirium.instance.mapping_calls[0].name == "cli-app"


def test_app_mappings_accepts_registered_app_name(monkeypatch):
    install_fakes(monkeypatch)
    result = CliRunner().invoke(app, [
        "app", "mappings", "running_average_example", "--json",
    ])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload[0]["input_point_uri"] == "urn:test:registered-input"
    assert FakeAcquirium.instance.list_calls == [{
        "app_id": "running_average_example"
    }]
    assert FakeAcquirium.instance.mapping_calls == []


def test_app_debug_opens_repl_with_prepared_namespace(monkeypatch):
    install_fakes(monkeypatch)
    interact_calls = []
    monkeypatch.setattr(
        "code.interact",
        lambda **kwargs: interact_calls.append(kwargs),
    )

    result = CliRunner().invoke(app, [
        "app", "debug", "fake.py:CliApp",
        "--build-params", '{"training": 10}',
        "--params", '{"window": 5}',
    ])

    assert result.exit_code == 0, result.output
    aq = FakeAcquirium.instance
    assert aq.debug_calls[0][1]["build_params"] == {"training": 10}
    assert aq.debug_calls[0][1]["params"] == {"window": 5}
    assert interact_calls[0]["local"]["stream"] == "first"
    assert "2 mapped stream(s)" in interact_calls[0]["banner"]
