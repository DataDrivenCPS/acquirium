import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from acquirium.cli import _load_app_target, app

APP_SOURCE = """
import acquirium as aq


class Doubler(aq.App):
    name = "doubler"
    outputs = {"doubled": aq.output.per_input(value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="input")

    def transform(self, inputs, output, context):
        pass
"""


class _FakeResponse:
    def __init__(self, payload):
        self.ok, self._payload, self.text = True, payload, "{}"

    def json(self):
        return self._payload


@pytest.fixture
def app_file(tmp_path: Path) -> Path:
    path = tmp_path / "doubler_app.py"
    path.write_text(APP_SOURCE)
    return path


def test_load_app_target_reads_a_file_path(app_file: Path):
    target, source_dir = _load_app_target(f"{app_file}:Doubler")

    assert target.__name__ == "Doubler"
    # The directory travels with the class so the server can be told where it is.
    assert source_dir == str(app_file.parent)
    with pytest.raises(ValueError, match="was not found"):
        _load_app_target(f"{app_file}:Missing")
    with pytest.raises(ValueError, match="module_or_file:ClassName"):
        _load_app_target("doubler_app")


def _run(monkeypatch, app_file: Path, bindings, capture=None, extra=("--limit", "2")):
    import requests

    def fake_post(url, json=None, params=None, timeout=None):
        if capture is not None:
            capture.update({"url": url, "definition": json, "params": params})
        return _FakeResponse({"ok": True, "app": "doubler", "graph_revision": 3, "bindings": bindings})

    monkeypatch.setattr(requests, "post", fake_post)
    return CliRunner().invoke(app, ["app", "check", f"{app_file}:Doubler", *extra])


def test_check_prints_matched_inputs_and_computed_rows(monkeypatch, app_file: Path):
    capture: dict = {}
    result = _run(monkeypatch, app_file, [{
        "inputs": {"input": [{"ref_uri": "urn:input", "label": "Inlet temp", "unit": None}]},
        "entities": {"hx": "urn:hx-1"},
        "input_rows": {"input": 2},
        "outputs": {"doubled": {"stream": "urn:derived", "ref_name": "doubled:abc",
                                "value_kind": "numeric", "rows": 2, "truncated": True,
                                "values": [{"time": "2026-01-01T00:00:00+00:00", "value": 4.0}]}},
        "error": None,
    }], capture)

    assert result.exit_code == 0
    assert "doubler: 1 input group(s) matched" in result.stdout
    assert "Inlet temp" in result.stdout and "urn:hx-1" in result.stdout
    assert "'doubled' -> doubled:abc (numeric, 2 rows)" in result.stdout
    assert "2026-01-01T00:00:00+00:00  4.0" in result.stdout
    assert "… 1 more row(s); pass -n 0 for all of them" in result.stdout
    # The class is sent as a definition; the check never deploys it.
    assert capture["url"].endswith("/apps/check")
    assert capture["definition"]["name"] == "doubler"
    assert capture["params"] == {"limit": 2, "search_path": str(app_file.parent)}


def test_check_defaults_to_five_rows_and_n_zero_asks_for_all(monkeypatch, app_file: Path):
    binding = [{"inputs": {"input": []}, "entities": {}, "outputs": {}, "error": None}]

    capture: dict = {}
    _run(monkeypatch, app_file, binding, capture, extra=())
    assert capture["params"]["limit"] == 5

    capture.clear()
    _run(monkeypatch, app_file, binding, capture, extra=("-n", "0"))
    assert "limit" not in capture["params"]


def test_a_module_spec_sends_no_search_path(monkeypatch, app_file: Path):
    import requests

    capture: dict = {}

    def fake_post(url, json=None, params=None, timeout=None):
        capture.update({"params": params})
        return _FakeResponse({"ok": True, "app": "doubler", "graph_revision": 1, "bindings": []})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.syspath_prepend(str(app_file.parent))
    result = CliRunner().invoke(app, ["app", "check", "doubler_app:Doubler"])

    # An importable module needs no hint about where it lives.
    assert result.exit_code == 0
    assert "search_path" not in capture["params"]


def test_check_exits_nonzero_when_a_binding_failed(monkeypatch, app_file: Path):
    result = _run(monkeypatch, app_file, [{
        "inputs": {"input": [{"ref_uri": "urn:input", "label": None, "unit": None}]},
        "entities": {}, "outputs": {}, "error": "ValueError: calibration missing",
    }])

    assert result.exit_code == 1
    assert "calibration missing" in result.output


def test_check_rejects_malformed_parameters(app_file: Path):
    result = CliRunner().invoke(app, ["app", "check", f"{app_file}:Doubler", "--params", "{oops"])

    assert result.exit_code == 1
