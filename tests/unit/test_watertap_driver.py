from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from acquirium.Drivers.BuiltInDrivers.watertap import (
    WaterTAPDriver,
    get_observation_from_model,
    get_value_from_model,
)
from acquirium.internals.internals_namespaces import HAS_PYOMO_VAR


def _mapping_json() -> str:
    return json.dumps({
        "namespace": "urn:point:",
        "properties": {
            "urn:point:value_a": "fs.unit.value_a",
            "urn:point:value_b": "fs.unit.value_b",
        },
    })


MODULE_TEXT = """
class DummyComponent:
    def __init__(self, value):
        self.value = value


class DummyModel:
    def __init__(self, values):
        self._values = values

    def find_component(self, name):
        value = self._values.get(name)
        if value is None:
            return None
        return DummyComponent(value)


def build_model(flow=1.0):
    return DummyModel({
        "fs.unit.value_a": flow,
        "fs.unit.value_b": flow * 2,
    }), {"status": "ok"}


def change_inputs(model, inputs):
    flow = inputs.get("flow")
    if flow is not None:
        model._values["fs.unit.value_a"] = flow
        model._values["fs.unit.value_b"] = flow * 2


def solve(model):
    return None


class ResultWrapper:
    def __init__(self, model):
        self.model = model


def build_wrapped_model():
    return ResultWrapper(DummyModel({"fs.unit.value_a": 3.0, "fs.unit.value_b": 6.0}))


def build_status_model():
    return DummyModel({"fs.unit.value_a": "Manual Control", "fs.unit.value_b": 6.0})
"""


def _write_mapping(tmp_path: Path) -> Path:
    path = tmp_path / "watertap-mapping.json"
    path.write_text(_mapping_json())
    return path


def _write_module(tmp_path: Path) -> Path:
    path = tmp_path / "dummy_watertap.py"
    path.write_text(MODULE_TEXT)
    return path


def _make_driver(tmp_path: Path, **driver_cfg) -> WaterTAPDriver:
    aq = MagicMock()
    aq.register_datasource.return_value = "watertap"
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 2}
    module_path = _write_module(tmp_path)
    config = {
        "driver": {
            "watertap_mapping_path": str(_write_mapping(tmp_path)),
            "watertap_build_spec": f"{module_path}:build_model",
            "watertap_solve_spec": f"{module_path}:solve",
            **driver_cfg,
        }
    }
    return WaterTAPDriver(aq, config)


def test_setup_registers_datasource_and_streams_from_mapping(tmp_path):
    driver = _make_driver(tmp_path)

    driver.setup()

    driver.aq.register_datasource.assert_called_once_with("watertap")
    # Streams are registered in one batch, straight from the mapping.
    driver.aq.register_streams.assert_called_once()
    specs = driver.aq.register_streams.call_args.args[0]
    by_name = {s["ref_name"]: s for s in specs}
    assert set(by_name) == {"value_a", "value_b"}
    # Each spec links the ontology point and records its Pyomo var so
    # registration writes hasExternalReference + hasPyomoVar.
    assert by_name["value_a"]["point_uri"] == "urn:point:value_a"
    assert by_name["value_a"]["value_kind"] == "numeric"
    assert by_name["value_a"]["properties"][HAS_PYOMO_VAR] == "fs.unit.value_a"


def test_setup_can_insert_model_graph(tmp_path):
    model_ttl = tmp_path / "model.ttl"
    model_ttl.write_text("@prefix ex: <urn:ex#> .\nex:Pump a ex:Thing .\n")
    driver = _make_driver(
        tmp_path,
        watertap_graph_path=str(model_ttl),
        watertap_insert_graph=True,
        watertap_insert_graph_replace=True,
    )

    driver.setup()

    # The path is handed over as-is; the client reads it and infers the format
    # from the suffix, so the driver never has to name a serialisation.
    driver.aq.insert_graph.assert_called_once_with(
        model_ttl.resolve(),
        format=None,
        replace=True,
        source_id="watertap",
    )


def test_loop_builds_model_and_ingests_batch(tmp_path):
    driver = _make_driver(tmp_path, watertap_build_kwargs={"flow": 2.5})
    driver.setup()

    driver.tick()

    source_id, df = driver.aq.insert_timeseries_arrow.call_args.args
    assert source_id == "watertap"
    values = dict(zip(df["ref_name"].to_pylist(), df["value"].to_pylist()))
    assert values["value_a"] == "2.5"
    assert values["value_b"] == "5.0"


def test_loop_can_extract_model_via_result_attr(tmp_path):
    module_path = _write_module(tmp_path)
    driver = _make_driver(
        tmp_path,
        watertap_build_spec=f"{module_path}:build_wrapped_model",
        watertap_result_attr="model",
    )
    driver.setup()

    driver.tick()

    _, df = driver.aq.insert_timeseries_arrow.call_args.args
    values = dict(zip(df["ref_name"].to_pylist(), df["value"].to_pylist()))
    assert values["value_a"] == "3.0"
    assert values["value_b"] == "6.0"


def test_loop_preserves_nonnumeric_component_values_for_numeric_streams(tmp_path):
    module_path = _write_module(tmp_path)
    driver = _make_driver(
        tmp_path,
        watertap_build_spec=f"{module_path}:build_status_model",
    )
    driver.setup()

    driver.tick()

    _, df = driver.aq.insert_timeseries_arrow.call_args.args
    values = dict(zip(df["ref_name"].to_pylist(), df["value"].to_pylist()))
    assert values["value_a"] == "Manual Control"
    assert values["value_b"] == "6.0"


def test_loop_applies_change_inputs_then_solves(tmp_path):
    module_path = _write_module(tmp_path)
    driver = _make_driver(
        tmp_path,
        watertap_change_inputs_spec=f"{module_path}:change_inputs",
        watertap_inputs={"flow": 4.0},
    )
    driver.setup()

    driver.tick()

    _, df = driver.aq.insert_timeseries_arrow.call_args.args
    values = dict(zip(df["ref_name"].to_pylist(), df["value"].to_pylist()))
    # change_inputs overrode the build defaults before the read
    assert values["value_a"] == "4.0"
    assert values["value_b"] == "8.0"


def test_missing_mapping_path_raises(tmp_path):
    aq = MagicMock()
    driver = WaterTAPDriver(aq, {"driver": {
        "watertap_build_spec": f"{_write_module(tmp_path)}:build_model",
    }})

    with pytest.raises(ValueError, match="watertap_mapping_path"):
        driver.setup()


def test_missing_build_spec_raises(tmp_path):
    aq = MagicMock()
    driver = WaterTAPDriver(aq, {"driver": {
        "watertap_mapping_path": str(_write_mapping(tmp_path)),
        "watertap_solve_spec": f"{_write_module(tmp_path)}:solve",
    }})

    with pytest.raises(ValueError, match="watertap_build_spec"):
        driver.setup()


def test_missing_solve_spec_raises(tmp_path):
    aq = MagicMock()
    driver = WaterTAPDriver(aq, {"driver": {
        "watertap_mapping_path": str(_write_mapping(tmp_path)),
        "watertap_build_spec": f"{_write_module(tmp_path)}:build_model",
    }})

    with pytest.raises(ValueError, match="watertap_solve_spec"):
        driver.setup()


def test_get_value_from_model_falls_back_to_component_value():
    class Model:
        def find_component(self, name):
            assert name == "x"
            return type("Component", (), {"value": 4.5})()

    assert get_value_from_model(Model(), "x") == 4.5


def test_get_observation_from_model_preserves_nonnumeric_component_value():
    class Model:
        def find_component(self, name):
            assert name == "x"
            return type("Component", (), {"value": "Manual Control"})()

    assert get_observation_from_model(Model(), "x") == (True, "Manual Control")
    assert get_value_from_model(Model(), "x") is None
