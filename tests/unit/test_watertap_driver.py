from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from acquirium.BuiltinDrivers.watertap import (
    WaterTAPDriver,
    get_observation_from_model,
    get_value_from_model,
)
from acquirium.internals.models import compute_ref_uri


def _graph_text() -> str:
    ref_one = compute_ref_uri("watertap", "value_a")
    ref_two = compute_ref_uri("watertap", "value_b")
    return f"""@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix acq: <urn:acquirium#> .

<urn:point:one> ref:hasExternalReference <{ref_one}> ;
    acq:hasPyomoVar "fs.unit.value_a" .

<{ref_one}> acq:sourceId "watertap" ;
    acq:refName "value_a" .

<urn:point:two> ref:hasExternalReference <{ref_two}> ;
    acq:hasPyomoVar "fs.unit.value_b" .

<{ref_two}> acq:sourceId "watertap" ;
    acq:refName "value_b" .
"""


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


class ResultWrapper:
    def __init__(self, model):
        self.model = model


def build_wrapped_model():
    return ResultWrapper(DummyModel({"fs.unit.value_a": 3.0, "fs.unit.value_b": 6.0}))


def build_status_model():
    return DummyModel({"fs.unit.value_a": "Manual Control", "fs.unit.value_b": 6.0})
"""


def _write_graph(tmp_path: Path) -> Path:
    path = tmp_path / "watertap.ttl"
    path.write_text(_graph_text())
    return path


def _write_module(tmp_path: Path) -> Path:
    path = tmp_path / "dummy_watertap.py"
    path.write_text(MODULE_TEXT)
    return path


def _make_driver(tmp_path: Path, **driver_cfg) -> WaterTAPDriver:
    aq = MagicMock()
    aq.register_datasource.return_value = "watertap"
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 2}
    config = {
        "driver": {
            "watertap_graph_path": str(_write_graph(tmp_path)),
            "watertap_build_spec": f"{_write_module(tmp_path)}:build_model",
            **driver_cfg,
        }
    }
    return WaterTAPDriver(aq, config)


def test_setup_registers_datasource_and_streams(tmp_path):
    driver = _make_driver(tmp_path)

    driver.setup()

    driver.aq.register_datasource.assert_called_once_with("watertap")
    assert driver.aq.register_stream.call_count == 2
    first_call = driver.aq.register_stream.call_args_list[0]
    assert first_call.args == ()
    assert first_call.kwargs == {
        "source_id": "watertap",
        "ref_name": "value_a",
        "value_kind": "numeric",
    }


def test_setup_can_insert_graph(tmp_path):
    graph_path = _write_graph(tmp_path)
    graph_text = graph_path.read_text()
    driver = _make_driver(
        tmp_path,
        watertap_graph_path=str(graph_path),
        watertap_insert_graph=True,
        watertap_insert_graph_replace=True,
    )

    driver.setup()

    driver.aq.insert_graph.assert_called_once_with(
        graph_text,
        format="turtle",
        replace=True,
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


def test_missing_required_config_raises(tmp_path):
    aq = MagicMock()
    driver = WaterTAPDriver(aq, {"driver": {"watertap_graph_path": str(_write_graph(tmp_path))}})

    with pytest.raises(ValueError, match="watertap_build_spec"):
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
