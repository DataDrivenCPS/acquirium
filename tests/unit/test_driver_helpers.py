from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from acquirium.Drivers.Driver import Driver
from acquirium.cli import _driver_source_dir, _import_driver_class
from acquirium.Drivers.BuiltInDrivers.csv_ingest import CSVIngestDriver
from acquirium.Drivers.runner import DriverRunner
from acquirium.internals.models import compute_ref_uri

DRIVER_SRC = (
    "from acquirium.Drivers.Driver import Driver\n"
    "class TempDriver(Driver):\n"
    "    def setup(self):\n"
    "        self.source_id = 'tmp'\n"
    "    def tick(self):\n"
    "        return None\n"
)


class DummyDriver(Driver):
    def setup(self) -> None:
        self.source_id = "demo-source"

    def tick(self) -> None:
        return None


def test_reference_uri_uses_driver_source_id():
    driver = DummyDriver(aq=object(), config={})
    driver.setup()
    assert driver.source_id == "demo-source"
    assert driver.reference_uri("cpu_percent") == compute_ref_uri("demo-source", "cpu_percent")


def test_source_id_attribute_exists_only_when_driver_sets_default():
    driver = DummyDriver(aq=object(), config={})
    with pytest.raises(AttributeError):
        driver.source_id


def test_graph_helpers_always_use_the_driver_source_id():
    aq = MagicMock()
    driver = DummyDriver(aq=aq, config={})
    driver.setup()

    driver.insert_graph("@prefix ex: <urn:ex:> . ex:x ex:p ex:y .")
    driver.sparql_update("DELETE WHERE { ?s ?p ?o }")

    assert aq.insert_graph.call_args.kwargs["source_id"] == "demo-source"
    assert aq.sparql_update.call_args.kwargs["source_id"] == "demo-source"


def test_config_relative_driver_spec_import(tmp_path: Path):
    driver_file = tmp_path / "custom_driver.py"
    driver_file.write_text(
        "from acquirium.Drivers.Driver import Driver\n"
        "class TempDriver(Driver):\n"
        "    def setup(self):\n"
        "        self.source_id = 'tmp'\n"
        "    def tick(self):\n"
        "        return None\n"
    )
    cls, source_dir = _import_driver_class("./custom_driver.py:TempDriver", base_dir=tmp_path)
    assert cls.__name__ == "TempDriver"
    # The driver's directory goes to the caller so it can reach a Ray worker's
    # PYTHONPATH; without it the file's sibling imports die on deserialization.
    assert source_dir == str(tmp_path.resolve())


def test_module_driver_spec_reports_no_source_dir():
    cls, source_dir = _import_driver_class(
        "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
    )
    assert cls is CSVIngestDriver
    assert source_dir is None


def test_source_dir_resolves_without_importing(tmp_path: Path):
    # A file whose import would explode: _driver_source_dir must not care.
    bad = tmp_path / "explosive.py"
    bad.write_text("raise RuntimeError('must never be imported server-side')\n")
    assert _driver_source_dir("./explosive.py:X", base_dir=tmp_path) == str(tmp_path.resolve())
    # Module specs resolve to None — also without importing the module.
    assert _driver_source_dir("no.such.module:Cls") is None
    with pytest.raises(ValueError, match="not found"):
        _driver_source_dir("./missing.py:X", base_dir=tmp_path)


def test_file_specs_get_distinct_module_names(tmp_path: Path):
    # Two different driver files must not clobber each other's module (the
    # old fixed "_acquirium_driver_module" name did exactly that).
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    (tmp_path / "a" / "drv.py").write_text(DRIVER_SRC)
    (tmp_path / "b" / "drv.py").write_text(DRIVER_SRC.replace("'tmp'", "'other'"))

    cls_a, _ = _import_driver_class("./a/drv.py:TempDriver", base_dir=tmp_path)
    cls_b, _ = _import_driver_class("./b/drv.py:TempDriver", base_dir=tmp_path)
    assert cls_a is not cls_b
    assert cls_a.__module__ != cls_b.__module__


def test_runner_imports_spec_string_in_the_actor(tmp_path: Path):
    # The undecorated actor class stands in for the actor process: handed a
    # spec string, the constructor performs the import itself.
    (tmp_path / "drv.py").write_text(DRIVER_SRC)
    aq = MagicMock()
    runner = DriverRunner.__ray_actor_class__(
        "./drv.py:TempDriver",
        {"__config_dir": str(tmp_path)},
        aq,
        1.0,
        str(tmp_path),
    )
    assert type(runner.driver).__name__ == "TempDriver"


def test_csv_watch_dir_resolves_relative_to_config_dir(tmp_path: Path):
    config_dir = tmp_path / "cfg"
    aq = object()
    driver = CSVIngestDriver(
        aq,
        {
            "__config_dir": str(config_dir),
            "driver": {
                "watch_dir": "./raw", "glob": "*.csv",
                "source_id": "csv", "format": "wide",
            },
        },
    )
    driver.setup()
    assert driver.watch_dir == (config_dir / "raw").resolve()
    # A watch dir that does not exist yet is not an error; it simply yields no
    # files until something creates it.
    assert list(driver.watch_dir.rglob("*.csv")) == []
