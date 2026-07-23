from __future__ import annotations

from pathlib import Path

import pytest

from acquirium.Drivers.Driver import Driver
from acquirium.cli import _import_driver_class
from acquirium.Drivers.BuiltInDrivers.csv_ingest import CSVIngestDriver
from acquirium.internals.models import compute_ref_uri


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


def test_csv_watch_dir_resolves_relative_to_config_dir(tmp_path: Path):
    config_dir = tmp_path / "cfg"
    aq = object()
    driver = CSVIngestDriver(
        aq,
        {
            "__config_dir": str(config_dir),
            "driver": {"watch_dir": "./raw"},
        },
    )
    driver.setup()
    assert driver._watch_dir == (config_dir / "raw").resolve()
