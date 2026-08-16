from __future__ import annotations

from pathlib import Path
import inspect
from unittest.mock import MagicMock

import pytest

from acquirium.Drivers.Driver import Driver
from acquirium.Client.acquirium import Acquirium
from acquirium.cli import _import_app_class, _import_driver_class
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


def test_config_relative_app_spec_import(tmp_path: Path):
    app_file = tmp_path / "custom_app.py"
    app_file.write_text(
        "from acquirium import App\n"
        "class TempApp(App):\n"
        "    name = 'temp-app'\n"
        "    def build_query(self, aq): return {}\n"
        "    def run(self, ctx): return []\n"
    )
    cls = _import_app_class("./custom_app.py:TempApp", base_dir=tmp_path)
    assert cls.__name__ == "TempApp"
    assert Path(inspect.getsourcefile(cls)).resolve() == app_file.resolve()

    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.register_app(cls())

    spec = aq.client.register_app.call_args.args[0]
    assert spec.entry_file == "custom_app.py"
    assert spec.source_spec == f"{app_file.resolve()}:TempApp"
    assert "class TempApp(App):" in spec.source_code


def test_file_app_imports_keep_distinct_source_paths(tmp_path: Path):
    first = tmp_path / "first_app.py"
    second = tmp_path / "second_app.py"
    source = (
        "from acquirium import App\n"
        "class TempApp(App):\n"
        "    name = {!r}\n"
        "    def build_query(self, aq): return {{}}\n"
        "    def run(self, ctx): return []\n"
    )
    first.write_text(source.format("first"))
    second.write_text(source.format("second"))

    first_cls = _import_app_class(f"{first}:TempApp")
    second_cls = _import_app_class(f"{second}:TempApp")

    assert Path(inspect.getsourcefile(first_cls)).resolve() == first.resolve()
    assert Path(inspect.getsourcefile(second_cls)).resolve() == second.resolve()


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
