from pathlib import Path

from acquirium.Materialization import App, output
from acquirium.Server.app import _deploy_config_apps, _load_config_app_target


def test_config_app_class_spec_loads_from_its_config_directory(tmp_path: Path):
    app_file = tmp_path / "configured_temperature_app.py"
    app_file.write_text(
        "from acquirium.Materialization import App, output\n"
        "class TemperatureApp(App):\n"
        "    outputs = {'out': output.per_input(value_kind='numeric')}\n"
    )

    target = _load_config_app_target("./configured_temperature_app.py:TemperatureApp", base_dir=tmp_path)

    assert isinstance(target, type)
    assert issubclass(target, App)
    assert target.__module__ == "configured_temperature_app"


def test_config_app_registrar_receives_table_options_and_deploys_returned_classes(tmp_path: Path, monkeypatch):
    app_file = tmp_path / "configured_dag.py"
    app_file.write_text(
        "from acquirium.Materialization import App, output\n"
        "class First(App):\n"
        "    outputs = {'out': output.per_input(value_kind='numeric')}\n"
        "class Second(App):\n"
        "    outputs = {'out': output.per_input(value_kind='numeric')}\n"
        "def register(aq, config):\n"
        "    assert config == {'threshold': 7}\n"
        "    return [First, Second]\n"
    )

    deployed = []

    class FakeAcquirium:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def deploy_app(self, transformation, *, parameters=None):
            deployed.append((transformation, parameters))

    monkeypatch.setattr("acquirium.Client.acquirium.Acquirium", FakeAcquirium)
    _deploy_config_apps({
        "__config_dir": str(tmp_path),
        "server": {"port": 8000},
        "apps": [{"spec": "./configured_dag.py:register", "threshold": 7}],
    })

    assert [item.__name__ for item, _ in deployed] == ["First", "Second"]
    assert [parameters for _, parameters in deployed] == [{}, {}]
