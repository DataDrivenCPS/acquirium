from pathlib import Path

from acquirium.Materialization import App, output
from acquirium.Server.app import _deploy_config_apps, _load_config_app_target


def test_config_app_class_spec_loads_from_its_config_directory(tmp_path: Path):
    app_file = tmp_path / "configured_temperature_app.py"
    app_file.write_text(
        "from acquirium.Materialization import App, output\n"
        "class TemperatureApp(App):\n"
        "    outputs = {'out': output.stream(value_kind='numeric')}\n"
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
        "    outputs = {'out': output.stream(value_kind='numeric')}\n"
        "class Second(App):\n"
        "    outputs = {'out': output.stream(value_kind='numeric')}\n"
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


_APP_SOURCE = (
    "from acquirium.Materialization import App, output\n"
    "class {name}(App):\n"
    "    outputs = {{'out': output.stream(value_kind='numeric')}}\n"
)


def test_config_app_files_with_one_stem_in_two_directories_are_rejected(tmp_path: Path, monkeypatch):
    import sys
    import pytest

    monkeypatch.setattr(sys, "path", list(sys.path))
    for folder, name in (("a", "First"), ("b", "Second")):
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "shared_stem_transform.py").write_text(_APP_SOURCE.format(name=name))
    try:
        first = _load_config_app_target("a/shared_stem_transform.py:First", base_dir=tmp_path)
        # Reloading the same file is fine; it names the same module.
        assert _load_config_app_target("a/shared_stem_transform.py:First", base_dir=tmp_path) is first
        with pytest.raises(ValueError, match="would import as 'shared_stem_transform'"):
            _load_config_app_target("b/shared_stem_transform.py:Second", base_dir=tmp_path)
    finally:
        sys.modules.pop("shared_stem_transform", None)
    # The app directory is searched after installed modules, never before.
    assert sys.path.index(str(tmp_path / "a")) == len(sys.path) - 1


def test_config_app_file_named_like_an_installed_module_is_rejected(tmp_path: Path, monkeypatch):
    import sys
    import colorsys
    import pytest

    monkeypatch.setattr(sys, "path", list(sys.path))
    (tmp_path / "colorsys.py").write_text(_APP_SOURCE.format(name="Shadow"))
    with pytest.raises(ValueError, match="would import as 'colorsys'"):
        _load_config_app_target("./colorsys.py:Shadow", base_dir=tmp_path)
    assert sys.modules["colorsys"] is colorsys
    assert str(tmp_path) not in sys.path
