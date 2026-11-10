import tomllib

from typer.testing import CliRunner

from acquirium._templates import starter_config
from acquirium.cli import _import_driver_class, app
from acquirium.Server.config import load_config


def test_config_init_writes_a_loadable_starter_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(app, ["config", "init"])

    assert result.exit_code == 0, result.output
    written = tmp_path / "acquirium.toml"
    assert written.read_text(encoding="utf-8") == starter_config()
    server = load_config(written).data["server"]
    assert server["timeseries_backend"] == "duckdb"
    assert server["host"] == "127.0.0.1"


def test_config_init_does_not_overwrite_without_force(tmp_path):
    target = tmp_path / "acquirium.toml"
    target.write_text("[server]\nport = 9999\n")

    result = CliRunner().invoke(app, ["config", "init", str(target)])

    assert result.exit_code == 1
    assert "already exists" in result.output
    assert target.read_text() == "[server]\nport = 9999\n"

    result = CliRunner().invoke(app, ["config", "init", str(target), "--force"])

    assert result.exit_code == 0, result.output
    assert target.read_text(encoding="utf-8") == starter_config()


def test_config_init_accepts_a_directory_or_a_new_path(tmp_path):
    result = CliRunner().invoke(app, ["config", "init", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "acquirium.toml").is_file()

    nested = tmp_path / "plant" / "dev.toml"
    result = CliRunner().invoke(app, ["config", "init", str(nested)])
    assert result.exit_code == 0, result.output
    assert nested.is_file()


def test_starter_config_drivers_are_importable():
    drivers = tomllib.loads(starter_config())["drivers"]

    assert drivers
    for entry in drivers:
        _import_driver_class(entry["spec"])
