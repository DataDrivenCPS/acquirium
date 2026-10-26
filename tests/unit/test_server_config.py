import os
from pathlib import Path

from acquirium.Server.config import (
    DEFAULT_LOCAL_CONFIG,
    load_active_config,
    load_config,
    load_local_config,
)


def test_repository_config_uses_the_supported_exact_only_key():
    config = Path(__file__).parents[2] / "acquirium.toml"

    loaded = load_config(config)

    assert loaded.data["server"]["exact_only"] is True
    assert "exact_match" not in loaded.data["server"]


def test_local_config_has_explicit_safe_defaults(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    loaded = load_local_config(discover=False)

    assert loaded.path is None
    assert loaded.data["server"] == {
        "enabled": True,
        "data_dir": ".acquirium",
        "timeseries_backend": "duckdb",
        "host": "127.0.0.1",
        "port": 0,
        "workers": 1,
        "recreate": False,
        "exact_only": True,
    }
    assert loaded.data["server"] is not DEFAULT_LOCAL_CONFIG["server"]


def test_local_config_overlays_explicit_server_values(tmp_path):
    config = tmp_path / "acquirium.toml"
    config.write_text('[server]\nport = 8123\nexact_only = false\n')

    loaded = load_local_config(config)

    assert loaded.data["server"]["port"] == 8123
    assert loaded.data["server"]["exact_only"] is False
    assert loaded.data["server"]["timeseries_backend"] == "duckdb"


def test_loaded_config_owns_path_directory_and_fingerprint(tmp_path):
    config = tmp_path / "acquirium.toml"
    config.write_text('[server]\ngraph_path = "state/graph"\nexact_only = true\n')

    loaded = load_config(config)

    assert loaded.path == config
    assert loaded.directory == tmp_path
    assert loaded.data["__config_dir"] == str(tmp_path)
    assert loaded.fingerprint


def test_server_environment_resolves_paths_without_overriding(monkeypatch, tmp_path):
    config = tmp_path / "acquirium.toml"
    config.write_text(
        '[server]\ndata_dir = "configured-data"\ngraph_path = "state/graph"\n'
        "exact_only = true\nworkers = 1\n"
    )
    monkeypatch.setenv("ACQUIRIUM_DATA_DIR", "/explicit-data")
    monkeypatch.delenv("ACQUIRIUM_GRAPH_PATH", raising=False)
    monkeypatch.delenv("ACQUIRIUM_EXACT_ONLY", raising=False)
    monkeypatch.delenv("ACQUIRIUM_WORKERS", raising=False)

    loaded = load_config(config)
    loaded.apply_server_env()

    assert loaded.path == config
    assert loaded.directory == tmp_path
    assert loaded.data["__config_dir"] == str(tmp_path)
    assert loaded.fingerprint
    assert loaded.data["server"]["graph_path"] == "state/graph"
    assert loaded.data["server"]["exact_only"] is True
    assert loaded.data["server"]["workers"] == 1
    assert loaded.data["server"]["data_dir"] == "configured-data"
    assert os.environ["ACQUIRIUM_DATA_DIR"] == "/explicit-data"
    assert os.environ["ACQUIRIUM_GRAPH_PATH"] == str(tmp_path / "state" / "graph")
    assert os.environ["ACQUIRIUM_EXACT_ONLY"] == "true"
    assert os.environ["ACQUIRIUM_WORKERS"] == "1"


def test_empty_active_config_suppresses_cwd_discovery(monkeypatch, tmp_path):
    (tmp_path / "acquirium.toml").write_text("[server]\nexact_only = true\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ACQUIRIUM_CONFIG", "")

    loaded = load_active_config()

    assert loaded.path is None
    assert loaded.data == {}
    assert loaded.directory == tmp_path
    assert loaded.fingerprint is None


def test_activate_round_trips_the_selected_config(monkeypatch, tmp_path):
    config = tmp_path / "acquirium.toml"
    config.write_text("[server]\nport = 8123\n")
    loaded = load_config(config)

    loaded.activate()

    active = load_active_config()
    assert active.path == loaded.path
    assert active.data == loaded.data
    assert active.fingerprint == loaded.fingerprint
