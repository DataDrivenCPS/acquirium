from unittest.mock import Mock

import pytest

from acquirium import runtime


@pytest.fixture(autouse=True)
def clean_session():
    runtime.shutdown()
    yield
    runtime.shutdown()


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout(timeout):
    with pytest.raises(ValueError, match="positive"):
        runtime.init(timeout=timeout)


@pytest.mark.parametrize("address", ["localhost:8000", "ftp://host", "http://user:pass@host", "http://host/path", "http://host?x=1"])
def test_invalid_address(address):
    with pytest.raises(ValueError, match="address"):
        runtime.init(address=address)


def test_explicit_server_is_never_started_or_stopped(monkeypatch):
    client = Mock()
    factory = Mock(return_value=client)
    popen = Mock()
    monkeypatch.setattr(runtime, "Acquirium", factory)
    monkeypatch.setattr(runtime.subprocess, "Popen", popen)
    assert runtime.init(address="https://example.org:8443") is client
    assert runtime.init(address="https://example.org:8443") is client
    factory.assert_called_once_with(server_url="example.org", server_port=8443,
                                    use_ssl=True, health_timeout=600)
    with pytest.raises(ValueError, match="different options"):
        runtime.init(address="http://other")
    runtime.shutdown()
    runtime.shutdown()
    client.client._http.close.assert_called_once()
    popen.assert_not_called()


def test_client_address_is_public(monkeypatch):
    client = Mock()
    client.address = "https://example.org:8443"
    monkeypatch.setattr(runtime, "Acquirium", Mock(return_value=client))
    assert runtime.init(address="https://example.org:8443").address == "https://example.org:8443"


def test_remote_rejects_local_options():
    with pytest.raises(ValueError, match="local server options"):
        runtime.init(address="http://host", data_dir="data")


def test_forked_child_does_not_stop_parent(monkeypatch):
    client, process = Mock(), Mock()
    monkeypatch.setattr(runtime, "_session", (-1, "parent", client, process, False))
    runtime.shutdown()
    client.client._http.close.assert_not_called()
    process.terminate.assert_not_called()


def test_owner_shutdown_removes_stale_registry(monkeypatch, tmp_path):
    client, process, stop = Mock(), Mock(), Mock()
    registry = tmp_path / ".runtime" / "server.json"
    registry.parent.mkdir()
    registry.write_text("stale")
    monkeypatch.setattr(runtime, "_stop", stop)
    monkeypatch.setattr(runtime, "_session", (runtime.os.getpid(), (str(tmp_path), None), client, process, False))
    runtime.shutdown()
    stop.assert_called_once_with(process)
    client.client._http.close.assert_called_once()
    assert not registry.exists()


def test_bad_registry_is_not_a_server(tmp_path):
    registry = tmp_path / "server.json"
    for contents in ('broken', '{}', '{"port": -1}', '{"port": null}'):
        registry.write_text(contents)
        assert runtime._discover(tmp_path) is None


def test_stale_registry_is_removed_only_when_no_server_holds_lock(tmp_path):
    registry = tmp_path / "server.json"
    registry.write_text("stale")
    runtime._forget_stale_runtime(tmp_path)
    assert not registry.exists()
    registry.write_text("live")
    with runtime.FileLock(tmp_path / "server.lock", timeout=0):
        runtime._forget_stale_runtime(tmp_path)
    assert registry.read_text() == "live"


def test_dead_child_reports_log_and_can_retry(monkeypatch, tmp_path):
    process = Mock()
    process.poll.return_value = 1
    monkeypatch.setattr(runtime.subprocess, "Popen", Mock(return_value=process))
    with pytest.raises(RuntimeError, match="server.log"):
        runtime.init(data_dir=tmp_path)
    assert runtime._session is None
    assert not runtime._running(tmp_path / ".runtime")


def test_startup_timeout_stops_owned_child(monkeypatch, tmp_path):
    process = Mock()
    process.poll.return_value = None
    stop = Mock()
    monkeypatch.setattr(runtime.subprocess, "Popen", Mock(return_value=process))
    monkeypatch.setattr(runtime, "_discover", lambda _: None)
    monkeypatch.setattr(runtime, "_stop", stop)
    with pytest.raises(TimeoutError, match="server.log"):
        runtime.init(data_dir=tmp_path, timeout=0.01)
    stop.assert_called_once_with(process)
    assert runtime._session is None


def test_discovery_checks_identity(monkeypatch, tmp_path):
    import json
    info = {"port": 8123, "token": "expected", "exact_only": False}
    (tmp_path / "server.json").write_text(json.dumps(info))
    http = Mock()
    http.get.return_value.json.return_value = {**info, "token": "other"}
    session = Mock()
    session.__enter__ = Mock(return_value=http)
    session.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(runtime.requests, "Session", lambda: session)
    assert runtime._discover(tmp_path) is None


def test_configuration_discovery_and_relative_paths(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    config = tmp_path / "acquirium.toml"
    config.write_text('[server]\ndata_dir = "stored"\nexact_only = true\n')
    path, root, cfg = runtime._configuration(None, None)
    assert path == config
    assert root == tmp_path / "stored"
    assert cfg["server"]["exact_only"] is True
    # An explicit data directory requests local defaults, not the cwd config.
    path, root, cfg = runtime._configuration(None, tmp_path / "other")
    assert path is None and cfg == {}
    assert root == tmp_path / "other"
    monkeypatch.chdir(tmp_path.parent)
    assert runtime._configuration(config, None)[1] == tmp_path / "stored"


def test_missing_explicit_config_is_an_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        runtime.init(tmp_path / "missing.toml")


@pytest.mark.parametrize("setting", ['recreate = true', 'enabled = false', 'workers = 2'])
def test_config_rejects_unsupported_lifecycle_settings(tmp_path, setting):
    config = tmp_path / "custom.toml"
    config.write_text('[server]\n' + setting)
    with pytest.raises(ValueError):
        runtime.init(config)
    assert not (tmp_path / ".acquirium").exists()
