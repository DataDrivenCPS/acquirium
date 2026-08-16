"""Tests for Drivers.local_runner (edge mode). Ray and the supervisor are
stubbed at the module boundary; the health probe is stubbed too."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

import acquirium.Drivers.local_runner as lr


class FakeSupervisor:
    instances: list = []
    fail_specs: set[str] = set()          # class-level: set by tests before run()

    def __init__(self, **kw):
        self.kw = kw
        self.started: list[dict] = []
        self.stopped = False
        FakeSupervisor.instances.append(self)

    def start_driver(self, *, spec, config, interval, name=None):
        if spec in self.fail_specs:
            raise RuntimeError(f"boom {spec}")
        self.started.append({"spec": spec, "config": config, "interval": interval, "name": name})
        return {"name": name or spec.rsplit(":", 1)[-1], "interval": interval}

    def list_drivers(self):
        return [{"name": d["spec"]} for d in self.started]

    def stop_all(self, **kw):
        self.stopped = True


@pytest.fixture
def stubs(monkeypatch):
    import ray
    import acquirium.Drivers.supervisor as sup_mod
    fake_ray = MagicMock()
    monkeypatch.setattr(ray, "init", fake_ray.init)
    monkeypatch.setattr(ray, "shutdown", fake_ray.shutdown)
    monkeypatch.setattr(sup_mod, "DriverSupervisor", FakeSupervisor)
    monkeypatch.setattr(lr, "_wait_for_server", lambda base, **kw: True)
    FakeSupervisor.instances.clear()
    return fake_ray


CFG = {
    "__config_dir": "/cfg",
    "driver": {"interval": 7.0, "server_url": "ignored", "server_port": 1},
    "drivers": [
        {"spec": "pkg.a:A", "interval": 2.0, "watch_dir": "./x"},
        {"spec": "pkg.b:B", "name": "bee"},
    ],
}


def run(cfg=CFG, **kw):
    stop = threading.Event()
    stop.set()                                    # return immediately after start
    return lr.run_drivers_locally(
        cfg, server_url="remote.host", server_port=8000, stop_event=stop, **kw,
    )


def test_starts_every_driver_against_the_remote(stubs):
    assert run() == 0
    (sup,) = FakeSupervisor.instances
    assert sup.kw["server_url"] == "remote.host" and sup.kw["server_port"] == 8000
    specs = [d["spec"] for d in sup.started]
    assert specs == ["pkg.a:A", "pkg.b:B"]
    # Entry overrides win; the connect address in the merged config is the
    # remote server, whatever the file said.
    a = sup.started[0]
    assert a["interval"] == 2.0 and a["config"]["driver"]["watch_dir"] == "./x"
    assert a["config"]["driver"]["server_url"] == "remote.host"
    assert sup.started[1]["name"] == "bee" and sup.started[1]["interval"] == 7.0
    # Clean shutdown path: drivers stopped (buffers flushed), Ray down.
    assert sup.stopped and stubs.shutdown.called


def test_env_storage_root_is_forwarded(stubs):
    run(env_storage_root="/edge/envs")
    assert FakeSupervisor.instances[0].kw["env_storage_root"] == "/edge/envs"


def test_partial_failure_keeps_going_and_exits_nonzero(stubs):
    FakeSupervisor.fail_specs = {"pkg.a:A"}
    try:
        code = run()
    finally:
        FakeSupervisor.fail_specs = set()
    assert code == 1
    (sup,) = FakeSupervisor.instances
    assert [d["spec"] for d in sup.started] == ["pkg.b:B"]   # B still ran
    assert sup.stopped


def test_no_drivers_is_an_error(stubs):
    assert run({"driver": {}, "drivers": []}) == 1


def test_unreachable_server_is_an_error(stubs, monkeypatch):
    monkeypatch.setattr(lr, "_wait_for_server", lambda base, **kw: False)
    assert run() == 1
    assert FakeSupervisor.instances == []         # never got as far as Ray


def test_all_drivers_failing_shuts_ray_down(stubs):
    FakeSupervisor.fail_specs = {"pkg.a:A", "pkg.b:B"}
    try:
        assert run() == 1
    finally:
        FakeSupervisor.fail_specs = set()
    assert stubs.shutdown.called


def test_cli_run_command_is_registered():
    from typer.testing import CliRunner
    from acquirium.cli import app
    result = CliRunner().invoke(app, ["driver", "run", "--help"])
    assert result.exit_code == 0
    assert "THIS machine" in result.output
