from __future__ import annotations

import asyncio

from acquirium import App, Output
from acquirium.Server.app import _restore_registered_apps, _start_config_apps
from acquirium.internals.models import AppSpec


class ConfigApp(App):
    name = "config-app"

    def build_query(self, aq):
        return {}

    def run(self, ctx) -> list[Output]:
        return []


class FakeSupervisor:
    def __init__(self, existing=()):
        self.existing = list(existing)

    def list_apps(self):
        return [{"name": name} for name in self.existing]


class FakeAcquirium:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.register_calls = []
        self.run_calls = []
        type(self).instances.append(self)

    def register_app(self, app, **kwargs):
        self.register_calls.append((app, kwargs))
        return {"ok": True}

    def run_app(self, app_id, **kwargs):
        self.run_calls.append((app_id, kwargs))
        return {"ok": True}


def install_fakes(monkeypatch):
    FakeAcquirium.instances.clear()
    monkeypatch.setattr("acquirium.Client.acquirium.Acquirium", FakeAcquirium)
    monkeypatch.setattr("acquirium.cli._import_app_class", lambda spec, base_dir=None: ConfigApp)


def test_config_app_registers_and_autostarts_with_defaults(monkeypatch, tmp_path):
    install_fakes(monkeypatch)
    cfg = {
        "__config_dir": str(tmp_path),
        "apps": [{
            "spec": "config_app.py:ConfigApp",
            "build_params": {"training": 10},
            "params": {"window": 5},
            "interval": 30,
        }],
    }

    asyncio.run(_start_config_apps(FakeSupervisor(), cfg))

    aq = FakeAcquirium.instances[0]
    app, register_kwargs = aq.register_calls[0]
    assert app.name == "config-app"
    assert register_kwargs == {"params": {"training": 10}, "replace": True}
    assert aq.run_calls == [(
        "config-app",
        {
            "start": None,
            "end": None,
            "params": {"window": 5},
            "keep_alive": True,
            "interval": 30.0,
        },
    )]


def test_config_app_can_reuse_restored_registration(monkeypatch, tmp_path):
    install_fakes(monkeypatch)
    cfg = {
        "__config_dir": str(tmp_path),
        "apps": [{
            "spec": "config_app.py:ConfigApp",
            "replace": False,
            "autostart": False,
        }],
    }

    asyncio.run(_start_config_apps(FakeSupervisor(existing=["config-app"]), cfg))

    aq = FakeAcquirium.instances[0]
    assert aq.register_calls == []
    assert aq.run_calls == []


def test_disabled_config_app_is_skipped(monkeypatch, tmp_path):
    install_fakes(monkeypatch)
    cfg = {
        "__config_dir": str(tmp_path),
        "apps": [{"spec": "config_app.py:ConfigApp", "enabled": False}],
    }

    asyncio.run(_start_config_apps(FakeSupervisor(), cfg))

    aq = FakeAcquirium.instances[0]
    assert aq.register_calls == []
    assert aq.run_calls == []


def test_restore_restarts_only_active_keep_alive_apps(monkeypatch):
    specs = [
        AppSpec(
            name="active",
            resume_keep_alive=True,
            run_interval=45,
            run_params={"window": 7},
        ),
        AppSpec(name="inactive"),
    ]
    monkeypatch.setattr(
        "acquirium.Apps.supervisor.restore_app_specs",
        lambda manager: specs,
    )

    class RestoreSupervisor:
        def __init__(self):
            self.restored = []
            self.run_requests = []

        def restore_app(self, spec):
            self.restored.append(spec.name)

        def run_app(self, request):
            self.run_requests.append(request)

    supervisor = RestoreSupervisor()
    asyncio.run(_restore_registered_apps(supervisor, object()))

    assert supervisor.restored == ["active", "inactive"]
    assert len(supervisor.run_requests) == 1
    request = supervisor.run_requests[0]
    assert request.app_id == "active"
    assert request.keep_alive is True
    assert request.interval == 45
    assert request.params == {"window": 7}
