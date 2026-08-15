"""Lock-hygiene tests for AppSupervisor and DriverSupervisor.

The property under test: the record lock is never held across an actor
call. Actor calls re-enter the server over HTTP, so a supervisor blocking
its request thread on ray.get while holding the lock other request threads
need is a deadlock machine. Registration instead reserves the name with a
placeholder and does actor work outside the lock.

Ray is stubbed at the module boundary (ray.get/kill/wait + the runner
classes), so no cluster is involved.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest
import ray

import acquirium.Apps.supervisor as app_sup_mod
import acquirium.Drivers.supervisor as drv_sup_mod
import acquirium.Client.acquirium as aq_mod
import acquirium.cli as cli_mod
from acquirium.internals.models import AppSpec


# ─────────────────────── ray/actor stubs ───────────────────────


class FakeRef:
    def __init__(self, tag: str, fn):
        self.tag = tag
        self.fn = fn


class FakeMethod:
    def __init__(self, tag: str, fn):
        self.tag = tag
        self.fn = fn

    def remote(self, *a, **k):
        return FakeRef(self.tag, lambda: self.fn(*a, **k))


class FakeActor:
    def __init__(self, **behaviors):
        for tag in ("register", "setup", "stop", "deregister", "status", "run"):
            self.__dict__[tag] = FakeMethod(tag, behaviors.get(tag, lambda *a, **k: {}))


class FakeRunnerCls:
    """Stub for AppRunner/DriverRunner: .remote() hands out the next actor."""

    next_actor: FakeActor | None = None

    @classmethod
    def remote(cls, *a, **k):
        actor = cls.next_actor or FakeActor()
        cls.next_actor = None
        return actor

    @classmethod
    def options(cls, **k):
        return cls


@pytest.fixture
def stub_ray(monkeypatch):
    killed: list = []

    def fake_get(ref, timeout=None):
        return ref.fn()

    monkeypatch.setattr(ray, "get", fake_get)
    monkeypatch.setattr(ray, "kill", killed.append)
    monkeypatch.setattr(ray, "wait", lambda refs, **k: ([], list(refs)))
    monkeypatch.setattr(aq_mod, "Acquirium", lambda **kw: SimpleNamespace())
    FakeRunnerCls.next_actor = None
    return killed


@pytest.fixture
def app_sup(stub_ray, monkeypatch, tmp_path):
    monkeypatch.setattr(app_sup_mod, "AppRunner", FakeRunnerCls)
    return app_sup_mod.AppSupervisor(
        app_storage_root=tmp_path, server_url="localhost", server_port=1,
    )


@pytest.fixture
def drv_sup(stub_ray, monkeypatch):
    monkeypatch.setattr(drv_sup_mod, "DriverRunner", FakeRunnerCls)
    monkeypatch.setattr(cli_mod, "_import_driver_class", lambda spec, base_dir=None: (object, None))
    return drv_sup_mod.DriverSupervisor(server_url="localhost", server_port=1)


def spec(name: str = "x") -> AppSpec:
    return AppSpec(name=name)


# ─────────────────────── AppSupervisor ───────────────────────


class TestAppRegistration:
    def test_register_publishes_record(self, app_sup):
        info = app_sup.register_app(spec())
        assert info["replaced"] is False
        assert app_sup._apps["x"]["actor"] is not None

    def test_lock_free_and_name_reserved_while_building(self, app_sup):
        in_setup = threading.Event()
        release = threading.Event()

        def blocking_setup():
            in_setup.set()
            assert release.wait(5)
            return {}

        FakeRunnerCls.next_actor = FakeActor(setup=blocking_setup)
        t = threading.Thread(target=lambda: app_sup.register_app(spec()), daemon=True)
        t.start()
        assert in_setup.wait(5)

        # The name is reserved: a duplicate registration is rejected mid-build.
        with pytest.raises(app_sup_mod.AppAlreadyRegistered):
            app_sup.register_app(spec())
        # The record lock is NOT held across the in-flight actor call.
        assert app_sup._lock.acquire(timeout=1)
        app_sup._lock.release()
        # And actor-routing paths fail fast instead of touching None.
        with pytest.raises(RuntimeError, match="still registering"):
            app_sup._actor("x")

        release.set()
        t.join(5)
        assert app_sup._apps["x"]["actor"] is not None

    def test_register_failure_frees_the_name(self, app_sup, stub_ray):
        def boom():
            raise RuntimeError("registration graph write failed")

        FakeRunnerCls.next_actor = FakeActor(register=boom)
        with pytest.raises(RuntimeError):
            app_sup.register_app(spec())
        assert "x" not in app_sup._apps
        assert len(stub_ray) == 1  # the half-built actor was killed

        app_sup.register_app(spec())  # the name is immediately reusable
        assert app_sup._apps["x"]["actor"] is not None

    def test_build_failure_is_nonfatal(self, app_sup):
        def boom():
            raise RuntimeError("build_query failed")

        FakeRunnerCls.next_actor = FakeActor(setup=boom)
        info = app_sup.register_app(spec())
        assert info["replaced"] is False
        assert app_sup._apps["x"]["actor"] is not None

    def test_restore_publishes_record(self, app_sup):
        app_sup.restore_app(spec())
        assert app_sup._apps["x"]["actor"] is not None


class TestAppTeardown:
    def test_stop_timeout_skips_deregister_and_kills(self, app_sup, stub_ray, monkeypatch):
        gets: list[str] = []

        def timing_out_get(ref, timeout=None):
            gets.append(ref.tag)
            if ref.tag == "stop":
                raise ray.exceptions.GetTimeoutError("wedged")
            return ref.fn()

        monkeypatch.setattr(ray, "get", timing_out_get)
        actor = FakeActor()
        record = {"name": "x", "actor": actor, "running": True}

        app_sup._teardown_app(record)  # must not raise
        assert gets == ["stop"]        # deregister skipped: actor is wedged
        assert stub_ray == [actor]     # killed anyway

    def test_placeholder_teardown_is_a_noop(self, app_sup, stub_ray):
        app_sup._teardown_app({"name": "x", "actor": None, "running": False})
        assert stub_ray == []


# ─────────────────────── DriverSupervisor ───────────────────────


class TestDriverStart:
    def test_start_publishes_record(self, drv_sup):
        info = drv_sup.start_driver(spec="pkg.mod:Cls", config={})
        assert info["name"] == "Cls"
        assert info["status"] == "running"

    def test_lock_free_and_name_reserved_while_setting_up(self, drv_sup):
        in_setup = threading.Event()
        release = threading.Event()

        def blocking_setup():
            in_setup.set()
            assert release.wait(5)
            return {}

        FakeRunnerCls.next_actor = FakeActor(setup=blocking_setup)
        t = threading.Thread(
            target=lambda: drv_sup.start_driver(spec="pkg.mod:Cls", config={}),
            daemon=True,
        )
        t.start()
        assert in_setup.wait(5)

        with pytest.raises(ValueError, match="already running"):
            drv_sup.start_driver(spec="pkg.mod:Cls", config={})
        assert drv_sup._lock.acquire(timeout=1)
        drv_sup._lock.release()
        # The pending driver is visible as "starting" and cannot be stopped yet.
        (listed,) = drv_sup.list_drivers()
        assert listed["status"] == "starting"
        with pytest.raises(ValueError, match="still starting"):
            drv_sup.stop_driver("Cls")

        release.set()
        t.join(5)
        assert drv_sup._drivers["Cls"]["actor"] is not None

    def test_setup_failure_frees_the_name(self, drv_sup, stub_ray):
        def boom():
            raise RuntimeError("driver setup failed")

        FakeRunnerCls.next_actor = FakeActor(setup=boom)
        with pytest.raises(RuntimeError):
            drv_sup.start_driver(spec="pkg.mod:Cls", config={})
        assert "Cls" not in drv_sup._drivers
        assert len(stub_ray) == 1

        drv_sup.start_driver(spec="pkg.mod:Cls", config={})
        assert drv_sup._drivers["Cls"]["actor"] is not None

    def test_stop_all_during_start_kills_the_late_driver(self, drv_sup, stub_ray):
        in_setup = threading.Event()
        release = threading.Event()
        result: dict = {}

        def blocking_setup():
            in_setup.set()
            assert release.wait(5)
            return {}

        FakeRunnerCls.next_actor = FakeActor(setup=blocking_setup)

        def start():
            try:
                drv_sup.start_driver(spec="pkg.mod:Cls", config={})
            except RuntimeError as exc:
                result["error"] = str(exc)

        t = threading.Thread(target=start, daemon=True)
        t.start()
        assert in_setup.wait(5)

        drv_sup.stop_all(timeout=0.1)   # clears the reservation
        release.set()
        t.join(5)

        assert "stopped during startup" in result["error"]
        assert len(stub_ray) == 1       # the late runner was killed
        assert drv_sup._drivers == {}
