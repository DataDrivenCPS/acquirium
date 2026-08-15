"""Tests for internals.env_spec.build_runtime_env."""

import os

import pytest

from acquirium.internals.env_spec import build_runtime_env, worker_pythonpath
from acquirium.internals.models import EnvSpec


def test_undeclared_env_is_none():
    # The zero-cost inherit path must stay exactly that.
    assert build_runtime_env(None) is None
    assert build_runtime_env(EnvSpec()) is None


def test_source_dir_alone_sets_pythonpath(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/inherited")
    env = build_runtime_env(None, source_dir="/drivers/x")
    assert env == {"env_vars": {"PYTHONPATH": f"/drivers/x{os.pathsep}/inherited"}}


def test_pip_env_carries_packages_and_timeout():
    env = build_runtime_env(EnvSpec(pip=["paho-mqtt>=2.1.0"]))
    assert env["pip"] == ["paho-mqtt>=2.1.0"]
    assert env["config"]["setup_timeout_seconds"] == 1800
    env2 = build_runtime_env(EnvSpec(pip=["x"]), setup_timeout_seconds=60)
    assert env2["config"]["setup_timeout_seconds"] == 60


def test_env_vars_pass_through_and_pythonpath_merges(monkeypatch):
    monkeypatch.delenv("PYTHONPATH", raising=False)
    env = build_runtime_env(
        EnvSpec(env_vars={"IDAES_DIR": "/opt/idaes", "PYTHONPATH": "/user/libs"}),
        source_dir="/drivers/x",
    )
    assert env["env_vars"]["IDAES_DIR"] == "/opt/idaes"
    # source_dir first, user path preserved, nothing clobbered.
    assert env["env_vars"]["PYTHONPATH"] == f"/drivers/x{os.pathsep}/user/libs"


def test_inherited_pythonpath_survives(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/inherited")
    env = build_runtime_env(EnvSpec(env_vars={"PYTHONPATH": "/user/libs"}), source_dir="/d")
    parts = env["env_vars"]["PYTHONPATH"].split(os.pathsep)
    assert parts[0] == "/d"
    assert "/user/libs" in parts and "/inherited" in parts


def test_py_modules_pass_through():
    env = build_runtime_env(EnvSpec(py_modules=["/pkgs/helper"]))
    assert env == {"py_modules": ["/pkgs/helper"]}


@pytest.mark.parametrize("req", ["ray", "ray==2.56.0", "ray>=2", "ray[default]", "Ray "])
def test_ray_requirement_rejected(req):
    with pytest.raises(ValueError, match="must not declare ray"):
        build_runtime_env(EnvSpec(pip=[req]))


def test_raylike_names_are_not_rejected():
    env = build_runtime_env(EnvSpec(pip=["rayon", "raytools>=1"]))
    assert env["pip"] == ["rayon", "raytools>=1"]


def test_worker_pythonpath_dedup(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", f"/a{os.pathsep}/b")
    assert worker_pythonpath("/a") == f"/a{os.pathsep}/b"
    assert worker_pythonpath("/c") == f"/c{os.pathsep}/a{os.pathsep}/b"
