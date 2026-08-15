"""Tests for internals.env_spec.build_runtime_env."""

import json
import os
import threading

import pytest

from acquirium.internals.env_spec import (
    SETUP_COMMANDS_ENV_VAR,
    SETUP_HOOK,
    SETUP_MARKER_DIR_ENV_VAR,
    build_runtime_env,
    run_setup_commands,
    run_setup_commands_hook,
    worker_pythonpath,
)
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


# ─────────────────────── setup commands ───────────────────────


def test_setup_commands_join_the_runtime_env():
    env = build_runtime_env(EnvSpec(setup_commands=["idaes get-extensions"]))
    assert env["worker_process_setup_hook"] == SETUP_HOOK
    assert json.loads(env["env_vars"][SETUP_COMMANDS_ENV_VAR]) == ["idaes get-extensions"]


def test_setup_commands_run_once_per_node(tmp_path):
    log = tmp_path / "log"
    cmds = [f"echo ran >> {log}"]
    assert run_setup_commands(cmds, marker_dir=tmp_path) is True
    assert run_setup_commands(cmds, marker_dir=tmp_path) is False  # marker hit
    assert log.read_text().count("ran") == 1
    # A different command list is a different marker.
    assert run_setup_commands([f"echo other >> {log}"], marker_dir=tmp_path) is True


def test_concurrent_workers_run_commands_once(tmp_path):
    log = tmp_path / "log"
    cmds = [f"sleep 0.1 && echo ran >> {log}"]
    threads = [
        threading.Thread(target=run_setup_commands, args=(cmds,),
                         kwargs={"marker_dir": tmp_path})
        for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert log.read_text().count("ran") == 1


def test_failing_command_raises_with_stderr(tmp_path):
    with pytest.raises(RuntimeError, match="no-such-binary"):
        run_setup_commands(["no-such-binary-xyz --flag"], marker_dir=tmp_path)
    # No marker was written: the next worker retries instead of skipping.
    assert run_setup_commands([f"echo ok >> {tmp_path}/log"], marker_dir=tmp_path)


def test_hook_reads_env_var(tmp_path, monkeypatch):
    log = tmp_path / "log"
    monkeypatch.setenv(SETUP_MARKER_DIR_ENV_VAR, str(tmp_path))
    monkeypatch.setenv(SETUP_COMMANDS_ENV_VAR, json.dumps([f"echo hook >> {log}"]))
    run_setup_commands_hook()
    run_setup_commands_hook()
    assert log.read_text().count("hook") == 1


def test_hook_without_commands_is_a_noop(monkeypatch):
    monkeypatch.delenv(SETUP_COMMANDS_ENV_VAR, raising=False)
    run_setup_commands_hook()  # must not raise
