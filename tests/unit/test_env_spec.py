"""Tests for internals.env_spec.build_runtime_env."""

import json
import os
import threading

import pytest

from acquirium.internals.env_spec import (
    SETUP_COMMANDS_ENV_VAR,
    SETUP_HOOK,
    SETUP_MARKER_DIR_ENV_VAR,
    _create_overlay_venv,
    build_runtime_env,
    ensure_env,
    env_fingerprint,
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


def test_pip_spec_requires_materialized_env():
    # Silently spawning a pip-declaring spec on the inherit path would fake
    # isolation — build_runtime_env refuses instead.
    with pytest.raises(ValueError, match="ensure_env"):
        build_runtime_env(EnvSpec(pip=["paho-mqtt>=2.1.0"]))


def test_py_executable_carries_through():
    env = build_runtime_env(
        EnvSpec(pip=["paho-mqtt>=2.1.0"]), py_executable="/data/envs/abc/bin/python",
    )
    assert env["py_executable"] == "/data/envs/abc/bin/python"
    assert "pip" not in env  # ray's session-scoped pip path is not used


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
def test_ray_requirement_rejected(req, tmp_path):
    with pytest.raises(ValueError, match="must not declare ray"):
        ensure_env(EnvSpec(pip=[req]), tmp_path, installer=lambda d, p: None)


def test_raylike_names_are_not_rejected(tmp_path):
    python = ensure_env(
        EnvSpec(pip=["rayon", "raytools>=1"]), tmp_path,
        installer=lambda d, p: d.mkdir(parents=True),
    )
    assert python is not None


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


# ─────────────────────── persistent env store ───────────────────────


def make_installer(calls):
    def installer(env_dir, pip):
        calls.append(list(pip))
        (env_dir / "bin").mkdir(parents=True)
        (env_dir / "bin" / "python").write_text("")
    return installer


def test_ensure_env_builds_once_and_reuses(tmp_path):
    calls = []
    spec = EnvSpec(pip=["watertap", "pyomo>=6"])
    p1 = ensure_env(spec, tmp_path, installer=make_installer(calls))
    p2 = ensure_env(spec, tmp_path, installer=make_installer(calls))
    assert p1 == p2
    assert calls == [["watertap", "pyomo>=6"]]        # built exactly once
    assert (tmp_path / env_fingerprint(spec) / ".ready").exists()


def test_ensure_env_none_without_pip(tmp_path):
    assert ensure_env(None, tmp_path) is None
    assert ensure_env(EnvSpec(setup_commands=["echo hi"]), tmp_path) is None


def test_distinct_declarations_get_distinct_envs(tmp_path):
    calls = []
    inst = make_installer(calls)
    p1 = ensure_env(EnvSpec(pip=["a"]), tmp_path, installer=inst)
    p2 = ensure_env(EnvSpec(pip=["b"]), tmp_path, installer=inst)
    assert p1 != p2
    assert len(calls) == 2


def test_failed_build_is_removed_and_retried(tmp_path):
    spec = EnvSpec(pip=["pkg"])

    def failing(env_dir, pip):
        (env_dir / "bin").mkdir(parents=True)   # half-built
        raise RuntimeError("download died")

    with pytest.raises(RuntimeError, match="download died"):
        ensure_env(spec, tmp_path, installer=failing)
    assert not (tmp_path / env_fingerprint(spec)).exists()  # cleaned up

    calls = []
    assert ensure_env(spec, tmp_path, installer=make_installer(calls)) is not None
    assert len(calls) == 1                                   # retry succeeded


def test_concurrent_builds_share_one_install(tmp_path):
    calls = []
    spec = EnvSpec(pip=["pkg"])

    def slow_installer(env_dir, pip):
        import time
        time.sleep(0.1)
        make_installer(calls)(env_dir, pip)

    threads = [
        threading.Thread(target=ensure_env, args=(spec, tmp_path),
                         kwargs={"installer": slow_installer})
        for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert len(calls) == 1


def test_overlay_venv_inherits_the_server_environment(tmp_path):
    # The mechanism that broke in the field: venv-from-venv chains to the
    # bare base interpreter, so --system-site-packages saw an empty python
    # (workers died with "No module named 'ray'"). The overlay must instead
    # see the *server venv's* site-packages via its .pth hook — appended
    # after its own (declared packages shadow), with the server's .pth files
    # processed (editable installs resolve).
    import subprocess
    import sysconfig

    python = _create_overlay_venv(tmp_path / "env")
    assert python.exists()

    base_site = sysconfig.get_paths()["purelib"]
    result = subprocess.run(
        [str(python), "-c",
         "import sys, pyoxigraph; print('\\n'.join(sys.path))"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    paths = result.stdout.splitlines()
    assert base_site in paths                       # server env inherited
    own_site = str(next((tmp_path / "env" / "lib").glob("python*/site-packages")))
    assert paths.index(own_site) < paths.index(base_site)  # overlay shadows


def test_path_carries_the_overlay_bin(monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin")
    env = build_runtime_env(
        EnvSpec(pip=["pkg"]), py_executable="/data/envs/abc/bin/python",
    )
    assert env["env_vars"]["PATH"] == f"/data/envs/abc/bin{os.pathsep}/usr/bin"

    # A user-declared PATH is prepended to, not clobbered.
    env2 = build_runtime_env(
        EnvSpec(pip=["pkg"], env_vars={"PATH": "/custom"}),
        py_executable="/data/envs/abc/bin/python",
    )
    assert env2["env_vars"]["PATH"] == f"/data/envs/abc/bin{os.pathsep}/custom"
