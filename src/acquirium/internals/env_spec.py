"""Resolve an :class:`~acquirium.internals.models.EnvSpec` to a Ray runtime_env.

Declared pip dependencies are materialized by :func:`ensure_env` into a
**persistent overlay venv** under the acquirium data dir
(``<data_dir>/envs/<fingerprint>/``) and handed to Ray as
``runtime_env["py_executable"]``. Ray's own ``pip`` runtime envs are
deliberately not used: their cache lives in the Ray *session* temp dir, so
every server restart re-cloned the venv and re-downloaded everything.
An overlay venv is created with ``--system-site-packages``, so it inherits
the server environment (acquirium, ray, polars, ...) and only the declared
packages are installed into it — built once, reused across restarts, shared
by every driver/app with the same declaration.

Dict construction itself stays ``ray``-free, so the same code serves the
server supervisors, the task host, and local (edge) runners, and tests
without a cluster.

Ray behaviours this module encodes (verified against ray 2.57):

- ``runtime_env["env_vars"]`` is applied per key into the worker's inherited
  environment: unset keys pass through, set keys clobber. ``PYTHONPATH``
  must therefore carry the parent's value explicitly or the worker loses it.
- An unresolved ``${VAR}`` inside a value silently expands to ``""`` in the
  worker — declare such variables explicitly rather than relying on
  substitution.
- A declared ``ray`` requirement is rejected with a readable error: the
  overlay inherits the cluster's ray, and installing a different one breaks
  the worker.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from acquirium.internals.models import EnvSpec

logger = logging.getLogger("acquirium.env_spec")

#: Default home for materialized environments when no data dir is wired in
#: (local/edge runners); the server passes ``<data_dir>/envs``.
DEFAULT_ENV_STORAGE_ROOT = Path.home() / ".cache" / "acquirium" / "envs"

#: env_vars carrying the declared setup commands into the worker, where the
#: setup hook reads them (the hook is referenced by import path, so it takes
#: no arguments of its own).
SETUP_COMMANDS_ENV_VAR = "ACQUIRIUM_SETUP_COMMANDS"
#: Overrides the node-level marker directory (tests, relocated caches).
SETUP_MARKER_DIR_ENV_VAR = "ACQUIRIUM_SETUP_MARKER_DIR"
#: Import path Ray resolves inside the worker after the env is built.
SETUP_HOOK = "acquirium.internals.env_spec.run_setup_commands_hook"


def _marker_dir() -> Path:
    override = os.environ.get(SETUP_MARKER_DIR_ENV_VAR)
    if override:
        return Path(override)
    return Path.home() / ".cache" / "acquirium" / "setup_markers"


def run_setup_commands(commands: list[str], *, marker_dir: Path | None = None) -> bool:
    """Run declared setup commands once per node; return True if they ran.

    Guarded by a marker file keyed on the command list's hash, taken under a
    file lock so concurrent workers on one node can't race the same download
    (``idaes get-extensions`` is not idempotent unguarded). Steady state is
    one ``stat``. A failing command raises with the command and its stderr
    tail — surfacing at registration through the synchronous setup call.
    """
    if not commands:
        return False
    import fcntl  # POSIX-only, as is Ray's worker model here

    key = hashlib.sha1(json.dumps(commands, ensure_ascii=True).encode()).hexdigest()[:16]
    directory = marker_dir or _marker_dir()
    directory.mkdir(parents=True, exist_ok=True)
    marker = directory / f"{key}.done"
    if marker.exists():
        return False

    with open(directory / f"{key}.lock", "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            if marker.exists():
                # Another worker finished them while we waited on the lock.
                return False
            for command in commands:
                logger.info("setup command: %s", command)
                result = subprocess.run(
                    command, shell=True, capture_output=True, text=True,
                )
                if result.returncode != 0:
                    stderr_tail = (result.stderr or result.stdout or "").strip()[-2000:]
                    raise RuntimeError(
                        f"setup command failed (exit {result.returncode}): "
                        f"{command!r}\n{stderr_tail}"
                    )
            marker.write_text(json.dumps({
                "commands": commands,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }))
            return True
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def run_setup_commands_hook() -> None:
    """Ray ``worker_process_setup_hook``: run this env's setup commands.

    Runs in every new worker process of an env that declares
    ``setup_commands``; the marker makes all but the first a stat. A raise
    here drains the worker, which is what surfaces the failure to the
    caller's synchronous setup call.
    """
    commands = json.loads(os.environ.get(SETUP_COMMANDS_ENV_VAR, "[]"))
    run_setup_commands(commands)


def worker_pythonpath(source_dir: str) -> str:
    """Prepend ``source_dir`` to this process's PYTHONPATH for a Ray worker.

    runtime_env env_vars replace rather than extend the keys they set, so the
    inherited value has to be carried over explicitly or the worker loses it.
    """
    inherited = os.environ.get("PYTHONPATH", "")
    if not inherited:
        return source_dir
    if source_dir in inherited.split(os.pathsep):
        return inherited
    return source_dir + os.pathsep + inherited


def _reject_ray_requirement(pip: list[str]) -> None:
    for req in pip:
        name = req.strip().lower()
        if name == "ray" or (
            name.startswith("ray") and len(name) > 3 and name[3] in "=<>!~[ ("
        ):
            raise ValueError(
                f"EnvSpec.pip must not declare ray ({req!r}): the worker "
                "inherits the cluster's ray, and a different version breaks it"
            )


#: Bumped when the overlay layout changes; part of the fingerprint, so envs
#: built by an older layout are ignored (never reused) rather than migrated.
_ENV_LAYOUT_VERSION = 2


def env_fingerprint(spec: "EnvSpec") -> str:
    """Stable key for one pip declaration on one interpreter version."""
    payload = json.dumps({
        "layout": _ENV_LAYOUT_VERSION,
        "pip": list(spec.pip),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
    }, sort_keys=True, ensure_ascii=True)
    return hashlib.sha1(payload.encode()).hexdigest()[:16]


def _create_overlay_venv(env_dir: Path) -> Path:
    """Create a venv that layers on the *server's* environment; return its python.

    ``--system-site-packages`` is deliberately not used: a venv created from
    a venv chains to the base interpreter (for uv-managed pythons, a bare
    CPython with empty site-packages), so the overlay would see nothing —
    not even ray. Inheritance is explicit instead: a ``.pth`` hook runs
    ``site.addsitedir`` on the server venv's site-packages, which appends
    them *after* the overlay's own (declared packages shadow the server's)
    and processes the server's ``.pth`` files, so editable installs like a
    dev checkout of acquirium resolve too.
    """
    result = subprocess.run(
        [sys.executable, "-m", "venv", str(env_dir)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"venv creation failed (exit {result.returncode}): "
            f"{(result.stderr or result.stdout or '').strip()[-2000:]}"
        )
    import sysconfig

    base_site = sysconfig.get_paths()["purelib"]
    site_dirs = list((env_dir / "lib").glob("python*/site-packages"))
    if not site_dirs:
        raise RuntimeError(f"no site-packages dir in created venv {env_dir}")
    (site_dirs[0] / "_acquirium_base_env.pth").write_text(
        f"import site; site.addsitedir({base_site!r})\n"
    )
    return env_dir / "bin" / "python"


def _default_installer(env_dir: Path, pip: list[str]) -> None:
    """Overlay venv + install the declared packages (uv when available)."""
    python = _create_overlay_venv(env_dir)
    uv = shutil.which("uv")
    if uv:
        cmd = [uv, "pip", "install", "--python", str(python), *pip]
    else:
        cmd = [str(python), "-m", "pip", "install", *pip]
    logger.info("building env %s: %s", env_dir.name, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"env install failed (exit {result.returncode}) for {pip}: "
            f"{(result.stderr or result.stdout or '').strip()[-2000:]}"
        )


def ensure_env(
    spec: "EnvSpec | None",
    envs_root: Path | str | None = None,
    *,
    installer: "Callable[[Path, list[str]], None] | None" = None,
) -> str | None:
    """Materialize the persistent env for ``spec.pip``; return its python.

    Idempotent and cheap in steady state (one stat on the ``.ready``
    marker). Builds are guarded by a file lock so concurrent registrations
    of the same declaration wait instead of racing; a failed build removes
    the half-built dir, so the next attempt retries cleanly. ``None`` when
    the spec declares no pip packages — the caller then spawns on the
    inherit path. **Call outside any supervisor lock**: a cold build can
    download for minutes and must never block unrelated drivers/apps.
    """
    if spec is None or not spec.pip:
        return None
    _reject_ray_requirement(spec.pip)
    import fcntl  # POSIX-only, as is Ray's worker model here

    root = Path(envs_root) if envs_root is not None else DEFAULT_ENV_STORAGE_ROOT
    key = env_fingerprint(spec)
    env_dir = root / key
    python = env_dir / "bin" / "python"
    ready = env_dir / ".ready"
    if ready.exists():
        return str(python)

    root.mkdir(parents=True, exist_ok=True)
    with open(root / f"{key}.lock", "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            if ready.exists():
                # Another registration built it while we waited on the lock.
                return str(python)
            if env_dir.exists():
                shutil.rmtree(env_dir)  # half-built leftover from a crash
            try:
                (installer or _default_installer)(env_dir, list(spec.pip))
            except BaseException:
                shutil.rmtree(env_dir, ignore_errors=True)
                raise
            ready.write_text(json.dumps({
                "pip": list(spec.pip),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }))
            return str(python)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def build_runtime_env(
    spec: "EnvSpec | None",
    *,
    source_dir: str | None = None,
    py_executable: str | None = None,
) -> dict[str, Any] | None:
    """Build the Ray runtime_env dict for one app/driver.

    ``None`` in, ``None`` out (plus no ``source_dir``): an undeclared
    environment keeps today's zero-cost inherit-the-server-env path.

    ``py_executable`` is the interpreter of the env :func:`ensure_env`
    materialized for the spec's pip declaration; passing a spec that
    declares pip packages *without* one raises — silently spawning on the
    inherit path would fake isolation.

    ``source_dir`` is a file-spec driver's directory; it lands on the
    worker's PYTHONPATH (merged with the inherited value) so sibling imports
    resolve. A user-supplied ``PYTHONPATH`` in ``env_vars`` is merged the
    same way rather than clobbered.
    """
    if spec is not None and spec.pip and py_executable is None:
        raise ValueError(
            "EnvSpec declares pip packages but no materialized environment "
            "was provided — call ensure_env(spec, ...) first"
        )
    if spec is None and source_dir is None and py_executable is None:
        return None

    env: dict[str, Any] = {}
    env_vars: dict[str, str] = {}

    if py_executable is not None:
        env["py_executable"] = str(py_executable)

    if spec is not None and spec.env_vars:
        env_vars.update({str(k): str(v) for k, v in spec.env_vars.items()})

    if py_executable is not None:
        # Console scripts installed into the overlay (e.g. `idaes` for
        # setup_commands) must be findable by the worker's subprocesses —
        # prepend the overlay's bin to the user-declared or inherited PATH.
        base_path = env_vars.get("PATH") or os.environ.get("PATH", "")
        env_vars["PATH"] = str(Path(py_executable).parent) + os.pathsep + base_path

    if spec is not None and spec.setup_commands:
        env_vars[SETUP_COMMANDS_ENV_VAR] = json.dumps(
            spec.setup_commands, ensure_ascii=True
        )
        env["worker_process_setup_hook"] = SETUP_HOOK

    paths = [p for p in (source_dir, env_vars.get("PYTHONPATH")) if p]
    if paths:
        merged = paths[0]
        for extra in paths[1:]:
            for part in extra.split(os.pathsep):
                if part and part not in merged.split(os.pathsep):
                    merged = merged + os.pathsep + part
        env_vars["PYTHONPATH"] = worker_pythonpath(merged)

    if env_vars:
        env["env_vars"] = env_vars

    if spec is not None and spec.py_modules:
        env["py_modules"] = list(spec.py_modules)

    return env or None
