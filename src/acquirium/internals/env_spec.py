"""Resolve an :class:`~acquirium.internals.models.EnvSpec` to a Ray runtime_env.

Pure dict construction — no ``ray`` import — so the same builder serves the
server supervisors, the task host, and local (edge) runners, and stays unit
testable without a cluster.

Ray behaviours this module encodes (verified against ray 2.57):

- ``runtime_env["env_vars"]`` is applied per key into the worker's inherited
  environment: unset keys pass through, set keys clobber. ``PYTHONPATH``
  must therefore carry the parent's value explicitly or the worker loses it.
- An unresolved ``${VAR}`` inside a value silently expands to ``""`` in the
  worker — declare such variables explicitly rather than relying on
  substitution.
- A ``pip`` list is installed into a clone of the active venv, cached per
  node by the hash of the list, and rebuilt after every Ray restart. A pip
  list that drags in a different ``ray`` fails the env build, so declaring
  ``ray`` is rejected here with a readable error instead.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from acquirium.internals.models import EnvSpec

logger = logging.getLogger("acquirium.env_spec")

#: Ceiling for building one runtime env. A pip env build clones the venv and
#: downloads packages; the Ray default (10 min) is tight for heavy stacks
#: like WaterTAP on a cold cache.
DEFAULT_SETUP_TIMEOUT_SECONDS = 1800.0

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
                "inherits the cluster's ray, and a different version fails "
                "the environment build"
            )


def build_runtime_env(
    spec: "EnvSpec | None",
    *,
    source_dir: str | None = None,
    setup_timeout_seconds: float = DEFAULT_SETUP_TIMEOUT_SECONDS,
) -> dict[str, Any] | None:
    """Build the Ray runtime_env dict for one app/driver.

    ``None`` in, ``None`` out (plus no ``source_dir``): an undeclared
    environment keeps today's zero-cost inherit-the-server-env path — no
    venv clone, no cache, nothing.

    ``source_dir`` is a file-spec driver's directory; it lands on the
    worker's PYTHONPATH (merged with the inherited value) so sibling imports
    resolve. A user-supplied ``PYTHONPATH`` in ``env_vars`` is merged the
    same way rather than clobbered.
    """
    if spec is None and source_dir is None:
        return None

    env: dict[str, Any] = {}
    env_vars: dict[str, str] = {}

    if spec is not None and spec.env_vars:
        env_vars.update({str(k): str(v) for k, v in spec.env_vars.items()})

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

    if spec is not None and spec.pip:
        _reject_ray_requirement(spec.pip)
        env["pip"] = list(spec.pip)
        # Only pip envs pay a build; the timeout ceiling is theirs.
        env["config"] = {"setup_timeout_seconds": int(setup_timeout_seconds)}

    if spec is not None and spec.py_modules:
        env["py_modules"] = list(spec.py_modules)

    return env or None
