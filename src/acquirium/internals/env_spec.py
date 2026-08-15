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

import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from acquirium.internals.models import EnvSpec

#: Ceiling for building one runtime env. A pip env build clones the venv and
#: downloads packages; the Ray default (10 min) is tight for heavy stacks
#: like WaterTAP on a cold cache.
DEFAULT_SETUP_TIMEOUT_SECONDS = 1800.0


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
