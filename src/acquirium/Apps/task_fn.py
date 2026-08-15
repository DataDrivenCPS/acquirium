"""Ship a task's function to the server and load it back.

A task is a plain function ``fn(ctx) -> list[Output]``. Two representations
travel with it:

- ``fn_source`` — ``inspect.getsource(fn)``, the **authoritative** persisted
  form. Rehydrated by exec'ing it in a namespace that already provides the
  acquirium public API, so a task body written in a notebook against
  ``from acquirium import Output`` works unchanged. Any other module the
  body needs must be imported *inside* the function (tasks carry no
  dependencies beyond the acquirium package by contract).
- ``fn_blob`` — ``cloudpickle.dumps(fn)``, a fast path that also carries
  closures and helper functions defined alongside. cloudpickle is not
  portable across Python versions and is documented as unfit for long-term
  storage, so the blob is used only when the server's interpreter matches
  ``python_version`` and falls back to the source otherwise.

The pickler is Ray's vendored cloudpickle (ray is already a dependency;
no standalone cloudpickle is required), imported lazily so shipping from a
client never initializes a cluster.
"""
from __future__ import annotations

import inspect
import sys
import textwrap
from typing import Any, Callable


def python_version() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def _cloudpickle():
    from ray import cloudpickle
    return cloudpickle


def ship_function(fn: Callable) -> dict[str, Any]:
    """Return ``{fn_name, fn_source, fn_blob, python_version}`` for ``fn``.

    Raises ``ValueError`` when the source is unobtainable (lambdas, functions
    defined in a REPL without history, C functions): the source is the
    contract for surviving a server upgrade, so registration must not
    succeed without it.
    """
    if not callable(fn) or not hasattr(fn, "__name__") or fn.__name__ == "<lambda>":
        raise ValueError("a task must be a named function, not a lambda or callable object")
    try:
        source = textwrap.dedent(inspect.getsource(fn))
    except (OSError, TypeError) as exc:
        raise ValueError(
            f"could not read the source of {fn.__name__!r}: {exc}. Tasks are "
            "persisted as source; define the function in a file or a "
            "notebook cell rather than an interactive prompt."
        ) from exc
    try:
        blob: bytes | None = _cloudpickle().dumps(fn)
    except Exception:
        blob = None
    return {
        "fn_name": fn.__name__,
        "fn_source": source,
        "fn_blob": blob,
        "python_version": python_version(),
    }


def _source_namespace() -> dict[str, Any]:
    """The names a task body may use without importing them itself."""
    import acquirium
    from acquirium import Output
    from acquirium.Client.explore.attributes import Not

    return {"__name__": "acquirium_task", "acquirium": acquirium, "Output": Output, "Not": Not}


def load_function(
    *,
    fn_name: str,
    fn_source: str,
    fn_blob: bytes | None = None,
    blob_python_version: str | None = None,
) -> Callable:
    """Rebuild a task function, preferring the blob when it is safe to use."""
    if fn_blob is not None and blob_python_version == python_version():
        try:
            fn = _cloudpickle().loads(fn_blob)
            if callable(fn):
                return fn
        except Exception:
            pass  # fall back to the source form
    namespace = _source_namespace()
    exec(compile(fn_source, f"<task {fn_name}>", "exec"), namespace)
    fn = namespace.get(fn_name)
    if not callable(fn):
        raise ValueError(f"task source did not define a function named {fn_name!r}")
    return fn
