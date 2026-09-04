"""Worker-local immutable definition cache for materialization workers."""
from __future__ import annotations
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, TypeVar
import importlib
import importlib.util
import sys
from threading import Lock

from acquirium.Materialization.definitions import source_digest

T = TypeVar("T")

class DefinitionCache:
    """Cache only digest-addressed immutable loaded definitions."""
    def __init__(self) -> None:
        self._items: dict[str, object] = {}
        self._lock = Lock()
    def load(self, digest: str, loader: Callable[[], T]) -> T:
        with self._lock:
            cached = self._items.get(digest)
            if cached is None:
                # Definitions are content-addressed. Never overwrite one cache
                # entry with mutable worker-local state under the same digest.
                cached = loader()
                self._items[digest] = cached
        return cached  # type: ignore[return-value]
    def clear(self) -> None:
        with self._lock:
            self._items.clear()


def _file_digest(module: Any) -> str | None:
    """Digest a module's file as it currently stands on disk."""
    path = getattr(module, "__file__", None)
    if not path:
        return None
    try:
        return sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return None


def _load_module(module_name: str, search_path: str | None):
    """Import a module, preferring a caller-supplied directory, never stale.

    A module already imported in this process keeps executing the code it was
    first built from, while the file on disk moves on. Since the digest is
    computed from that file, the two can silently disagree — an edited app
    would report results from the code it replaced. Reload whenever the file
    has changed, and load the named file outright when a search path points
    at a different one under the same module name.
    """
    candidate = Path(search_path).resolve() / f"{module_name}.py" if search_path else None
    if candidate is not None and candidate.is_file():
        cached = sys.modules.get(module_name)
        same_file = cached is not None and Path(getattr(cached, "__file__", "") or "").resolve() == candidate
        if cached is None or not same_file or _file_digest(cached) != getattr(cached, "__acquirium_digest__", None):
            spec = importlib.util.spec_from_file_location(module_name, candidate)
            if spec is None or spec.loader is None:
                raise ValueError(f"could not load {candidate}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
        else:
            module = cached
    else:
        if search_path and search_path not in sys.path:
            sys.path.insert(0, search_path)
        module = importlib.import_module(module_name)
        if getattr(module, "__acquirium_digest__", _file_digest(module)) != _file_digest(module):
            module = importlib.reload(module)
    module.__acquirium_digest__ = _file_digest(module)
    return module


def load_entrypoint(entrypoint: str, expected_digest: str | None = None,
                    search_path: str | None = None):
    """Load trusted code and prove it matches its immutable identity.

    ``search_path`` is a directory to look in first, used by dry-run checks so
    an app file that is not otherwise importable by the server can still be
    run. Deployments never pass it: a deployed app must be importable on its
    own, since it is reloaded long after the request that created it.
    """
    try:
        module_name, qualname = entrypoint.split(":", 1)
    except ValueError as error:
        raise ValueError("entrypoint must be module:qualname") from error
    try:
        target = _load_module(module_name, search_path)
    except ModuleNotFoundError as error:
        where = f" or in {search_path!r}" if search_path else ""
        raise ModuleNotFoundError(
            f"the server could not import {module_name!r}{where} for entrypoint "
            f"{entrypoint!r}: {error}. The app must be importable by the server "
            f"process, not only by the client — install it, or put the file in a "
            f"directory the server already imports from (its config directory)."
        ) from error
    for part in qualname.split("."):
        if part == "<locals>":
            raise ValueError("local functions cannot be registered as durable entrypoints")
        target = getattr(target, part)
    if not callable(target):
        raise TypeError(f"entrypoint {entrypoint!r} is not callable")
    # Import paths are mutable deployment configuration; the digest pins the
    # executed code to the durable deployment record.
    actual_digest = source_digest(target)
    if expected_digest is not None and actual_digest != expected_digest:
        raise ValueError(
            f"entrypoint digest mismatch for {entrypoint!r}: "
            f"expected {expected_digest}, found {actual_digest}"
        )
    return target
