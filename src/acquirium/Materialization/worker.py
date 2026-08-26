"""Worker-local immutable definition cache for materialization workers."""
from __future__ import annotations
from typing import Callable, TypeVar
import importlib
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


def load_entrypoint(entrypoint: str, expected_digest: str | None = None):
    """Load trusted code and prove it matches its immutable identity."""
    try:
        module_name, qualname = entrypoint.split(":", 1)
    except ValueError as error:
        raise ValueError("entrypoint must be module:qualname") from error
    target = importlib.import_module(module_name)
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
