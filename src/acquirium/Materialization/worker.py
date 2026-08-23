"""Worker-local immutable definition cache used by local and Ray executors."""
from __future__ import annotations
from typing import Callable, TypeVar
import importlib

from acquirium.Materialization.definitions import source_digest

T = TypeVar("T")

class DefinitionCache:
    """Cache only digest-addressed immutable loaded definitions."""
    def __init__(self) -> None:
        self._items: dict[str, object] = {}
    def load(self, digest: str, loader: Callable[[], T]) -> T:
        cached = self._items.get(digest)
        if cached is None:
            cached = loader()
            self._items[digest] = cached
        return cached  # type: ignore[return-value]
    def clear(self) -> None:
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
    actual_digest = source_digest(target)
    if expected_digest is not None and actual_digest != expected_digest:
        raise ValueError(
            f"entrypoint digest mismatch for {entrypoint!r}: "
            f"expected {expected_digest}, found {actual_digest}"
        )
    return target
