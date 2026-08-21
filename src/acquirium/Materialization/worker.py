"""Worker-local immutable definition cache used by local and Ray executors."""
from __future__ import annotations
from typing import Callable, TypeVar
import importlib

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


def load_entrypoint(entrypoint: str):
    """Load trusted local ``module:qualname`` code without serializing objects."""
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
    return target
