"""Worker-local immutable definition cache used by local and Ray executors."""
from __future__ import annotations
from typing import Callable, TypeVar

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
