"""Persistent driver state management.

Provides a simple key-value store backed by JSON files, allowing drivers
to persist state across restarts (e.g., row offsets, API cursors, checkpoints).
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger("acquirium.driver_state")


class DriverState:
    """Persistent key-value store for driver state.

    State is stored in a JSON file and automatically saved on mutations.
    Thread-safe for concurrent access.

    Example::

        # Store a value
        self.state.set("last_offset", 150)

        # Retrieve a value with default
        offset = self.state.get("last_offset", default=0)

        # Delete a key
        self.state.delete("temp_data")

        # List all keys
        keys = self.state.keys()
    """

    def __init__(self, state_file: Path) -> None:
        """Initialize driver state.

        Args:
            state_file: Path to the JSON file for storing state.
                       Parent directories will be created if needed.
        """
        self._state_file = state_file
        self._lock = threading.Lock()
        self._cache: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Load state from JSON file."""
        if not self._state_file.exists():
            self._cache = {}
            return

        try:
            content = self._state_file.read_text(encoding="utf-8")
            self._cache = json.loads(content)
            if not isinstance(self._cache, dict):
                logger.warning(
                    "State file %s is not a JSON object, resetting state",
                    self._state_file,
                )
                self._cache = {}
        except json.JSONDecodeError:
            logger.error(
                "State file %s contains invalid JSON, resetting state",
                self._state_file,
            )
            self._cache = {}
        except Exception:
            logger.exception("Failed to load state from %s", self._state_file)
            self._cache = {}

    def _save(self) -> None:
        """Save state to JSON file."""
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            content = json.dumps(self._cache, indent=2, default=str)
            self._state_file.write_text(content, encoding="utf-8")
        except Exception:
            logger.exception("Failed to save state to %s", self._state_file)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from state.

        Args:
            key: The state key to retrieve.
            default: Value to return if key doesn't exist.

        Returns:
            The stored value, or default if key not found.
        """
        with self._lock:
            return self._cache.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a value in state.

        Automatically saves to disk after mutation.

        Args:
            key: The state key to set.
            value: The value to store (must be JSON-serializable).
        """
        with self._lock:
            self._cache[key] = value
            self._save()

    def delete(self, key: str) -> bool:
        """Delete a key from state.

        Automatically saves to disk after mutation.

        Args:
            key: The state key to delete.

        Returns:
            True if the key existed and was deleted, False otherwise.
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._save()
                return True
            return False

    def keys(self) -> list[str]:
        """Get all state keys.

        Returns:
            List of all keys in the state store.
        """
        with self._lock:
            return list(self._cache.keys())

    def clear(self) -> None:
        """Clear all state.

        Automatically saves to disk after mutation.
        """
        with self._lock:
            self._cache = {}
            self._save()

    def update(self, data: dict[str, Any]) -> None:
        """Update state with multiple key-value pairs.

        Automatically saves to disk after mutation.

        Args:
            data: Dictionary of key-value pairs to update.
        """
        with self._lock:
            self._cache.update(data)
            self._save()

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in state."""
        with self._lock:
            return key in self._cache

    def __repr__(self) -> str:
        with self._lock:
            return f"DriverState({self._state_file.name}: {len(self._cache)} keys)"
