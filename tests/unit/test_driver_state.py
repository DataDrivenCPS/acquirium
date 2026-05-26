"""Tests for DriverState persistent storage."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from acquirium.DriverState import DriverState


class TestDriverState:
    """Tests for DriverState class."""

    def test_get_set_basic(self, tmp_path: Path) -> None:
        """Test basic get/set operations."""
        state_file = tmp_path / "state.json"
        state = DriverState(state_file)

        state.set("key1", "value1")
        assert state.get("key1") == "value1"

    def test_get_with_default(self, tmp_path: Path) -> None:
        """Test get with default value."""
        state = DriverState(tmp_path / "state.json")

        # Non-existent key returns default
        assert state.get("nonexistent", default="default_val") == "default_val"

        # None is valid default
        assert state.get("also_missing") is None

    def test_set_persists_to_disk(self, tmp_path: Path) -> None:
        """Test that set() writes to disk immediately."""
        state_file = tmp_path / "state.json"
        state = DriverState(state_file)

        state.set("key1", "value1")

        # Verify file was created and contains data
        assert state_file.exists()
        content = json.loads(state_file.read_text())
        assert content["key1"] == "value1"

    def test_persistence_across_instances(self, tmp_path: Path) -> None:
        """Test that state survives driver restart (new instance)."""
        state_file = tmp_path / "state.json"

        # First instance: set value
        state1 = DriverState(state_file)
        state1.set("counter", 42)
        state1.set("name", "test_driver")

        # Second instance: verify value loaded
        state2 = DriverState(state_file)
        assert state2.get("counter") == 42
        assert state2.get("name") == "test_driver"

    def test_delete(self, tmp_path: Path) -> None:
        """Test delete operation."""
        state = DriverState(tmp_path / "state.json")

        state.set("to_delete", "value")
        assert "to_delete" in state

        result = state.delete("to_delete")
        assert result is True
        assert "to_delete" not in state
        assert state.get("to_delete") is None

    def test_delete_nonexistent(self, tmp_path: Path) -> None:
        """Test delete on non-existent key returns False."""
        state = DriverState(tmp_path / "state.json")
        assert state.delete("never_existed") is False

    def test_keys(self, tmp_path: Path) -> None:
        """Test keys() returns all stored keys."""
        state = DriverState(tmp_path / "state.json")

        state.set("a", 1)
        state.set("b", 2)
        state.set("c", 3)

        keys = set(state.keys())
        assert keys == {"a", "b", "c"}

    def test_clear(self, tmp_path: Path) -> None:
        """Test clear() removes all keys."""
        state = DriverState(tmp_path / "state.json")

        state.set("key1", "value1")
        state.set("key2", "value2")

        state.clear()

        assert state.keys() == []
        assert state.get("key1") is None

    def test_update(self, tmp_path: Path) -> None:
        """Test update() with multiple key-value pairs."""
        state = DriverState(tmp_path / "state.json")

        state.update({"a": 1, "b": 2, "c": 3})

        assert state.get("a") == 1
        assert state.get("b") == 2
        assert state.get("c") == 3

    def test_contains(self, tmp_path: Path) -> None:
        """Test __contains__ operator."""
        state = DriverState(tmp_path / "state.json")

        state.set("exists", "value")

        assert "exists" in state
        assert "not_exists" not in state

    def test_repr(self, tmp_path: Path) -> None:
        """Test __repr__ shows state file name and key count."""
        state = DriverState(tmp_path / "my_state.json")
        state.set("k1", "v1")
        state.set("k2", "v2")

        repr_str = repr(state)
        assert "my_state.json" in repr_str
        assert "2 keys" in repr_str

    def test_handles_corrupt_json(self, tmp_path: Path) -> None:
        """Test that corrupt JSON file resets state gracefully."""
        state_file = tmp_path / "state.json"
        state_file.write_text("not valid json {{{")

        # Should not raise, should reset to empty state
        state = DriverState(state_file)
        assert state.keys() == []

    def test_handles_non_object_json(self, tmp_path: Path) -> None:
        """Test that non-object JSON (array, string) resets state."""
        state_file = tmp_path / "state.json"
        state_file.write_text('["not", "an", "object"]')

        state = DriverState(state_file)
        assert state.keys() == []

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        """Test that parent directories are created if needed."""
        state_file = tmp_path / "subdir" / "nested" / "state.json"

        state = DriverState(state_file)
        state.set("key", "value")

        assert state_file.exists()

    def test_complex_types(self, tmp_path: Path) -> None:
        """Test storing complex JSON-serializable types."""
        state = DriverState(tmp_path / "state.json")

        # Dict
        state.set("config", {"timeout": 30, "retries": 3})

        # List
        state.set("items", [1, 2, 3])

        # Nested
        state.set("nested", {"outer": {"inner": "value"}})

        assert state.get("config") == {"timeout": 30, "retries": 3}
        assert state.get("items") == [1, 2, 3]
        assert state.get("nested") == {"outer": {"inner": "value"}}

    def test_rows_seen_pattern(self, tmp_path: Path) -> None:
        """Test the common pattern of storing rows_seen dict."""
        state = DriverState(tmp_path / "state.json")

        # Simulate CSV driver pattern
        rows_seen = {
            "/path/to/file1.csv": 150,
            "/path/to/file2.csv": 42,
        }
        state.set("rows_seen", rows_seen)

        # Reload and verify
        state2 = DriverState(tmp_path / "state.json")
        loaded = state2.get("rows_seen", default={})

        assert loaded == rows_seen
        assert loaded["/path/to/file1.csv"] == 150

    def test_api_cursor_pattern(self, tmp_path: Path) -> None:
        """Test the common pattern of storing API cursor."""
        state = DriverState(tmp_path / "state.json")

        # Simulate API polling driver pattern
        cursor = state.get("api_cursor", default=None)
        assert cursor is None

        state.set("api_cursor", "abc123xyz")

        state2 = DriverState(tmp_path / "state.json")
        assert state2.get("api_cursor") == "abc123xyz"

    def test_thread_safety_basic(self, tmp_path: Path) -> None:
        """Test basic thread safety with concurrent writes."""
        import threading

        state = DriverState(tmp_path / "state.json")
        errors = []

        def writer(thread_id: int) -> None:
            try:
                for i in range(100):
                    state.set(f"thread_{thread_id}_key_{i}", i)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0

    def test_empty_file_initialization(self, tmp_path: Path) -> None:
        """Test handling of empty state file."""
        state_file = tmp_path / "state.json"
        state_file.write_text("")

        state = DriverState(state_file)
        assert state.keys() == []

    def test_nonexistent_file(self, tmp_path: Path) -> None:
        """Test initialization when state file doesn't exist."""
        state_file = tmp_path / "nonexistent.json"

        # Should not raise
        state = DriverState(state_file)
        assert state.keys() == []

        # After set, file should exist
        state.set("key", "value")
        assert state_file.exists()
