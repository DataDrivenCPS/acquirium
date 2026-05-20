"""Tests for Driver state integration and identifier derivation."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from acquirium.Driver import Driver, _sanitize_filename


class MockAcquirium:
    """Mock Acquirium client for testing."""

    pass


class MockDriver(Driver):
    """Mock driver for testing state initialization."""

    def setup(self) -> None:
        pass

    def tick(self) -> None:
        pass


class TestSanitizeFilename:
    """Tests for _sanitize_filename helper."""

    def test_simple_name(self) -> None:
        """Test simple alphanumeric name."""
        assert _sanitize_filename("my-driver") == "my-driver"

    def test_spaces_replaced(self) -> None:
        """Test spaces are replaced with underscores."""
        assert _sanitize_filename("my driver") == "my_driver"

    def test_colon_replaced(self) -> None:
        """Test colons are replaced."""
        assert _sanitize_filename("module:Driver") == "module_Driver"

    def test_pipe_replaced(self) -> None:
        """Test pipe characters are replaced."""
        assert _sanitize_filename("driver|name") == "driver_name"

    def test_question_mark_replaced(self) -> None:
        """Test question marks are replaced."""
        assert _sanitize_filename("driver?name") == "driver_name"

    def test_asterisk_replaced(self) -> None:
        """Test asterisks are replaced."""
        assert _sanitize_filename("driver*name") == "driver_name"

    def test_backslash_replaced(self) -> None:
        """Test backslashes are replaced."""
        assert _sanitize_filename("driver\\name") == "driver_name"

    def test_less_than_replaced(self) -> None:
        """Test less-than signs are replaced."""
        assert _sanitize_filename("driver<name") == "driver_name"

    def test_greater_than_replaced(self) -> None:
        """Test greater-than signs are replaced."""
        assert _sanitize_filename("driver>name") == "driver_name"

    def test_pipe_replaced(self) -> None:
        """Test pipe characters are replaced."""
        assert _sanitize_filename("driver|name") == "driver_name"

    def test_mixed_unsafe_chars(self) -> None:
        """Test multiple unsafe characters."""
        assert _sanitize_filename("module:Driver-Class") == "module_Driver-Class"


class TestDriverStateIntegration:
    """Tests for Driver state initialization."""

    def test_state_initialized_with_driver_id(self, tmp_path: Path) -> None:
        """Test state file uses explicit driver_id when provided."""
        config = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "test.module:MockDriver",
                "driver_id": "my-custom-driver",
            },
        }

        driver = MockDriver(MockAcquirium(), config)

        # Verify state file path
        expected_path = tmp_path / ".acquirium" / "drivers" / "my-custom-driver.json"
        assert driver.state._state_file == expected_path

    def test_state_initialized_without_driver_id(self, tmp_path: Path) -> None:
        """Test state file is derived from spec when driver_id not provided."""
        config = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "test.module:MockDriver",
            },
        }

        driver = MockDriver(MockAcquirium(), config)

        # Verify state file exists in correct location
        state_file = driver.state._state_file
        assert state_file.parent == tmp_path / ".acquirium" / "drivers"
        assert state_file.suffix == ".json"

        # Verify filename contains class name
        assert "MockDriver" in state_file.stem

    def test_state_file_created_on_first_set(self, tmp_path: Path) -> None:
        """Test state file is created when first value is set."""
        config = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "test.module:MockDriver",
                "driver_id": "test-driver",
            },
        }

        driver = MockDriver(MockAcquirium(), config)

        # File shouldn't exist yet
        assert not driver.state._state_file.exists()

        # Set a value
        driver.state.set("key", "value")

        # File should now exist
        assert driver.state._state_file.exists()

    def test_state_persists_across_driver_instances(self, tmp_path: Path) -> None:
        """Test state survives across driver restarts (new instances)."""
        config = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "test.module:MockDriver",
                "driver_id": "persistent-driver",
            },
        }

        # First driver instance
        driver1 = MockDriver(MockAcquirium(), config)
        driver1.state.set("counter", 42)

        # Second driver instance (simulating restart)
        driver2 = MockDriver(MockAcquirium(), config)
        assert driver2.state.get("counter") == 42

    def test_state_isolated_between_drivers(self, tmp_path: Path) -> None:
        """Test different drivers have isolated state files."""
        config1 = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "test.module:Driver1",
                "driver_id": "driver-one",
            },
        }

        config2 = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "test.module:Driver2",
                "driver_id": "driver-two",
            },
        }

        driver1 = MockDriver(MockAcquirium(), config1)
        driver2 = MockDriver(MockAcquirium(), config2)

        driver1.state.set("key", "value1")
        driver2.state.set("key", "value2")

        # Each driver should only see its own value
        assert driver1.state.get("key") == "value1"
        assert driver2.state.get("key") == "value2"

    def test_state_with_relative_config_dir(self, tmp_path: Path) -> None:
        """Test state file path works with relative config directory."""
        # Create a subdirectory to use as config dir
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        config = {
            "__config_dir": str(config_dir),
            "driver": {
                "spec": "test.module:MockDriver",
                "driver_id": "test-driver",
            },
        }

        driver = MockDriver(MockAcquirium(), config)

        expected_path = config_dir / ".acquirium" / "drivers" / "test-driver.json"
        assert driver.state._state_file == expected_path

    def test_state_loads_existing_data(self, tmp_path: Path) -> None:
        """Test state loads existing data from file on initialization."""
        # Create state file with pre-existing data
        state_dir = tmp_path / ".acquirium" / "drivers"
        state_dir.mkdir(parents=True)
        state_file = state_dir / "preload-driver.json"
        state_file.write_text('{"counter": 100, "cursor": "abc123"}')

        config = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "test.module:MockDriver",
                "driver_id": "preload-driver",
            },
        }

        driver = MockDriver(MockAcquirium(), config)

        # Verify data was loaded
        assert driver.state.get("counter") == 100
        assert driver.state.get("cursor") == "abc123"

    def test_state_default_spec_hash(self, tmp_path: Path) -> None:
        """Test that spec hash is used when no driver_id is provided."""
        config = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver",
            },
        }

        # We need a real driver class for this test
        from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver

        # Create a minimal mock for the Acquirium client
        class MockAQ:
            pass

        driver = CSVIngestDriver(MockAQ(), config)

        # Verify state file was created
        state_file = driver.state._state_file
        assert state_file.parent == tmp_path / ".acquirium" / "drivers"
        assert state_file.suffix == ".json"
        assert "CSVIngestDriver" in state_file.stem


class TestDriverIdentifierDerivation:
    """Tests for driver identifier derivation logic."""

    def test_spec_hash_is_deterministic(self, tmp_path: Path) -> None:
        """Test that the same spec always produces the same hash."""
        spec = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
        expected_hash = hashlib.sha256(spec.encode()).hexdigest()[:16]

        config1 = {
            "__config_dir": str(tmp_path),
            "driver": {"spec": spec},
        }

        config2 = {
            "__config_dir": str(tmp_path),
            "driver": {"spec": spec},
        }

        from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver

        class MockAQ:
            pass

        driver1 = CSVIngestDriver(MockAQ(), config1)
        driver2 = CSVIngestDriver(MockAQ(), config2)

        # Both should have the same hash in their filename
        assert expected_hash in driver1.state._state_file.stem
        assert expected_hash in driver2.state._state_file.stem

    def test_different_specs_different_hashes(self, tmp_path: Path) -> None:
        """Test that different specs produce different hashes."""
        spec1 = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
        spec2 = "acquirium.BuiltinDrivers.xlsx_ingest:XLSXIngestDriver"

        config1 = {
            "__config_dir": str(tmp_path),
            "driver": {"spec": spec1},
        }

        config2 = {
            "__config_dir": str(tmp_path),
            "driver": {"spec": spec2},
        }

        from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver
        from acquirium.BuiltinDrivers.xlsx_ingest import XLSXIngestDriver

        class MockAQ:
            pass

        driver1 = CSVIngestDriver(MockAQ(), config1)
        driver2 = XLSXIngestDriver(MockAQ(), config2)

        # Filenames should be different
        assert driver1.state._state_file.name != driver2.state._state_file.name

    def test_driver_id_takes_precedence_over_spec(self, tmp_path: Path) -> None:
        """Test that driver_id is used instead of spec hash when provided."""
        config = {
            "__config_dir": str(tmp_path),
            "driver": {
                "spec": "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver",
                "driver_id": "explicit-name",
            },
        }

        from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver

        class MockAQ:
            pass

        driver = CSVIngestDriver(MockAQ(), config)

        # Should use explicit name, not hash
        assert driver.state._state_file.stem == "explicit-name"
