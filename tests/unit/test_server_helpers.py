"""Tests for server helper functions in acquirium.Server.app."""

import pytest
from datetime import datetime, timezone

from acquirium.Server.app import _parse_dt


class TestParseDt:
    def test_none_returns_none(self):
        assert _parse_dt(None) is None

    def test_iso_with_z(self):
        result = _parse_dt("2025-12-17T10:30:00Z")
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 12
        assert result.hour == 10

    def test_iso_with_offset(self):
        result = _parse_dt("2025-12-17T10:30:00+05:00")
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_invalid_raises(self):
        with pytest.raises(Exception):
            _parse_dt("not-a-date")
