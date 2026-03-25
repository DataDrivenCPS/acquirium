"""Tests for MQTT payload/timestamp parsing in acquirium.Server.mqtt_ingestion."""

import pytest
from datetime import datetime, timezone, timedelta

from acquirium.Server.mqtt_ingestion import _decode_payload, _parse_ts


# ── _decode_payload ────────────────────────────────────────


class TestDecodePayload:
    def test_valid_json_dict(self):
        result = _decode_payload('{"time": 1, "value": 42}')
        assert result == {"time": 1, "value": 42}

    def test_python_literal_dict(self):
        result = _decode_payload("{'time': 1, 'value': 42}")
        assert result == {"time": 1, "value": 42}

    def test_json_array_returns_empty(self):
        result = _decode_payload("[1, 2, 3]")
        assert result == {}

    def test_garbage_returns_empty(self):
        result = _decode_payload("not valid at all")
        assert result == {}

    def test_whitespace_stripped(self):
        result = _decode_payload('  {"a": 1}  ')
        assert result == {"a": 1}

    def test_nested_dict(self):
        result = _decode_payload('{"data": {"value": 99}, "ts": 1000}')
        assert result["data"]["value"] == 99

    def test_empty_dict(self):
        result = _decode_payload("{}")
        assert result == {}


# ── _parse_ts ──────────────────────────────────────────────


class TestParseTimestamp:
    def test_iso_with_z(self):
        result = _parse_ts("2025-06-15T10:30:00Z")
        assert result == datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_iso_naive(self):
        result = _parse_ts("2025-06-15T10:30:00")
        assert result.tzinfo == timezone.utc
        assert result.year == 2025

    def test_unix_seconds(self):
        ts = 1718450000
        result = _parse_ts(ts)
        assert result.tzinfo == timezone.utc
        expected = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert result == expected

    def test_unix_milliseconds(self):
        ts_ms = 1718450000000
        result = _parse_ts(ts_ms)
        assert result.tzinfo == timezone.utc
        expected = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        assert result == expected

    def test_datetime_naive(self):
        dt = datetime(2025, 6, 15, 10, 30, 0)
        result = _parse_ts(dt)
        assert result.tzinfo == timezone.utc
        assert result.replace(tzinfo=None) == dt

    def test_datetime_aware_non_utc(self):
        eastern = timezone(timedelta(hours=-5))
        dt = datetime(2025, 6, 15, 10, 30, 0, tzinfo=eastern)
        result = _parse_ts(dt)
        assert result.tzinfo == timezone.utc
        assert result == dt.astimezone(timezone.utc)

    def test_sql_format(self):
        result = _parse_ts("2025-06-15 10:30:00")
        assert result.tzinfo == timezone.utc
        assert result.year == 2025
        assert result.hour == 10

    def test_numeric_string(self):
        result = _parse_ts("1718450000")
        assert result.tzinfo == timezone.utc

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _parse_ts("")

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unrecognized"):
            _parse_ts(object())

    def test_float_epoch(self):
        ts = 1718450000.5
        result = _parse_ts(ts)
        assert result.tzinfo == timezone.utc
