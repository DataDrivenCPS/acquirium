"""Tests for shared MQTT payload, broker, and timestamp parsing."""

import pytest
from datetime import datetime, timezone, timedelta

from acquirium.Drivers.BuiltInDrivers.mqtt_ingestion import (
    decode_mqtt_payload,
    parse_mqtt_broker,
    parse_mqtt_timestamp,
)


# ── decode_mqtt_payload ────────────────────────────────────


class TestDecodePayload:
    def test_valid_json_dict(self):
        result = decode_mqtt_payload('{"time": 1, "value": 42}')
        assert result == {"time": 1, "value": 42}

    def test_python_literal_dict(self):
        result = decode_mqtt_payload("{'time': 1, 'value': 42}")
        assert result == {"time": 1, "value": 42}

    def test_json_array_returns_empty(self):
        result = decode_mqtt_payload("[1, 2, 3]")
        assert result == {}

    def test_garbage_returns_empty(self):
        result = decode_mqtt_payload("not valid at all")
        assert result == {}

    def test_whitespace_stripped(self):
        result = decode_mqtt_payload('  {"a": 1}  ')
        assert result == {"a": 1}

    def test_nested_dict(self):
        result = decode_mqtt_payload('{"data": {"value": 99}, "ts": 1000}')
        assert result["data"]["value"] == 99

    def test_empty_dict(self):
        result = decode_mqtt_payload("{}")
        assert result == {}


# ── parse_mqtt_broker ──────────────────────────────────────


class TestParseMqttBroker:
    def test_host_defaults_to_mqtt_port(self):
        assert parse_mqtt_broker("broker.local") == ("broker.local", 1883)

    def test_host_port(self):
        assert parse_mqtt_broker("broker.local:1884") == ("broker.local", 1884)

    def test_mqtts_url_defaults_to_tls_port(self):
        assert parse_mqtt_broker("mqtts://broker.local/path") == ("broker.local", 8883)

    def test_empty_defaults_to_localhost(self):
        assert parse_mqtt_broker("") == ("localhost", 1883)


# ── parse_mqtt_timestamp ───────────────────────────────────


class TestParseTimestamp:
    def test_iso_with_z(self):
        result = parse_mqtt_timestamp("2025-06-15T10:30:00Z")
        assert result == datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_iso_naive(self):
        result = parse_mqtt_timestamp("2025-06-15T10:30:00")
        assert result.tzinfo == timezone.utc
        assert result.year == 2025

    def test_unix_seconds(self):
        ts = 1718450000
        result = parse_mqtt_timestamp(ts)
        assert result.tzinfo == timezone.utc
        expected = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert result == expected

    def test_unix_milliseconds(self):
        ts_ms = 1718450000000
        result = parse_mqtt_timestamp(ts_ms)
        assert result.tzinfo == timezone.utc
        expected = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        assert result == expected

    def test_datetime_naive(self):
        dt = datetime(2025, 6, 15, 10, 30, 0)
        result = parse_mqtt_timestamp(dt)
        assert result.tzinfo == timezone.utc
        assert result.replace(tzinfo=None) == dt

    def test_datetime_aware_non_utc(self):
        eastern = timezone(timedelta(hours=-5))
        dt = datetime(2025, 6, 15, 10, 30, 0, tzinfo=eastern)
        result = parse_mqtt_timestamp(dt)
        assert result.tzinfo == timezone.utc
        assert result == dt.astimezone(timezone.utc)

    def test_sql_format(self):
        result = parse_mqtt_timestamp("2025-06-15 10:30:00")
        assert result.tzinfo == timezone.utc
        assert result.year == 2025
        assert result.hour == 10

    def test_numeric_string(self):
        result = parse_mqtt_timestamp("1718450000")
        assert result.tzinfo == timezone.utc

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            parse_mqtt_timestamp("")

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unrecognized"):
            parse_mqtt_timestamp(object())

    def test_float_epoch(self):
        ts = 1718450000.5
        result = parse_mqtt_timestamp(ts)
        assert result.tzinfo == timezone.utc
