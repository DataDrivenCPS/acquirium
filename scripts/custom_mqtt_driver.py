"""Custom MQTT ingest driver with pluggable message encoding.

Subclasses MQTTIngestDriver and overrides decode_payload() to handle any wire
format (MessagePack, binary frames, CSV lines, etc.) without touching the
subscription or insertion logic.

Usage in acquirium.toml:
    [[drivers]]
    spec           = "scripts.custom_mqtt_driver:MyCustomMQTTIngestDriver"
    interval       = 5.0
    mqtt_source_id = "mqtt"
"""
from __future__ import annotations

import struct
from datetime import datetime, timezone
from typing import Any

from acquirium.BuiltinDrivers.mqtt_ingestion import MQTTIngestDriver, MQTTStreamSpec


class MyCustomMQTTIngestDriver(MQTTIngestDriver):
    """Custom MQTT ingest driver that decodes MessagePack payloads.
    Expects each message to be a map with at least the keys named by spec.time_key and spec.value_key.

    Requires: pip install msgpack
    """

    def decode_payload(self, payload: bytes, spec: MQTTStreamSpec) -> tuple[datetime, Any]:
        import msgpack
        obj = msgpack.unpackb(payload, raw=False)
        if not isinstance(obj, dict):
            raise ValueError(f"msgpack payload is not a map: {type(obj)}")
        raw_ts = obj.get(spec.time_key)
        raw_val = obj.get(spec.value_key)
        ts = _parse_ts(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
        return ts, raw_val



def _parse_ts(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):
        v = float(raw)
        if v > 1e11:
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=timezone.utc)
    if isinstance(raw, str):
        text = raw.strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        return datetime.fromtimestamp(float(text), tz=timezone.utc)
    raise ValueError(f"unrecognized timestamp: {raw!r}")
