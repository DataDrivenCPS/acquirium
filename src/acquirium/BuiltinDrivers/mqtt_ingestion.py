# acquirium/mqtt_ingestion.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any
import ast
import json
import logging

import paho.mqtt.client as mqtt

from acquirium.Driver import Driver
from acquirium.internals.internals_namespaces import (
    HAS_EXTERNAL_REFERENCE, MQTT_BROKER, MQTT_REFERENCE, MQTT_TOPIC, TIME_KEY, VALUE_KEY,
)

logger = logging.getLogger("acquirium.mqtt")

MQTT_SPARQL_QUERY = f"""
SELECT ?data ?ref ?broker ?topic ?tkey ?vkey
WHERE {{
  ?data <{HAS_EXTERNAL_REFERENCE}> ?ref .
  ?ref a <{MQTT_REFERENCE}> .
  OPTIONAL {{ ?ref <{MQTT_BROKER}> ?broker . }}
  OPTIONAL {{ ?ref <{MQTT_TOPIC}> ?topic . }}
  OPTIONAL {{ ?ref <{TIME_KEY}> ?tkey . }}
  OPTIONAL {{ ?ref <{VALUE_KEY}> ?vkey . }}
}}
"""


@dataclass(frozen=True)
class MQTTStreamSpec:
    point_uri: str
    ref_uri: str
    broker: str
    port: int
    topic: str
    time_key: str
    value_key: str


@dataclass(frozen=True)
class _Sample:
    point_uri: str
    ref_uri: str
    ts: datetime
    value: str | None


class MQTTIngestDriver(Driver):
    """Subscribes to MQTT topics declared in the knowledge graph and ingests
    samples via the standard Acquirium timeseries API.

    Run alongside the server:
        acquirium run acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver \\
            --config acquirium.toml

    Or list it in acquirium.toml under [[drivers]] for auto-start:
        [[drivers]]
        spec           = "acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver"
        interval       = 5.0
        mqtt_source_id = "mqtt"
    """

    def setup(self) -> None:
        driver_cfg = self.config.get("driver", {})
        self._source_id: str = driver_cfg.get("mqtt_source_id", "mqtt")
        self.qos: int = int(driver_cfg.get("mqtt_qos", 0))

        self._pending: dict[str, list[_Sample]] = {}
        self._pending_lock = Lock()
        self._clients: dict[str, mqtt.Client] = {}
        self._topic_specs: dict[str, dict[str, list[MQTTStreamSpec]]] = {}
        self._spec_keys: set[str] = set()
        self._clients_lock = Lock()

        self.aq.register_datasource(self._source_id)
        self._sync_subscriptions()

    def on_graph_change(self) -> None:
        self._sync_subscriptions()

    def loop(self) -> None:
        with self._pending_lock:
            if not self._pending:
                return
            batch, self._pending = self._pending, {}

        streams: dict[str, list[tuple[datetime, Any]]] = {
            ref_uri: [(s.ts, s.value) for s in samples]
            for ref_uri, samples in batch.items()
        }
        self.aq.insert_timeseries_batch(self._source_id, streams)

    def stop(self) -> None:
        with self._clients_lock:
            for client in self._clients.values():
                try:
                    client.loop_stop()
                except Exception:
                    pass
                try:
                    client.disconnect()
                except Exception:
                    pass
            self._clients.clear()
            self._topic_specs.clear()
            self._spec_keys.clear()

    # ---- subscription management ----

    def _sync_subscriptions(self) -> None:
        """Query the graph for MQTTReference nodes and subscribe to any new ones."""
        try:
            result = self.aq.client.sparql_query(MQTT_SPARQL_QUERY)
        except Exception:
            logger.warning("mqtt: failed to query graph for subscriptions", exc_info=True)
            return

        for row in result.get("rows", []):
            data_uri, ref_uri, broker, topic, tkey, vkey = row
            topic_s = (topic or "").strip('"')
            if not topic_s:
                continue
            host, port = _parse_mqtt_broker((broker or "localhost").strip('"'))
            spec = MQTTStreamSpec(
                point_uri=str(data_uri),
                ref_uri=str(ref_uri),
                broker=host,
                port=port,
                topic=topic_s,
                time_key=(tkey or "Timestamp").strip('"'),
                value_key=(vkey or "Value").strip('"'),
            )
            if self._spec_key(spec) in self._spec_keys:
                continue
            try:
                self.aq.register_stream(
                    str(data_uri),
                    source_id=self._source_id,
                    ref_name=str(ref_uri),
                )
            except Exception:
                logger.warning("mqtt: failed to register stream %s", ref_uri, exc_info=True)
                continue
            self._ensure_subscribed(spec)

    def _ensure_subscribed(self, spec: MQTTStreamSpec) -> None:
        client_key = self._client_key(spec.broker, spec.port)

        with self._clients_lock:
            client = self._clients.get(client_key)
            if client is None:
                client = mqtt.Client(client_id=f"acquirium_{abs(hash(client_key))}")
                client.enable_logger(logger)
                client.on_connect = self._on_connect(client_key)
                client.on_message = self._on_message(client_key)
                self._clients[client_key] = client
                self._topic_specs[client_key] = {}
                client.connect(spec.broker, spec.port, keepalive=60)
                client.loop_start()
                logger.info("mqtt client created broker=%s port=%d", spec.broker, spec.port)

            self._topic_specs[client_key].setdefault(spec.topic, []).append(spec)
            self._spec_keys.add(self._spec_key(spec))

            try:
                client.subscribe(spec.topic, qos=self.qos)
            except Exception:
                pass

            logger.info(
                "mqtt subscribed point=%s broker=%s port=%d topic=%s",
                spec.point_uri, spec.broker, spec.port, spec.topic,
            )

    # ---- internal ----

    def _client_key(self, broker: str, port: int) -> str:
        return f"{broker}|{port}"

    def _spec_key(self, spec: MQTTStreamSpec) -> str:
        return f"{spec.point_uri}|{spec.ref_uri}|{spec.broker}|{spec.port}|{spec.topic}|{spec.time_key}|{spec.value_key}"

    def _on_connect(self, client_key: str):
        def on_connect(client: mqtt.Client, userdata, flags, rc):
            if rc != 0:
                logger.error("mqtt connect failed client=%s rc=%s", client_key, rc)
                return
            with self._clients_lock:
                topics = list(self._topic_specs.get(client_key, {}).keys())
            if topics:
                client.subscribe([(t, self.qos) for t in topics])
            logger.info("mqtt connected client=%s topics=%d", client_key, len(topics))
        return on_connect

    def _on_message(self, client_key: str):
        def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
            topic = msg.topic
            try:
                payload = msg.payload.decode("utf-8", errors="replace")
                payload_dict = _decode_payload(payload)
                logger.debug("mqtt message client=%s topic=%s", client_key, topic)
                with self._clients_lock:
                    specs = list(self._topic_specs.get(client_key, {}).get(topic, []))
                if not specs:
                    return
                for spec in specs:
                    raw_ts = payload_dict.get(spec.time_key)
                    raw_val = payload_dict.get(spec.value_key)
                    ts = _parse_ts(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
                    val = None if raw_val is None else str(raw_val)
                    sample = _Sample(point_uri=spec.point_uri, ref_uri=spec.ref_uri, ts=ts, value=val)
                    with self._pending_lock:
                        self._pending.setdefault(spec.ref_uri, []).append(sample)
            except Exception as exc:
                logger.warning("mqtt decode failed client=%s topic=%s err=%s", client_key, topic, exc)
        return on_message


def _parse_mqtt_broker(raw: str) -> tuple[str, int]:
    """Split a ref:MQTTBroker literal into (host, port).

    Accepts ``"host"``, ``"host:port"``, or ``"mqtt(s)://host[:port]"``.
    """
    s = raw.strip()
    default_port = 1883
    if "://" in s:
        scheme, _, rest = s.partition("://")
        if scheme.lower() == "mqtts":
            default_port = 8883
        s = rest.split("/", 1)[0]
    if ":" in s:
        host, _, port_str = s.rpartition(":")
        try:
            return host or "localhost", int(port_str)
        except ValueError:
            return s, default_port
    return s or "localhost", default_port


def _decode_payload(payload: str) -> dict[str, Any]:
    payload = payload.strip()
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict):
            return obj
        raise ValueError("not a JSON object")
    except Exception:
        pass
    try:
        obj = ast.literal_eval(payload)
        if isinstance(obj, dict):
            return obj
        raise ValueError("not a dict literal")
    except Exception as e:
        logger.warning("mqtt payload decode failed payload=%r error=%s", payload, e)
        return {}


def _parse_ts(raw: Any) -> datetime:
    """Parse a timestamp into a timezone-aware UTC datetime."""
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=timezone.utc)
        return raw.astimezone(timezone.utc)

    if isinstance(raw, (int, float)):
        ts = float(raw)
        if ts > 1e11:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            raise ValueError("Empty timestamp string")
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
        try:
            return _parse_ts(float(text))
        except Exception:
            pass

    raise ValueError(f"Unrecognized timestamp format: {raw!r}")
