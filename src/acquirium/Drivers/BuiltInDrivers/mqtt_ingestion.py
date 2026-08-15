# acquirium/mqtt_ingestion.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, TYPE_CHECKING
import ast
import json
import logging

if TYPE_CHECKING:
    # paho-mqtt is an optional runtime dependency (the driver's EnvSpec
    # installs it in its own Ray env). Deferred so importing this module —
    # which happens in the server process at driver start — never requires
    # it; only creating a client does.
    import paho.mqtt.client as mqtt

from acquirium.Drivers.Driver import DriverBufferFull, EventIngestDriver
from acquirium.Storage.values import normalize_value_kind
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_VALUE_KIND,
    HAS_EXTERNAL_REFERENCE,
    MQTT_BROKER,
    MQTT_REFERENCE,
    MQTT_TOPIC,
    TIME_KEY,
    VALUE_KEY,
)

logger = logging.getLogger("acquirium.mqtt")

MQTT_SPARQL_QUERY = f"""
SELECT ?data ?ref ?ref_name ?value_kind ?broker ?topic ?tkey ?vkey
WHERE {{
  ?data <{HAS_EXTERNAL_REFERENCE}> ?ref .
  ?ref a <{MQTT_REFERENCE}> .
  ?ref <{ACQUIRIUM_REF_NAME}> ?ref_name .
  OPTIONAL {{ ?ref <{ACQUIRIUM_VALUE_KIND}> ?value_kind . }}
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
    ref_name: str
    broker: str
    port: int
    topic: str
    time_key: str
    value_key: str
    value_kind: str


class MQTTIngestDriver(EventIngestDriver):
    """Subscribes to MQTT topics declared in the knowledge graph and ingests
    samples via the standard Acquirium timeseries API.

    List it in acquirium.toml under [[drivers]] for auto-start:
        [[drivers]]
        spec      = "acquirium.Drivers.BuiltInDrivers.mqtt_ingestion:MQTTIngestDriver"
        interval  = 5.0
        source_id = "mqtt"
    then run `acquirium server --config acquirium.toml`, or push it to a
        running server with `acquirium driver start acquirium.toml`.
    """

    def setup(self) -> None:
        driver_cfg = self.config.get("driver", {})
        source_id = driver_cfg.get("source_id")
        if not source_id:
            raise ValueError("MQTT ingestion requires driver.source_id")
        self.source_id = str(source_id)
        self.qos: int = int(driver_cfg.get("mqtt_qos", 0))
        self.default_value_kind: str = normalize_value_kind(driver_cfg.get("mqtt_value_kind"))
        logger.debug(
            "mqtt setup source=%s qos=%d default_kind=%s",
            self.source_id,
            self.qos,
            self.default_value_kind,
        )

        self._clients: dict[str, mqtt.Client] = {}
        self._topic_specs: dict[str, dict[str, list[MQTTStreamSpec]]] = {}
        self._spec_keys: set[str] = set()
        self._clients_lock = Lock()

        self._sync_subscriptions()

    def on_graph_change(self) -> None:
        self._sync_subscriptions()

    def stop(self) -> None:
        with self._clients_lock:
            clients = list(self._clients.values())
        # Do not hold _clients_lock while waiting for network loops: an active
        # callback may need the same lock before loop_stop can return.
        for client in clients:
            try:
                client.loop_stop()
            except Exception:
                pass
            try:
                client.disconnect()
            except Exception:
                pass
        with self._clients_lock:
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

        rows = result.get("rows", [])
        logger.debug("mqtt _sync_subscriptions: %d candidate rows from graph", len(rows))
        for row in rows:
            data_uri, ref_uri, ref_name, value_kind, broker, topic, tkey, vkey = row
            topic_s = (topic or "").strip('"')
            if not topic_s:
                continue
            ref_name_s = (ref_name or "").strip('"')
            if not ref_name_s:
                logger.warning("mqtt: skipping ref %s with no acq:refName", ref_uri)
                continue
            expected_ref_uri = str(self.reference_uri(ref_name_s))
            actual_ref_uri = str(ref_uri)
            if actual_ref_uri != expected_ref_uri:
                logger.warning(
                    "mqtt: skipping point=%s ref=%s because canonical ref URI for %s is %s",
                    data_uri,
                    actual_ref_uri,
                    ref_name_s,
                    expected_ref_uri,
                )
                continue
            host, port = parse_mqtt_broker((broker or "localhost").strip('"'))
            spec = MQTTStreamSpec(
                point_uri=str(data_uri),
                ref_uri=actual_ref_uri,
                ref_name=ref_name_s,
                broker=host,
                port=port,
                topic=topic_s,
                time_key=(tkey or "Timestamp").strip('"'),
                value_key=(vkey or "Value").strip('"'),
                value_kind=normalize_value_kind(
                    str(value_kind).strip('"') if value_kind is not None else self.default_value_kind
                ),
            )
            if self._spec_key(spec) in self._spec_keys:
                continue
            try:
                self.declare(ref_name_s, value_kind=spec.value_kind)
                self.register_declared()
            except Exception:
                logger.warning("mqtt: failed to register stream %s", ref_uri, exc_info=True)
                continue
            self._ensure_subscribed(spec)

    def _ensure_subscribed(self, spec: MQTTStreamSpec) -> None:
        client_key = self._client_key(spec.broker, spec.port)

        import paho.mqtt.client as mqtt

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
        return "|".join((
            spec.point_uri,
            spec.ref_uri,
            spec.ref_name,
            spec.broker,
            str(spec.port),
            spec.topic,
            spec.time_key,
            spec.value_key,
            spec.value_kind,
        ))

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

    def decode_payload(self, payload: bytes, spec: MQTTStreamSpec) -> tuple[datetime, Any]:
        """Decode a raw MQTT message into a (timestamp, value) pair for *spec*.

        Override in a subclass to support any wire format (MessagePack,
        Protobuf, binary frames, CSV lines, etc.).  The stream identity is
        already known via ``spec.ref_uri`` — only the observation itself needs
        to be returned.

        Args:
            payload: raw bytes from the broker
            spec:    the MQTTStreamSpec for the stream this message belongs to;
                     use ``spec.time_key`` / ``spec.value_key`` to select fields
                     from the decoded object when the format is dict-shaped

        Returns:
            ``(ts, value)`` where *ts* is a timezone-aware UTC datetime and
            *value* is any type accepted by Acquirium observation insertion.

        Raises:
            ValueError: if the payload cannot be decoded
        """
        text = payload.decode("utf-8", errors="replace")
        payload_dict = decode_mqtt_payload(text)
        raw_ts = payload_dict.get(spec.time_key)
        raw_val = payload_dict.get(spec.value_key)
        ts = parse_mqtt_timestamp(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
        return ts, raw_val

    def _on_message(self, client_key: str):
        def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
            topic = msg.topic
            try:
                logger.debug("mqtt message client=%s topic=%s", client_key, topic)
                with self._clients_lock:
                    specs = list(self._topic_specs.get(client_key, {}).get(topic, []))
                if not specs:
                    return
                for spec in specs:
                    ts, value = self.decode_payload(msg.payload, spec)
                    # Buffered, not inserted: the tick flushes the batch, so a
                    # busy topic costs one round-trip per interval rather than
                    # one per message.
                    self.add(spec.ref_name, value, ts)
            except DriverBufferFull as exc:
                logger.error(
                    "mqtt observation rejected: driver buffer is full "
                    "client=%s topic=%s err=%s",
                    client_key,
                    topic,
                    exc,
                )
            except Exception as exc:
                logger.warning("mqtt decode failed client=%s topic=%s err=%s", client_key, topic, exc)
        return on_message


def parse_mqtt_broker(raw: str) -> tuple[str, int]:
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


def decode_mqtt_payload(payload: str) -> dict[str, Any]:
    """Decode an MQTT payload as a dict.

    Strict JSON is preferred. Python literal dicts are accepted as a fallback
    for older integrations that publish single-quoted payloads.
    """
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
    except Exception as exc:
        logger.warning("mqtt payload decode failed payload=%r error=%s", payload, exc)
        return {}


def parse_mqtt_timestamp(raw: Any) -> datetime:
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
            return parse_mqtt_timestamp(float(text))
        except Exception:
            pass

    raise ValueError(f"Unrecognized timestamp format: {raw!r}")
