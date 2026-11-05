# acquirium/mqtt_ingestion.py
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Callable
import ast
import hashlib
import json
import logging
import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

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


@dataclass
class _PendingMessage:
    observations: deque[tuple[str, datetime, Any]]
    acknowledge: Callable[[], None]
    qos: int


class _Window:
    """Messages the broker has sent one client that are not yet acknowledged.

    While all ``size`` slots are taken the broker holds back further messages,
    so a window that stays full means a backlog is building on the broker.
    """

    def __init__(self, size: int) -> None:
        self.size = size
        self._unacked = 0
        self._lock = Lock()
        self._full_since: float | None = None
        self._full_time = 0.0
        self._sampled = time.monotonic()

    def received(self) -> None:
        with self._lock:
            self._unacked += 1
            if self._unacked >= self.size and self._full_since is None:
                self._full_since = time.monotonic()

    def acked(self) -> None:
        with self._lock:
            self._unacked -= 1
            if self._full_since is not None and self._unacked < self.size:
                self._full_time += time.monotonic() - self._full_since
                self._full_since = None

    def sample(self) -> tuple[float, float]:
        """Return the seconds since the last sample and the share of them
        the window spent full."""
        with self._lock:
            now = time.monotonic()
            full = self._full_time
            if self._full_since is not None:
                full += now - self._full_since
                self._full_since = now
            elapsed, self._sampled, self._full_time = now - self._sampled, now, 0.0
        return elapsed, (full / elapsed if elapsed > 0 else 0.0)


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

    #: Least seconds between checks that the driver is keeping up. Checks run
    #: on ticks, so a longer ``interval`` spaces them further.
    lag_period: float = 30.0
    #: Share of a period with a full window that counts as falling behind.
    lag_share: float = 0.5

    def setup(self) -> None:
        driver_cfg = self.config.get("driver", {})
        source_id = driver_cfg.get("source_id")
        if not source_id:
            raise ValueError("MQTT ingestion requires driver.source_id")
        self.source_id = str(source_id)
        # QoS 1 with a persistent session: the broker keeps each message until
        # it is acknowledged, which happens only once the server accepts it.
        self.qos: int = int(driver_cfg.get("mqtt_qos", 1))
        self.client_id: str | None = driver_cfg.get("mqtt_client_id")
        # Unacknowledged messages the broker may send at once (MQTT 5 Receive
        # Maximum, at most 65535). Each round trip to the server acknowledges
        # up to this many, so it bounds throughput, and the client sets it, so
        # it needs no broker change; brokers otherwise default to about 20.
        self.receive_maximum: int = int(driver_cfg.get("mqtt_receive_maximum", 65535))
        # How long the broker keeps the session, and queues for it, while the
        # driver is away.
        self.session_expiry: int = int(driver_cfg.get("mqtt_session_expiry", 7 * 24 * 3600))
        self.default_value_kind: str = normalize_value_kind(driver_cfg.get("mqtt_value_kind"))
        logger.debug(
            "mqtt setup source=%s qos=%d default_kind=%s",
            self.source_id,
            self.qos,
            self.default_value_kind,
        )

        self._clients: dict[str, mqtt.Client] = {}
        self._windows: dict[str, _Window] = {}
        self._behind: set[str] = set()
        self._lag_checked = time.monotonic()
        self._topic_specs: dict[str, dict[str, list[MQTTStreamSpec]]] = {}
        self._spec_keys: set[str] = set()
        self._clients_lock = Lock()
        # QoS messages that do not yet fit in the observation buffer still
        # occupy broker receive slots, which bound this queue. Keep decoded
        # times and only the unsent rows when one message feeds many streams.
        self._messages_lock = Lock()
        self._messages: deque[_PendingMessage] = deque()

        self._sync_subscriptions()

    def on_graph_change(self) -> None:
        self._sync_subscriptions()

    def tick(self) -> None:
        self._check_lag()
        super().tick()

    def flush(self) -> dict[str, Any]:
        with self._messages_lock:
            self._buffer_messages()
        result = super().flush()
        with self._messages_lock:
            if self._messages:
                self._added.set()
        return result

    def _buffer_messages(self) -> None:
        """Fill the observation buffer, leaving overflow for the next flush.

        Called with ``_messages_lock`` held. A partially buffered message is
        acknowledged only after its final row has been accepted.
        """
        while self._messages:
            pending = self._messages[0]
            try:
                while pending.observations:
                    name, ts, value = pending.observations[0]
                    self.add(name, value, ts)
                    pending.observations.popleft()
            except DriverBufferFull:
                if pending.qos:
                    return
                logger.warning("mqtt: dropping QoS 0 readings because the driver buffer is full")
            else:
                self.when_accepted(pending.acknowledge)
            self._messages.popleft()

    def _check_lag(self) -> None:
        """Warn about each broker whose window stayed full: the broker is
        queueing messages for this driver, and drops them past its limit."""
        if time.monotonic() - self._lag_checked < self.lag_period:
            return
        self._lag_checked = time.monotonic()
        with self._clients_lock:
            windows = list(self._windows.items())
        for client_key, window in windows:
            elapsed, share = window.sample()
            if share >= self.lag_share:
                self._behind.add(client_key)
                logger.warning(
                    "mqtt falling behind client=%s: all %d receive slots were taken "
                    "for %.0f%% of the last %.0fs, so the broker is queueing messages "
                    "and drops those past its queue limit; check that the server "
                    "accepts inserts, or raise mqtt_receive_maximum",
                    client_key, window.size, share * 100, elapsed,
                )
            elif client_key in self._behind:
                self._behind.discard(client_key)
                logger.info("mqtt caught up client=%s", client_key)

    def stop(self) -> None:
        # Send what has arrived and acknowledge it while still connected;
        # anything later stays unacknowledged and is redelivered next session.
        try:
            self.flush()
        except Exception:
            logger.warning("mqtt: flush before disconnect failed", exc_info=True)
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
            self._windows.clear()
            self._topic_specs.clear()
            self._spec_keys.clear()
        with self._messages_lock:
            # QoS messages left here remain unacknowledged on the broker.
            self._messages.clear()

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
        specs: list[MQTTStreamSpec] = []
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
            specs.append(spec)
        self._subscribe(specs)

    def _subscribe(self, specs: list[MQTTStreamSpec]) -> None:
        """Register every spec before a new client connects. A persistent
        session's queued messages arrive on connect, and one for a topic with
        no spec yet would be acknowledged and dropped."""
        connecting = []
        with self._clients_lock:
            for spec in specs:
                client_key = self._client_key(spec.broker, spec.port)
                if client_key not in self._clients:
                    client = mqtt.Client(
                        mqtt.CallbackAPIVersion.VERSION2,
                        client_id=self._session_id(client_key),
                        protocol=mqtt.MQTTv5,
                        manual_ack=True,
                    )
                    client.enable_logger(logger)
                    self._windows[client_key] = _Window(self.receive_maximum)
                    client.on_connect = self._on_connect(client_key)
                    client.on_message = self._on_message(client_key)
                    self._clients[client_key] = client
                    self._topic_specs[client_key] = {}
                    connecting.append((client, spec))
                self._topic_specs[client_key].setdefault(spec.topic, []).append(spec)
                self._spec_keys.add(self._spec_key(spec))
            clients = {spec: self._clients[self._client_key(spec.broker, spec.port)] for spec in specs}
        for client, spec in connecting:
            # on_connect subscribes to every registered topic.
            properties = Properties(PacketTypes.CONNECT)
            properties.ReceiveMaximum = self.receive_maximum
            properties.SessionExpiryInterval = self.session_expiry
            client.connect(
                spec.broker, spec.port, keepalive=60, clean_start=False, properties=properties,
            )
            client.loop_start()
            logger.info("mqtt client created broker=%s port=%d", spec.broker, spec.port)
        for spec, client in clients.items():
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

    def _session_id(self, client_key: str) -> str:
        """A broker keeps one session per client ID, so the ID must be the same
        on every start and differ from other drivers' on the same broker."""
        if self.client_id:
            return str(self.client_id)
        digest = hashlib.sha256(f"{self.source_id}|{client_key}".encode()).hexdigest()
        return f"acquirium-{digest[:12]}"

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
        def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties):
            if reason_code.is_failure:
                logger.error("mqtt connect failed client=%s reason=%s", client_key, reason_code)
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
        window = self._windows[client_key]

        def acknowledge(client: mqtt.Client, msg: mqtt.MQTTMessage) -> None:
            client.ack(msg.mid, msg.qos)
            if msg.qos:
                window.acked()

        def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
            topic = msg.topic
            if msg.qos:
                # A QoS 0 message takes no slot.
                window.received()
            observations = deque()
            try:
                logger.debug("mqtt message client=%s topic=%s", client_key, topic)
                with self._clients_lock:
                    specs = list(self._topic_specs.get(client_key, {}).get(topic, []))
                for spec in specs:
                    ts, value = self.decode_payload(msg.payload, spec)
                    observations.append((spec.ref_name, ts, value))
            except Exception as exc:
                logger.warning("mqtt decode failed client=%s topic=%s err=%s", client_key, topic, exc)
            with self._messages_lock:
                if not msg.qos and self._messages:
                    logger.warning("mqtt: dropping QoS 0 readings while waiting for buffer space")
                    return
                self._messages.append(_PendingMessage(
                    observations, lambda: acknowledge(client, msg), msg.qos,
                ))
                self._buffer_messages()
            self._added.set()
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
