# acquirium/mqtt_ingest.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from queue import Queue, Empty
from threading import Thread, Event, Lock
from typing import Any, Optional
import ast
import json
import logging

import paho.mqtt.client as mqtt

from acquirium.Storage.timescale_store import TimescaleStore  # adjust import if needed
from acquirium.Internals.internals_namespaces import *
import numpy as np

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
    ts: datetime
    value: str | None


class MQTTIngestService:
    """
    One MQTT client per broker and port, many topic subscriptions per client.
    All messages go through one writer thread that owns the DB connection.
    """

    def __init__(
            self,
            *,
            pg_dsn: str,
            connect_timeout: int | None = None,
            queue_max: int = 200_000,
            qos: int = 0,
            batch_size: int = 1000,
            flush_ms: int = 5000,
        ):
        self.pg_dsn = pg_dsn
        self.connect_timeout = connect_timeout
        self.qos = qos

        self._stop = Event()
        self._queue: Queue[_Sample] = Queue(maxsize=queue_max)
        self._writer_thread = Thread(target=self._writer_loop, name="acquirium-mqtt-writer", daemon=True)
        self._writer_started = False
        self.batch_size = batch_size
        self.flush_ms = flush_ms

        self._lock = Lock()

        # key: "broker|port"
        self._clients: dict[str, mqtt.Client] = {}

        # per client key: topic -> list of specs
        self._topic_specs: dict[str, dict[str, list[MQTTStreamSpec]]] = {}

        # used for dedup so repeated graph refresh does not subscribe duplicates
        self._spec_keys: set[str] = set()

    def start(self) -> None:
        with self._lock:
            if self._writer_started:
                return
            self._writer_started = True
            self._writer_thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            for _, client in list(self._clients.items()):
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

    def ensure_subscribed(self, spec: MQTTStreamSpec) -> None:
        """
        Ensure subscription exists.
        Reuses one MQTT client for each broker and port.
        """

        self.start()

        spec_key = self._spec_key(spec)
        client_key = self._client_key(spec.broker, spec.port)

        with self._lock:
            if spec_key in self._spec_keys:
                return

            client = self._clients.get(client_key)
            if client is None:
                client = mqtt.Client(client_id=f"acquirium_{abs(hash(client_key))}")
                client.enable_logger(logging.getLogger("acquirium.mqtt"))

                client.on_connect = self._on_connect(client_key)
                client.on_message = self._on_message(client_key)

                self._clients[client_key] = client
                self._topic_specs[client_key] = {}

                client.connect(spec.broker, spec.port, keepalive=60)
                client.loop_start()

                logging.info("acquirium: mqtt client created broker=%s port=%d", spec.broker, spec.port)

            # register spec under topic
            topic_map = self._topic_specs[client_key]
            topic_map.setdefault(spec.topic, []).append(spec)
            self._spec_keys.add(spec_key)

            # subscribe now if already connected, otherwise on_connect will subscribe everything
            try:
                client.subscribe(spec.topic, qos=self.qos)
            except Exception:
                pass

            logging.info(
                "acquirium: mqtt subscribed point=%s broker=%s port=%d topic=%s",
                spec.point_uri,
                spec.broker,
                spec.port,
                spec.topic,
            )

    def _client_key(self, broker: str, port: int) -> str:
        return f"{broker}|{port}"

    def _spec_key(self, spec: MQTTStreamSpec) -> str:
        # includes point_uri and keys so same topic can feed multiple points safely
        return f"{spec.point_uri}|{spec.ref_uri}|{spec.broker}|{spec.port}|{spec.topic}|{spec.time_key}|{spec.value_key}"

    def _on_connect(self, client_key: str):
        def on_connect(client: mqtt.Client, userdata, flags, rc):
            if rc != 0:
                logging.error("acquirium: mqtt connect failed client=%s rc=%s", client_key, rc)
                return
            with self._lock:
                topics = list(self._topic_specs.get(client_key, {}).keys())
            if topics:
                client.subscribe([(t, self.qos) for t in topics])
            logging.info("acquirium: mqtt connected client=%s topics=%d", client_key, len(topics))
        return on_connect

    def _on_message(self, client_key: str):
        def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
            topic = msg.topic
            try:
                payload = msg.payload.decode("utf-8", errors="replace")
                payload_dict = _decode_payload(payload)
                

                with self._lock:
                    specs = list(self._topic_specs.get(client_key, {}).get(topic, []))

                if not specs:
                    return

                for spec in specs:
                    raw_ts = payload_dict.get(spec.time_key)
                    raw_val = payload_dict.get(spec.value_key)
                    ts = _parse_ts(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
                    val = None if raw_val is None else str(raw_val)

                    try:
                        self._queue.put_nowait(_Sample(point_uri=spec.point_uri, ts=ts, value=val))
                    except Exception:
                        logging.warning("acquirium: mqtt queue full, dropping sample point=%s", spec.point_uri)

            except Exception as exc:
                logging.warning("acquirium: mqtt decode failed client=%s topic=%s err=%s", client_key, topic, exc)
        return on_message

    def _writer_loop(self) -> None:
        '''
        Connect to timescale store separately 
        '''
        ts_store = TimescaleStore(dsn=self.pg_dsn, connect_timeout=self.connect_timeout)

        pending: dict[str, list[tuple[datetime, Any]]] = {}
        last_flush = _now_ms()

        while not self._stop.is_set():
            try:
                item = self._queue.get(timeout=self.flush_ms / 1000.0)
                pending.setdefault(item.point_uri, []).append((item.ts, item.value))
            except Empty:
                pass

            total = sum(len(v) for v in pending.values())
            should_flush = total >= self.batch_size or (_now_ms() - last_flush >= self.flush_ms)

            if should_flush and pending:
                try:
                    with ts_store.conn.transaction():
                        for point_uri, rows in pending.items():
                            ts_store.ensure_stream_handle(point_uri)
                            ts_store.upsert_rows(point_uri, rows)
                except Exception as exc:
                    logging.error("acquirium: mqtt db flush failed err=%s", exc)
                pending.clear()
                last_flush = _now_ms()

        if pending:
            try:
                with ts_store.conn.transaction():
                    for point_uri, rows in pending.items():
                        ts_store.ensure_stream_handle(point_uri)
                        ts_store.upsert_rows(point_uri, rows)
            except Exception:
                logging.error("acquirium: mqtt db final flush failed")
        ts_store.close()


def _decode_payload(payload: str) -> dict[str, Any]:
    payload = payload.strip()

    # 1) Try strict JSON first
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict):
            return obj
        raise ValueError("not a JSON object")
    except Exception:
        pass

    # 2) Fallback: Python literal dict (single quotes, etc.)
    try:
        obj = ast.literal_eval(payload)
        if isinstance(obj, dict):
            return obj
        raise ValueError("not a dict literal")
    except Exception as e:
        logging.warning(
            "acquirium: mqtt payload decode failed payload=%r error=%s",
            payload,
            e,
        )
        return {}


def _parse_ts(raw: Any) -> datetime:
    """
    Parse a timestamp into a timezone-aware UTC datetime.

    Accepted inputs:
    - datetime (naive assumed UTC, aware converted to UTC)
    - int / float (Unix timestamp in seconds or milliseconds)
    - ISO 8601 strings (with or without timezone)
    - Common SQL datetime strings

    Returns:
        datetime with tzinfo=UTC

    Raises:
        ValueError if the timestamp cannot be parsed
    """

    # -----------------------
    # datetime input
    # -----------------------
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=timezone.utc)
        return raw.astimezone(timezone.utc)

    # -----------------------
    # numeric input (epoch)
    # -----------------------
    if isinstance(raw, (int, float)):
        ts = float(raw)

        # Heuristic: milliseconds vs seconds
        # anything larger than year ~2286 in seconds is probably ms
        if ts > 1e11:
            ts /= 1000.0

        return datetime.fromtimestamp(ts, tz=timezone.utc)

    # -----------------------
    # string input
    # -----------------------
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            raise ValueError("Empty timestamp string")

        # Try ISO 8601 first (fast path)
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

        # Common SQL formats
        sql_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d",
        ]

        for fmt in sql_formats:
            try:
                dt = datetime.strptime(text, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        # Last resort: numeric string
        try:
            return _parse_ts(float(text))
        except Exception:
            pass

    # -----------------------
    # unsupported
    # -----------------------
    raise ValueError(f"Unrecognized timestamp format: {raw!r}")
            


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)