"""MQTT messages are acknowledged only once the server has accepted them.

Needs a Mosquitto broker at ``ACQUIRIUM_TEST_MQTT`` (default
``localhost:1883``); the server is a fake client that can refuse inserts.
"""

import os
import socket
import time
import uuid
from unittest.mock import MagicMock

import paho.mqtt.client as mqtt
import polars as pl
import pytest

from acquirium.Drivers.BuiltInDrivers.mqtt_ingestion import MQTTIngestDriver, parse_mqtt_broker
from acquirium.internals.models import compute_ref_uri

BROKER = os.getenv("ACQUIRIUM_TEST_MQTT", "localhost:1883")
HOST, PORT = parse_mqtt_broker(BROKER)


def _broker_reachable() -> bool:
    try:
        socket.create_connection((HOST, PORT), timeout=1).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _broker_reachable(), reason=f"no MQTT broker at {BROKER}")


def server(topic: str, source_id: str, *, refuse: bool) -> MagicMock:
    """A fake Acquirium client whose graph declares one MQTT stream."""
    ref = str(compute_ref_uri(source_id, "temp"))
    aq = MagicMock()
    aq.client.sparql_query.return_value = {
        "rows": [["urn:point", ref, "temp", "numeric", BROKER, topic, "Timestamp", "Value"]]
    }
    aq.stored = []

    def insert(source, table, **kwargs):
        if refuse:
            raise ConnectionError("server down")
        aq.stored.extend(pl.from_arrow(table)["value"].to_list())
        return {"ok": True, "rows_inserted": table.num_rows}

    aq.insert_timeseries_arrow.side_effect = insert
    return aq


def start(aq, source_id, tmp_path) -> MQTTIngestDriver:
    driver = MQTTIngestDriver(
        aq, {"driver": {"source_id": source_id}, "server": {"data_dir": str(tmp_path)}}
    )
    driver.setup()
    driver._after_setup()
    return driver


def crash(driver: MQTTIngestDriver) -> None:
    """Drop the connection as a killed process would: no flush, no DISCONNECT."""
    driver._stopping.set()
    for client in driver._clients.values():
        client.loop_stop()
        client.socket().close()


def eventually(condition, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.05)


def test_unaccepted_messages_survive_a_crash_and_accepted_ones_are_not_resent(tmp_path):
    topic, source_id = f"acquirium-test/{uuid.uuid4()}", f"ack-{uuid.uuid4().hex[:8]}"
    refused = server(topic, source_id, refuse=True)
    first = start(refused, source_id, tmp_path)
    publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"pub-{uuid.uuid4().hex[:8]}")
    publisher.connect(HOST, PORT)
    publisher.loop_start()
    try:
        time.sleep(0.5)  # Let the session subscribe before publishing.
        for value in range(5):
            publisher.publish(topic, f'{{"Value": {value}}}', qos=1).wait_for_publish()
        eventually(lambda: refused.insert_timeseries_arrow.call_count > 0)
        crash(first)

        accepting = server(topic, source_id, refuse=False)
        second = start(accepting, source_id, tmp_path)
        eventually(lambda: sorted(set(accepting.stored)) == [0, 1, 2, 3, 4])
        second._shutdown()

        third = start(server(topic, source_id, refuse=False), source_id, tmp_path)
        time.sleep(1.0)
        assert third.aq.stored == []  # Everything accepted was acknowledged.
        third._shutdown()
    finally:
        publisher.loop_stop()
        publisher.disconnect()
