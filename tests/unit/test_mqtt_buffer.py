"""MQTT receive slots recover as the observation buffer drains."""

from datetime import datetime, timezone
from types import SimpleNamespace
from threading import Event
from unittest.mock import Mock

import polars as pl
import pytest

from acquirium.Drivers.BuiltInDrivers.mqtt_ingestion import (
    MQTTIngestDriver, MQTTStreamSpec, _Window,
)


@pytest.fixture
def driver(tmp_path, monkeypatch):
    monkeypatch.setattr(MQTTIngestDriver, "_sync_subscriptions", lambda self: None)
    aq = Mock()
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 1}
    driver = MQTTIngestDriver(aq, {
        "driver": {"source_id": "mqtt"}, "server": {"data_dir": str(tmp_path)},
    })
    driver.setup()
    driver.max_buffered_rows = 1
    driver._windows["broker"] = _Window(3)
    driver._topic_specs["broker"] = {"topic": []}
    for name in ("a", "b", "c"):
        driver.declare(name, value_kind="numeric")
        driver._topic_specs["broker"]["topic"].append(MQTTStreamSpec(
            "urn:point", "urn:ref", name, "localhost", 1883, "topic", "Timestamp", "Value", "numeric",
        ))
    return driver


def message(mid, *, qos=1):
    return SimpleNamespace(topic="topic", qos=qos, mid=mid, payload=f'{{"Value": {mid}}}'.encode())


def sent(driver):
    return [pl.from_arrow(call.args[1]) for call in driver.aq.insert_timeseries_arrow.call_args_list]


@pytest.mark.parametrize("fail_first", [False, True])
def test_overflowed_messages_resume_without_reconnecting(driver, fail_first):
    driver._topic_specs["broker"]["topic"] = driver._topic_specs["broker"]["topic"][:1]
    receive, client = driver._on_message("broker"), Mock()
    receive(client, None, message(1))
    receive(client, None, message(2))
    receive(client, None, message(3))
    assert driver._windows["broker"]._unacked == 3
    if fail_first:
        driver.aq.insert_timeseries_arrow.side_effect = ConnectionError("offline")
        with pytest.raises(ConnectionError):
            driver.flush()
        client.ack.assert_not_called()
        driver.aq.insert_timeseries_arrow.side_effect = None
        driver.aq.insert_timeseries_arrow.reset_mock()
    for expected in (1, 2, 3):
        driver.tick()
        assert len(driver._pending) <= driver.max_buffered_rows
        assert [call.args[0] for call in client.ack.call_args_list] == list(range(1, expected + 1))
    assert [frame["value"].item() for frame in sent(driver)] == [1, 2, 3]
    assert not driver._messages
    assert driver._windows["broker"]._unacked == 0


def test_one_message_spanning_several_flushes_is_decoded_once_and_acked_last(driver):
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    driver.decode_payload = Mock(return_value=(when, 42))
    receive, client = driver._on_message("broker"), Mock()
    receive(client, None, message(1))
    for _ in range(2):
        driver.flush()
        client.ack.assert_not_called()
    driver.flush()
    client.ack.assert_called_once_with(1, 1)
    assert driver.decode_payload.call_count == 3  # Once for each stream spec.
    frames = sent(driver)
    assert [frame["ref_name"].item() for frame in frames] == ["a", "b", "c"]
    assert all(frame["ts"].item() == when for frame in frames)
    assert driver._windows["broker"]._unacked == 0


def test_qos_zero_does_not_grow_a_blocked_retry_queue(driver):
    driver._topic_specs["broker"]["topic"] = driver._topic_specs["broker"]["topic"][:1]
    receive, client = driver._on_message("broker"), Mock()
    receive(client, None, message(1))
    receive(client, None, message(2))
    for mid in (3, 4, 5):
        receive(client, None, message(mid, qos=0))
    assert len(driver._messages) == 1
    driver.flush()
    driver.flush()
    assert [frame["value"].item() for frame in sent(driver)] == [1, 2]
    assert driver._windows["broker"]._unacked == 0


def test_background_flush_drains_overflow_without_ticks(driver):
    driver._topic_specs["broker"]["topic"] = driver._topic_specs["broker"]["topic"][:1]
    receive, client = driver._on_message("broker"), Mock()
    drained = Event()
    client.ack.side_effect = lambda mid, qos: drained.set() if mid == 3 else None
    for mid in (1, 2, 3):
        receive(client, None, message(mid))
    driver._after_setup()
    try:
        assert drained.wait(5), "overflow did not drain in the background"
        assert [frame["value"].item() for frame in sent(driver)] == [1, 2, 3]
    finally:
        driver._shutdown()
