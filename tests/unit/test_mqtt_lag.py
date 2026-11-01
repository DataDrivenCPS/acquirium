"""The MQTT driver warns when its receive window stays full."""

import logging

from acquirium.Drivers.BuiltInDrivers import mqtt_ingestion
from acquirium.Drivers.BuiltInDrivers.mqtt_ingestion import MQTTIngestDriver, _Window


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


def fill(window: _Window) -> None:
    for _ in range(window.size):
        window.received()


def drain(window: _Window) -> None:
    for _ in range(window.size):
        window.acked()


def test_window_reports_the_share_of_time_it_was_full(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(mqtt_ingestion, "time", clock)
    window = _Window(3)

    clock.now += 2
    fill(window)
    clock.now += 6
    drain(window)
    clock.now += 2
    assert window.sample() == (10, 0.6)

    fill(window)
    clock.now += 4
    assert window.sample() == (4, 1.0)  # Still full when sampled.
    clock.now += 1
    window.acked()
    clock.now += 4
    assert window.sample() == (5, 0.2)


def test_driver_warns_while_a_window_stays_full_and_says_when_it_catches_up(
    tmp_path, monkeypatch, caplog
):
    clock = Clock()
    monkeypatch.setattr(mqtt_ingestion, "time", clock)
    monkeypatch.setattr(MQTTIngestDriver, "_sync_subscriptions", lambda self: None)
    driver = MQTTIngestDriver(object(), {
        "__config_dir": str(tmp_path),
        "driver": {"source_id": "building-mqtt", "mqtt_receive_maximum": 2},
    })
    driver.setup()
    window = driver._windows["broker|1883"] = _Window(driver.receive_maximum)
    caplog.set_level(logging.INFO, logger="acquirium.mqtt")

    fill(window)
    clock.now += 10
    driver._check_lag()
    assert caplog.records == []  # Too soon to judge.

    clock.now += 20
    driver._check_lag()
    [warning] = caplog.records
    assert warning.levelno == logging.WARNING
    assert "falling behind client=broker|1883" in warning.getMessage()
    assert "100% of the last 30s" in warning.getMessage()

    caplog.clear()
    clock.now += 10
    drain(window)
    clock.now += 20
    driver._check_lag()
    [notice] = caplog.records
    assert notice.levelno == logging.INFO
    assert notice.getMessage() == "mqtt caught up client=broker|1883"

    caplog.clear()
    clock.now += 30
    driver._check_lag()
    assert caplog.records == []
