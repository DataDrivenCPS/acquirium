"""Real HTTP delivery, native publication, crash recovery, and resumable sinks."""

import json
import time

import pytest
import requests

import acquirium as aq
from acquirium import runtime
from examples.streaming.consumer import ChangeConsumer
from examples.streaming.producer import WorkloadDriver
from examples.streaming.workload import run, verify


def drain(client, consumer, *, timeout=30):
    deadline = time.monotonic() + timeout
    while True:
        page = consumer.poll()
        status = client.engine_status()
        if (
            not status["engine"]["pending_requests"]
            and page["complete_through"] >= status["engine"]["epoch"]
        ):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(status)
        if not page["has_more"]:
            time.sleep(0.01)


@pytest.mark.integration
def test_sustained_bursts_late_data_and_restarts_match_independent_oracle(tmp_path):
    report = run(
        tmp_path, ticks=16, burst=4, interval=0.01, page_size=3, restart_every=8
    )
    assert report["verified"]
    assert report["requests"] == 16
    assert report["server_restarts"] == 2
    assert report["snapshot_epoch"] == report["consumer_complete_through"]
    assert report["publication_latency_upper_bound"]["count"] == 16
    assert report["consumption_latency_upper_bound"]["count"] == 16
    assert report["server_peak_rss_bytes"] > 0
    assert json.loads((tmp_path / "metrics.json").read_text()) == report
    # Reopening the workload appends requests and resumes the saved sink cursor.
    resumed = run(tmp_path, ticks=2, interval=0, page_size=1)
    assert resumed["requests"] == 18 and resumed["verified"]


@pytest.mark.integration
def test_lost_response_partial_epoch_and_abrupt_server_restart(tmp_path, monkeypatch):
    client = aq.init(data_dir=tmp_path / "server", exact_only=True, timeout=180)
    driver = WorkloadDriver(client, tmp_path / "source")
    try:
        driver.setup()
        record = driver.prepare()
        receipt = None
        original_post = client.client._http.post

        def lost_response(*args, **kwargs):
            nonlocal receipt
            response = original_post(*args, **kwargs)
            receipt = response.json()["receipts"][0]
            raise requests.ConnectionError("test: response lost after acceptance")

        with monkeypatch.context() as patch:
            patch.setattr(client.client._http, "post", lost_response)
            with pytest.raises(requests.ConnectionError):
                driver.deliver(record)
        # No source acknowledgment was saved. A new driver repeats its exact ID
        # and payload, and receives the original database-scoped sequence.
        driver = WorkloadDriver(client, tmp_path / "source")
        pending = driver.prepare()
        assert pending["request_id"] == record["request_id"]
        assert pending["streams"] == record["streams"]
        acknowledged = driver.tick()
        assert acknowledged["receipt"]["sequence"] == receipt["sequence"]
        assert acknowledged["attempts"] == 2
        client.wait(acknowledged["receipt"])

        consumer = ChangeConsumer(client, tmp_path / "sink.json", page_size=1)
        page = consumer.poll()
        assert page["has_more"] and consumer.pending
        assert consumer.values() == {}  # A partial epoch is never visible.
        # Discard the consumer with a partially read epoch. It must replay from
        # the committed cursor, not its in-memory read cursor.
        consumer = ChangeConsumer(client, tmp_path / "sink.json", page_size=1)
        drain(client, consumer)
        verify(client, driver, consumer)
        first_count = consumer.state["changes"]
        saved_cursor = dict(consumer.state["cursor"])

        accepted = driver.tick()
        owner = runtime._session[
            3
        ]  # Fault injection into the actual server subprocess.
        owner.kill()
        owner.wait(timeout=10)
        with pytest.raises(requests.RequestException):
            driver.tick()  # Persist the next request while the server is offline.
        offline = driver.records()[-1]
        assert offline["receipt"] is None
        aq.shutdown()
        client = aq.init(data_dir=tmp_path / "server", exact_only=True, timeout=180)
        driver = WorkloadDriver(client, tmp_path / "source")
        repeated = driver.deliver(driver.records()[-2])
        assert repeated["receipt"]["sequence"] == accepted["receipt"]["sequence"]
        client.wait(repeated["receipt"])
        recovered = driver.tick()
        assert recovered["request_id"] == offline["request_id"]
        assert recovered["streams"] == offline["streams"]
        client.wait(recovered["receipt"])
        consumer = ChangeConsumer(client, tmp_path / "sink.json", page_size=1)
        assert consumer.state["cursor"] == saved_cursor
        drain(client, consumer)
        verify(client, driver, consumer)
        assert consumer.state["changes"] > first_count
        # Redelivery and a caught-up consumer introduce no duplicate feed rows.
        final = dict(consumer.state)
        driver.deliver(driver.records()[-1])
        consumer = ChangeConsumer(client, tmp_path / "sink.json", page_size=1)
        drain(client, consumer)
        assert consumer.state == final
    finally:
        aq.shutdown()


@pytest.mark.integration
def test_sink_write_failure_replays_without_losing_or_double_applying_changes(
    tmp_path, monkeypatch
):
    from examples.streaming import consumer as consumer_module

    client = aq.init(data_dir=tmp_path / "server", exact_only=True, timeout=180)
    try:
        driver = WorkloadDriver(client, tmp_path / "source")
        driver.setup()
        accepted = driver.tick()
        client.wait(accepted["receipt"])
        pool = client.engine_status()["python_workers"]
        telemetry = pool["telemetry"]
        assert pool["queued"] == 0
        assert telemetry["returned_jobs"] >= 4
        assert telemetry["warm_service"]["count"] >= 2
        assert telemetry["total"]["count"] == sum(telemetry["total"]["buckets"])
        assert "rss_complete" in telemetry
        consumer = ChangeConsumer(client, tmp_path / "sink.json")
        with monkeypatch.context() as patch:

            def failed_checkpoint(*args):
                raise OSError("test: sink disk unavailable")

            patch.setattr(consumer_module, "save", failed_checkpoint)
            with pytest.raises(OSError, match="sink disk"):
                consumer.poll()
        assert not (tmp_path / "sink.json").exists()
        assert consumer.values() == {}
        consumer = ChangeConsumer(client, tmp_path / "sink.json", page_size=2)
        drain(client, consumer)
        verify(client, driver, consumer)
    finally:
        aq.shutdown()
