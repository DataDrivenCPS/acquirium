"""Unit tests for the IngestDriver add()/flush() buffer — no server required."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.Drivers.Driver import (
    CAPACITY_RETRY_S,
    DriverBufferFull,
    EventIngestDriver,
    IngestDriver,
    PollingIngestDriver,
    UndeclaredStreamError,
)


# ------------------------------------------------------------------ fixtures


def make_aq() -> MagicMock:
    aq = MagicMock()
    aq.client = MagicMock()
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 0, "receipts": []}
    return aq


class AddDriver(PollingIngestDriver):
    """Reports readings one at a time, sMAP-style."""

    def setup(self) -> None:
        self.source_id = "sensors"
        self.declare("temp")
        self.declare("rh")

    def read(self) -> None:
        self.add("temp", 21.5)
        self.add("rh", 55.0)


class CollectDriver(PollingIngestDriver):
    """The pre-existing bulk style, which must keep working unchanged."""

    def setup(self) -> None:
        self.source_id = "sensors"
        self.declare("temp")

    def collect(self) -> pl.DataFrame:
        return pl.DataFrame({
            "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            "stream_name": ["temp"],
            "value": [21.5],
        })


@pytest.mark.parametrize("dtype", [pl.Int64, pl.Object])
def test_driver_value_transport_preserves_large_integers(dtype):
    from acquirium.Drivers.Driver import _value_column

    values = [7, 2**53 + 1]
    column = pl.Series("value", values, dtype=dtype)
    transport = column.to_frame().select(_value_column(column)).to_arrow()
    assert transport["value"].to_pylist() == values


def make_driver(cls, tmp_path, **cfg):
    driver = cls(make_aq(), {"driver": {**cfg}, "server": {"data_dir": str(tmp_path)}})
    driver.setup()
    driver._after_setup()
    return driver


def inserted(driver) -> list[tuple[str, dict[str, list]]]:
    """Return [(source_id, {column: values})] for each insert call."""
    out = []
    for call in driver.aq.insert_timeseries_arrow.call_args_list:
        source_id, table = call[0]
        out.append((source_id, pl.from_arrow(table).to_dict(as_series=False)))
    return out


def test_add_batches_readings_until_flush(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    empty = {"ok": True, "rows_inserted": 0, "receipts": []}
    assert driver.flush() == empty

    # Buffer three samples across two streams without contacting the server.
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    driver.add("temp", 21.5, ts)
    driver.add("rh", 55.0, ts)
    driver.add("temp", 22.0, ts + timedelta(minutes=1))
    driver.aq.insert_timeseries_arrow.assert_not_called()

    # One flush sends one batch. The next sees an empty buffer.
    driver.flush()
    assert driver.flush() == empty
    (source, columns), = inserted(driver)
    assert source == "sensors"
    assert columns["stream_name"] == ["temp", "rh", "temp"]
    assert columns["value"] == [21.5, 55.0, 22.0]


def test_add_defaults_timestamp_to_now_utc(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    before = datetime.now(timezone.utc)
    driver.add("temp", 21.5)
    driver.flush()

    (_, cols), = inserted(driver)
    ts = cols["ts"][0]
    assert ts.tzinfo is not None
    assert before <= ts <= datetime.now(timezone.utc)


def test_add_reads_naive_timestamps_as_utc(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.add("temp", 21.5, datetime(2024, 1, 1, 12, 0))
    driver.flush()

    (_, cols), = inserted(driver)
    assert cols["ts"][0] == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_add_converts_non_utc_timestamps(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    tz = timezone(timedelta(hours=-7))
    driver.add("temp", 21.5, datetime(2024, 1, 1, 5, 0, tzinfo=tz))
    driver.flush()

    (_, cols), = inserted(driver)
    assert cols["ts"][0] == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


# ------------------------------------------------------------------ tick lifecycle


def test_tick_reads_then_flushes(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.tick()

    (source_id, cols), = inserted(driver)
    assert source_id == "sensors"
    assert cols["stream_name"] == ["temp", "rh"]


def test_collect_style_driver_still_works(tmp_path):
    driver = make_driver(CollectDriver, tmp_path)
    driver.tick()

    (source_id, cols), = inserted(driver)
    assert source_id == "sensors"
    assert cols["stream_name"] == ["temp"]
    assert cols["value"] == [21.5]


class Events(EventIngestDriver):
    def setup(self) -> None:
        self.source_id = "mqtt"
        self.declare("temp")


def eventually(condition, timeout=5.0):
    import time
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.01)


def test_event_driver_sends_a_burst_without_waiting_for_a_tick(tmp_path):
    driver = make_driver(Events, tmp_path)
    try:
        for value in (21.5, 22.0, 22.5):  # as if from a broker callback
            driver.add("temp", value)
        eventually(lambda: inserted(driver))
        (source_id, cols), = inserted(driver)
        assert source_id == "mqtt"
        assert cols["value"] == [21.5, 22.0, 22.5]
    finally:
        driver._shutdown()


def test_acceptance_callbacks_wait_for_their_rows(tmp_path):
    driver = make_driver(Events, tmp_path)
    driver._shutdown()  # Stop the background flusher; flush by hand.
    accepted = []
    driver.add("temp", 1.0)
    driver.when_accepted(lambda: accepted.append("first"))
    driver.add("temp", 2.0)
    driver.when_accepted(lambda: accepted.append("second"))
    driver.aq.insert_timeseries_arrow.side_effect = ConnectionError("server down")
    with pytest.raises(ConnectionError):
        driver.flush()
    assert accepted == []
    driver.aq.insert_timeseries_arrow.side_effect = None
    driver.flush()
    assert accepted == ["first", "second"]
    assert inserted(driver)[-1][1]["value"] == [1.0, 2.0]
    # A callback with no rows before it runs at the next flush.
    driver.when_accepted(lambda: accepted.append("empty"))
    assert driver.flush()["rows_inserted"] == 0
    assert accepted == ["first", "second", "empty"]


def test_driver_implementing_neither_read_nor_collect_is_rejected(tmp_path):
    class Neither(PollingIngestDriver):
        def setup(self) -> None:
            self.source_id = "sensors"

    with pytest.raises(TypeError, match="must implement read"):
        Neither(make_aq(), {"driver": {}, "server": {"data_dir": str(tmp_path)}})


def test_flush_partitions_readings_by_explicit_and_default_sources(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.declare("temp", source_id="site-a")
    driver.declare("temp", source_id="site-b")
    driver.add("temp", 21.5, source_id="site-a")
    driver.add("temp", 30.0, source_id="site-b")
    driver.add("temp", 22.0)
    driver.flush()

    by_source = {source: columns["value"] for source, columns in inserted(driver)}
    assert by_source == {"site-a": [21.5], "site-b": [30.0], "sensors": [22.0]}


@pytest.mark.parametrize(("failure", "message"), [
    pytest.param(RuntimeError("server down"), "server down", id="exception"),
    pytest.param({"ok": False, "rows_inserted": 0}, "reported failure", id="rejected"),
])
def test_failed_flush_retries_the_same_batch_before_newer_rows(tmp_path, failure, message):
    driver = make_driver(AddDriver, tmp_path)
    submit = driver.aq.insert_timeseries_arrow
    success = {"ok": True, "rows_inserted": 1, "receipts": []}
    submit.side_effect = [failure, success, success]
    accepted = []

    # The failed write must keep its readings and postpone its acknowledgment.
    driver.add("temp", 1.0)
    driver.when_accepted(lambda: accepted.append("first"))
    with pytest.raises(RuntimeError, match=message):
        driver.flush()
    assert accepted == []

    # Retrying sends the original batch with the original request ID before new readings.
    driver.add("temp", 2.0)
    driver.when_accepted(lambda: accepted.append("second"))
    assert driver.flush()["rows_inserted"] == 2
    assert [cols["value"] for _, cols in inserted(driver)] == [[1.0], [1.0], [2.0]]
    first, retry, newer = submit.call_args_list
    assert first.kwargs["request_id"] == retry.kwargs["request_id"] != newer.kwargs["request_id"]
    assert accepted == ["first", "second"]


def test_full_buffer_rejects_readings_until_the_pending_batch_is_sent(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.max_buffered_rows = 2
    submit = driver.aq.insert_timeseries_arrow
    submit.side_effect = RuntimeError("server down")

    # Hitting the ceiling does not send a batch from add().
    driver.add("temp", 1.0)
    driver.add("temp", 2.0)
    with pytest.raises(DriverBufferFull, match="buffer is full"):
        driver.add("temp", 3.0)
    submit.assert_not_called()

    # A failed flush still counts against the ceiling and add() cannot retry it.
    with pytest.raises(RuntimeError, match="server down"):
        driver.flush()
    with pytest.raises(DriverBufferFull):
        driver.add("temp", 3.0)
    submit.assert_called_once()

    # A successful tick retries the old batch, freeing room for the next sample.
    submit.side_effect = None
    driver.tick()
    assert [cols["value"] for _, cols in inserted(driver)] == [
        [1.0, 2.0], [1.0, 2.0], [21.5, 55.0],
    ]
    driver.add("temp", 3.0)
    driver.add("temp", 4.0)
    with pytest.raises(DriverBufferFull):
        driver.add("temp", 5.0)


# ------------------------------------------------------------------ value kinds


@pytest.mark.parametrize("integer", [3, 2**53 + 1, 2**64 - 1])
def test_add_preserves_integers_alongside_text_and_floats(tmp_path, integer):
    driver = make_driver(AddDriver, tmp_path)
    driver.declare("state")
    driver.declare("count", value_kind="numeric")
    driver.add("state", "ON")
    driver.add("count", integer)
    driver.add("count", 7.5)
    driver.aq.insert_timeseries_batch.return_value = {"ok": True, "rows_inserted": 3, "receipts": []}
    driver.flush()

    # JSON keeps each scalar's type so the server can check numeric integers.
    driver.aq.insert_timeseries_arrow.assert_not_called()
    (source, streams), options = driver.aq.insert_timeseries_batch.call_args
    assert source == driver.source_id
    assert [value for _, value in streams["state"]] == ["ON"]
    assert [value for _, value in streams["count"]] == [integer, 7.5]
    assert type(streams["count"][0][1]) is int
    assert options["request_id"]


def test_add_is_available_on_the_shared_ingest_base(tmp_path):
    assert hasattr(IngestDriver, "add")
    assert hasattr(IngestDriver, "flush")


def test_ingest_driver_exposes_shared_timestamp_conversion(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    converted = driver.to_timestamp(
        pl.Series("Date", ["12/1/2024"]),
        pl.Series("Time", ["5:32:52 PM"]),
    )
    assert converted.to_list() == [
        datetime(2024, 12, 1, 17, 32, 52, tzinfo=timezone.utc)
    ]


def test_undeclared_observation_is_rejected(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    with pytest.raises(UndeclaredStreamError, match="was not declared"):
        driver.add("missing", 1.0)


def test_undeclared_bulk_observation_is_rejected(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    frame = pl.DataFrame({
        "ts": [datetime.now(timezone.utc)],
        "stream_name": ["missing"],
        "value": [1.0],
    })
    with pytest.raises(UndeclaredStreamError, match="undeclared streams"):
        driver.insert_observations(frame)


def test_null_only_batch_does_not_freeze_inferred_value_kind(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.declare("optional")
    driver.add("optional", None)
    driver.flush()
    driver.add("optional", 3.0)
    driver.flush()

    specs = [
        spec
        for call in driver.aq.register_streams.call_args_list
        for spec in call.args[0]
        if spec["stream_name"] == "optional"
    ]
    assert specs and all("value_kind" not in spec for spec in specs)


def test_conflicting_redeclaration_after_registration_raises(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.declare("state", property_uri="urn:first")
    driver.register_declared()
    with pytest.raises(ValueError, match="conflicting declaration"):
        driver.declare("state", property_uri="urn:second")


def test_graceful_shutdown_flushes_after_stopping_producer(tmp_path):
    class Events(EventIngestDriver):
        def setup(self) -> None:
            self.source_id = "events"
            self.declare("state")

        def stop(self) -> None:
            self.stopped = True

    driver = make_driver(Events, tmp_path)
    driver.add("state", "ON")
    driver._shutdown()
    assert driver.stopped is True
    assert inserted(driver)[0][1]["value"] == ["ON"]


def test_insert_resends_requests_refused_at_capacity_for_the_driver_budget(tmp_path):
    driver = make_driver(CollectDriver, tmp_path)
    observations = pl.DataFrame({
        "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
        "stream_name": ["temp"],
        "value": [1.0],
    })

    driver.insert_observations(observations)
    # The client resends a 503; a driver gives it longer than interactive calls.
    assert driver.aq.insert_timeseries_arrow.call_args.kwargs["retry_for"] == CAPACITY_RETRY_S


def test_insert_raises_other_http_errors(tmp_path):
    from requests import HTTPError

    driver = make_driver(CollectDriver, tmp_path)
    driver.aq.insert_timeseries_arrow.side_effect = HTTPError(
        "bad", response=MagicMock(status_code=400, headers={})
    )
    observations = pl.DataFrame({
        "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
        "stream_name": ["temp"],
        "value": [1.0],
    })
    with pytest.raises(HTTPError):
        driver.insert_observations(observations)
    assert driver.aq.insert_timeseries_arrow.call_count == 1


class BooleanDriver(PollingIngestDriver):
    def setup(self) -> None:
        self.source_id = "sensors"
        self.declare("running")
        self.declare("alarm", value_kind="text")

    def read(self) -> None:
        pass


def test_a_boolean_is_sent_as_a_number_unless_the_stream_is_text(tmp_path) -> None:
    driver = make_driver(BooleanDriver, tmp_path)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    driver.add("running", True, ts)
    driver.add("running", 3.5, ts)
    driver.add("alarm", False, ts)
    frame = driver.normalize_observations(driver._pending_frame(driver._pending))
    assert frame["value"].to_list() == ["1.0", "3.5", "False"]

    typed = pl.DataFrame({"ts": [ts, ts], "stream_name": ["running", "alarm"], "value": [True, True]})
    assert driver.normalize_observations(typed)["value"].to_list() == ["1.0", "True"]
