"""Unit tests for the IngestDriver add()/flush() buffer — no server required."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.Drivers.Driver import (
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
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 0}
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
            "ref_name": ["temp"],
            "value": [21.5],
        })


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


# ------------------------------------------------------------------ add + flush


def test_add_buffers_until_flush(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.add("temp", 21.5)
    driver.aq.insert_timeseries_arrow.assert_not_called()

    driver.flush()
    (source_id, cols), = inserted(driver)
    assert source_id == "sensors"
    assert cols["ref_name"] == ["temp"]
    assert cols["value"] == ["21.5"]


def test_flush_sends_one_frame_for_all_buffered_rows(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    driver.add("temp", 21.5, ts)
    driver.add("rh", 55.0, ts)
    driver.add("temp", 22.0, ts + timedelta(minutes=1))
    driver.flush()

    assert driver.aq.insert_timeseries_arrow.call_count == 1
    (_, cols), = inserted(driver)
    assert cols["ref_name"] == ["temp", "rh", "temp"]
    assert cols["value"] == ["21.5", "55.0", "22.0"]


def test_flush_clears_the_buffer(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.add("temp", 21.5)
    driver.flush()
    driver.flush()
    assert driver.aq.insert_timeseries_arrow.call_count == 1


def test_flush_with_empty_buffer_does_not_insert(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    assert driver.flush() == {"ok": True, "rows_inserted": 0}
    driver.aq.insert_timeseries_arrow.assert_not_called()


# ------------------------------------------------------------------ timestamps


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
    assert cols["ref_name"] == ["temp", "rh"]


def test_collect_style_driver_still_works(tmp_path):
    driver = make_driver(CollectDriver, tmp_path)
    driver.tick()

    (source_id, cols), = inserted(driver)
    assert source_id == "sensors"
    assert cols["ref_name"] == ["temp"]
    assert cols["value"] == ["21.5"]


def test_event_driver_tick_flushes_buffered_callbacks(tmp_path):
    class Events(EventIngestDriver):
        def setup(self) -> None:
            self.source_id = "mqtt"
            self.declare("temp")

    driver = make_driver(Events, tmp_path)
    driver.add("temp", 21.5)  # as if from a broker callback
    driver.aq.insert_timeseries_arrow.assert_not_called()

    driver.tick()
    (source_id, cols), = inserted(driver)
    assert source_id == "mqtt"
    assert cols["ref_name"] == ["temp"]


def test_driver_implementing_neither_read_nor_collect_is_rejected(tmp_path):
    class Neither(PollingIngestDriver):
        def setup(self) -> None:
            self.source_id = "sensors"

    with pytest.raises(TypeError, match="must implement read"):
        Neither(make_aq(), {"driver": {}, "server": {"data_dir": str(tmp_path)}})


# ------------------------------------------------------------------ safety valve


def test_buffer_over_ceiling_rejects_new_row_without_flushing(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.max_buffered_rows = 3

    for value in (1.0, 2.0, 3.0):
        driver.add("temp", value)
    driver.aq.insert_timeseries_arrow.assert_not_called()

    with pytest.raises(DriverBufferFull, match="buffer is full"):
        driver.add("temp", 4.0)  # one past the ceiling

    driver.aq.insert_timeseries_arrow.assert_not_called()
    driver.flush()
    (_, cols), = inserted(driver)
    assert cols["value"] == ["1.0", "2.0", "3.0"]


# ------------------------------------------------------------------ multi-source


def test_add_with_explicit_source_id_partitions_inserts(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.declare("temp", source_id="site-a")
    driver.declare("temp", source_id="site-b")
    driver.add("temp", 21.5, source_id="site-a")
    driver.add("temp", 30.0, source_id="site-b")
    driver.flush()

    by_source = dict(
        (source_id, cols["value"]) for source_id, cols in inserted(driver)
    )
    assert by_source == {"site-a": ["21.5"], "site-b": ["30.0"]}


def test_rows_without_source_id_fall_back_to_driver_default(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.declare("temp", source_id="site-a")
    driver.add("temp", 21.5, source_id="site-a")
    driver.add("temp", 22.0)
    driver.flush()

    by_source = dict(
        (source_id, cols["value"]) for source_id, cols in inserted(driver)
    )
    assert by_source == {"site-a": ["21.5"], "sensors": ["22.0"]}


# ------------------------------------------------------------------ failure retry


def test_failed_flush_keeps_rows_for_the_next_attempt(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.aq.insert_timeseries_arrow.side_effect = RuntimeError("server down")
    driver.add("temp", 21.5)

    with pytest.raises(RuntimeError, match="server down"):
        driver.flush()

    driver.aq.insert_timeseries_arrow.side_effect = None
    driver.flush()
    (_, cols), = inserted(driver)[1:]
    assert cols["value"] == ["21.5"]


def test_false_insert_result_keeps_rows_for_retry(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.aq.insert_timeseries_arrow.return_value = {"ok": False, "rows_inserted": 0}
    driver.add("temp", 21.5)

    with pytest.raises(RuntimeError, match="reported failure"):
        driver.flush()

    driver.aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 1}
    driver.flush()
    assert inserted(driver)[1][1]["value"] == ["21.5"]


def test_failed_flush_preserves_order_against_newer_rows(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.aq.insert_timeseries_arrow.side_effect = RuntimeError("server down")
    driver.add("temp", 1.0)
    with pytest.raises(RuntimeError):
        driver.flush()

    driver.aq.insert_timeseries_arrow.side_effect = None
    driver.add("temp", 2.0)
    driver.flush()

    (_, cols), = inserted(driver)[1:]
    assert cols["value"] == ["1.0", "2.0"]


def test_retry_backlog_rejects_new_rows_at_the_limit(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.max_buffered_rows = 2
    driver.aq.insert_timeseries_arrow.side_effect = RuntimeError("server down")

    driver.add("temp", 1.0)
    driver.add("temp", 2.0)
    with pytest.raises(RuntimeError):
        driver.flush()

    with pytest.raises(DriverBufferFull):
        driver.add("temp", 3.0)
    driver.aq.insert_timeseries_arrow.side_effect = None
    driver.flush()
    (_, cols), = inserted(driver)[1:]
    assert cols["value"] == ["1.0", "2.0"]


def test_failing_inserts_are_retried_only_by_flush(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.max_buffered_rows = 2
    driver.aq.insert_timeseries_arrow.side_effect = RuntimeError("server down")

    driver.add("temp", 1.0)
    driver.add("temp", 2.0)
    with pytest.raises(RuntimeError):
        driver.flush()
    assert driver.aq.insert_timeseries_arrow.call_count == 1

    for value in (3.0, 4.0):
        with pytest.raises(DriverBufferFull):
            driver.add("temp", value)
    assert driver.aq.insert_timeseries_arrow.call_count == 1

    driver.aq.insert_timeseries_arrow.side_effect = None
    driver.tick()
    # One retry batch, then one batch for this tick's newly sampled values.
    assert driver.aq.insert_timeseries_arrow.call_count == 3


def test_successful_flush_releases_buffer_capacity(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.max_buffered_rows = 2
    driver.aq.insert_timeseries_arrow.side_effect = RuntimeError("server down")
    driver.add("temp", 1.0)
    with pytest.raises(RuntimeError):
        driver.flush()

    driver.aq.insert_timeseries_arrow.side_effect = None
    driver.flush()

    driver.add("temp", 2.0)
    driver.add("temp", 3.0)
    with pytest.raises(DriverBufferFull):
        driver.add("temp", 4.0)


# ------------------------------------------------------------------ value kinds


def test_add_accepts_non_numeric_values(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.declare("state")
    driver.declare("count")
    driver.add("state", "ON")
    driver.add("count", 3)
    driver.flush()

    (_, cols), = inserted(driver)
    assert cols["value"] == ["ON", "3"]


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
        "ref_name": ["missing"],
        "value": [1.0],
    })
    with pytest.raises(UndeclaredStreamError, match="undeclared streams"):
        driver.insert_observations(frame)


def test_platform_registers_declared_datasource(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.aq.register_datasource.assert_called_once_with("sensors")


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
        if spec["ref_name"] == "optional"
    ]
    assert specs[-1]["value_kind"] == "numeric"


def test_conflicting_redeclaration_after_registration_raises(tmp_path):
    driver = make_driver(AddDriver, tmp_path)
    driver.declare("state", point_uri="urn:first")
    driver.register_declared()
    with pytest.raises(ValueError, match="conflicting declaration"):
        driver.declare("state", point_uri="urn:second")


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
