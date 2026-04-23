"""Unit tests for CSVIngestDriver — parsing logic only, no server required."""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver, _to_dt


# ------------------------------------------------------------------ fixtures


def make_driver(cfg_overrides: dict | None = None) -> CSVIngestDriver:
    """Return a driver with setup() called against a temp watch dir."""
    aq = MagicMock()
    aq.register_datasource.return_value = "csv_files"
    aq.register_stream.return_value = None
    aq.insert_timeseries_batch.return_value = {"ok": True, "rows_inserted": 0}
    config = {"driver": {"csv_watch_dir": "/tmp/csv_test_watch", **(cfg_overrides or {})}}
    return CSVIngestDriver(aq, config)


def _wide_csv(tmp_path: Path) -> Path:
    p = tmp_path / "wide.csv"
    p.write_text("time,temp,rh\n2024-01-01T00:00:00Z,22.5,55.0\n2024-01-02T00:00:00Z,23.0,60.0\n")
    return p


def _narrow_csv(tmp_path: Path) -> Path:
    p = tmp_path / "narrow.csv"
    p.write_text(
        "time,id,value\n"
        "2024-01-01T00:00:00Z,sensor/temp,22.5\n"
        "2024-01-01T00:00:00Z,sensor/rh,55.0\n"
        "2024-01-02T00:00:00Z,sensor/temp,23.0\n"
    )
    return p


# ------------------------------------------------------------------ _to_dt


def test_to_dt_datetime_naive():
    dt = datetime(2024, 1, 1, 12, 0, 0)
    result = _to_dt(dt)
    assert result.tzinfo == timezone.utc
    assert result.hour == 12


def test_to_dt_datetime_aware():
    from datetime import timedelta, timezone as tz
    eastern = tz(timedelta(hours=-5))
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=eastern)
    result = _to_dt(dt)
    assert result.tzinfo == timezone.utc
    assert result.hour == 17


def test_to_dt_iso_string():
    result = _to_dt("2024-01-01T00:00:00Z")
    assert result == datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_to_dt_iso_string_no_tz():
    result = _to_dt("2024-01-01 12:00:00")
    assert result == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_to_dt_epoch_seconds():
    result = _to_dt(0.0)
    assert result == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_to_dt_epoch_millis():
    result = _to_dt(1_000_000_000_000)  # > 1e11 → divide by 1000
    assert result.year == 2001


def test_to_dt_date_object():
    from datetime import date
    result = _to_dt(date(2024, 6, 15))
    assert result == datetime(2024, 6, 15, tzinfo=timezone.utc)


def test_to_dt_unrecognized_raises():
    with pytest.raises(ValueError):
        _to_dt(object())


# ------------------------------------------------------------------ _parse_wide


def test_parse_wide_basic(tmp_path):
    driver = make_driver({"csv_watch_dir": str(tmp_path)})
    driver.setup()
    path = _wide_csv(tmp_path)
    batch = driver.parse_file(path)

    assert set(batch.keys()) == {"temp", "rh"}
    assert len(batch["temp"]) == 2
    assert batch["temp"][0] == (datetime(2024, 1, 1, tzinfo=timezone.utc), 22.5)
    assert batch["rh"][0][1] == 55.0


def test_parse_wide_skips_null_values(tmp_path):
    p = tmp_path / "nulls.csv"
    p.write_text("time,temp\n2024-01-01T00:00:00Z,\n2024-01-02T00:00:00Z,23.0\n")
    driver = make_driver({"csv_watch_dir": str(tmp_path)})
    driver.setup()
    batch = driver.parse_file(p)
    assert len(batch["temp"]) == 1
    assert batch["temp"][0][1] == 23.0


def test_parse_wide_missing_time_col_raises(tmp_path):
    p = tmp_path / "no_time.csv"
    p.write_text("ts,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver = make_driver({"csv_watch_dir": str(tmp_path)})
    driver.setup()
    with pytest.raises(ValueError, match="time"):
        driver.parse_file(p)


# ------------------------------------------------------------------ _parse_narrow


def test_parse_narrow_basic(tmp_path):
    driver = make_driver({"csv_watch_dir": str(tmp_path)})
    driver.setup()
    path = _narrow_csv(tmp_path)
    batch = driver.parse_file(path)

    assert set(batch.keys()) == {"sensor/temp", "sensor/rh"}
    assert len(batch["sensor/temp"]) == 2
    assert batch["sensor/rh"][0][1] == 55.0


def test_parse_narrow_explicit_format(tmp_path):
    driver = make_driver({"csv_watch_dir": str(tmp_path), "csv_format": "narrow"})
    driver.setup()
    path = _narrow_csv(tmp_path)
    batch = driver.parse_file(path)
    assert "sensor/temp" in batch


def test_parse_narrow_missing_id_col_raises(tmp_path):
    p = tmp_path / "no_id.csv"
    p.write_text("time,value\n2024-01-01T00:00:00Z,1.0\n")
    driver = make_driver({"csv_watch_dir": str(tmp_path), "csv_format": "narrow"})
    driver.setup()
    with pytest.raises(ValueError, match="id"):
        driver.parse_file(p)


# ------------------------------------------------------------------ auto-detection


def test_auto_detects_wide(tmp_path):
    driver = make_driver({"csv_watch_dir": str(tmp_path)})
    driver.setup()
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "sensor_a": [1.0]})
    assert driver._detect_format(df) == "wide"


def test_auto_detects_narrow(tmp_path):
    driver = make_driver({"csv_watch_dir": str(tmp_path)})
    driver.setup()
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "id": ["s1"], "value": [1.0]})
    assert driver._detect_format(df) == "narrow"


# ------------------------------------------------------------------ TSV support


def test_tsv_parsed_correctly(tmp_path):
    p = tmp_path / "data.tsv"
    p.write_text("time\ttemp\n2024-01-01T00:00:00Z\t22.5\n")
    driver = make_driver({"csv_watch_dir": str(tmp_path)})
    driver.setup()
    batch = driver.parse_file(p)
    assert "temp" in batch
    assert batch["temp"][0][1] == 22.5


# ------------------------------------------------------------------ loop / archiving


def test_loop_archives_file_after_insert(tmp_path):
    watch = tmp_path / "watch"
    archive = tmp_path / "archive"
    watch.mkdir()
    driver = make_driver({
        "csv_watch_dir": str(watch),
        "csv_archive_dir": str(archive),
    })
    driver.setup()

    (watch / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()

    assert not (watch / "data.csv").exists()
    assert (archive / "data.csv").exists()
    driver.aq.insert_timeseries_batch.assert_called_once()


def test_loop_keeps_file_on_insert_failure(tmp_path):
    watch = tmp_path / "watch"
    watch.mkdir()
    driver = make_driver({"csv_watch_dir": str(watch)})
    driver.setup()
    driver.aq.insert_timeseries_batch.side_effect = RuntimeError("server down")

    (watch / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()

    assert (watch / "data.csv").exists()


def test_loop_handles_archive_name_collision(tmp_path):
    watch = tmp_path / "watch"
    archive = tmp_path / "archive"
    watch.mkdir()
    archive.mkdir()
    # Pre-create a collision
    (archive / "data.csv").write_text("old")

    driver = make_driver({
        "csv_watch_dir": str(watch),
        "csv_archive_dir": str(archive),
    })
    driver.setup()
    (watch / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()

    archived = list(archive.glob("data*.csv"))
    assert len(archived) == 2  # original + renamed


def test_loop_skips_bad_file_and_continues(tmp_path):
    watch = tmp_path / "watch"
    watch.mkdir()
    driver = make_driver({"csv_watch_dir": str(watch)})
    driver.setup()

    (watch / "bad.csv").write_text("garbage,,,,\n")
    (watch / "good.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()

    # good file should be archived; bad file stays
    assert (watch / "bad.csv").exists() or not (watch / "good.csv").exists()
    driver.aq.insert_timeseries_batch.assert_called_once()
