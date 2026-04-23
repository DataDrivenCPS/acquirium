"""Unit tests for CSVIngestDriver — parsing logic only, no server required."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver, _to_dt


# ------------------------------------------------------------------ fixtures


def make_driver(cfg_overrides: dict | None = None, tmp_path: Path | None = None) -> CSVIngestDriver:
    aq = MagicMock()
    aq.register_datasource.return_value = "csv_files"
    aq.register_stream.return_value = None
    aq.insert_timeseries_batch.return_value = {"ok": True, "rows_inserted": 0}
    watch = str(tmp_path) if tmp_path else "/tmp/csv_test_watch"
    config = {"driver": {"csv_watch_dir": watch, **(cfg_overrides or {})}}
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
    result = _to_dt(datetime(2024, 1, 1, 12, 0, 0))
    assert result.tzinfo == timezone.utc
    assert result.hour == 12


def test_to_dt_datetime_aware():
    from datetime import timedelta, timezone as tz
    eastern = tz(timedelta(hours=-5))
    result = _to_dt(datetime(2024, 1, 1, 12, 0, 0, tzinfo=eastern))
    assert result.tzinfo == timezone.utc
    assert result.hour == 17


def test_to_dt_iso_string():
    assert _to_dt("2024-01-01T00:00:00Z") == datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_to_dt_iso_string_no_tz():
    assert _to_dt("2024-01-01 12:00:00") == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_to_dt_epoch_seconds():
    assert _to_dt(0.0) == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_to_dt_epoch_millis():
    assert _to_dt(1_000_000_000_000).year == 2001


def test_to_dt_date_object():
    from datetime import date
    assert _to_dt(date(2024, 6, 15)) == datetime(2024, 6, 15, tzinfo=timezone.utc)


def test_to_dt_unrecognized_raises():
    with pytest.raises(ValueError):
        _to_dt(object())


# ------------------------------------------------------------------ _parse_wide


def test_parse_wide_basic(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(_wide_csv(tmp_path))
    assert set(batch.keys()) == {"temp", "rh"}
    assert rows == 2
    assert batch["temp"][0] == (datetime(2024, 1, 1, tzinfo=timezone.utc), 22.5)
    assert batch["rh"][0][1] == 55.0


def test_parse_wide_skips_null_values(tmp_path):
    p = tmp_path / "nulls.csv"
    p.write_text("time,temp\n2024-01-01T00:00:00Z,\n2024-01-02T00:00:00Z,23.0\n")
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    batch, _ = driver.parse_file(p)
    assert len(batch["temp"]) == 1
    assert batch["temp"][0][1] == 23.0


def test_parse_wide_missing_time_col_raises(tmp_path):
    p = tmp_path / "no_time.csv"
    p.write_text("ts,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    with pytest.raises(ValueError, match="time"):
        driver.parse_file(p)


# ------------------------------------------------------------------ _parse_narrow


def test_parse_narrow_basic(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(_narrow_csv(tmp_path))
    assert set(batch.keys()) == {"sensor/temp", "sensor/rh"}
    assert rows == 3
    assert len(batch["sensor/temp"]) == 2
    assert batch["sensor/rh"][0][1] == 55.0


def test_parse_narrow_explicit_format(tmp_path):
    driver = make_driver({"csv_format": "narrow"}, tmp_path=tmp_path)
    driver.setup()
    batch, _ = driver.parse_file(_narrow_csv(tmp_path))
    assert "sensor/temp" in batch


def test_parse_narrow_missing_id_col_raises(tmp_path):
    p = tmp_path / "no_id.csv"
    p.write_text("time,value\n2024-01-01T00:00:00Z,1.0\n")
    driver = make_driver({"csv_format": "narrow"}, tmp_path=tmp_path)
    driver.setup()
    with pytest.raises(ValueError, match="id"):
        driver.parse_file(p)


# ------------------------------------------------------------------ auto-detection


def test_auto_detects_wide(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "sensor_a": [1.0]})
    assert driver._detect_format(df) == "wide"


def test_auto_detects_narrow(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "id": ["s1"], "value": [1.0]})
    assert driver._detect_format(df) == "narrow"


# ------------------------------------------------------------------ TSV support


def test_tsv_parsed_correctly(tmp_path):
    p = tmp_path / "data.tsv"
    p.write_text("time\ttemp\n2024-01-01T00:00:00Z\t22.5\n")
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    batch, _ = driver.parse_file(p)
    assert "temp" in batch
    assert batch["temp"][0][1] == 22.5


# ------------------------------------------------------------------ row offset / append tracking


def test_row_offset_skips_already_seen_rows(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_csv(tmp_path)  # 2 data rows
    batch, rows = driver.parse_file(path, row_offset=1)
    assert rows == 1
    assert len(batch["temp"]) == 1
    assert batch["temp"][0][1] == 23.0  # second row only


def test_row_offset_past_end_returns_empty(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_csv(tmp_path)  # 2 rows
    batch, rows = driver.parse_file(path, row_offset=10)
    assert rows == 0
    assert batch == {}


def test_loop_advances_state_on_each_tick(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()

    path = tmp_path / "growing.csv"
    path.write_text("time,temp\n2024-01-01T00:00:00Z,1.0\n")
    driver.loop()
    assert driver.aq.insert_timeseries_batch.call_count == 1
    key = str(path)
    assert driver._state[key] == 1

    # Append a new row
    with path.open("a") as f:
        f.write("2024-01-02T00:00:00Z,2.0\n")
    driver.loop()
    assert driver.aq.insert_timeseries_batch.call_count == 2
    assert driver._state[key] == 2

    # No new rows — no insert
    driver.loop()
    assert driver.aq.insert_timeseries_batch.call_count == 2


def test_state_persisted_and_reloaded(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_csv(tmp_path)
    driver.loop()
    assert driver._state[str(path)] == 2

    # Simulate restart — new driver instance, same watch_dir
    driver2 = make_driver(tmp_path=tmp_path)
    driver2.setup()
    assert driver2._state.get(str(path)) == 2


# ------------------------------------------------------------------ archive mode


def test_archive_mode_moves_file_after_insert(tmp_path):
    archive = tmp_path / "archive"
    driver = make_driver(
        {"csv_archive": True, "csv_archive_dir": str(archive)}, tmp_path=tmp_path
    )
    driver.setup()
    (tmp_path / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()

    assert not (tmp_path / "data.csv").exists()
    assert (archive / "data.csv").exists()
    driver.aq.insert_timeseries_batch.assert_called_once()


def test_archive_mode_handles_name_collision(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    (archive / "data.csv").write_text("old")

    driver = make_driver(
        {"csv_archive": True, "csv_archive_dir": str(archive)}, tmp_path=tmp_path
    )
    driver.setup()
    (tmp_path / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()

    assert len(list(archive.glob("data*.csv"))) == 2


def test_no_archive_by_default_file_stays(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    (tmp_path / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()
    assert (tmp_path / "data.csv").exists()


# ------------------------------------------------------------------ error recovery


def test_loop_keeps_file_on_insert_failure(tmp_path):
    driver = make_driver({"csv_archive": True}, tmp_path=tmp_path)
    driver.setup()
    driver.aq.insert_timeseries_batch.side_effect = RuntimeError("server down")

    (tmp_path / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()

    assert (tmp_path / "data.csv").exists()
    assert driver._state.get(str(tmp_path / "data.csv"), 0) == 0


def test_loop_skips_bad_file_and_continues(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    (tmp_path / "bad.csv").write_text("not,valid\ncsvgarbagehere\n")
    (tmp_path / "good.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()
    driver.aq.insert_timeseries_batch.assert_called_once()
