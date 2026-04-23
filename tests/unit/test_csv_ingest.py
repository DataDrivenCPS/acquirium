"""Unit tests for CSVIngestDriver — parsing logic only, no server required."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.BuiltinDrivers._tabular_base import _safe_name
from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver


# ------------------------------------------------------------------ fixtures


def make_driver(cfg_overrides: dict | None = None, tmp_path: Path | None = None) -> CSVIngestDriver:
    aq = MagicMock()
    aq.register_datasource.return_value = "csv_files"
    aq.register_stream.return_value = None
    aq.insert_timeseries_batch.return_value = {"ok": True, "rows_inserted": 0}
    watch = str(tmp_path) if tmp_path else "/tmp/csv_test_watch"
    config = {"driver": {"watch_dir": watch, **(cfg_overrides or {})}}
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


# ------------------------------------------------------------------ _normalize_time_col


def test_normalize_date_col(tmp_path):
    """pl.Date column is cast to UTC Datetime at midnight."""
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    df = pl.DataFrame({"time": ["2024-01-01", "2024-01-02"], "val": [1.0, 2.0]}).with_columns(
        pl.col("time").str.to_datetime("%Y-%m-%d").cast(pl.Date)
    )
    result = driver._normalize_time_col(df)
    ts = result["time"].to_list()
    assert ts[0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert ts[1] == datetime(2024, 1, 2, tzinfo=timezone.utc)


def test_normalize_datetime_naive(tmp_path):
    """Naive Datetime gets UTC attached."""
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    df = pl.DataFrame({"time": [datetime(2024, 1, 1, 12, 0)], "val": [1.0]})
    result = driver._normalize_time_col(df)
    ts = result["time"].to_list()[0]
    assert ts.tzinfo is not None
    assert ts.hour == 12


def test_normalize_iso_string_col(tmp_path):
    """ISO date strings are parsed automatically."""
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    df = pl.DataFrame({"time": ["2024-01-15", "2024-02-20"], "val": [1.0, 2.0]})
    result = driver._normalize_time_col(df)
    ts = result["time"].to_list()
    assert ts[0] == datetime(2024, 1, 15, tzinfo=timezone.utc)


def test_normalize_string_with_explicit_format(tmp_path):
    """Non-ISO date strings are parsed when date_format is provided."""
    driver = make_driver({"date_format": "%m/%d/%Y"}, tmp_path=tmp_path)
    driver.setup()
    df = pl.DataFrame({"time": ["01/15/2024", "02/20/2024"], "val": [1.0, 2.0]})
    result = driver._normalize_time_col(df)
    ts = result["time"].to_list()
    assert ts[0] == datetime(2024, 1, 15, tzinfo=timezone.utc)
    assert ts[1] == datetime(2024, 2, 20, tzinfo=timezone.utc)


# ------------------------------------------------------------------ _parse_wide


def test_parse_wide_basic(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(_wide_csv(tmp_path))
    assert set(batch.keys()) == {"temp", "rh"}
    assert rows == 2
    assert batch["temp"][0] == (datetime(2024, 1, 1, tzinfo=timezone.utc), 22.5)
    assert batch["rh"][0][1] == 55.0


def test_parse_wide_date_only_col(tmp_path):
    """Date-only timestamps (no time component) work correctly."""
    p = tmp_path / "dates.csv"
    p.write_text("Date,temp\n2024-01-01,22.5\n2024-01-02,23.0\n")
    driver = make_driver({"time_col": "Date"}, tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(p)
    assert rows == 2
    ts0 = batch["temp"][0][0]
    assert ts0 == datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_parse_wide_non_iso_date_with_format(tmp_path):
    """Non-ISO date strings are handled when date_format is set."""
    p = tmp_path / "us_dates.csv"
    p.write_text("Date,temp\n01/15/2024,22.5\n01/16/2024,23.0\n")
    driver = make_driver({"time_col": "Date", "date_format": "%m/%d/%Y"}, tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(p)
    assert rows == 2
    assert batch["temp"][0][0] == datetime(2024, 1, 15, tzinfo=timezone.utc)


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


def test_parse_narrow_explicit_format(tmp_path):
    driver = make_driver({"format": "narrow"}, tmp_path=tmp_path)
    driver.setup()
    batch, _ = driver.parse_file(_narrow_csv(tmp_path))
    assert "sensor/temp" in batch


def test_parse_narrow_missing_id_col_raises(tmp_path):
    p = tmp_path / "no_id.csv"
    p.write_text("time,value\n2024-01-01T00:00:00Z,1.0\n")
    driver = make_driver({"format": "narrow"}, tmp_path=tmp_path)
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


# ------------------------------------------------------------------ per-file source_id


def test_loop_uses_full_path_as_source_id(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_csv(tmp_path)
    driver.loop()
    source_id, streams = driver.aq.insert_timeseries_batch.call_args[0]
    assert source_id == _safe_name(str(path))
    assert "temp" in streams
    assert "rh" in streams


def test_loop_full_path_source_id_includes_subdirectory(tmp_path):
    sub = tmp_path / "sensors"
    sub.mkdir()
    path = sub / "data.csv"
    path.write_text("time,flow\n2024-01-01T00:00:00Z,1.0\n")
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    driver.loop()
    source_id, streams = driver.aq.insert_timeseries_batch.call_args[0]
    assert source_id == _safe_name(str(path))
    assert "flow" in streams


# ------------------------------------------------------------------ row offset / append tracking


def test_row_offset_skips_already_seen_rows(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_csv(tmp_path)  # 2 data rows
    batch, rows = driver.parse_file(path, row_offset=1)
    assert rows == 1
    assert batch["temp"][0][1] == 23.0


def test_row_offset_past_end_returns_empty(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(_wide_csv(tmp_path), row_offset=10)
    assert rows == 0
    assert batch == {}


def test_loop_advances_cursor_on_each_tick(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()

    path = tmp_path / "growing.csv"
    path.write_text("time,temp\n2024-01-01T00:00:00Z,1.0\n")
    driver.loop()
    assert driver.aq.insert_timeseries_batch.call_count == 1
    assert driver._rows_seen[str(path)] == 1

    with path.open("a") as f:
        f.write("2024-01-02T00:00:00Z,2.0\n")
    driver.loop()
    assert driver.aq.insert_timeseries_batch.call_count == 2
    assert driver._rows_seen[str(path)] == 2

    driver.loop()
    assert driver.aq.insert_timeseries_batch.call_count == 2


def test_file_stays_in_place_after_insert(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_csv(tmp_path)
    driver.loop()
    assert path.exists()


# ------------------------------------------------------------------ error recovery


def test_loop_does_not_advance_cursor_on_insert_failure(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    driver.aq.insert_timeseries_batch.side_effect = RuntimeError("server down")
    path = _wide_csv(tmp_path)
    driver.loop()
    assert driver._rows_seen.get(str(path), 0) == 0


def test_loop_skips_bad_file_and_continues(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    (tmp_path / "bad.csv").write_text("not,valid\ncsvgarbagehere\n")
    (tmp_path / "good.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.loop()
    driver.aq.insert_timeseries_batch.assert_called_once()
