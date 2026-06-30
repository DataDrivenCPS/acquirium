"""Unit tests for ParquetIngestDriver — parsing logic only, no server required."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl

from acquirium.BuiltinDrivers.parquet_ingest import ParquetIngestDriver


def make_driver(cfg_overrides: dict | None = None, tmp_path: Path | None = None) -> ParquetIngestDriver:
    aq = MagicMock()
    aq.register_datasource.return_value = None
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 0}
    watch = str(tmp_path) if tmp_path else "/tmp/parquet_test_watch"
    config = {"driver": {"watch_dir": watch, **(cfg_overrides or {})}}
    return ParquetIngestDriver(aq, config)


def _wide_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "wide.parquet"
    pl.DataFrame({
        "time": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"],
        "temp": [22.5, 23.0],
        "rh": [55.0, 60.0],
    }).write_parquet(p)
    return p


def _narrow_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "narrow.parquet"
    pl.DataFrame({
        "time": ["2024-01-01T00:00:00", "2024-01-01T00:00:00"],
        "id": ["sensor/temp", "sensor/rh"],
        "value": [22.5, 55.0],
    }).write_parquet(p)
    return p


def test_parse_parquet_wide(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(_wide_parquet(tmp_path))
    assert rows == 2
    assert "temp" in batch and "rh" in batch
    assert batch["temp"][0][1] == 22.5


def test_parse_parquet_narrow(tmp_path):
    driver = make_driver({"format": "narrow"}, tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(_narrow_parquet(tmp_path))
    assert rows == 2
    assert "sensor/temp" in batch and "sensor/rh" in batch


def test_skip_cols_dropped(tmp_path):
    p = tmp_path / "notes.parquet"
    pl.DataFrame({
        "time": ["2024-01-01T00:00:00"],
        "temp": [22.5],
        "notes": ["ok"],
    }).write_parquet(p)
    driver = make_driver({"skip_cols": ["notes"]}, tmp_path=tmp_path)
    driver.setup()
    batch, _ = driver.parse_file(p)
    assert "temp" in batch and "notes" not in batch


def test_row_offset_pages_new_rows_only(tmp_path):
    path = _wide_parquet(tmp_path)
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    # Reading from an offset of 1 should only yield the second row.
    batch, rows = driver.parse_file(path, row_offset=1)
    assert rows == 1
    assert batch["temp"][0][1] == 23.0


def test_custom_time_col(tmp_path):
    p = tmp_path / "ts.parquet"
    pl.DataFrame({
        "timestamp": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"],
        "flow": [1.2, 1.3],
    }).write_parquet(p)
    driver = make_driver({"time_col": "timestamp", "format": "wide"}, tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(p)
    assert rows == 2 and "flow" in batch
