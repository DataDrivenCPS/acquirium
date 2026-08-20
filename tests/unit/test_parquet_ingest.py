"""Unit tests for ParquetIngestDriver — no server required."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl

from acquirium.Drivers.BuiltInDrivers.parquet_ingest import ParquetIngestDriver


def make_driver(cfg_overrides: dict | None = None, tmp_path: Path | None = None) -> ParquetIngestDriver:
    aq = MagicMock()
    aq.register_datasource.return_value = None
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 0}
    watch = str(tmp_path) if tmp_path else "/tmp/parquet_test_watch"
    driver = ParquetIngestDriver(aq, {"driver": {
        "watch_dir": watch, "glob": ["*.parquet", "*.pq"],
        "source_id": "parquet_files", "format": "wide",
        **(cfg_overrides or {}),
    }})
    driver.setup()
    return driver


def parse(driver, path, cursor=None):
    result = driver.read(path, cursor)
    observations, next_cursor = result.observations, result.next_cursor
    batch: dict[str, list] = {}
    if observations is not None:
        for row in observations.iter_rows(named=True):
            batch.setdefault(row["ref_name"], []).append((row["ts"], row["value"]))
    return batch, next_cursor


def _wide_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "wide.parquet"
    pl.DataFrame({
        "time": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"],
        "temp": [22.5, 23.0],
        "rh": [55.0, 60.0],
    }).write_parquet(p)
    return p


def test_parse_parquet_wide(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_parquet(tmp_path))
    assert cursor == 2
    assert set(batch) == {"temp", "rh"}
    assert batch["temp"][0][1] == "22.5"


def test_parse_parquet_narrow(tmp_path):
    p = tmp_path / "narrow.parquet"
    pl.DataFrame({
        "time": ["2024-01-01T00:00:00", "2024-01-01T00:00:00"],
        "id": ["sensor/temp", "sensor/rh"],
        "value": [22.5, 55.0],
    }).write_parquet(p)
    driver = make_driver({"format": "narrow"}, tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 2
    assert set(batch) == {"sensor/temp", "sensor/rh"}


def test_skip_cols_dropped(tmp_path):
    p = tmp_path / "notes.parquet"
    pl.DataFrame({
        "time": ["2024-01-01T00:00:00"], "temp": [22.5], "notes": ["ok"],
    }).write_parquet(p)
    driver = make_driver({"skip_cols": ["notes"]}, tmp_path=tmp_path)
    batch, _ = parse(driver, p)
    assert set(batch) == {"temp"}


def test_cursor_pages_new_rows_only(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_parquet(tmp_path), cursor=1)
    assert cursor == 2
    assert batch["temp"][0][1] == "23.0"


def test_custom_time_col(tmp_path):
    p = tmp_path / "ts.parquet"
    pl.DataFrame({
        "timestamp": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"], "flow": [1.2, 1.3],
    }).write_parquet(p)
    driver = make_driver({"time_col": "timestamp", "format": "wide"}, tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 2 and "flow" in batch


def test_loop_uses_explicit_source_id(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    path = _wide_parquet(tmp_path)
    driver.tick()
    source_id, table = driver.aq.insert_timeseries_arrow.call_args[0]
    assert source_id == "parquet_files"
    assert "temp" in pl.from_arrow(table)["ref_name"].to_list()
