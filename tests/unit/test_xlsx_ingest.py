"""Unit tests for XLSXIngestDriver — no server required."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import openpyxl
import polars as pl
import pytest

from acquirium.Drivers.BuiltInDrivers.xlsx_ingest import XLSXIngestDriver


def make_driver(cfg_overrides: dict | None = None, tmp_path: Path | None = None) -> XLSXIngestDriver:
    aq = MagicMock()
    aq.register_datasource.return_value = None
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 0}
    watch = str(tmp_path) if tmp_path else "/tmp/xlsx_test_watch"
    driver = XLSXIngestDriver(aq, {"driver": {
        "watch_dir": watch, "glob": "*.xlsx",
        "source_id": "xlsx_files", "format": "wide",
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


def _wide_xlsx(tmp_path: Path, name: str = "wide.xlsx", notes: bool = False) -> Path:
    p = tmp_path / name
    wb = openpyxl.Workbook()
    ws = wb.active
    header = ["time", "temp", "rh"] + (["notes"] if notes else [])
    ws.append(header)
    ws.append(["2024-01-01T00:00:00", 22.5, 55.0] + (["ok"] if notes else []))
    ws.append(["2024-01-02T00:00:00", 23.0, 60.0] + (["still ok"] if notes else []))
    wb.save(p)
    return p


def test_parse_xlsx_wide(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_xlsx(tmp_path))
    assert cursor == 2
    assert set(batch) == {"temp", "rh"}
    assert float(batch["temp"][0][1]) == pytest.approx(22.5)


def test_parse_xlsx_cursor(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_xlsx(tmp_path), cursor=1)
    assert cursor == 2
    assert float(batch["temp"][0][1]) == pytest.approx(23.0)


def test_parse_xlsx_cursor_past_end(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_xlsx(tmp_path), cursor=10)
    assert cursor == 10
    assert batch == {}


def test_parse_xlsx_skip_cols_from_config(tmp_path):
    driver = make_driver({"skip_cols": ["notes"]}, tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_xlsx(tmp_path, "notes.xlsx", notes=True))
    assert cursor == 2
    assert set(batch) == {"temp", "rh"}


def test_loop_uses_full_path_as_source_id(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    path = _wide_xlsx(tmp_path)
    driver.tick()
    source_id, table = driver.aq.insert_timeseries_arrow.call_args[0]
    assert source_id == "xlsx_files"
    assert "temp" in pl.from_arrow(table)["ref_name"].to_list()


def test_loop_does_not_pick_up_csv_files(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    (tmp_path / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,1.0\n")
    driver.tick()
    driver.aq.insert_timeseries_arrow.assert_not_called()


def test_loop_advances_cursor_on_each_tick(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    path = _wide_xlsx(tmp_path)
    driver.tick()
    assert driver.aq.insert_timeseries_arrow.call_count == 1
    assert driver._cursors[str(path)] == 2

    driver.tick()  # no new rows
    assert driver.aq.insert_timeseries_arrow.call_count == 1


def test_csv_driver_does_not_glob_xlsx(tmp_path):
    from acquirium.Drivers.BuiltInDrivers.csv_ingest import CSVIngestDriver

    aq = MagicMock()
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 0}
    driver = CSVIngestDriver(aq, {"driver": {
        "watch_dir": str(tmp_path), "glob": ["*.csv", "*.tsv"],
        "source_id": "csv_files", "format": "wide",
    }})
    driver.setup()
    _wide_xlsx(tmp_path)
    driver.tick()
    aq.insert_timeseries_arrow.assert_not_called()
