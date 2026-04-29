"""Unit tests for XLSXIngestDriver — parsing logic only, no server required."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from acquirium.BuiltinDrivers._tabular_base import _safe_name
from acquirium.BuiltinDrivers.xlsx_ingest import XLSXIngestDriver


# ------------------------------------------------------------------ helpers


def make_driver(cfg_overrides: dict | None = None, tmp_path: Path | None = None) -> XLSXIngestDriver:
    aq = MagicMock()
    aq.register_datasource.return_value = None
    aq.register_stream.return_value = None
    aq.insert_timeseries_batch.return_value = {"ok": True, "rows_inserted": 0}
    aq.insert_timeseries_polars.return_value = {"ok": True, "rows_inserted": 0}
    watch = str(tmp_path) if tmp_path else "/tmp/xlsx_test_watch"
    config = {"driver": {"watch_dir": watch, **(cfg_overrides or {})}}
    return XLSXIngestDriver(aq, config)


def _wide_xlsx(tmp_path: Path) -> Path:
    import openpyxl
    p = tmp_path / "wide.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["time", "temp", "rh"])
    ws.append(["2024-01-01T00:00:00", 22.5, 55.0])
    ws.append(["2024-01-02T00:00:00", 23.0, 60.0])
    wb.save(p)
    return p


# ------------------------------------------------------------------ parse_file


def test_parse_xlsx_wide(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_xlsx(tmp_path)
    batch, rows = driver.parse_file(path)
    assert rows == 2
    assert "temp" in batch
    assert "rh" in batch
    assert batch["temp"][0][1] == pytest.approx(22.5)


def test_parse_xlsx_row_offset(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_xlsx(tmp_path)
    batch, rows = driver.parse_file(path, row_offset=1)
    assert rows == 1
    assert batch["temp"][0][1] == pytest.approx(23.0)


def test_parse_xlsx_offset_past_end(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    batch, rows = driver.parse_file(_wide_xlsx(tmp_path), row_offset=10)
    assert rows == 0
    assert batch == {}


# ------------------------------------------------------------------ loop / source_id


def test_loop_uses_full_path_as_source_id(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_xlsx(tmp_path)
    driver.loop()
    source_id, df = driver.aq.insert_timeseries_polars.call_args[0]
    assert source_id == _safe_name(str(path))
    assert "temp" in df["ref_name"].to_list()


def test_loop_does_not_pick_up_csv_files(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    (tmp_path / "data.csv").write_text("time,temp\n2024-01-01T00:00:00Z,1.0\n")
    driver.loop()
    driver.aq.insert_timeseries_batch.assert_not_called()


def test_loop_advances_cursor_on_each_tick(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.setup()
    path = _wide_xlsx(tmp_path)
    driver.loop()
    assert driver.aq.insert_timeseries_polars.call_count == 1
    assert driver._rows_seen[str(path)] == 2

    # No new rows — second tick should be a no-op.
    driver.loop()
    assert driver.aq.insert_timeseries_polars.call_count == 1


# ------------------------------------------------------------------ config


def test_driver_does_not_glob_xlsx_when_using_csv_driver(tmp_path):
    """Sanity check: CSVIngestDriver does not touch .xlsx files."""
    from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver

    aq = MagicMock()
    aq.insert_timeseries_batch.return_value = {"ok": True}
    driver = CSVIngestDriver(aq, {"driver": {"watch_dir": str(tmp_path)}})
    driver.setup()
    _wide_xlsx(tmp_path)
    driver.loop()
    aq.insert_timeseries_batch.assert_not_called()
