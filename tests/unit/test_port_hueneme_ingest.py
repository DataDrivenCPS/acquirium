"""Unit tests for the Port Hueneme custom ingest parser."""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import openpyxl
import pytest


def _load_driver_class():
    script = Path(__file__).parents[2] / "deployments/PORT_HUENEME/scripts/port_hueneme_ingest.py"
    spec = importlib.util.spec_from_file_location("port_hueneme_ingest", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.PortHuenemeUFIngestDriver


def _make_driver(tmp_path: Path):
    cls = _load_driver_class()
    return cls(MagicMock(), {"driver": {"watch_dir": str(tmp_path)}})


def test_port_hueneme_wide_time_xlsx(tmp_path):
    path = tmp_path / "20121111.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["TIME", "FE/FT-100", "PT-100", "__UNNAMED__18"])
    ws.append(["(sec)", "GPM", "PSIG", "CMS-200"])
    ws.append([0, 1.5, 2.5, 99])
    ws.append([2, 3.5, 4.5, 88])
    wb.save(path)

    df, rows = _make_driver(tmp_path).read_frame(path)

    assert rows == 2
    assert df.columns == ["FE/FT-100", "PT-100", "time"]
    assert df["FE/FT-100"].to_list() == pytest.approx([1.5, 3.5])
    assert df["time"].to_list() == [
        datetime(2012, 11, 11, 0, 0, 0),
        datetime(2012, 11, 11, 0, 0, 2),
    ]


def test_port_hueneme_summary_export_uses_tag_row_and_row_index_time(tmp_path):
    path = tmp_path / "20130816.csv"
    path.write_text(
        "\n".join(
            [
                "UF Inflow Rate,UF Element 1 (E1) Inflow rate,Cycle,Coagulant Dose",
                "FE/FT-100,FE/FT-101,,",
                "GPM,GPM,,ppm",
                "10.0,1.0,0,8.5",
                "11.0,1.5,0,",
                "12.0,2.0,1,",
            ]
        ),
        encoding="utf-8",
    )

    df, rows = _make_driver(tmp_path).read_frame(path, row_offset=1)

    assert rows == 2
    assert df.columns == ["FE/FT-100", "FE/FT-101", "time"]
    assert df["FE/FT-100"].to_list() == pytest.approx([11.0, 12.0])
    assert df["time"].to_list() == [
        datetime(2013, 8, 16, 0, 0, 1),
        datetime(2013, 8, 16, 0, 0, 2),
    ]
