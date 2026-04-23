from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.BuiltinDrivers._tabular_base import _TabularIngestBase

logger = logging.getLogger("acquirium.xlsx_ingest")


class XLSXIngestDriver(_TabularIngestBase):
    """Watches a directory for Excel (XLSX) files and ingests new rows into Acquirium.

    Row positions are tracked in memory so only rows added since the last tick
    are inserted.  Files are never moved or deleted.

    Wide and narrow formats are supported — see ``CSVIngestDriver`` for details.
    When multiple sheets are specified they are concatenated before parsing.

    Config keys (all optional, under ``self.config["driver"]``):

    .. code-block:: toml

        [[drivers]]
        spec         = "acquirium.BuiltinDrivers.xlsx_ingest:XLSXIngestDriver"
        interval     = 5.0
        watch_dir    = "./data/incoming"
        format       = "auto"        # "auto" | "wide" | "narrow"
        time_col     = "time"
        id_col       = "id"          # narrow only
        value_col    = "value"       # narrow only
        date_format  = "%m/%d/%Y"    # optional; only needed for non-ISO date strings
        sheets       = ["Sheet1"]    # omit to read the first sheet only

    Override ``parse_file()`` to handle custom layouts::

        class MyDriver(XLSXIngestDriver):
            def parse_file(self, path, row_offset=0):
                df = pl.read_excel(path, sheet_name="Data", engine="calamine")
                df = df.slice(row_offset).rename({"Timestamp": "time"})
                return self._parse_wide(df), len(df)
    """

    _glob_patterns = ("*.xlsx",)

    def setup(self) -> None:
        self._setup_common()
        raw_sheets = self.config.get("driver", {}).get("sheets", None)
        self._sheets: list[str] | None = list(raw_sheets) if raw_sheets else None
        logger.info("xlsx_ingest watching %s", self._watch_dir)

    def parse_file(
        self, path: Path, row_offset: int = 0
    ) -> tuple[dict[str, list[tuple[datetime, Any]]], int]:
        df = self._read_excel(path, row_offset)
        rows_read = len(df)
        if rows_read == 0:
            return {}, 0
        fmt = self._detect_format(df)
        batch = self._parse_narrow(df) if fmt == "narrow" else self._parse_wide(df)
        return batch, rows_read

    def _read_excel(self, path: Path, row_offset: int) -> pl.DataFrame:
        """Read an Excel workbook, merging requested sheets into one DataFrame."""
        if self._sheets:
            result = pl.read_excel(path, sheet_name=self._sheets, engine="calamine")
            if isinstance(result, dict):
                frames = list(result.values())
                df = pl.concat(frames, how="diagonal_relaxed") if len(frames) > 1 else frames[0]
            else:
                df = result
        else:
            df = pl.read_excel(path, engine="calamine")
        return df.slice(row_offset)
