from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Drivers.Driver import FileBatch, FileIngestDriver
from acquirium.Drivers.tabular import to_observations

logger = logging.getLogger("acquirium.xlsx_ingest")


class XLSXIngestDriver(FileIngestDriver):
    """Watches a directory for Excel (XLSX) files and ingests new rows.

    All files use the required configured datasource. Wide and narrow layouts
    are supported — see ``CSVIngestDriver``. Multiple sheets are concatenated
    before reshaping.

    Config keys under ``self.config["driver"]``. ``source_id``, ``watch_dir``,
    ``glob``, and ``format`` are required; the remaining keys are optional:

    .. code-block:: toml

        [[drivers]]
        spec        = "acquirium.Drivers.BuiltInDrivers.xlsx_ingest:XLSXIngestDriver"
        interval    = 5.0
        watch_dir   = "./data/incoming"
        format      = "wide"        # required: "wide" | "narrow"
        source_id   = "incoming-xlsx"
        glob        = "*.xlsx"
        time_col    = "time"
        # date_col  = "Date"        # alternative split timestamp
        # clock_col = "Time"
        id_col      = "id"          # narrow only
        value_col   = "value"       # narrow only
        skip_cols   = ["notes"]     # columns to ignore entirely
        date_format = "%m/%d/%Y"    # optional override for timestamp parsing
        timezone    = "UTC"
        day_first   = false
        sheets      = ["Sheet1"]    # omit to read the first sheet only
    """

    def read(self, path: Path, cursor: Any) -> FileBatch:
        offset = cursor or 0
        cfg = self.config.get("driver", {})

        sheets = cfg.get("sheets") or None
        if sheets:
            result = pl.read_excel(path, sheet_name=list(sheets), engine="calamine")
            frames = list(result.values()) if isinstance(result, dict) else [result]
            df = pl.concat(frames, how="diagonal_relaxed") if len(frames) > 1 else frames[0]
        else:
            df = pl.read_excel(path, engine="calamine")

        skip_cols = cfg.get("skip_cols", [])
        skip_cols = [skip_cols] if isinstance(skip_cols, str) else list(skip_cols)
        drop = [c for c in skip_cols if c in df.columns]
        if drop:
            df = df.drop(drop)

        df = df.slice(offset)
        if df.is_empty():
            return FileBatch(None, cursor)

        layout = cfg.get("format")
        if layout is None:
            raise ValueError("XLSX ingestion requires driver.format = 'wide' or 'narrow'")
        observations = to_observations(
            df,
            time_col=cfg.get("time_col"),
            date_col=cfg.get("date_col"),
            clock_col=cfg.get("clock_col"),
            id_col=cfg.get("id_col", "id"),
            value_col=cfg.get("value_col", "value"),
            layout=layout,
            date_format=cfg.get("date_format"),
            timezone=cfg.get("timezone", "UTC"),
            day_first=bool(cfg.get("day_first", False)),
        )
        for name in observations["ref_name"].unique():
            self.declare_stream(name)
        return FileBatch(observations, offset + len(df))
