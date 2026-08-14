from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Drivers.Driver import FileIngestDriver, safe_stream_name
from acquirium.Drivers.BuiltInDrivers.tabular import to_observations

logger = logging.getLogger("acquirium.xlsx_ingest")


class XLSXIngestDriver(FileIngestDriver):
    """Watches a directory for Excel (XLSX) files and ingests new rows.

    Each file becomes its own datasource, named after its path. Wide and narrow
    layouts are supported — see ``CSVIngestDriver``. Multiple sheets are
    concatenated before reshaping.

    Config keys (all optional, under ``self.config["driver"]``):

    .. code-block:: toml

        [[drivers]]
        spec        = "acquirium.Drivers.BuiltInDrivers.xlsx_ingest:XLSXIngestDriver"
        interval    = 5.0
        watch_dir   = "./data/incoming"
        format      = "auto"        # "auto" | "wide" | "narrow"
        time_col    = "time"
        id_col      = "id"          # narrow only
        value_col   = "value"       # narrow only
        skip_cols   = ["notes"]     # columns to ignore entirely
        date_format = "%m/%d/%Y"    # only needed for non-ISO date strings
        sheets      = ["Sheet1"]    # omit to read the first sheet only
    """

    glob = "*.xlsx"

    def read(self, path: Path, cursor: Any) -> tuple[pl.DataFrame, Any]:
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
            return df, cursor

        # One datasource for the whole directory when the driver has one —
        # from `source_id` in config, or assigned by a subclass in setup() —
        # else one per file so identical column names stay distinct.
        source_id = self._source_id or safe_stream_name(str(path))
        observations = to_observations(
            df,
            time_col=cfg.get("time_col", "time"),
            id_col=cfg.get("id_col", "id"),
            value_col=cfg.get("value_col", "value"),
            layout=cfg.get("format", "auto"),
            date_format=cfg.get("date_format"),
        )
        for name in observations["ref_name"].unique():
            self.declare(name, source_id=source_id)
        return observations.with_columns(pl.lit(source_id).alias("source_id")), offset + len(df)
