from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Drivers.Driver import FileBatch, FileIngestDriver
from acquirium.Drivers.tabular import to_observations

logger = logging.getLogger("acquirium.parquet_ingest")


class ParquetIngestDriver(FileIngestDriver):
    """Watches a directory for Parquet files and ingests new rows.

    All files use the required configured datasource. Wide and narrow layouts
    are supported — see ``CSVIngestDriver``. Parquet keeps native column dtypes,
    including real timestamp types, so date parsing is rarely needed.

    Config keys under ``self.config["driver"]``. ``source_id``, ``watch_dir``,
    ``glob``, and ``format`` are required; the remaining keys are optional:

    .. code-block:: toml

        [[drivers]]
        spec      = "acquirium.Drivers.BuiltInDrivers.parquet_ingest:ParquetIngestDriver"
        interval  = 5.0
        source_id = "parquet-exports"
        watch_dir = "./data/incoming"
        glob      = ["*.parquet", "*.pq"]
        format    = "wide"        # required: "wide" | "narrow"
        time_col  = "timestamp"   # WaterTAP's data generator writes "timestamp"
        # date_col  = "Date"      # alternative split timestamp
        # clock_col = "Time"
        id_col    = "id"          # narrow only
        value_col = "value"       # narrow only
        skip_cols = ["notes"]     # columns to ignore entirely
    """

    def read(self, path: Path, cursor: Any) -> FileBatch:
        offset = cursor or 0
        cfg = self.config.get("driver", {})
        batch = read_parquet_batch(
            path,
            cursor,
            time_col=cfg.get("time_col"),
            date_col=cfg.get("date_col"),
            clock_col=cfg.get("clock_col"),
            id_col=cfg.get("id_col", "id"),
            value_col=cfg.get("value_col", "value"),
            layout=cfg.get("format"),
            date_format=cfg.get("date_format"),
            timezone=cfg.get("timezone", "UTC"),
            day_first=bool(cfg.get("day_first", False)),
            skip_cols=cfg.get("skip_cols", []),
        )
        if batch.observations is not None:
            for name in batch.observations["ref_name"].unique():
                if not self.is_declared(name):
                    self.declare(name)
        return batch


def read_parquet_batch(
    path: Path,
    cursor: Any,
    *,
    time_col: str | None,
    id_col: str,
    value_col: str,
    layout: str | None,
    date_format: str | None = None,
    date_col: str | None = None,
    clock_col: str | None = None,
    timezone: str = "UTC",
    day_first: bool = False,
    skip_cols: str | list[str] | tuple[str, ...] = (),
) -> FileBatch:
    """Read one explicit Parquet file page into canonical observations."""
    if layout is None:
        raise ValueError("Parquet ingestion requires driver.format = 'wide' or 'narrow'")
    offset = cursor or 0
    df = pl.read_parquet(path)
    skipped = [skip_cols] if isinstance(skip_cols, str) else list(skip_cols)
    drop = [column for column in skipped if column in df.columns]
    if drop:
        df = df.drop(drop)
    df = df.slice(offset)
    if df.is_empty():
        return FileBatch(None, cursor)
    observations = to_observations(
        df,
        time_col=time_col,
        date_col=date_col,
        clock_col=clock_col,
        id_col=id_col,
        value_col=value_col,
        layout=layout,
        date_format=date_format,
        timezone=timezone,
        day_first=day_first,
    )
    return FileBatch(observations, offset + len(df))
