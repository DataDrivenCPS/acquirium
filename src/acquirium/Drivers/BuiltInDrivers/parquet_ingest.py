from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Drivers.Driver import FileIngestDriver, safe_stream_name
from acquirium.Drivers.BuiltInDrivers.tabular import to_observations

logger = logging.getLogger("acquirium.parquet_ingest")


class ParquetIngestDriver(FileIngestDriver):
    """Watches a directory for Parquet files and ingests new rows.

    Each file becomes its own datasource, named after its path. Wide and narrow
    layouts are supported — see ``CSVIngestDriver``. Parquet keeps native column
    dtypes, including real timestamp types, so date parsing is rarely needed.

    Config keys (all optional, under ``self.config["driver"]``):

    .. code-block:: toml

        [[drivers]]
        spec      = "acquirium.Drivers.BuiltInDrivers.parquet_ingest:ParquetIngestDriver"
        interval  = 5.0
        watch_dir = "./data/incoming"
        format    = "auto"        # "auto" | "wide" | "narrow"
        time_col  = "timestamp"   # WaterTAP's data generator writes "timestamp"
        id_col    = "id"          # narrow only
        value_col = "value"       # narrow only
        skip_cols = ["notes"]     # columns to ignore entirely
    """

    glob = ("*.parquet", "*.pq")

    def read(self, path: Path, cursor: Any) -> tuple[pl.DataFrame, Any]:
        offset = cursor or 0
        cfg = self.config.get("driver", {})

        df = pl.read_parquet(path)
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
