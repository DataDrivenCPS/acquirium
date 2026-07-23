from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from acquirium.Drivers.BuiltInDrivers.tabular_base import TabularIngestBase
from acquirium.internals._log import timed_debug

logger = logging.getLogger("acquirium.parquet_ingest")


class ParquetIngestDriver(TabularIngestBase):
    """Watches a directory for Parquet files and ingests new rows into Acquirium.

    Row positions are tracked in memory so only rows added since the last tick
    are inserted.  Files are never moved or deleted.

    Wide and narrow formats are supported — see ``CSVIngestDriver`` for details.
    Parquet keeps native column dtypes (including real timestamp types), so no
    date parsing is usually needed.

    Config keys (all optional, under ``self.config["driver"]``):

    .. code-block:: toml

        [[drivers]]
        spec         = "acquirium.Drivers.BuiltInDrivers.parquet_ingest:ParquetIngestDriver"
        interval     = 5.0
        watch_dir    = "./data/incoming"
        format       = "auto"        # "auto" | "wide" | "narrow"
        time_col     = "timestamp"   # WaterTAP data-generator writes "timestamp"
        id_col       = "id"          # narrow only
        value_col    = "value"       # narrow only
        skip_cols    = ["notes"]     # optional columns to ignore entirely

    Override ``read_frame()`` to handle custom layouts::

        class MyDriver(ParquetIngestDriver):
            def read_frame(self, path, row_offset=0):
                df = pl.read_parquet(path).slice(row_offset).rename({"Timestamp": "time"})
                return df, len(df)
    """

    _glob_patterns = ("*.parquet", "*.pq")

    def configure_tabular_driver(self) -> None:
        logger.info("parquet_ingest watching %s", self._watch_dir)

    def read_frame(self, path: Path, row_offset: int = 0) -> tuple[pl.DataFrame, int]:
        df = self._read_parquet(path, row_offset)
        return df, len(df)

    def _read_parquet(self, path: Path, row_offset: int) -> pl.DataFrame:
        with timed_debug(logger, "parquet read path=%s offset=%d", path.name, row_offset):
            df = pl.read_parquet(path)
            skip_cols = set(self.skip_cols(path, [str(name) for name in df.columns]))
            if skip_cols:
                df = df.drop(list(skip_cols))
        return df.slice(row_offset)
