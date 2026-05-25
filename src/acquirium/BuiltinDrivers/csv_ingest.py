from __future__ import annotations

from io import StringIO
import logging
from pathlib import Path

import polars as pl

from acquirium.BuiltinDrivers._tabular_base import _TabularIngestBase
from acquirium.internals._log import timed_debug

logger = logging.getLogger("acquirium.csv_ingest")


class CSVIngestDriver(_TabularIngestBase):
    """Watches a directory for CSV and TSV files and ingests new rows into Acquirium.

    Row positions are tracked in memory so only rows added since the last tick
    are inserted.  Files are never moved or deleted.

    Two formats are supported:

    **Wide** — one column per stream, one row per timestamp::

        time,              temp, rh,   flow
        2024-01-01T00:00Z, 22.5, 55.0, 1.2

    **Narrow** — (time, id, value) triples::

        time,              id,          value
        2024-01-01T00:00Z, sensor/temp, 22.5
        2024-01-01T00:00Z, sensor/rh,   55.0

    Config keys (all optional, under ``self.config["driver"]``):

    .. code-block:: toml

        [[drivers]]
        spec         = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
        interval     = 5.0
        watch_dir    = "./data/incoming"
        format       = "auto"        # "auto" | "wide" | "narrow"
        time_col     = "time"
        id_col       = "id"          # narrow only
        value_col    = "value"       # narrow only
        date_format  = "%m/%d/%Y"    # optional; only needed for non-ISO date strings
        skip_rows    = [1, 3]        # or { "subdir/data.csv" = [2, 5] }
        encoding     = "utf8-lossy"  # "utf8", "utf8-lossy", "latin1", etc.

    Override ``read_frame()`` to handle custom layouts.  Call ``read_df()``
    inside it to get the raw CSV frame with offset and skip-row handling
    already applied::

        class MyDriver(CSVIngestDriver):
            def read_frame(self, path, row_offset=0):
                df = self.read_df(path, row_offset)
                return df, len(df)
    """

    _glob_patterns = ("*.csv", "*.tsv")

    def setup(self) -> None:
        self._setup_common()
        self._encoding: str = self.config.get("driver", {}).get("encoding", "utf8-lossy")
        logger.info("csv_ingest watching %s", self._watch_dir)

    def read_frame(self, path: Path, row_offset: int = 0) -> tuple[pl.DataFrame, int]:
        df = self.read_df(path, row_offset)
        return df, len(df)

    def read_df(
        self,
        path: Path,
        row_offset: int,
        schema_overrides: dict | None = None,
    ) -> pl.DataFrame:
        sep = "\t" if path.suffix.lower() == ".tsv" else ","
        skip = self.skip_rows_for(path)
        with timed_debug(logger, "csv read_df path=%s offset=%d skip=%d sep=%r", path.name, row_offset, len(skip), sep):
            if skip:
                return pl.read_csv(
                    StringIO(self._filtered_csv_text(path)),
                    separator=sep, try_parse_dates=True,
                    skip_rows_after_header=row_offset,
                    encoding=self._encoding,
                    schema_overrides=schema_overrides,
                )
            lf = pl.scan_csv(
                path, separator=sep, try_parse_dates=True,
                encoding=self._encoding, schema_overrides=schema_overrides,
            )
            if row_offset:
                lf = lf.slice(row_offset)
            return lf.collect()

    def _filtered_csv_text(self, path: Path) -> str:
        skip_rows = set(self.skip_rows_for(path))
        raw = path.read_bytes()

        if self._encoding == "utf8-lossy":
            text = raw.decode("utf-8", errors="replace")
        else:
            text = raw.decode(self._encoding)

        return "".join(
            line
            for lineno, line in enumerate(text.splitlines(keepends=True), start=1)
            if lineno not in skip_rows
        )
