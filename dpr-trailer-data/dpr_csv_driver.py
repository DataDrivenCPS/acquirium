from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver


class DPRTrailerCSVDriver(CSVIngestDriver):
    """Custom CSV driver for DPR trailer SCADA exports.

    Expected file shape:
    - row 1 is a metadata banner and should be skipped
    - row 2 is the CSV header
    - the timestamp is split across ``Date`` and ``Time`` columns
    - remaining columns are treated as wide-format streams
    """

    def setup(self) -> None:
        super().setup()
        cfg = self.config.get("driver", {})
        self.source_id = str(cfg.get("source_id", "dpr-trailer"))
        self._date_col = cfg.get("date_col", "Date")
        self._clock_col = cfg.get("clock_col", "Time")
        self._combined_time_col = "__acquirium_timestamp"
        if not self._skip_rows:
            self._skip_rows = [1]

    def _source_id_for_path(self, path: Path) -> str:
        return self.source_id

    def read_frame(self, path: Path, row_offset: int = 0) -> tuple[pl.DataFrame, int]:
        df = self._read_df(path, row_offset)
        rows_read = len(df)
        if rows_read == 0:
            return df, 0

        for col in (self._date_col, self._clock_col):
            if col not in df.columns:
                raise ValueError(f"column '{col}' not found in {df.columns}")

        date_expr = pl.col(self._date_col)
        if df[self._date_col].dtype == pl.Date:
            date_expr = date_expr.dt.strftime("%m/%d/%Y")
        else:
            date_expr = date_expr.cast(pl.String)

        df = df.with_columns(
            pl.concat_str(
                [
                    date_expr,
                    pl.lit(" "),
                    pl.col(self._clock_col).cast(pl.String),
                ]
            ).alias(self._combined_time_col)
        ).drop([self._date_col, self._clock_col])

        # Configure the base class to use our combined time column and its format
        self._time_col = self._combined_time_col
        self._date_fmt = "%m/%d/%Y %I:%M:%S %p"

        return df, rows_read

    def _time_column_reference_id(self) -> str:
        return f"{self._date_col},{self._clock_col}"
