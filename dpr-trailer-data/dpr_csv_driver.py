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
        self._date_col = cfg.get("date_col", "Date")
        self._clock_col = cfg.get("clock_col", "Time")
        self._combined_time_col = "__acquirium_timestamp"
        if not self._skip_rows:
            self._skip_rows = [1]

    def parse_file(
        self, path: Path, row_offset: int = 0
    ) -> tuple[dict[str, list[tuple[Any, Any]]], int]:
        text = self._filtered_csv_text(path)
        df = pl.read_csv(
            StringIO(text),
            try_parse_dates=False,
            skip_rows_after_header=row_offset,
            encoding=self._encoding,
        )
        rows_read = len(df)
        if rows_read == 0:
            return {}, 0

        for col in (self._date_col, self._clock_col):
            if col not in df.columns:
                raise ValueError(f"column '{col}' not found in {df.columns}")

        df = (
            df.with_columns(
                pl.concat_str(
                    [
                        pl.col(self._date_col).cast(pl.String),
                        pl.lit(" "),
                        pl.col(self._clock_col).cast(pl.String),
                    ]
                ).alias(self._combined_time_col)
            )
            .drop([self._date_col, self._clock_col])
        )

        old_time_col = self._time_col
        old_date_fmt = self._date_fmt
        self._time_col = self._combined_time_col
        self._date_fmt = "%m/%d/%Y %I:%M:%S %p"
        try:
            batch = self._parse_wide(df)
        finally:
            self._time_col = old_time_col
            self._date_fmt = old_date_fmt

        return batch, rows_read

    def _time_column_reference_id(self) -> str:
        return f"{self._date_col},{self._clock_col}"
