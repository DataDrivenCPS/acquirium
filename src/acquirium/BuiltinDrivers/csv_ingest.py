from __future__ import annotations

import csv
from io import StringIO
import logging
from pathlib import Path

import polars as pl

from acquirium.internals._log import timed_debug
from acquirium.BuiltinDrivers.tabular_base import TabularIngestBase

logger = logging.getLogger("acquirium.csv_ingest")

# How many leading lines to scan when locating the header row by content.
_HEADER_SCAN_LIMIT = 100

_RAGGED_MODES = ("ignore", "skip", "error")


class CSVIngestDriver(TabularIngestBase):
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
        skip_cols    = ["notes"]     # optional columns to ignore entirely
        date_format  = "%m/%d/%Y"    # optional; only needed for non-ISO date strings
        skip_rows    = [1, 3]        # or { "subdir/data.csv" = [2, 5] }
        encoding     = "utf8-lossy"  # "utf8", "utf8-lossy", "latin1", etc.
        ragged_lines = "ignore"      # "ignore" | "skip" | "error"
        header_contains = ["time"]   # cell values that identify the header row

    ``ragged_lines`` controls what happens to data rows whose cell count
    differs from the header:

    - ``"ignore"`` (default) — keep the row; cells beyond the header are
      dropped and missing trailing cells become null
    - ``"skip"`` — drop the whole row
    - ``"error"`` — raise, refusing the file

    ``header_contains`` locates the header row by content instead of by
    position: any metadata/banner lines before the first line containing all
    of the listed cell values are skipped.  Use it when files sometimes carry
    a variable number of banner lines before an otherwise identical header;
    when set it takes precedence over ``skip_rows``.

    Override ``read_frame()`` to handle custom layouts.  Call ``read_df()``
    inside it to get the raw CSV frame with offset and skip-row handling
    already applied::

        class MyDriver(CSVIngestDriver):
            def read_frame(self, path, row_offset=0):
                df = self.read_df(path, row_offset)
                return df, len(df)
    """

    _glob_patterns = ("*.csv", "*.tsv")

    def configure_tabular_driver(self) -> None:
        self._encoding: str = self.config.get("driver", {}).get("encoding", "utf8-lossy")
        logger.info("csv_ingest watching %s", self._watch_dir)

    # ---------------------------------------------------------- config hooks

    def ragged_lines(self) -> str:
        """Policy for rows whose cell count differs from the header.

        One of ``"ignore"`` (default), ``"skip"``, or ``"error"``; see the
        class docstring.  Reads ``driver.ragged_lines`` from config.
        """
        mode = self.config.get("driver", {}).get("ragged_lines", "ignore")
        if mode not in _RAGGED_MODES:
            raise ValueError(
                f"driver.ragged_lines must be one of {_RAGGED_MODES}, got {mode!r}"
            )
        return mode

    def header_contains_for(self, path: Path) -> tuple[str, ...]:
        """Cell values that identify the header row of *path*.

        When non-empty, the header is located by content: the first line
        containing all of these cell values is treated as the header and any
        lines before it are skipped.  This handles files that carry a
        variable number of metadata/banner lines (including none) before an
        otherwise identical header.

        The default implementation reads ``driver.header_contains`` from
        config; subclasses can override to hard-code the expected header::

            def header_contains_for(self, path):
                return ("Date", "Time")
        """
        cells = self.config.get("driver", {}).get("header_contains", [])
        if isinstance(cells, str):
            cells = [cells]
        if not isinstance(cells, (list, tuple, set)):
            raise TypeError("driver.header_contains must be a cell value or a list of cell values")
        return tuple(str(cell) for cell in cells)

    def skip_rows_for(self, path: Path) -> tuple[int, ...]:
        markers = self.header_contains_for(path)
        if markers:
            header_line = self._detect_header_line(path, markers)
            return tuple(range(1, header_line))
        return super().skip_rows_for(path)

    # ------------------------------------------------------------------ read

    def read_frame(self, path: Path, row_offset: int = 0) -> tuple[pl.DataFrame, int]:
        df = self.read_df(path, row_offset)
        return df, len(df)

    def read_df(
        self,
        path: Path,
        row_offset: int,
        schema_overrides: dict | None = None,
    ) -> pl.DataFrame:
        sep = self._sep_for(path)
        skip = self.skip_rows_for(path)
        ragged = self.ragged_lines()
        truncate = ragged != "error"
        with timed_debug(logger, "csv read_df path=%s offset=%d skip=%d sep=%r", path.name, row_offset, len(skip), sep):
            include_cols = self._included_columns(path, sep)
            if skip or ragged == "skip":
                text = self._filtered_csv_text(path)
                if ragged == "skip":
                    text = self._drop_ragged_lines(text, sep, path)
                return pl.read_csv(
                    StringIO(text),
                    separator=sep, try_parse_dates=True,
                    skip_rows_after_header=row_offset,
                    encoding=self._encoding,
                    columns=include_cols,
                    schema_overrides=schema_overrides,
                    truncate_ragged_lines=truncate,
                )
            lf = pl.scan_csv(
                path, separator=sep, try_parse_dates=True,
                encoding=self._encoding, schema_overrides=schema_overrides,
                truncate_ragged_lines=truncate,
            )
            if row_offset:
                lf = lf.slice(row_offset)
            return lf.select(include_cols).collect()

    @staticmethod
    def _sep_for(path: Path) -> str:
        return "\t" if path.suffix.lower() == ".tsv" else ","

    def _included_columns(self, path: Path, sep: str) -> list[str]:
        col_names = self._column_names(path, sep)
        skip_cols = set(self.skip_cols(path, col_names))
        include_cols = [name for name in col_names if name not in skip_cols]
        if not include_cols:
            raise ValueError(f"all columns were skipped for {path}")
        return include_cols

    def _column_names(self, path: Path, sep: str) -> list[str]:
        truncate = self.ragged_lines() != "error"
        if self.skip_rows_for(path):
            df = pl.read_csv(
                StringIO(self._filtered_csv_text(path)),
                separator=sep,
                try_parse_dates=True,
                encoding=self._encoding,
                n_rows=0,
                truncate_ragged_lines=truncate,
            )
        else:
            df = pl.read_csv(
                path,
                separator=sep,
                try_parse_dates=True,
                encoding=self._encoding,
                n_rows=0,
                truncate_ragged_lines=truncate,
            )
        return [str(name) for name in df.columns]

    def _detect_header_line(self, path: Path, markers: tuple[str, ...]) -> int:
        """Return the 1-indexed line number of the header row of *path*.

        The header is the first line whose parsed cells include all of
        *markers*.  Raises ``ValueError`` when no such line exists within the
        first ``_HEADER_SCAN_LIMIT`` lines, rather than silently mis-parsing
        the file with a banner line as its header.
        """
        needed = {marker.strip() for marker in markers}
        sep = self._sep_for(path)
        with self._open_text(path) as f:
            for lineno, line in enumerate(f, start=1):
                if lineno > _HEADER_SCAN_LIMIT:
                    break
                cells = next(csv.reader([line], delimiter=sep), [])
                if needed <= {cell.strip() for cell in cells}:
                    return lineno
        raise ValueError(
            f"no header row containing {sorted(needed)} found in the first "
            f"{_HEADER_SCAN_LIMIT} lines of {path}"
        )

    def _drop_ragged_lines(self, text: str, sep: str, path: Path) -> str:
        """Drop data rows whose cell count differs from the header's."""
        out = StringIO()
        writer = csv.writer(out, delimiter=sep, lineterminator="\n")
        expected: int | None = None
        dropped = 0
        for row in csv.reader(StringIO(text), delimiter=sep):
            if expected is None:
                expected = len(row)
            elif len(row) != expected:
                dropped += 1
                continue
            writer.writerow(row)
        if dropped:
            logger.warning(
                'csv_ingest: dropped %d ragged row(s) from %s (ragged_lines = "skip")',
                dropped, path.name,
            )
        return out.getvalue()

    def _open_text(self, path: Path):
        if self._encoding == "utf8-lossy":
            return open(path, encoding="utf-8", errors="replace", newline="")
        return open(path, encoding=self._encoding, newline="")

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
