from __future__ import annotations

import csv
import logging
from io import StringIO
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Drivers.Driver import FileIngestDriver, safe_stream_name
from acquirium.Drivers.BuiltInDrivers.tabular import to_observations

logger = logging.getLogger("acquirium.csv_ingest")

# How many leading lines to scan when locating the header row by content.
HEADER_SCAN_LIMIT = 100
RAGGED_MODES = ("ignore", "skip", "error")


class CSVIngestDriver(FileIngestDriver):
    """Watches a directory for CSV and TSV files and ingests new rows.

    Each file becomes its own datasource, named after its path, so two files
    with the same column name produce distinct streams. Only rows added since
    the last tick are read; files are never moved or deleted.

    **Wide** — one column per stream, one row per timestamp::

        time,              temp, rh,   flow
        2024-01-01T00:00Z, 22.5, 55.0, 1.2

    **Narrow** — (time, id, value) triples::

        time,              id,          value
        2024-01-01T00:00Z, sensor/temp, 22.5

    Config keys (all optional, under ``self.config["driver"]``):

    .. code-block:: toml

        [[drivers]]
        spec         = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
        interval     = 5.0
        watch_dir    = "./data/incoming"
        format       = "auto"        # "auto" | "wide" | "narrow"
        time_col     = "time"
        id_col       = "id"          # narrow only
        value_col    = "value"       # narrow only
        skip_cols    = ["notes"]     # columns to ignore entirely
        date_format  = "%m/%d/%Y"    # only needed for non-ISO date strings
        skip_rows    = [1, 3]        # or { "subdir/data.csv" = [2, 5] }
        encoding     = "utf8-lossy"  # "utf8", "utf8-lossy", "latin1", ...
        ragged_lines = "ignore"      # "ignore" | "skip" | "error"
        header_contains = ["time"]   # cell values identifying the header row

    ``ragged_lines`` controls rows whose cell count differs from the header:
    ``"ignore"`` keeps the row (extra cells dropped, missing ones null),
    ``"skip"`` drops the row, ``"error"`` refuses the file.

    ``header_contains`` locates the header by content instead of position,
    skipping any banner lines before the first line containing all the listed
    values. When set it takes precedence over ``skip_rows``.

    For a layout these keys cannot describe, subclass and override
    :meth:`read` — see ``FileIngestDriver``.
    """

    glob = ("*.csv", "*.tsv")

    def read(self, path: Path, cursor: Any) -> tuple[pl.DataFrame, Any]:
        offset = cursor or 0
        cfg = self.config.get("driver", {})
        separator = "\t" if path.suffix.lower() == ".tsv" else ","
        encoding = cfg.get("encoding", "utf8-lossy")

        ragged = cfg.get("ragged_lines", "ignore")
        if ragged not in RAGGED_MODES:
            raise ValueError(f"driver.ragged_lines must be one of {RAGGED_MODES}, got {ragged!r}")

        # A content-located header overrides skip_rows: everything above the
        # line carrying the marker values is banner, however much of it there is.
        markers = _as_tuple(cfg.get("header_contains", []), "header_contains")
        if markers:
            skip_rows = frozenset(range(1, _header_line(path, markers, separator, encoding)))
        else:
            skip_rows = _skip_rows_for(cfg.get("skip_rows", []), path, self.watch_dir)

        text = _read_text(path, encoding)
        if skip_rows:
            text = "".join(
                line for n, line in enumerate(text.splitlines(keepends=True), 1)
                if n not in skip_rows
            )
        if ragged == "skip":
            text = _drop_ragged_lines(text, separator, path)

        df = pl.read_csv(
            StringIO(text),
            separator=separator,
            try_parse_dates=True,
            skip_rows_after_header=offset,
            encoding=encoding,
            truncate_ragged_lines=ragged != "error",
        )
        skip_cols = [c for c in _as_tuple(cfg.get("skip_cols", []), "skip_cols") if c in df.columns]
        if skip_cols:
            df = df.drop(skip_cols)
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


# ------------------------------------------------------------------ helpers


def _as_tuple(value: Any, key: str) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, (list, tuple, set)):
        raise TypeError(f"driver.{key} must be a value or a list of values")
    return tuple(str(item) for item in value)


def _skip_rows_for(configured: Any, path: Path, watch_dir: Path) -> frozenset[int]:
    """Return the 1-indexed rows to drop from *path*."""
    if isinstance(configured, int):
        configured = [configured]
    if isinstance(configured, dict):
        configured = configured.get(path.relative_to(watch_dir).as_posix(), [])
    if not isinstance(configured, (list, tuple, set)):
        raise TypeError(
            "driver.skip_rows must be a list of 1-indexed row numbers "
            "or a dict keyed by paths relative to watch_dir"
        )
    return frozenset(int(row) for row in configured if int(row) > 0)


def _read_text(path: Path, encoding: str) -> str:
    raw = path.read_bytes()
    if encoding == "utf8-lossy":
        return raw.decode("utf-8", errors="replace")
    return raw.decode(encoding)


def _header_line(path: Path, markers: tuple[str, ...], separator: str, encoding: str) -> int:
    """Return the 1-indexed line number of the header row of *path*.

    Raises rather than silently mis-parsing the file with a banner line as its
    header when no line carries all the markers.
    """
    needed = {marker.strip() for marker in markers}
    for lineno, line in enumerate(_read_text(path, encoding).splitlines(), 1):
        if lineno > HEADER_SCAN_LIMIT:
            break
        cells = next(csv.reader([line], delimiter=separator), [])
        if needed <= {cell.strip() for cell in cells}:
            return lineno
    raise ValueError(
        f"no header row containing {sorted(needed)} found in the first "
        f"{HEADER_SCAN_LIMIT} lines of {path}"
    )


def _drop_ragged_lines(text: str, separator: str, path: Path) -> str:
    """Drop data rows whose cell count differs from the header's."""
    out = StringIO()
    writer = csv.writer(out, delimiter=separator, lineterminator="\n")
    expected: int | None = None
    dropped = 0
    for row in csv.reader(StringIO(text), delimiter=separator):
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
