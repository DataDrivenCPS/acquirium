from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Driver import Driver

logger = logging.getLogger("acquirium.csv_ingest")

_GLOB_PATTERNS = ("*.csv", "*.tsv", "*.xlsx")


class CSVIngestDriver(Driver):
    """Watches a directory for CSV, TSV, and Excel files and ingests new rows
    into Acquirium on each tick.

    Row positions are tracked in memory so only rows added since the last tick
    are inserted.  Because the Acquirium API deduplicates on (timestamp, value),
    a restart will at worst re-insert the full file — no data is duplicated in
    the store.  Files are never moved or deleted.

    Each stream is named ``"{rel_path}/{column}"`` (wide) or
    ``"{rel_path}/{id_value}"`` (narrow), where *rel_path* is the file's path
    relative to ``csv_watch_dir``.  This namespaces streams by file so two
    files with the same column name produce distinct streams.

    Two formats are supported out of the box:

    **Wide** — one column per stream, one row per timestamp::

        time,              temp, rh,   flow
        2024-01-01T00:00Z, 22.5, 55.0, 1.2

    **Narrow** — (time, id, value) triples::

        time,              id,          value
        2024-01-01T00:00Z, sensor/temp, 22.5
        2024-01-01T00:00Z, sensor/rh,   55.0

    Format is auto-detected: if the DataFrame has both the ``id`` and ``value``
    columns it is treated as narrow, otherwise wide.

    The timestamp column is normalised to UTC via Polars before iteration, so
    ``Date``, ``Datetime``, and string columns are all handled automatically.
    Set ``csv_date_format`` to a ``strptime``-style format string if your dates
    use a non-ISO format that Polars cannot infer (e.g. ``"%m/%d/%Y"``).

    Config keys (all optional, under ``self.config["driver"]``):

    .. code-block:: toml

        [[drivers]]
        spec              = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
        interval          = 5.0
        csv_source_id     = "csv_files"
        csv_watch_dir     = "./data/incoming"
        csv_format        = "auto"        # "auto" | "narrow" | "wide"
        csv_time_col      = "time"
        csv_id_col        = "id"          # narrow only
        csv_value_col     = "value"       # narrow only
        csv_xlsx_sheets   = ["Sheet1"]    # xlsx only; omit to read the first sheet
        csv_date_format   = "%m/%d/%Y"    # optional; only needed for non-ISO date strings
        csv_encoding      = "utf8-lossy"  # "utf8", "utf8-lossy", "latin1", etc.

    **Extending for custom formats**

    Override ``parse_file()`` and call the protected helpers as needed.  The
    returned batch keys should be bare stream names (without the file prefix) —
    ``parse_file`` applies the prefix automatically::

        class MyDriver(CSVIngestDriver):
            def parse_file(self, path, row_offset=0):
                import polars as pl
                df = pl.read_csv(path, skip_rows=3,
                                 skip_rows_after_header=row_offset)
                return self._parse_wide(df), len(df)
    """

    # ------------------------------------------------------------------ setup

    def setup(self) -> None:
        cfg = self.config.get("driver", {})
        self._source_id: str = cfg.get("csv_source_id", "csv_files")
        self._watch_dir = Path(cfg.get("csv_watch_dir", ".")).resolve()
        self._format: str = cfg.get("csv_format", "auto")
        self._time_col: str = cfg.get("csv_time_col", "time")
        self._id_col: str = cfg.get("csv_id_col", "id")
        self._value_col: str = cfg.get("csv_value_col", "value")
        self._date_fmt: str | None = cfg.get("csv_date_format", None)
        self._encoding: str = cfg.get("csv_encoding", "utf8-lossy")
        raw_sheets = cfg.get("csv_xlsx_sheets", None)
        self._xlsx_sheets: list[str] | None = list(raw_sheets) if raw_sheets else None

        self._rows_seen: dict[str, int] = {}
        self._registered: set[str] = set()

        self._watch_dir.mkdir(parents=True, exist_ok=True)
        self.aq.register_datasource(self._source_id)
        logger.info("csv_ingest watching %s", self._watch_dir)

    # ------------------------------------------------------------------ loop

    def loop(self) -> None:
        paths = sorted(
            p
            for pattern in _GLOB_PATTERNS
            for p in self._watch_dir.rglob(pattern)
        )
        for path in paths:
            key = str(path)
            offset = self._rows_seen.get(key, 0)
            try:
                raw_batch, rows_read = self.parse_file(path, row_offset=offset)
            except Exception:
                logger.exception("csv_ingest: failed to parse %s", path.name)
                continue

            if not raw_batch:
                continue

            rel = path.relative_to(self._watch_dir)
            batch = {f"{rel}/{stream}": rows for stream, rows in raw_batch.items()}

            try:
                self._ensure_streams(batch)
                self.aq.insert_timeseries_batch(self._source_id, batch)
                total = sum(len(v) for v in batch.values())
                logger.info(
                    "csv_ingest: %s — inserted %d row(s) across %d stream(s)",
                    rel, total, len(batch),
                )
            except Exception:
                logger.exception("csv_ingest: failed to insert data from %s", path.name)
                continue

            self._rows_seen[key] = offset + rows_read

    # ---------------------------------------------------------- public hook

    def parse_file(
        self, path: Path, row_offset: int = 0
    ) -> tuple[dict[str, list[tuple[datetime, Any]]], int]:
        """Parse new rows from *path* starting after *row_offset* already-seen rows.

        Returns ``(batch, rows_read)`` where batch keys are bare stream names
        (column names or id values — the file prefix is added by the caller).
        Override in subclasses for custom layouts.
        """
        suffix = path.suffix.lower()
        if suffix == ".xlsx":
            df = self._read_excel(path, row_offset)
        else:
            sep = "\t" if suffix == ".tsv" else ","
            df = pl.read_csv(
                path, separator=sep, try_parse_dates=True,
                skip_rows_after_header=row_offset,
                encoding=self._encoding,
            )

        rows_read = len(df)
        if rows_read == 0:
            return {}, 0

        fmt = self._detect_format(df)
        batch = self._parse_narrow(df) if fmt == "narrow" else self._parse_wide(df)
        return batch, rows_read

    # ---------------------------------------------------------- format helpers

    def _detect_format(self, df: pl.DataFrame) -> str:
        if self._format != "auto":
            return self._format
        cols = set(df.columns)
        if self._id_col in cols and self._value_col in cols:
            return "narrow"
        return "wide"

    def _parse_wide(self, df: pl.DataFrame) -> dict[str, list[tuple[datetime, Any]]]:
        """Parse a wide-format DataFrame (one column per stream)."""
        if self._time_col not in df.columns:
            raise ValueError(f"time column '{self._time_col}' not found in {df.columns}")

        df = self._normalize_time_col(df.drop_nulls(subset=[self._time_col]))
        stream_cols = [c for c in df.columns if c != self._time_col]

        batch: dict[str, list[tuple[datetime, Any]]] = {}
        ts_list = df[self._time_col].to_list()
        for col in stream_cols:
            rows = [
                (ts, val)
                for ts, val in zip(ts_list, df[col].to_list())
                if val is not None
            ]
            if rows:
                batch[col] = rows
        return batch

    def _parse_narrow(self, df: pl.DataFrame) -> dict[str, list[tuple[datetime, Any]]]:
        """Parse a narrow-format DataFrame ((time, id, value) triples)."""
        for col in (self._time_col, self._id_col, self._value_col):
            if col not in df.columns:
                raise ValueError(f"column '{col}' not found in {df.columns}")

        df = self._normalize_time_col(
            df.drop_nulls(subset=[self._time_col, self._id_col])
        )

        batch: dict[str, list[tuple[datetime, Any]]] = {}
        for row in df.select([self._time_col, self._id_col, self._value_col]).iter_rows():
            ts, stream_id, val = row
            if stream_id is None:
                continue
            batch.setdefault(str(stream_id), []).append((ts, val))
        return batch

    # ---------------------------------------------------------- time normalisation

    _FALLBACK_DATE_FORMATS = (
        "%m/%d/%Y",   # US: 1/15/2025
        "%d/%m/%Y",   # European: 15/1/2025
        "%Y/%m/%d",   # ISO with slashes
        "%m-%d-%Y",   # US with dashes
        "%d-%m-%Y",   # European with dashes
        "%m/%d/%y",   # US 2-digit year
    )

    def _normalize_time_col(self, df: pl.DataFrame) -> pl.DataFrame:
        """Cast the time column to Datetime(us, UTC) regardless of its source type.

        For string columns, tries (in order): the configured ``csv_date_format``,
        Polars auto-detection, then common date formats as fallbacks.  Rows whose
        timestamp still cannot be parsed are dropped with a warning.
        """
        col = df[self._time_col]
        dtype = col.dtype

        if dtype == pl.Date:
            col = col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        elif dtype in (pl.String, pl.Utf8):
            col = self._parse_string_timestamps(col)
        else:
            # Datetime with some time_unit / time_zone combination
            tz = getattr(dtype, "time_zone", None)
            if tz is None:
                col = col.dt.replace_time_zone("UTC")
            elif tz != "UTC":
                col = col.dt.convert_time_zone("UTC")

        df = df.with_columns(col.alias(self._time_col))

        null_count = df[self._time_col].null_count()
        if null_count:
            logger.warning(
                "csv_ingest: %d row(s) with unparseable timestamps skipped "
                "(hint: set csv_date_format, e.g. csv_date_format = \"%%m/%%d/%%Y\")",
                null_count,
            )
            df = df.drop_nulls(subset=[self._time_col])
        return df

    def _parse_string_timestamps(self, col: pl.Series) -> pl.Series:
        """Parse a string Series as UTC Datetime, trying multiple formats."""
        non_null = col.drop_nulls().len()
        if non_null == 0:
            return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")

        # Try configured format first, then auto-detect, then common fallbacks
        candidates: list[str | None] = []
        if self._date_fmt:
            candidates.append(self._date_fmt)
        candidates.append(None)  # Polars auto-detect (ISO 8601, RFC 3339, etc.)
        candidates.extend(self._FALLBACK_DATE_FORMATS)

        best: pl.Series | None = None
        best_nulls = non_null + 1
        for fmt in candidates:
            parsed = col.str.to_datetime(format=fmt, strict=False)
            nulls = parsed.null_count()
            if nulls < best_nulls:
                best, best_nulls = parsed, nulls
            if nulls == 0:
                break

        assert best is not None
        tz = getattr(best.dtype, "time_zone", None)
        return best.dt.replace_time_zone("UTC") if tz is None else best.dt.convert_time_zone("UTC")

    # ---------------------------------------------------------- Excel

    def _read_excel(self, path: Path, row_offset: int) -> pl.DataFrame:
        """Read an Excel workbook, merging requested sheets into one DataFrame."""
        if self._xlsx_sheets:
            result = pl.read_excel(path, sheet_name=self._xlsx_sheets, engine="calamine")
            if isinstance(result, dict):
                frames = list(result.values())
                df = pl.concat(frames, how="diagonal_relaxed") if len(frames) > 1 else frames[0]
            else:
                df = result
        else:
            df = pl.read_excel(path, engine="calamine")

        return df.slice(row_offset)

    # ---------------------------------------------------------- stream reg

    def _ensure_streams(self, batch: dict[str, list[tuple[datetime, Any]]]) -> None:
        for ref_name in batch:
            if ref_name in self._registered:
                continue
            try:
                self.aq.register_stream(
                    f"urn:csv:{self._source_id}:{ref_name}",
                    source_id=self._source_id,
                    ref_name=ref_name,
                )
                self._registered.add(ref_name)
            except Exception:
                logger.warning(
                    "csv_ingest: could not register stream %s; data will still be inserted",
                    ref_name,
                )
