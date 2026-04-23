from __future__ import annotations

import logging
from datetime import date, datetime, timezone
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
    files with the same column names produce distinct streams.

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
        raw_sheets = cfg.get("csv_xlsx_sheets", None)
        self._xlsx_sheets: list[str] | None = list(raw_sheets) if raw_sheets else None

        self._rows_seen: dict[str, int] = {}  # path → rows consumed so far
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

        df = df.drop_nulls(subset=[self._time_col])
        stream_cols = [c for c in df.columns if c != self._time_col]

        batch: dict[str, list[tuple[datetime, Any]]] = {}
        ts_series = df[self._time_col]
        for col in stream_cols:
            rows = [
                (_to_dt(ts), val)
                for ts, val in zip(ts_series.to_list(), df[col].to_list())
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

        df = df.drop_nulls(subset=[self._time_col, self._id_col])

        batch: dict[str, list[tuple[datetime, Any]]] = {}
        for row in df.select([self._time_col, self._id_col, self._value_col]).iter_rows():
            ts_raw, stream_id, val = row
            if stream_id is None:
                continue
            batch.setdefault(str(stream_id), []).append((_to_dt(ts_raw), val))
        return batch

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


# ------------------------------------------------------------------ helpers

def _to_dt(val: Any) -> datetime:
    """Coerce a value from a Polars column to a UTC-aware datetime."""
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)

    if isinstance(val, date):
        return datetime(val.year, val.month, val.day, tzinfo=timezone.utc)

    if isinstance(val, (int, float)):
        ts = float(val)
        if ts > 1e11:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if isinstance(val, str):
        text = val.strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        try:
            return _to_dt(float(text))
        except ValueError:
            pass

    raise ValueError(f"cannot coerce to datetime: {val!r}")
