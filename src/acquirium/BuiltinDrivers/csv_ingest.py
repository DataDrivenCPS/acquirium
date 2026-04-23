from __future__ import annotations

import json
import logging
import shutil
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Driver import Driver

logger = logging.getLogger("acquirium.csv_ingest")

_GLOB_PATTERNS = ("*.csv", "*.tsv", "*.xlsx")
_STATE_FILENAME = ".csv_ingest_state.json"


class CSVIngestDriver(Driver):
    """Watches a directory for CSV, TSV, and Excel files and ingests new rows
    into Acquirium on each tick.

    **Row-level tracking** — a state file (``{watch_dir}/.csv_ingest_state.json``)
    records how many rows each file has contributed.  On every ``loop()`` call
    only rows beyond that cursor are ingested, so both brand-new files *and*
    files that are still being appended to are handled correctly.  The state
    survives restarts.

    Set ``csv_archive = true`` to move each file to ``csv_archive_dir`` after
    ingestion instead of leaving it in place (useful for pure drop-box
    workflows where files are never appended to).

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
        csv_format        = "auto"         # "auto" | "narrow" | "wide"
        csv_time_col      = "time"
        csv_id_col        = "id"           # narrow only
        csv_value_col     = "value"        # narrow only
        csv_archive       = false          # move files to archive_dir after insert
        csv_archive_dir   = "./data/processed"  # only used when csv_archive = true
        csv_xlsx_sheets   = ["Sheet1"]     # xlsx only; omit to read the first sheet

    **Extending for custom formats**

    Override ``parse_file()`` and call the protected helpers as needed::

        class MyDriver(CSVIngestDriver):
            def parse_file(self, path, row_offset=0):
                import polars as pl
                # skip a 3-row metadata header, then treat as wide
                df = pl.read_csv(path, skip_rows=3,
                                 skip_rows_after_header=row_offset)
                return self._parse_wide(df)

        class MyDriver(CSVIngestDriver):
            def parse_file(self, path, row_offset=0):
                import polars as pl
                df = pl.read_csv(path,
                                 skip_rows_after_header=row_offset).rename(
                    {"Timestamp": "time", "Tag": "id", "Val": "value"})
                return self._parse_narrow(df)
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
        self._do_archive: bool = bool(cfg.get("csv_archive", False))
        self._archive_dir = Path(
            cfg.get("csv_archive_dir", self._watch_dir / "processed")
        ).resolve()
        raw_sheets = cfg.get("csv_xlsx_sheets", None)
        self._xlsx_sheets: list[str] | None = list(raw_sheets) if raw_sheets else None
        self._registered: set[str] = set()

        self._watch_dir.mkdir(parents=True, exist_ok=True)
        if self._do_archive:
            self._archive_dir.mkdir(parents=True, exist_ok=True)

        self._state: dict[str, int] = {}
        self._state_file = self._watch_dir / _STATE_FILENAME
        self._load_state()

        self.aq.register_datasource(self._source_id)
        logger.info("csv_ingest watching %s (archive=%s)", self._watch_dir, self._do_archive)

    # ------------------------------------------------------------------ loop

    def loop(self) -> None:
        paths = sorted(
            p
            for pattern in _GLOB_PATTERNS
            for p in self._watch_dir.glob(pattern)
        )
        for path in paths:
            key = str(path)
            offset = self._state.get(key, 0)
            try:
                batch, rows_read = self.parse_file(path, row_offset=offset)
            except Exception:
                logger.exception("csv_ingest: failed to parse %s", path.name)
                continue

            if not batch:
                if rows_read == 0 and offset == 0:
                    logger.warning("csv_ingest: %s produced no data", path.name)
                if self._do_archive:
                    self._archive(path, key)
                continue

            try:
                self._ensure_streams(batch)
                self.aq.insert_timeseries_batch(self._source_id, batch)
                total_rows = sum(len(v) for v in batch.values())
                logger.info(
                    "csv_ingest: inserted %d row(s) across %d stream(s) from %s",
                    total_rows, len(batch), path.name,
                )
            except Exception:
                logger.exception("csv_ingest: failed to insert data from %s", path.name)
                continue

            self._state[key] = offset + rows_read
            self._save_state()

            if self._do_archive:
                self._archive(path, key)

    # ---------------------------------------------------------- public hook

    def parse_file(
        self, path: Path, row_offset: int = 0
    ) -> tuple[dict[str, list[tuple[datetime, Any]]], int]:
        """Parse new rows from *path* starting after *row_offset* already-seen rows.

        Returns ``(batch, rows_read)`` where *rows_read* is the number of rows
        actually parsed (excluding the header).  Override in subclasses for
        custom layouts; call ``_parse_wide`` / ``_parse_narrow`` as needed.
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
        sheets = self._xlsx_sheets
        if sheets:
            result = pl.read_excel(path, sheet_name=sheets, engine="calamine")
            if isinstance(result, dict):
                frames = list(result.values())
                df = pl.concat(frames, how="diagonal_relaxed") if len(frames) > 1 else frames[0]
            else:
                df = result
        else:
            df = pl.read_excel(path, engine="calamine")

        return df.slice(row_offset)

    # ---------------------------------------------------------- internals

    def _ensure_streams(self, batch: dict[str, list[tuple[datetime, Any]]]) -> None:
        for ref_name in batch:
            if ref_name in self._registered:
                continue
            point_uri = f"urn:csv:{self._source_id}:{ref_name}"
            try:
                self.aq.register_stream(
                    point_uri,
                    source_id=self._source_id,
                    ref_name=ref_name,
                )
                self._registered.add(ref_name)
            except Exception:
                logger.warning(
                    "csv_ingest: could not register stream %s; data will still be inserted",
                    ref_name,
                )

    def _archive(self, path: Path, state_key: str) -> None:
        dest = self._archive_dir / path.name
        if dest.exists():
            dest = self._archive_dir / f"{path.stem}_{int(time.time() * 1000)}{path.suffix}"
        shutil.move(str(path), dest)
        self._state.pop(state_key, None)
        self._save_state()
        logger.debug("csv_ingest: archived %s → %s", path.name, dest.name)

    def _load_state(self) -> None:
        if self._state_file.exists():
            try:
                self._state = json.loads(self._state_file.read_text())
                logger.debug("csv_ingest: loaded state (%d entries)", len(self._state))
            except Exception:
                logger.warning("csv_ingest: could not load state file, starting fresh")
                self._state = {}

    def _save_state(self) -> None:
        try:
            self._state_file.write_text(json.dumps(self._state, indent=2))
        except Exception:
            logger.warning("csv_ingest: could not save state file", exc_info=True)


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
