from __future__ import annotations

import logging
import shutil
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Driver import Driver

logger = logging.getLogger("acquirium.csv_ingest")

_GLOB_PATTERNS = ("*.csv", "*.tsv")


class CSVIngestDriver(Driver):
    """Watches a directory for CSV/TSV files, ingests them into Acquirium, and
    archives each file after a successful insert so it isn't re-processed.

    Two formats are supported out of the box:

    **Wide** — one column per stream, one row per timestamp::

        time,              temp, rh,   flow
        2024-01-01T00:00Z, 22.5, 55.0, 1.2

    **Narrow** — (time, id, value) triples::

        time,              id,          value
        2024-01-01T00:00Z, sensor/temp, 22.5
        2024-01-01T00:00Z, sensor/rh,   55.0

    Format is auto-detected: if the DataFrame contains both the ``id`` and
    ``value`` columns it is treated as narrow, otherwise wide.

    Config keys (all optional, read from ``self.config["driver"]``):

    .. code-block:: toml

        [[drivers]]
        spec             = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
        interval         = 5.0
        csv_source_id    = "csv_files"
        csv_watch_dir    = "./data/incoming"
        csv_archive_dir  = "./data/processed"  # defaults to watch_dir/processed/
        csv_format       = "auto"              # "auto" | "narrow" | "wide"
        csv_time_col     = "time"
        csv_id_col       = "id"     # narrow only
        csv_value_col    = "value"  # narrow only

    **Extending for custom formats**

    Override ``parse_file()`` and call the protected helpers as needed::

        class MyDriver(CSVIngestDriver):
            def parse_file(self, path):
                import polars as pl
                # skip a 3-row metadata header, then treat as wide
                df = pl.read_csv(path, skip_rows=3)
                return self._parse_wide(df)

        class MyDriver(CSVIngestDriver):
            def parse_file(self, path):
                import polars as pl
                df = pl.read_csv(path).rename({"Timestamp": "time",
                                               "Tag": "id", "Val": "value"})
                return self._parse_narrow(df)
    """

    # ------------------------------------------------------------------ setup

    def setup(self) -> None:
        cfg = self.config.get("driver", {})
        self._source_id: str = cfg.get("csv_source_id", "csv_files")
        self._watch_dir = Path(cfg.get("csv_watch_dir", ".")).resolve()
        self._archive_dir = Path(
            cfg.get("csv_archive_dir", self._watch_dir / "processed")
        ).resolve()
        self._format: str = cfg.get("csv_format", "auto")
        self._time_col: str = cfg.get("csv_time_col", "time")
        self._id_col: str = cfg.get("csv_id_col", "id")
        self._value_col: str = cfg.get("csv_value_col", "value")
        self._registered: set[str] = set()

        self._watch_dir.mkdir(parents=True, exist_ok=True)
        self._archive_dir.mkdir(parents=True, exist_ok=True)

        self.aq.register_datasource(self._source_id)
        logger.info(
            "csv_ingest watching %s → archive %s", self._watch_dir, self._archive_dir
        )

    # ------------------------------------------------------------------ loop

    def loop(self) -> None:
        paths = sorted(
            p for pattern in _GLOB_PATTERNS for p in self._watch_dir.glob(pattern)
        )
        for path in paths:
            try:
                batch = self.parse_file(path)
            except Exception:
                logger.exception("csv_ingest: failed to parse %s", path.name)
                continue

            if not batch:
                logger.warning("csv_ingest: %s produced no data, archiving anyway", path.name)
            else:
                try:
                    self._ensure_streams(batch)
                    self.aq.insert_timeseries_batch(self._source_id, batch)
                    logger.info(
                        "csv_ingest: inserted %d stream(s) from %s",
                        len(batch), path.name,
                    )
                except Exception:
                    logger.exception("csv_ingest: failed to insert data from %s", path.name)
                    continue

            self._archive(path)

    # ---------------------------------------------------------- public hook

    def parse_file(self, path: Path) -> dict[str, list[tuple[datetime, Any]]]:
        """Parse *path* into a ``{ref_name: [(ts, val), ...]}`` batch.

        Override this in subclasses to handle non-standard layouts (extra header
        rows, Excel sheets, multi-table files, etc.).  The protected helpers
        ``_parse_wide`` and ``_parse_narrow`` are available for reuse.
        """
        sep = "\t" if path.suffix.lower() == ".tsv" else ","
        df = pl.read_csv(path, separator=sep, try_parse_dates=True)

        fmt = self._detect_format(df)
        if fmt == "narrow":
            return self._parse_narrow(df)
        return self._parse_wide(df)

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
            ref_name = str(stream_id)
            batch.setdefault(ref_name, []).append((_to_dt(ts_raw), val))
        return batch

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

    def _archive(self, path: Path) -> None:
        dest = self._archive_dir / path.name
        if dest.exists():
            stem, suffix = path.stem, path.suffix
            dest = self._archive_dir / f"{stem}_{int(time.time() * 1000)}{suffix}"
        shutil.move(str(path), dest)
        logger.debug("csv_ingest: archived %s → %s", path.name, dest.name)


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
