"""Shared base class for tabular file ingest drivers (CSV, XLSX, etc.)."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

import polars as pl
from rdflib import Literal
from rdflib.namespace import RDF

from acquirium.Driver import Driver
from acquirium.internals.internals_namespaces import (
    DATA_SOURCE,
    FILE_LOCATION,
    FILE_REFERENCE,
    TIME_COLUMN_ID,
    VALUE_COLUMN_ID,
)

logger = logging.getLogger("acquirium.tabular_ingest")

# Spaces and characters that break URIs / RDF IRIs, mapped to safe replacements
_URI_UNSAFE = str.maketrans({
    "\n": "_", "\r": "_", "\t": "_",
    " ": "_", "(": "", ")": "",
    "#": "", "?": "", "<": "", ">": "",
    '"': "", "'": "", "\\": "", "`": "",
})


def _safe_name(s: str) -> str:
    return " ".join(s.split()).translate(_URI_UNSAFE)



class _TabularIngestBase(Driver):
    """Base class for drivers that watch a directory for tabular data files.

    Subclasses must set ``_glob_patterns`` and implement ``read_frame()``.

    Each file gets its own datasource whose ID is the file's full absolute path
    (sanitised for use in a URI).  Stream ref_names are the bare column names,
    so two files with the same column produce distinct streams because their
    source IDs differ.

    Row positions are tracked in memory so only rows added since the last tick
    are inserted.  Because the Acquirium API deduplicates on (timestamp, value),
    a restart will at worst re-insert the full file — no data is duplicated in
    the store.  Files are never moved or deleted.

    Shared config keys (all optional, under ``self.config["driver"]``):

    .. code-block:: toml

        watch_dir    = "./data/incoming"
        format       = "auto"          # "auto" | "wide" | "narrow"
        time_col     = "time"
        id_col       = "id"            # narrow only
        value_col    = "value"         # narrow only
        date_format  = "%m/%d/%Y"      # optional; only needed for non-ISO date strings
        skip_rows    = [1, 3, 1337]    # or { "subdir/data.csv" = [2, 5] }
    """

    _glob_patterns: tuple[str, ...] = ()

    # ------------------------------------------------------------------ setup

    def _setup_common(self) -> None:
        cfg = self.config.get("driver", {})
        watch_dir = Path(cfg.get("watch_dir", "."))
        if not watch_dir.is_absolute():
            watch_dir = (self.config_dir() / watch_dir).resolve()
        self._watch_dir = watch_dir
        self._format: str = cfg.get("format", "auto")
        self._time_col: str = cfg.get("time_col", "time")
        self._id_col: str = cfg.get("id_col", "id")
        self._value_col: str = cfg.get("value_col", "value")
        self._date_fmt: str | None = cfg.get("date_format", None)
        self._skip_rows = cfg.get("skip_rows", [])

        self._rows_seen: dict[str, int] = {}
        self._registered: dict[str, set[str]] = {}  # source_id → registered ref_names

        self._watch_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ loop

    def loop(self) -> None:
        paths = sorted({
            p
            for pattern in self._glob_patterns
            for p in self._watch_dir.rglob(pattern)
        })
        for path in paths:
            key = str(path)
            offset = self._rows_seen.get(key, 0)
            source_id = _safe_name(key)
            rel = path.relative_to(self._watch_dir)

            try:
                df, rows_read = self.parse_polars(path, row_offset=offset)
            except Exception:
                logger.exception("tabular_ingest: failed to parse %s", path.name)
                continue

            if df.is_empty():
                continue

            try:
                if source_id not in self._registered:
                    self.aq.register_datasource(source_id)
                stream_names = df["ref_name"].unique().to_list()
                self._ensure_streams(dict.fromkeys(stream_names), source_id, path)
                result = self.aq.insert_timeseries_polars(source_id, df)
                total = int(result.get("rows_inserted", len(df))) if isinstance(result, dict) else len(df)
                logger.info(
                    "tabular_ingest: %s — inserted %d row(s) across %d stream(s)",
                    rel, total, len(stream_names),
                )
                self._rows_seen[key] = offset + rows_read
            except Exception:
                logger.exception("tabular_ingest: failed to insert data from %s", path.name)

    # ---------------------------------------------------------- public hook

    def read_frame(self, path: Path, row_offset: int = 0) -> tuple["pl.DataFrame", int]:
        """Read new file rows into a tabular DataFrame.

        Subclasses can normalize custom layouts here, for example by combining
        split date/time columns into the configured time column. The returned
        frame should still be wide or narrow according to the standard tabular
        ingest config.
        """
        raise NotImplementedError

    def parse_file(
        self, path: Path, row_offset: int = 0
    ) -> tuple[dict[str, list[tuple[datetime, Any]]], int]:
        """Compatibility wrapper returning Python rows grouped by stream."""
        df, rows_read = self.read_frame(path, row_offset)
        if rows_read == 0:
            return {}, 0

        fmt = self._detect_format(df)
        batch = self._parse_narrow(df) if fmt == "narrow" else self._parse_wide(df)
        return batch, rows_read

    def parse_polars(
        self, path: Path, row_offset: int = 0
    ) -> tuple["pl.DataFrame", int]:
        """Return a melted ``(ts, ref_name, value)`` DataFrame and rows_read.

        Subclasses usually override ``read_frame()`` rather than this method.
        """
        df, rows_read = self.read_frame(path, row_offset)
        if rows_read == 0:
            return pl.DataFrame({"ts": [], "ref_name": [], "value": []}), 0
        return self._to_timeseries_frame(df), rows_read

    # ---------------------------------------------------------- format helpers

    def _detect_format(self, df: pl.DataFrame) -> str:
        if self._format != "auto":
            return self._format
        cols = set(df.columns)
        if self._id_col in cols and self._value_col in cols:
            return "narrow"
        return "wide"

    def _skip_rows_for(self, path: Path) -> tuple[int, ...]:
        skip_rows = self._skip_rows
        if isinstance(skip_rows, int):
            skip_rows = [skip_rows]

        if isinstance(skip_rows, dict):
            rel = path.relative_to(self._watch_dir).as_posix()
            skip_rows = skip_rows.get(rel, [])

        if not isinstance(skip_rows, (list, tuple, set)):
            raise TypeError(
                "driver.skip_rows must be a list of 1-indexed row numbers "
                "or a dict keyed by paths relative to watch_dir"
            )

        return tuple(sorted({int(row) for row in skip_rows if int(row) > 0}))

    def _parse_wide(self, df: pl.DataFrame) -> dict[str, list[tuple[datetime, Any]]]:
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
            batch.setdefault(_safe_name(str(stream_id)), []).append((ts, val))
        return batch

    def _to_timeseries_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        fmt = self._detect_format(df)
        if fmt == "narrow":
            out = self._narrow_to_timeseries_frame(df)
        else:
            out = self._wide_to_timeseries_frame(df)
        return out.with_columns(
            pl.col("ref_name").map_elements(_safe_name, return_dtype=pl.Utf8)
        )

    def _wide_to_timeseries_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._time_col not in df.columns:
            raise ValueError(f"time column '{self._time_col}' not found in {df.columns}")

        df = self._normalize_time_col(df.drop_nulls(subset=[self._time_col]))
        value_cols = [c for c in df.columns if c != self._time_col]
        return (
            df.with_columns([pl.col(c).cast(pl.Utf8) for c in value_cols])
            .unpivot(index=[self._time_col], variable_name="ref_name", value_name="value")
            .drop_nulls(subset=["value"])
            .rename({self._time_col: "ts"})
        )

    def _narrow_to_timeseries_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        for col in (self._time_col, self._id_col, self._value_col):
            if col not in df.columns:
                raise ValueError(f"column '{col}' not found in {df.columns}")

        df = self._normalize_time_col(df.drop_nulls(subset=[self._time_col, self._id_col]))
        return df.select([
            pl.col(self._time_col).alias("ts"),
            pl.col(self._id_col).cast(pl.Utf8).alias("ref_name"),
            pl.col(self._value_col).cast(pl.Utf8).alias("value"),
        ])

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
        col = df[self._time_col]
        dtype = col.dtype

        if dtype == pl.Date:
            col = col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        elif dtype in (pl.String, pl.Utf8):
            col = self._parse_string_timestamps(col)
        else:
            tz = getattr(dtype, "time_zone", None)
            if tz is None:
                col = col.dt.replace_time_zone("UTC")
            elif tz != "UTC":
                col = col.dt.convert_time_zone("UTC")

        df = df.with_columns(col.alias(self._time_col))

        null_count = df[self._time_col].null_count()
        if null_count:
            logger.warning(
                "tabular_ingest: %d row(s) with unparseable timestamps skipped "
                "(hint: set date_format, e.g. date_format = \"%%m/%%d/%%Y\")",
                null_count,
            )
            df = df.drop_nulls(subset=[self._time_col])
        return df

    def _parse_string_timestamps(self, col: pl.Series) -> pl.Series:
        non_null = col.drop_nulls().len()
        if non_null == 0:
            return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")

        # Configured format takes priority; None triggers Polars auto-detect (ISO 8601 / RFC 3339)
        candidates: list[str | None] = (
            ([self._date_fmt] if self._date_fmt else []) + [None] + list(self._FALLBACK_DATE_FORMATS)
        )

        best = col.str.to_datetime(format=candidates[0], strict=False)
        best_nulls = best.null_count()
        for fmt in candidates[1:]:
            if best_nulls == 0:
                break
            parsed = col.str.to_datetime(format=fmt, strict=False)
            nulls = parsed.null_count()
            if nulls < best_nulls:
                best, best_nulls = parsed, nulls

        tz = getattr(best.dtype, "time_zone", None)
        return best.dt.replace_time_zone("UTC") if tz is None else best.dt.convert_time_zone("UTC")

    # ---------------------------------------------------------- stream reg

    def _stream_registration_properties(self, path: Path, ref_name: str) -> dict[Any, Any]:
        rel = path.resolve().relative_to(self._watch_dir).as_posix()
        return {
            RDF.type: FILE_REFERENCE,
            DATA_SOURCE: Literal("CSV"),
            FILE_LOCATION: Literal(rel),
            TIME_COLUMN_ID: Literal(self._time_column_reference_id()),
            VALUE_COLUMN_ID: Literal(ref_name),
        }

    def _time_column_reference_id(self) -> str:
        return self._time_col

    def _ensure_streams(
        self,
        batch: dict[str, list[tuple[datetime, Any]]],
        source_id: str,
        path: Path,
    ) -> None:
        registered = self._registered.setdefault(source_id, set())
        new_ref_names = [ref_name for ref_name in batch if ref_name not in registered]
        if not new_ref_names:
            return

        try:
            self.aq.register_streams(
                [
                    {
                        "source_id": source_id,
                        "ref_name": ref_name,
                        "data_source": "CSV",
                        "properties": self._stream_registration_properties(path, ref_name),
                    }
                    for ref_name in new_ref_names
                ]
            )
            registered.update(new_ref_names)
        except Exception:
            logger.warning(
                "tabular_ingest: could not register %d stream(s); data will still be inserted",
                len(new_ref_names),
                exc_info=True,
            )
            return
