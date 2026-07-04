"""Ingest driver for the Port Hueneme UF/RO SCADA dataset (Gu et al. 2018).

Dataset: Zenodo record 4630769 / Dryad 10.5068/D1310B (CC0), ~3 years of 1 Hz
SCADA data from the NAVFAC-EXWC integrated UF/RO seawater desalination pilot.

The files (a mix of ``.csv`` and ``.xlsx`` under ``training/`` and ``test/``)
use several non-standard layouts that the stock CSV/XLSX drivers can't parse:

* **row 1** = instrument-tag headers, **row 2** = engineering units, row 3+ = data
* the main CSV ``Time`` column is an **Excel serial date** (float days since
  1899-12-30), which ``TabularIngestBase.normalize_timestamps`` does not decode
* elapsed-only and no-time exports need filename-date anchoring
* columns are P&ID tags (``FE/FT-*``, ``PT-*``, ``TE/TT-*``, ...)

This subclass of :class:`XLSXIngestDriver` handles all three: it reads either
extension as strings, drops the units row, decodes the Excel-serial timestamps
to UTC, consolidates every file under one datasource (``port-hueneme-uf``), and
links each tag stream to its WaTr-model Property (``point_uri``) so the model in
``../models/port-hueneme-uf.ttl`` resolves to live data.

Run it via ``acquirium.toml`` (see ../README.md), pointing ``watch_dir`` at the
extracted ``data in UF-RO system/`` tree.

    [[drivers]]
    spec      = "deployments/PORT_HUENEME/scripts/port_hueneme_ingest.py:PortHuenemeUFIngestDriver"
    interval  = 10.0
    watch_dir = "./data in UF-RO system"
    driver_id = "port-hueneme-uf"
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.BuiltinDrivers.xlsx_ingest import XLSXIngestDriver

logger = logging.getLogger("acquirium.port_hueneme")

# Excel serial epoch (the 1899-12-30 base absorbs the 1900 leap-year bug).
_EXCEL_EPOCH = datetime(1899, 12, 30)

SOURCE_ID = "port-hueneme-uf"
_PH = "urn:port-hueneme#"

# Time-column names by layout (see below): the CSVs carry an absolute Excel
# serial "Time" plus an elapsed "Time Elapsed"; the XLSX dumps carry only an
# elapsed-seconds "TIME". A few later experiment-summary exports carry no time
# column at all; for those, row order is the only available elapsed signal.
_CSV_TIME_COL = "Time"
_CSV_ELAPSED_COL = "Time Elapsed"
_XLSX_TIME_COL = "TIME"
_FILE_DATE_RE = re.compile(r"(\d{8})")

# Canonical column order for the handful of CSVs that ship with no header/units
# rows at all (data starts on row 1) -- see read_frame()/_read_all_string().
_CSV_COLUMNS: tuple[str, ...] = (
    _CSV_TIME_COL, _CSV_ELAPSED_COL,
    "FE/FT-100", "FE/FT-101", "FE/FT-102", "FE/FT-103", "FE/FT-104",
    "pH/pHT-100", "ORP/ORPT-100",
    "PT-100", "PT-101", "PT-102", "PT-103", "PT-104", "PT-100x",
    "Tu/TuT-100", "VFD-100-FB", "TE/TT-100",
)


def _is_float(s: str) -> bool:
    try:
        float(s)
    except ValueError:
        return False
    return True

# --- tag -> model Property URI map (see model TTL header + README).
# Three parallel UF modules (Gu et al. sec. 2.1), each with its own filtrate flow +
# pressure; the per-module tags map one-for-one to that module's filtrate properties.
# A tag absent here is still ingested as a raw stream, just without a point link.
TAG_TO_POINT: dict[str, str] = {
    "FE/FT-100":    _PH + "UF-feed-flow",
    "FE/FT-101":    _PH + "UF1-filtrate-flow",
    "FE/FT-102":    _PH + "UF2-filtrate-flow",
    "FE/FT-103":    _PH + "UF3-filtrate-flow",
    "FE/FT-104":    _PH + "UF-backwash-flow",
    "PT-100":       _PH + "UF-feed-pressure",
    "PT-101":       _PH + "UF1-filtrate-pressure",
    "PT-102":       _PH + "UF2-filtrate-pressure",
    "PT-103":       _PH + "UF3-filtrate-pressure",
    "PT-104":       _PH + "UF-filtrate-header-pressure",
    "PT-100x":      _PH + "UF-backwash-pressure",
    "TE/TT-100":    _PH + "UF-feed-temperature",
    "Tu/TuT-100":   _PH + "UF-feed-turbidity",
    "pH/pHT-100":   _PH + "UF-feed-ph",
    "ORP/ORPT-100": _PH + "UF-feed-orp",
    "VFD-100-FB":   _PH + "LP-pump-speed",
}

_TAG_ROW_MARKERS = frozenset(TAG_TO_POINT)

# Human-readable column names used by the 2013 experiment-summary exports.
# Their first data row repeats the real SCADA tags for most columns; this map is
# the fallback for columns whose tag cell is blank or otherwise not a stream.
_SUMMARY_COLUMN_TO_TAG: dict[str, str] = {
    "UF Inflow Rate": "FE/FT-100",
    "UF Element 1 (E1) Inflow rate": "FE/FT-101",
    "UF Element 2 (E2) Inflow rate": "FE/FT-102",
    "UF Element 3 (E3) Inflow rate": "FE/FT-103",
    "UF Backwash Flow Rate": "FE/FT-104",
    "Filtrate pH": "pH/pHT-100",
    "ORP": "ORP/ORPT-100",
    "MF Inlet Pressure": "PT-100",
    "UF Inlet Filtration Pressure": "PT-100",
    "UF Feed-Side Backwash Pressure": "PT-100x",
    "UF filtrate side backwash Pressure": "PT-104",
    "UF Filtrate-side Pressure": "PT-104",
    "UF Feedwater Turbidity": "Tu/TuT-100",
    "UF Filtrate Turbidity": "Tu/TuT-100",
    "UF Feed Pump RPM": "VFD-100-FB",
    "Filtrate Temperature": "TE/TT-100",
}


class PortHuenemeUFIngestDriver(XLSXIngestDriver):
    """Tabular driver for the dual-header, Excel-time Port Hueneme UF/RO files."""

    # Both extensions live in the same tree and share the exact same layout.
    _glob_patterns = ("*.csv", "*.xlsx")

    def after_tabular_setup(self) -> None:
        """Optionally load the WaTr model graph once, so one config does it all.

        Set ``model_graph`` (relative to the config file) under ``[driver]`` /
        ``[[drivers]]`` to insert ``models/port-hueneme-uf.ttl`` into the main
        graph on startup — mirroring the WaterTAP driver's ``insert_graph``.
        """
        model = self.config.get("driver", {}).get("model_graph")
        if not model or getattr(self, "_model_loaded", False):
            return
        path = Path(model)
        if not path.is_absolute():
            path = (self.config_dir() / path).resolve()
        logger.info("port_hueneme: loading model graph %s", path)
        self.aq.insert_graph(path.read_text(), format="turtle", replace=False)
        self._model_loaded = True

    def time_col(self) -> str:
        return "time"

    def source_id_for(self, path: Path) -> str:
        # One datasource for the whole study: stream identity is (source, tag),
        # independent of which daily file a row came from.
        return SOURCE_ID

    def stream_name_for(self, raw_name: str) -> str:
        # Preserve the exact SCADA tag as the stream ref_name so the model's
        # point_uri binding (below) and the stored stream agree.
        return raw_name

    def stream_specs_for_names(
        self,
        path: Path,
        source_id: str,
        raw_names: list[str],
        value_kinds: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Register each tag as a numeric stream, linking known tags to points."""
        specs: list[dict[str, Any]] = []
        for raw in raw_names:
            spec: dict[str, Any] = {
                "source_id": source_id,
                "ref_name": raw,
                "value_kind": "numeric",
            }
            point = TAG_TO_POINT.get(raw)
            if point is not None:
                # register_streams links point --ref:hasExternalReference--> stream
                spec["point_uri"] = point
            specs.append(spec)
        return specs

    # -- custom file parsing --------------------------------------------------
    # Two on-disk layouts share the same 16 tag columns and a units row under the
    # header, but differ in *time*:
    #   * CSV  -> comma-delimited; absolute Excel-serial "Time" (+ "Time Elapsed")
    #   * XLSX -> a single fixed-width text column; only elapsed-seconds "TIME",
    #             with NO absolute timestamp. We reconstruct absolute time as
    #             filename-date (YYYYMMDD) midnight UTC + elapsed seconds. That
    #             anchor is approximate (the CSVs proved filenames need not equal
    #             the internal date), but it preserves each run's cycle structure
    #             exactly, which is what the fouling math relies on.
    #
    # A handful of files break one of these assumptions outright:
    #   * some ".csv" files are actually XLSX payloads (detected by content, not
    #     extension) -- routed through the fixed-width XLSX path above
    #   * a few CSVs have no header/units rows at all (data starts on row 1) --
    #     detected by an all-numeric first row and given the canonical header
    #   * a few files use an experiment-summary export: descriptive column
    #     headers, a row of SCADA tags, a units/notes row, then data, with no
    #     timestamp column at all. We retain only recognizable SCADA tags and
    #     synthesize elapsed seconds from row order anchored to the filename date.
    def read_frame(self, path: Path, row_offset: int = 0) -> tuple[pl.DataFrame, int]:
        raw, has_units_row = self._read_all_string(path)
        if raw is None:
            return self._skip_unsupported(path, "unexpected column count for a headerless CSV")

        if self._is_fixed_width(raw):
            raw = self._explode_fixed_width(raw)
            time_expr = self._elapsed_time_expr(path)
            drop_cols = [_XLSX_TIME_COL]
        elif _XLSX_TIME_COL in raw.columns:
            raw = self._drop_unnamed_columns(raw)
            time_expr = self._elapsed_time_expr(path)
            drop_cols = [_XLSX_TIME_COL]
        elif _CSV_TIME_COL in raw.columns:
            time_expr = (
                pl.lit(_EXCEL_EPOCH)
                + pl.duration(seconds=pl.col(_CSV_TIME_COL).cast(pl.Float64) * 86400.0)
            )
            drop_cols = [_CSV_TIME_COL, _CSV_ELAPSED_COL]
        elif self._is_summary_export(raw):
            raw = self._normalize_summary_export(raw)
            has_units_row = False
            time_expr = self._row_index_time_expr(path, row_offset)
            drop_cols = []
        else:
            return self._skip_unsupported(
                path, "no supported Time/TIME/tag-row schema"
            )

        if time_expr is None:
            return raw.clear(), 0  # unparseable filename date; skip (already logged)

        # Row 0 (post-header) is the engineering-units row -> drop it (when
        # present), then page.
        data = raw.slice(1 if has_units_row else 0).slice(row_offset)
        rows_read = len(data)
        if rows_read == 0:
            return data, 0

        value_cols = [c for c in data.columns if c not in drop_cols]
        out = (
            data.with_columns(time_expr.alias("time"))
            .with_columns(pl.col(value_cols).cast(pl.Float64, strict=False))
            .drop([c for c in drop_cols if c in data.columns])
            .drop_nulls(subset=["time"])
        )
        # Base class melts this wide frame (time_col="time") into (ts, ref_name,
        # value); rows_read is the *new* data-row count for offset paging.
        return out, rows_read

    def _read_all_string(self, path: Path) -> tuple[pl.DataFrame | None, bool]:
        """Read the whole file with every column as Utf8.

        Returns ``(frame, has_units_row)``. Layout is detected by *content*,
        not the file extension -- a few files in this dataset are XLSX
        payloads saved with a ``.csv`` suffix. ``frame`` is ``None`` if a
        headerless CSV's column count doesn't match the canonical schema.

        Reading as strings keeps the units row (e.g. ``GPM``) from corrupting
        the numeric column dtypes; value columns are cast to float in
        ``read_frame`` after that row (and any header/date handling) is done.
        """
        if self._is_zip(path) or path.suffix.lower() == ".xlsx":
            df = pl.read_excel(path, engine="calamine")
            return df.with_columns(pl.all().cast(pl.Utf8, strict=False)), True

        # A few files carry stray rows with extra fields (e.g. a burst of
        # fault-status columns during an anomalous logging period);
        # truncate_ragged_lines keeps those rows (trimmed to width) instead of
        # failing the whole file.
        if _is_float(self._first_field(path)):
            # No header or units row at all -- data starts on row 1.
            raw = pl.read_csv(
                path, infer_schema_length=0, has_header=False, truncate_ragged_lines=True
            )
            if raw.width != len(_CSV_COLUMNS):
                return None, False
            return raw.rename(dict(zip(raw.columns, _CSV_COLUMNS))), False

        return pl.read_csv(path, infer_schema_length=0, truncate_ragged_lines=True), True

    @staticmethod
    def _is_zip(path: Path) -> bool:
        with path.open("rb") as fh:
            return fh.read(2) == b"PK"

    @staticmethod
    def _first_field(path: Path) -> str:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            return fh.readline().split(",", 1)[0].strip()

    def _skip_unsupported(self, path: Path, reason: str) -> tuple[pl.DataFrame, int]:
        """Permanently skip a file whose layout we don't recognize.

        Logged once per path (not every tick) since the offset never advances
        for a file we never successfully parse.
        """
        warned = self.__dict__.setdefault("_warned_unsupported", set())
        if path not in warned:
            warned.add(path)
            logger.warning("port_hueneme: skipping %s (%s)", path.name, reason)
        return pl.DataFrame(), 0

    @staticmethod
    def _is_fixed_width(raw: pl.DataFrame) -> bool:
        """True for the XLSX dumps: one column whose *name* is many tag tokens."""
        return raw.width == 1 and len(str(raw.columns[0]).split()) > 1

    @staticmethod
    def _explode_fixed_width(raw: pl.DataFrame) -> pl.DataFrame:
        """Split the single whitespace-delimited column into its real columns."""
        col = raw.columns[0]
        names = str(col).split()
        width = len(names)
        rows = [str(v).split() for v in raw[col].to_list()]
        rows = [r for r in rows if len(r) == width]  # drop any ragged lines
        return pl.DataFrame({names[i]: [r[i] for r in rows] for i in range(width)})

    def _elapsed_time_expr(self, path: Path) -> pl.Expr | None:
        """Absolute-time expr for elapsed-seconds files: filename date + seconds."""
        base = self._filename_date(path, "elapsed-seconds file")
        if base is None:
            return None
        return pl.lit(base) + pl.duration(seconds=pl.col(_XLSX_TIME_COL).cast(pl.Float64))

    def _row_index_time_expr(self, path: Path, row_offset: int) -> pl.Expr | None:
        """Absolute-time expr for files with no clock: filename date + row index."""
        base = self._filename_date(path, "row-indexed no-time export")
        if base is None:
            return None
        return pl.lit(base) + pl.duration(seconds=pl.int_range(pl.len()) + row_offset)

    @staticmethod
    def _filename_date(path: Path, layout: str) -> datetime | None:
        """Parse the YYYYMMDD filename anchor required by relative-time layouts."""
        m = _FILE_DATE_RE.search(path.stem)
        if not m:
            logger.warning(
                "port_hueneme: %s has no YYYYMMDD date in its name; skipping "
                "(%s needs a filename anchor)", path.name, layout
            )
            return None
        return datetime.strptime(m.group(1), "%Y%m%d")

    @staticmethod
    def _is_summary_export(raw: pl.DataFrame) -> bool:
        """True for no-time exports whose first row contains SCADA tag names."""
        if raw.is_empty():
            return False
        first = raw.head(1).row(0, named=True)
        values = {str(v).strip() for v in first.values() if v is not None}
        return len(values & _TAG_ROW_MARKERS) >= 2

    @staticmethod
    def _normalize_summary_export(raw: pl.DataFrame) -> pl.DataFrame:
        """Rename summary-export columns to SCADA tags and drop metadata columns."""
        first = raw.head(1).row(0, named=True)
        selected: list[pl.Expr] = []
        seen: set[str] = set()
        for col in raw.columns:
            tag = str(first.get(col) or "").strip()
            if tag not in TAG_TO_POINT:
                tag = _SUMMARY_COLUMN_TO_TAG.get(col, "")
            if tag not in TAG_TO_POINT or tag in seen:
                continue
            seen.add(tag)
            selected.append(pl.col(col).alias(tag))

        if not selected:
            return pl.DataFrame()

        # Summary exports have two header rows after the column names: SCADA tag
        # labels, then units/notes. Data starts on the third physical row.
        return raw.select(selected).slice(2)

    @staticmethod
    def _drop_unnamed_columns(raw: pl.DataFrame) -> pl.DataFrame:
        """Drop calamine placeholder columns that are not named SCADA streams."""
        keep = [
            c for c in raw.columns
            if c == _XLSX_TIME_COL or not str(c).startswith("__UNNAMED__")
        ]
        return raw.select(keep)
