"""Shared base class for tabular file ingest drivers (CSV, XLSX, etc.)."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

import polars as pl

from acquirium.Driver import IngestDriver
from acquirium.Storage.values import assign_stream_value_kind, normalize_value_kind

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



class _TabularIngestBase(IngestDriver):
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
        # Load row offsets from persistent state (empty dict if not found)
        self._rows_seen: dict[str, int] = self.state.get("rows_seen", {})
        self._registered: dict[str, set[str]] = {}  # source_id → registered ref_names

        self._watch_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------- config hooks
    # Override any of these methods to customise behaviour without touching
    # private attributes.  Default implementations read from the driver config.

    def time_col(self) -> str:
        """Name of the column that holds timestamps."""
        return self.config.get("driver", {}).get("time_col", "time")

    def id_col(self) -> str:
        """Name of the stream-ID column (narrow format only)."""
        return self.config.get("driver", {}).get("id_col", "id")

    def value_col(self) -> str:
        """Name of the value column (narrow format only)."""
        return self.config.get("driver", {}).get("value_col", "value")

    def ingest_format(self) -> str:
        """Row layout: ``"wide"``, ``"narrow"``, or ``"auto"`` (default)."""
        return self.config.get("driver", {}).get("format", "auto")

    def date_format(self) -> str | None:
        """strptime format string for non-ISO timestamp strings, or ``None`` to auto-detect."""
        return self.config.get("driver", {}).get("date_format", None)

    def skip_rows_for(self, path: Path) -> tuple[int, ...]:
        """1-indexed row numbers to skip when reading *path*.

        Override to hard-code skip rows without touching config, e.g.::

            def skip_rows_for(self, path):
                return (1,)   # always skip the first row
        """
        skip_rows = self.config.get("driver", {}).get("skip_rows", [])
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

    # ------------------------------------------------------------------ loop

    def tick(self) -> None:
        for path in self._pending_paths():
            key = str(path)
            offset = self._rows_seen.get(key, 0)
            source_id = self.source_id_for(path)
            rel = path.relative_to(self._watch_dir)

            try:
                df, rows_read = self.parse_polars(path, row_offset=offset)
            except Exception:
                logger.exception("tabular_ingest: failed to parse %s", path.name)
                continue

            if df.is_empty():
                continue

            df, value_kinds = self._with_value_kinds(df)
            stream_names = value_kinds.keys()

            # Metadata registration — if the server is down these raise and
            # the file is skipped; the offset stays put so we retry next tick.
            if source_id not in self._registered:
                self.aq.register_datasource(source_id)
            self._ensure_streams(stream_names, source_id, value_kinds)

            logger.info(
                "tabular_ingest: %s — forwarding %d row(s) across %d stream(s) for source_id=%s",
                rel,
                len(df),
                len(stream_names),
                source_id,
            )
            result = self.insert_observations(
                df.with_columns(pl.lit(source_id).alias("source_id"))
            )

            if result.get("ok"):
                self._rows_seen[key] = offset + rows_read
                # Persist updated row offsets
                self.state.set("rows_seen", self._rows_seen)
                logger.info(
                    "tabular_ingest: %s — inserted %d row(s) across %d stream(s)",
                    rel, result.get("rows_inserted", 0), len(stream_names),
                )

    def source_id_for(self, path: Path) -> str:
        """Return the datasource ID to use for rows from *path*.

        Default: the sanitised absolute path, giving each file its own stream
        namespace.  Override to consolidate multiple files under one datasource.
        """
        return _safe_name(str(path))

    def _pending_paths(self) -> list[Path]:
        return sorted({
            p
            for pattern in self._glob_patterns
            for p in self._watch_dir.rglob(pattern)
        })

    def _source_id_for_path(self, path: Path) -> str:
        return _safe_name(str(path))

    def _with_value_kinds(self, df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, str]]:
        if "value_kind" in df.columns:
            kinds: dict[str, str] = {}
            for ref_name in df["ref_name"].unique().to_list():
                raw_kinds = (
                    df.filter(pl.col("ref_name") == ref_name)
                    .get_column("value_kind")
                    .drop_nulls()
                    .to_list()
                )
                normalized = {normalize_value_kind(value_kind) for value_kind in raw_kinds}
                if len(normalized) > 1:
                    raise ValueError(
                        f"stream {ref_name!r} has mixed value_kind values: {sorted(normalized)!r}"
                    )
                kinds[ref_name] = next(iter(normalized), "numeric")
        else:
            kinds = self._infer_value_kinds(df)

        return df.drop("value_kind") if "value_kind" in df.columns else df, kinds

    def _infer_value_kinds(self, df: pl.DataFrame) -> dict[str, str]:
        kinds: dict[str, str] = {}
        for ref_name in df["ref_name"].unique().to_list():
            values = (
                df.filter(pl.col("ref_name") == ref_name)
                .get_column("value")
                .to_list()
            )
            kinds[ref_name] = assign_stream_value_kind(
                values,
            )
        return kinds

    # ---------------------------------------------------------- public hook

    def read_frame(self, path: Path, row_offset: int = 0) -> tuple["pl.DataFrame", int]:
        """Read new file rows into a tabular DataFrame.

        Subclasses can normalize custom layouts here. The returned frame can be
        either wide/narrow according to the standard tabular ingest config, or
        an already-normalized timeseries frame with ``ts``, ``ref_name``, and
        ``value`` columns.
        """
        raise NotImplementedError

    def parse_file(
        self, path: Path, row_offset: int = 0
    ) -> tuple[dict[str, list[tuple[datetime, Any]]], int]:
        """Compatibility wrapper returning Python rows grouped by stream."""
        df, rows_read = self.read_frame(path, row_offset)
        if rows_read == 0:
            return {}, 0

        if self._is_timeseries_frame(df):
            return self._timeseries_frame_to_batch(df), rows_read

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
        if self._is_timeseries_frame(df):
            return self._normalize_timeseries_frame(df), rows_read
        return self._to_timeseries_frame(df), rows_read

    # ---------------------------------------------------------- format helpers

    def _detect_format(self, df: pl.DataFrame) -> str:
        fmt = self.ingest_format()
        if fmt != "auto":
            return fmt
        cols = set(df.columns)
        if self.id_col() in cols and self.value_col() in cols:
            return "narrow"
        return "wide"

    def _parse_wide(self, df: pl.DataFrame) -> dict[str, list[tuple[datetime, Any]]]:
        tc = self.time_col()
        if tc not in df.columns:
            raise ValueError(f"time column '{tc}' not found in {df.columns}")

        df = self._normalize_time_col(df.drop_nulls(subset=[tc]))
        stream_cols = [c for c in df.columns if c != tc]

        batch: dict[str, list[tuple[datetime, Any]]] = {}
        ts_list = df[tc].to_list()
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
        tc, ic, vc = self.time_col(), self.id_col(), self.value_col()
        for col in (tc, ic, vc):
            if col not in df.columns:
                raise ValueError(f"column '{col}' not found in {df.columns}")

        df = self._normalize_time_col(df.drop_nulls(subset=[tc, ic]))

        batch: dict[str, list[tuple[datetime, Any]]] = {}
        for row in df.select([tc, ic, vc]).iter_rows():
            ts, stream_id, val = row
            if stream_id is None:
                continue
            batch.setdefault(_safe_name(str(stream_id)), []).append((ts, val))
        return batch

    def _to_timeseries_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        if self._is_timeseries_frame(df):
            return self._normalize_timeseries_frame(df)

        fmt = self._detect_format(df)
        if fmt == "narrow":
            out = self._narrow_to_timeseries_frame(df)
        else:
            out = self._wide_to_timeseries_frame(df)
        return out.with_columns(
            pl.col("ref_name").map_elements(_safe_name, return_dtype=pl.Utf8)
        )

    def _wide_to_timeseries_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        tc = self.time_col()
        if tc not in df.columns:
            raise ValueError(f"time column '{tc}' not found in {df.columns}")

        df = self._normalize_time_col(df.drop_nulls(subset=[tc]))
        value_cols = [c for c in df.columns if c != tc]
        rows: list[tuple[Any, str, Any]] = []
        for col in value_cols:
            for ts, value in df.select([tc, col]).iter_rows():
                if value is not None:
                    rows.append((ts, col, value))
        return pl.DataFrame(
            rows,
            schema={"ts": df[tc].dtype, "ref_name": pl.Utf8, "value": pl.Object},
            orient="row",
        )

    def _narrow_to_timeseries_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        tc, ic, vc = self.time_col(), self.id_col(), self.value_col()
        for col in (tc, ic, vc):
            if col not in df.columns:
                raise ValueError(f"column '{col}' not found in {df.columns}")

        df = self._normalize_time_col(df.drop_nulls(subset=[tc, ic]))
        rows = [
            (ts, str(stream_id), value)
            for ts, stream_id, value in df.select([tc, ic, vc]).iter_rows()
            if stream_id is not None
        ]
        return pl.DataFrame(
            rows,
            schema={"ts": df[tc].dtype, "ref_name": pl.Utf8, "value": pl.Object},
            orient="row",
        )

    def _is_timeseries_frame(self, df: pl.DataFrame) -> bool:
        return {"ts", "ref_name", "value"}.issubset(df.columns)

    def _normalize_timeseries_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.select([
            pl.col("ts"),
            pl.col("ref_name").cast(pl.Utf8),
            pl.col("value"),
        ]).drop_nulls(subset=["ts", "ref_name"])
        df = df.with_columns(self.normalize_timestamps(df["ts"]).alias("ts"))
        df = df.drop_nulls(subset=["ts"])
        return df.with_columns(
            pl.col("ref_name").map_elements(_safe_name, return_dtype=pl.Utf8)
        )

    def _timeseries_frame_to_batch(self, df: pl.DataFrame) -> dict[str, list[tuple[datetime, Any]]]:
        df = self._normalize_timeseries_frame(df)
        batch: dict[str, list[tuple[datetime, Any]]] = {}
        for ts, ref_name, val in df.iter_rows():
            batch.setdefault(ref_name, []).append((ts, val))
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
        tc = self.time_col()
        df = df.with_columns(
            self.normalize_timestamps(df[tc], date_format=self.date_format()).alias(tc)
        )

        null_count = df[tc].null_count()
        if null_count:
            logger.warning(
                "tabular_ingest: %d row(s) with unparseable timestamps skipped "
                "(hint: set date_format, e.g. date_format = \"%%m/%%d/%%Y\")",
                null_count,
            )
            df = df.drop_nulls(subset=[tc])
        return df

    def normalize_timestamps(self, col: pl.Series, date_format: str | None = None) -> pl.Series:
        dtype = col.dtype

        if dtype == pl.Date:
            return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        if dtype in (pl.String, pl.Utf8):
            return self._parse_string_timestamps(col, date_format=date_format)

        tz = getattr(dtype, "time_zone", None)
        if tz is None:
            return col.dt.replace_time_zone("UTC")
        if tz != "UTC":
            return col.dt.convert_time_zone("UTC")
        return col

    def _parse_string_timestamps(
        self, col: pl.Series, date_format: str | None = None
    ) -> pl.Series:
        non_null = col.drop_nulls().len()
        if non_null == 0:
            return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")

        # Configured format takes priority; None triggers Polars auto-detect (ISO 8601 / RFC 3339)
        candidates: list[str | None] = (
            ([date_format] if date_format else []) + [None] + list(self._FALLBACK_DATE_FORMATS)
        )

        best: pl.Series | None = None
        best_nulls = non_null + 1

        for fmt in candidates:
            try:
                parsed = col.str.to_datetime(format=fmt, strict=False)
                nulls = parsed.null_count()
                if nulls < best_nulls:
                    best, best_nulls = parsed, nulls
                if best_nulls == 0:
                    break
            except Exception:
                continue

        if best is None:
            # Fallback to a null column if nothing worked
            best = col.str.to_datetime(format=None, strict=False)

        tz = getattr(best.dtype, "time_zone", None)
        return best.dt.replace_time_zone("UTC") if tz is None else best.dt.convert_time_zone("UTC")

    # ---------------------------------------------------------- stream reg

    def _ensure_streams(
        self,
        ref_names: list[str],
        source_id: str,
        value_kinds: dict[str, str] | None = None,
    ) -> None:
        registered = self._registered.setdefault(source_id, set())
        new_ref_names = [ref_name for ref_name in ref_names if ref_name not in registered]
        if not new_ref_names:
            return

        try:
            self.aq.register_streams(
                [
                    {
                        "source_id": source_id,
                        "ref_name": ref_name,
                        "value_kind": normalize_value_kind((value_kinds or {}).get(ref_name)),
                    }
                    for ref_name in new_ref_names
                ]
            )
            registered.update(new_ref_names)
        except Exception:
            logger.error(
                "tabular_ingest: could not register %d stream(s)",
                len(new_ref_names),
                exc_info=True,
            )
            raise
