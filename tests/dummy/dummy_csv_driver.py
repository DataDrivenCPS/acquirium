"""CSV driver for the fictional dummy plant, used by the Ray integration test.

Mirrors the file shape of the DPR trailer exports: row 1 is a metadata banner,
row 2 is the header, ``Date`` (MM/DD/YYYY) and ``Time`` (HH:MM:SS AM/PM) are
separate columns, and the rest are wide numeric / status streams.

The only TOML keys needed are ``watch_dir``, ``glob``, and ``interval``;
the source identity is fixed by this fictional deployment.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Drivers.Driver import FileBatch, FileIngestDriver

SOURCE_ID = "dummy-plant"
DATE_FMT = "%m/%d/%Y %I:%M:%S %p"

_THIS_DIR = Path(__file__).resolve().parent
MODEL_PATH = _THIS_DIR / "dummy_model.ttl"
COLUMN_LOOKUP_PATH = _THIS_DIR / "property_to_column.json"


class DummyCSVDriver(FileIngestDriver):
    def setup(self) -> None:
        self.source_id = SOURCE_ID
        super().setup()
        self.insert_graph_file(MODEL_PATH, replace=False)
        self._bind_points()

    def on_graph_change(self) -> None:
        print("on_graph_change fired", flush=True)
        self._bind_points()

    def _bind_points(self) -> None:
        mapping: dict[str, str] = json.loads(COLUMN_LOOKUP_PATH.read_text())
        for point_uri, column in mapping.items():
            self.declare(_dummy_ref_name(column), point_uri=point_uri)

    def read(self, path: Path, cursor: Any) -> FileBatch:
        offset = cursor or 0
        df = pl.read_csv(
            path, skip_rows=1, skip_rows_after_header=offset,
            encoding="utf8-lossy", truncate_ragged_lines=True,
            schema_overrides={"Date": pl.Utf8, "Time": pl.Utf8},
        )
        if df.is_empty():
            return FileBatch(None, cursor)

        streams = {c: _dummy_ref_name(c) for c in df.columns if c not in ("Date", "Time")}
        for name in streams.values():
            if not self.is_declared(name):
                self.declare(name)

        observations = (
            df.rename(streams)
            .with_columns(
                pl.concat_str([pl.col("Date"), pl.lit(" "), pl.col("Time")])
                .str.to_datetime(DATE_FMT, strict=False)
                .dt.replace_time_zone("UTC").alias("ts")
            )
            .select("ts", pl.exclude("ts", "Date", "Time").cast(pl.Utf8, strict=False))
            .unpivot(index="ts", variable_name="ref_name", value_name="value")
            .drop_nulls(["ts", "value"])
        )
        return FileBatch(observations, offset + len(df))


def _dummy_ref_name(column: str) -> str:
    """The dummy plant's deliberate legacy header-to-reference mapping."""
    unsafe = str.maketrans({" ": "_", "(": "", ")": ""})
    return " ".join(column.split()).translate(unsafe)
