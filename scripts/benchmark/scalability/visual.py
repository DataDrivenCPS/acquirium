#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import polars as pl
import numpy as np
import matplotlib.pyplot as plt


LAT_COLS = [
    "latency_measurement_to_received_ms",
    "latency_received_to_completed_ms",
    "latency_completed_to_endpoint_ms",
    "latency_total_ms",
]


def load_df(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    df = pl.read_csv(path)

    missing = [c for c in LAT_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{path} is missing columns: {missing}")

    # Select only what we need and coerce to float for safety
    return df.select([pl.col(c).cast(pl.Float64) for c in LAT_COLS])


def setup_matplotlib() -> None:
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "font.size": 16,
            "axes.titlesize": 18,
            "axes.labelsize": 16,
            "xtick.labelsize": 14,
            "ytick.labelsize": 14,
        }
    )


def boxplot_one_column(
    dfs: Sequence[pl.DataFrame],
    labels: Sequence[str],
    col: str,
    out_path: Path,
) -> None:
    # Extract values for this column from each df
    series_list = [df.get_column(col).drop_nulls() for df in dfs]
    data = [s.to_numpy() for s in series_list]

    # Compute medians (nan-safe)
    medians = [float(np.nanmedian(arr)) if arr.size else float("nan") for arr in data]

    plt.figure(figsize=(8, 3.2))
    plt.boxplot(
        data,
        tick_labels=labels,
        showmeans=False,
        meanline=False,
        whis=1.5,
        notch=False,
        showfliers=False,
    )

    ax = plt.gca()

    # Annotate medians
    y_min, y_max = ax.get_ylim()
    y_offset = (y_max - y_min) * 0.03
    for i, m in enumerate(medians, start=1):
        if np.isfinite(m):
            ax.text(i, m + y_offset, f"{m:.1f}", ha="center", va="bottom", fontsize=12)

    ax.set_title(col)
    ax.set_ylabel("Latency (ms)")
    ax.set_xlabel("CSV file")
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=200)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Save 4 boxplots (one per latency column) comparing multiple CSVs."
    )
    parser.add_argument(
        "csvs",
        nargs=3,
        type=Path,
        help="Three CSV files to compare (exactly 3).",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("plots"),
        help="Output directory for saved PNGs (default: ./plots).",
    )
    args = parser.parse_args()

    setup_matplotlib()

    csv_paths = args.csvs
    dfs = [load_df(p) for p in csv_paths]
    labels = [p.stem for p in csv_paths]

    for col in LAT_COLS:
        out = args.outdir / f"boxplot_{col}.png"
        boxplot_one_column(dfs, labels, col, out)


if __name__ == "__main__":
    main()
