#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import numpy as np
import polars as pl
import matplotlib.pyplot as plt


LAT_COLS = [
    "latency_measurement_to_received_ms",
    "latency_received_to_completed_ms",
    "latency_completed_to_endpoint_ms",
    "latency_total_ms",
    "processing_time_ms",
]

META_COLS = [
    "msg_id",
    "app_id",
    "level",
    "chain_depth",
]


def load_df(path: Path) -> pl.DataFrame:
    memo_path = Path("scripts/benchmark/chain/memo") / path.name.replace(".csv", ".parquet")
    if memo_path.exists():
        # Memoization: if the memo file exists, load from it instead of the original CSV.
            return None    
    else:
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        df = pl.read_csv(path)

        required = META_COLS + LAT_COLS
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"{path} is missing columns: {missing}")

        a = df.select(
            [
                pl.col("msg_id").cast(pl.Utf8),
                pl.col("app_id").cast(pl.Utf8),
                pl.col("level").cast(pl.Int64),
                pl.col("chain_depth").cast(pl.Int64),
                *[pl.col(c).cast(pl.Float64) for c in LAT_COLS],
            ]
        )

    # Keep only what we need; coerce types for safety
    return a


def collapse_to_chain_sums(df: pl.DataFrame, path=None) -> pl.DataFrame:
    """
    Turn many rows (one per chain container) into one row per chain run.

    Robust to out-of-order rows by using app_id as the completeness key.
    A run is complete when we have exactly one row for each expected app_id.

    Also robust to interleaving runs: we keep multiple "open" runs and greedily
    assign each incoming row to the earliest run that still needs that app_id.
    """
    memo_path = Path("scripts/benchmark/chain/memo") / path.name.replace(".csv", ".parquet") if path else None
    if path and memo_path.exists():
        # Memoization: if the memo file exists, load from it instead of recomputing.
        return pl.read_parquet(memo_path)

    # Choose a stable ordering column for streaming assignment
    if "msg_id" in df.columns:
        sort_col = "msg_id"
    elif "measurement_time" in df.columns:
        sort_col = "measurement_time"
    else:
        raise ValueError("Need 'msg_id' or 'measurement_time' to order rows.")

    df = df.sort(sort_col)

    if "app_id" not in df.columns:
        raise ValueError("Missing required column: app_id")

    # Expected app_ids for this CSV.
    # This learns the set from the file itself and filters to the dominant chain_depth
    # to avoid mixing if something weird is present.
    depth = int(df.select(pl.col("chain_depth").max()).item())

    expected = (
        df.filter(pl.col("chain_depth") == depth)
        .select(pl.col("app_id").unique())
        .to_series()
        .to_list()
    )
    expected_set = set(expected)

    if len(expected_set) < 2:
        raise ValueError(f"Not enough distinct app_id values to form runs: {expected_set}")

    # Greedy multi-run assignment based on app_id needs.
    # Each open run stores which app_ids are still missing.
    open_runs: list[set[str]] = []
    chain_run_ids: list[int] = []

    # Iterate rows in order and assign to a run that still needs that app_id.
    # This is O(n * concurrency) and usually fine because concurrency is small.
    app_ids = df.get_column("app_id").to_list()

    for a in app_ids:
        assigned_run_index = None
        for i, missing in enumerate(open_runs):
            if a in missing:
                assigned_run_index = i
                break

        if assigned_run_index is None:
            # Start a new run
            missing = set(expected_set)
            missing.discard(a)
            open_runs.append(missing)
            assigned_run_index = len(open_runs) - 1
        else:
            # Fill this run's missing slot
            open_runs[assigned_run_index].discard(a)

        chain_run_ids.append(assigned_run_index + 1)

        # If that run is now complete, close it by removing from open_runs.
        # IMPORTANT: removing shifts indices, so we must remap future ids.
        # To avoid remapping complexity, we do not pop here.
        # Instead, we mark completion later and filter complete runs.
        # (So open_runs is only used as a "needs tracker" during assignment.)

    df = df.with_columns(pl.Series("chain_run", chain_run_ids).cast(pl.Int64))

    # Validate completeness: keep only runs that contain all expected app_ids exactly once.
    # If duplicates exist for an app_id in a run, this will drop that run.
    run_ok = (
        df.group_by("chain_run")
        .agg(
            [
                pl.col("app_id").n_unique().alias("n_unique_app_ids"),
                pl.len().alias("n_rows"),
                pl.col("app_id").unique().alias("app_ids_in_run"),
            ]
        )
        .with_columns(
            (pl.col("n_unique_app_ids") == len(expected_set)).alias("complete")
        )
        .filter(pl.col("complete"))
        .select("chain_run")
    )

    df = df.join(run_ok, on="chain_run", how="inner")

    # Sum each latency column across the run
    agg_exprs = [pl.col(c).sum().alias(c) for c in LAT_COLS]

    out = (
        df.group_by("chain_run", maintain_order=True)
        .agg([pl.col("chain_depth").max().alias("chain_depth"), *agg_exprs])
        .sort("chain_run")
    )

    # Your requested normalization:
    # Note: if depth=10 and you truly have 11 containers (0..10),
    # the "average per container" divisor should be (chain_depth + 1), not chain_depth.
    out = out.with_columns([(pl.col(c) / (pl.col("chain_depth") + 1)).alias(c) for c in LAT_COLS])
    out.write_parquet(memo_path) if memo_path else None

    return out




def setup_matplotlib() -> None:
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "font.size": 22,
            "axes.titlesize": 24,
            "axes.labelsize": 22,
            "xtick.labelsize": 20,
            "ytick.labelsize": 20,
        }
    )


def boxplot_one_column(
    dfs: Sequence[pl.DataFrame],
    labels: Sequence[str],
    col: str,
    out_path: Path,
    ylabel: str,
    xlabel: str,
) -> None:
    series_list = [df.get_column(col).drop_nulls() for df in dfs]
    data = [s.to_numpy() for s in series_list]

    medians = [float(np.nanmedian(arr)) if arr.size else float("nan") for arr in data]

    plt.figure(figsize=(10.5, 3.5))
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

    y_min, y_max = ax.get_ylim()
    y_offset = (y_max - y_min) * 0.03 if y_max > y_min else 0.0
    for i, m in enumerate(medians, start=1):
        if np.isfinite(m):
            ax.text(i + 0.375, m - y_offset, f"{m:.1f}", ha="center", va="bottom", fontsize=12)

    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=200)
    plt.close()


def infer_label_from_depth(chain_sum_df: pl.DataFrame, fallback: str) -> str:
    # Prefer reading the chain depth from the data
    try:
        d = int(chain_sum_df.get_column("chain_depth").max())
        return str(d)
    except Exception:
        return fallback


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Save boxplots of per-chain summed latencies comparing multiple CSVs."
    )
    parser.add_argument(
        "csvs",
        nargs=6,
        type=Path,
        help="Six CSV files to compare (for example chain depths 1, 10, 100).",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("plots"),
        help="Output directory for saved PNGs (default: ./plots).",
    )
    args = parser.parse_args()

    setup_matplotlib()

    raw_dfs = [load_df(p) for p in args.csvs]
    chain_sum_dfs = [collapse_to_chain_sums(df, p) for df, p in zip(raw_dfs, args.csvs)]

    # Labels as chain depth (from data). Fallback to filename suffix if needed.
    fallback_labels = [str(p.stem).split("_")[-1] for p in args.csvs]
    labels = [
        infer_label_from_depth(df, fb) for df, fb in zip(chain_sum_dfs, fallback_labels)
    ]

    # Plot the per-chain sums for each metric
    for col in LAT_COLS:
        out = args.outdir / f"boxplot_chain_sum_{col}.png"
        boxplot_one_column(
            chain_sum_dfs,
            labels,
            col,
            out,
            ylabel="Avg. latency / step (ms)",
            xlabel="Chain depth",
        )


if __name__ == "__main__":
    main()
