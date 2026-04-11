#!/usr/bin/env python3
"""
Visualize configure-latency benchmark results.

Reads results_<N>.csv files produced by run_configure_latency.py and produces
two boxplots (one per event) of per-container build_query+execute latency vs
N — "initial" (cold) and "refresh" (warm, after a graph version bump).

Refresh rows are gated so that only events recorded after the last initial
event for a given run are included. Any refresh that arrived while a slow
container was still completing its initial build is discarded: we want a
steady-state measurement where all N containers were live and responding.

Usage:
    uv run scripts/benchmark/configure_latency/visualize.py
    uv run scripts/benchmark/configure_latency/visualize.py --results-dir <dir>
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl


RESULTS_RE = re.compile(r"results_(\d+)\.csv$")


def setup_matplotlib() -> None:
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "font.size": 18,
            "axes.titlesize": 20,
            "axes.labelsize": 18,
            "xtick.labelsize": 16,
            "ytick.labelsize": 16,
            "legend.fontsize": 14,
        }
    )


def _parse_endpoint_receipt(df: pl.DataFrame) -> pl.DataFrame:
    """Add a parsed ``endpoint_receipt_ts`` column (UTC datetime)."""
    if df.is_empty() or "endpoint_receipt" not in df.columns:
        return df
    return df.with_columns(
        pl.col("endpoint_receipt")
        .str.replace("Z", "+00:00")
        .str.to_datetime(strict=False, time_zone="UTC")
        .alias("endpoint_receipt_ts")
    )


def _filter_refreshes_after_all_initials(df: pl.DataFrame) -> pl.DataFrame:
    """Keep initial rows, and drop any refresh rows that arrived before the
    last initial event.

    Motivation: at high N the driver waits for all initials before bumping
    the graph version, but startup is still staggered, so a fast container
    can start emitting refresh events while a slow container is still
    finishing its initial build. Those early refreshes ran against a
    partial, unrepresentative system load and should be excluded from
    steady-state stats.
    """
    if df.is_empty() or "event" not in df.columns:
        return df
    parsed = _parse_endpoint_receipt(df)
    initials = parsed.filter(pl.col("event") == "initial")
    if initials.is_empty():
        # Nothing to anchor on; drop every refresh defensively.
        return parsed.filter(pl.col("event") != "refresh")
    last_initial_ts = initials.get_column("endpoint_receipt_ts").max()
    return parsed.filter(
        (pl.col("event") == "initial")
        | (
            (pl.col("event") == "refresh")
            & (pl.col("endpoint_receipt_ts") >= last_initial_ts)
        )
    )


def load_results_dir(results_dir: Path) -> dict[int, pl.DataFrame]:
    """Map N -> filtered DataFrame for every results_<N>.csv in the directory."""
    out: dict[int, pl.DataFrame] = {}
    for csv in sorted(results_dir.glob("results_*.csv")):
        m = RESULTS_RE.search(csv.name)
        if not m:
            continue
        n = int(m.group(1))
        df = pl.read_csv(csv, infer_schema_length=10_000, ignore_errors=True)
        out[n] = _filter_refreshes_after_all_initials(df)
    return out


def per_app_distribution(df: pl.DataFrame, event: str) -> np.ndarray:
    """Per-container total_ms values for one event."""
    if df.is_empty() or "event" not in df.columns:
        return np.array([], dtype=float)
    sub = (
        df.filter(pl.col("event") == event)
        .get_column("total_ms")
        .drop_nulls()
        .cast(pl.Float64, strict=False)
        .drop_nulls()
    )
    return sub.to_numpy()


def boxplot_per_app(
    n_to_df: dict[int, pl.DataFrame],
    *,
    event: str,
    out_path: Path,
    title: str,
) -> None:
    ns = sorted(n_to_df.keys())
    data = [per_app_distribution(n_to_df[n], event) for n in ns]
    medians = [float(np.median(arr)) if arr.size else float("nan") for arr in data]

    plt.figure(figsize=(11.0, 4.5))
    plt.boxplot(
        data,
        tick_labels=[str(n) for n in ns],
        whis=1.5,
        showfliers=False,
    )
    ax = plt.gca()
    y_min, y_max = ax.get_ylim()
    y_offset = (y_max - y_min) * 0.04
    for i, m in enumerate(medians, start=1):
        if np.isfinite(m):
            ax.text(i + 0.35, m - y_offset, f"{m:.1f}", ha="center", va="bottom", fontsize=12)
    ax.set_xlabel("N (concurrent apps)")
    ax.set_ylabel("build + SPARQL (ms)")
    ax.set_title(title)
    ax.grid(True, axis="y", linestyle=":", alpha=0.5)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=200)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot configure-latency results.")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("scripts/benchmark/configure_latency/results"),
    )
    parser.add_argument(
        "--plots-dir",
        type=Path,
        default=Path("scripts/benchmark/configure_latency/plots"),
    )
    args = parser.parse_args()

    setup_matplotlib()

    if not args.results_dir.is_dir():
        raise SystemExit(f"results dir not found: {args.results_dir}")

    n_to_df = load_results_dir(args.results_dir)
    if not n_to_df:
        raise SystemExit(f"no results_*.csv files in {args.results_dir}")

    boxplot_per_app(
        n_to_df,
        event="initial",
        out_path=args.plots_dir / "per_app_initial.png",
        title="Per-app initial configure latency",
    )
    boxplot_per_app(
        n_to_df,
        event="refresh",
        out_path=args.plots_dir / "per_app_refresh.png",
        title="Per-app refresh configure latency",
    )
    print(f"wrote plots to {args.plots_dir}")


if __name__ == "__main__":
    main()
