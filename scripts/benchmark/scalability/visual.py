from __future__ import annotations
import argparse
from pathlib import Path
from typing import List, Tuple, Optional
import os
#!/usr/bin/env python3
"""
Create a box-and-whisker plot from 4 single-column float txt files (no header).

Usage:
    python visual.py
"""



import matplotlib.pyplot as plt


def load_values(path: Path) -> List[float]:
        values: List[float] = []
        with path.open("r", encoding="utf-8") as f:
                for i, line in enumerate(f, start=1):
                        s = line.strip()
                        if not s:
                                continue
                        try:
                                values.append(float(s)/1000.0)  # convert ms to s
                        except ValueError as e:
                                raise ValueError(f"Invalid float in {path} at line {i}: {s!r}") from e
        if not values:
                raise ValueError(f"No numeric values found in file: {path}")
        return values

def main() -> None:
        files= os.listdir("scripts/benchmark/scalability/")
        files = [f for f in files if f.startswith("results_") and f.endswith(".txt")]
        paths = [Path("scripts/benchmark/scalability/") /x for x in sorted(files)]
        for p in paths:
            if not p.exists():
                raise FileNotFoundError(f"File not found: {p}")

        data = {int(str(p).split('_')[-2]): load_values(p) for p in paths} 
        data = dict(sorted(data.items()))  # sort by key (number of apps)
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

        # Median values (in seconds) to annotate after plotting
        keys = list(data.keys())
        medians = [float(__import__("numpy").median(v)) for v in data.values()]
 
        plt.figure(figsize=(8, 5))
        plt.boxplot(
                data.values(),
                tick_labels=data.keys(),
                showmeans=False,
                meanline=False,
                whis=1.5,   # standard Tukey whiskers
                notch=False,
                showfliers=False
        )
        # Annotate medians above each box
        ax = plt.gca()
        y_min, y_max = ax.get_ylim()
        y_offset = (y_max - y_min) * -0.04  # small vertical offset
        x_offset = 0.35  # horizontal offset (not used here)
        for i, m in enumerate(medians, start=1):
            ax.text(
                i + x_offset,
                m + y_offset,
                f"{m:.1f}",
                ha="center",
                va="bottom",
                fontsize=12,
            )
        # plt.title("Box Plot")
        plt.ylabel("Latency (seconds)")
        plt.xlabel("Number of Concurrent Apps")
        # plt.grid(axis="y", linestyle="--", alpha=0.4)
        plt.tight_layout()
        
        plt.gcf().set_size_inches(8, 3.2)  # reduce figure height
        out = Path("scripts/benchmark/scalability/boxplot.png")
        plt.savefig(out, dpi=200)
        plt.close()


if __name__ == "__main__":
        main()