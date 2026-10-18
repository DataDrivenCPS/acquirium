"""Export a comparison of native model workload runs as SVG and PNG."""

import argparse
import gzip
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def plot(directories, output):
    figure, axes = plt.subplots(
        5, 1, figsize=(10, 12), sharex=True, constrained_layout=True
    )
    for directory in directories:
        result = json.loads((directory / "result.json").read_text())
        path = directory / "samples.jsonl"
        if path.exists():
            lines = path.read_text().splitlines()
        else:
            with gzip.open(path.with_suffix(".jsonl.gz"), "rt") as handle:
                lines = handle.read().splitlines()
        samples = [json.loads(line) for line in lines]
        origin = next(
            sample["seconds"] for sample in samples if sample["phase"] == "low"
        )
        times = [sample["seconds"] - origin for sample in samples]
        count = result["config"]["pool_size"]
        label = f"{count} worker{'s' if count != 1 else ''}"
        (line,) = axes[0].plot(
            times,
            [sample["engine"]["applied"] - 1 for sample in samples],
            label=f"{label} published",
        )
        axes[0].plot(
            times,
            [sample["admitted"] for sample in samples],
            linestyle="--",
            color=line.get_color(),
            label=f"{label} admitted",
        )
        for axis, values in zip(
            axes[1:],
            (
                [sample["engine"]["pending_requests"] for sample in samples],
                [sample["python_workers"]["oldest_queue_age_ms"] for sample in samples],
                [
                    sample["python_workers"]["telemetry"]["rss_sampled_bytes"] / 2**20
                    for sample in samples
                ],
                [
                    sample["python_workers"]["telemetry"]["warm_service"]["mean_ms"]
                    for sample in samples
                ],
            ),
            strict=True,
        ):
            axis.plot(times, values, label=label)
    for axis, label in zip(
        axes,
        (
            "Cumulative source requests",
            "Pending requests",
            "Oldest pool wait (ms)",
            "Worker RSS sampled (MiB)",
            "Warm service cumulative mean\n(ms/job)",
        ),
        strict=True,
    ):
        axis.set_ylabel(label)
        axis.grid(alpha=0.2)
    for axis in axes:
        axis.axvspan(
            result["config"]["duration"],
            2 * result["config"]["duration"],
            color="black",
            alpha=0.04,
        )
    axes[0].legend(ncol=3, fontsize=8)
    axes[0].set_title(
        "Synthetic model workload · fixed worker pools · low / burst / recovery"
    )
    axes[-1].set_xlabel("Seconds since low phase started (shaded: burst)")
    output.parent.mkdir(parents=True, exist_ok=True)
    for suffix in (".svg", ".png"):
        figure.savefig(output.with_suffix(suffix), dpi=150)
    svg = output.with_suffix(".svg")
    svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")
    plt.close(figure)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    plot(arguments.directories, arguments.output)
