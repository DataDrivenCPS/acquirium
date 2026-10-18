"""Render standalone figures from an execution profile's summary.json."""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def plot(summary, directory):
    cases = summary["cases"]
    figure, axes = plt.subplots(1, 2, figsize=(12, 4.5), layout="constrained")
    scenarios = sorted({case["scenario"] for case in cases})
    for scenario in scenarios:
        controls = sorted(
            (
                case
                for case in cases
                if case["scenario"] == scenario and not case["instrumented"]
            ),
            key=lambda case: case["workers"],
        )
        axes[0].plot(
            [case["workers"] for case in controls],
            [case["compute_ms"] for case in controls],
            marker="o",
            label=scenario,
        )
    axes[0].set(
        xlabel="Configured operator workers",
        ylabel="Median epoch computation (ms)",
        title="Uninstrumented native epochs",
    )
    axes[0].set_xticks(sorted({case["workers"] for case in cases}))
    axes[0].grid(alpha=0.2)
    axes[0].legend(fontsize=8)
    representative = next(
        (
            case
            for case in cases
            if case["scenario"] == "tiny"
            and case["workers"] == 2
            and case["instrumented"]
        ),
        next(case for case in cases if case["instrumented"]),
    )
    worker = representative["worker_mean"]
    stages = {
        "Polars import": worker["polars_import_ms"],
        "PyArrow import": worker["pyarrow_import_ms"],
        "Input Arrow/JSON": worker["input_ms"],
        "Calculation": worker["function_ms"],
        "Output validation/Arrow": worker["output_ms"],
        "Other worker setup": worker["stdlib_import_ms"]
        + worker["function_setup_ms"]
        + worker["finalize_ms"],
        "Profiler setup": worker["wrapper_prepare_ms"],
        "External/unattributed": worker["external_and_unattributed_ms"],
    }
    names = list(stages)
    values = list(stages.values())
    axes[1].barh(
        names[::-1],
        values[::-1],
        color=[
            "#4477aa" if name != "Calculation" else "#228833" for name in names[::-1]
        ],
    )
    axes[1].set(
        xlabel="Mean milliseconds per operator invocation",
        title=f"Actual worker stages: {representative['scenario']}, {representative['workers']} workers",
    )
    for index, value in enumerate(values[::-1]):
        axes[1].text(value + 0.5, index, f"{value:.2f}", va="center", fontsize=8)
    axes[1].set_xlim(0, max(values) * 1.2)
    figure.suptitle("Acquirium / Escapement execution profile")
    directory = Path(directory)
    svg = directory / "execution-profile.svg"
    figure.savefig(svg)
    svg.write_text(
        "\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n"
    )
    figure.savefig(directory / "execution-profile.png", dpi=160)
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path)
    args = parser.parse_args()
    plot(json.loads(args.summary.read_text()), args.summary.parent)


if __name__ == "__main__":
    main()
