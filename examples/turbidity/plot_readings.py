"""Render the tutorial's input data: python -m examples.turbidity.plot_readings."""

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from examples.turbidity.run import LOCATIONS, MINUTE, START, readings


def plot(path):
    # Use every expected sample time so missing readings leave gaps in the lines.
    times = [START + i * MINUTE / 2 for i in range(50)]
    colors = {"filter-1": "#17816b", "filter-2": "#d47b16",
              "filter-3": "#2767ad", "combined": "#8956a4"}
    labels = {"filter-1": "Filter 1", "filter-2": "Filter 2",
              "filter-3": "Filter 3", "combined": "Combined effluent"}
    with plt.rc_context({"svg.fonttype": "none", "font.size": 10}):
        fig, (full, detail) = plt.subplots(
            2, 1, figsize=(9, 6), sharex=True,
            gridspec_kw={"height_ratios": [1, 2]}, layout="constrained",
        )
        for location, normal in LOCATIONS.items():
            samples = dict(readings(location, normal))
            values = [samples.get(t) for t in times]
            # A fault code has no numeric value to plot.
            values = [float("nan") if v is None or isinstance(v, str) else v for v in values]
            detail.plot(times, values, ".-", color=colors[location],
                        label=labels[location], linewidth=1, markersize=3)
            if location == "filter-3":
                full.plot(times, values, ".-", color=colors[location], linewidth=1, markersize=3)

        full.set(title="Filter 3: full range", ylim=(0, 10.7), yticks=[0, 5, 10])
        full.annotate("9.9 NTU spike", xy=(START + 22.5 * MINUTE, 9.9),
                      xytext=(-105, -5), textcoords="offset points",
                      arrowprops={"arrowstyle": "->", "color": "#555555"})
        detail.set(title="All sensors: detail from 0 to 1 NTU", ylim=(0, 1.05))
        detail.axhline(0.5, color="#666666", linestyle="--", linewidth=1)
        detail.text(times[-1], 0.52, "Filter 3 limit: 0.5 NTU", ha="right", color="#555555")
        for minute, label in [(5.5, "Fault"), (10.25, "Missing")]:
            detail.text(START + minute * MINUTE, 0.98, label, ha="center", color="#555555")
        detail.legend(loc="lower center", bbox_to_anchor=(0.5, -0.42), ncol=4, frameon=False)
        detail.set_xlabel("Time (UTC), 1 January 2026")
        detail.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
        detail.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        for ax in (full, detail):
            ax.set_ylabel("Turbidity (NTU)")
            ax.grid(axis="y", alpha=0.2)
            ax.spines[["top", "right"]].set_visible(False)
            ax.set_xlim(times[0], times[-1])
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path)
        plt.close(fig)
        if path.suffix == ".svg":
            path.write_text("\n".join(line.rstrip() for line in path.read_text().splitlines()) + "\n")


if __name__ == "__main__":
    plot(Path(__file__).resolve().parents[2] / "docs/images/turbidity-readings.svg")
