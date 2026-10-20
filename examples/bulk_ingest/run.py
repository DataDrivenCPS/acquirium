"""Bulk-insert synthetic plant data through a CSV driver into a six-app soft-sensor chain.

uv run --locked python -m examples.bulk_ingest.run --output /tmp/acquirium-bulk

Pass --batch to run the same apps with batch_transforms.py, whose functions
get every affected window in one call.

Generates one wide CSV per day (4 turbidity columns plus filler columns, one
row every 10 s), starts a local server whose driver watches an empty
directory, deploys the chain, then moves the files in and times the insert.
Every output is checked against an independent recomputation from the
generated values.
"""

import argparse
import json
import math
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import acquirium as aq
from acquirium.apps import output_reference

from .chain import MAX_GAP, SOFT_SENSORS, SOURCE, SUSTAINED_MINUTES
from .transforms import MINUTE_NS, THRESHOLDS_NTU

DIRECTORY = Path(__file__).resolve().parent
START = datetime(2024, 12, 1, tzinfo=timezone.utc)
SECOND_NS = 1_000_000_000
LOCATIONS = sorted(THRESHOLDS_NTU)
# Normal level and excursion size per location, against its threshold.
LEVELS = {"urn:bulk/loc-0": (1.5, 6.0), "urn:bulk/loc-1": (1.2, 6.0),
          "urn:bulk/loc-2": (0.3, 0.6), "urn:bulk/loc-3": (0.1, 0.4)}
# Removed from every column: a 78-minute gap the fill covers, a 3-hour one it does not.
GAPS = [(timedelta(hours=6), timedelta(minutes=78)), (timedelta(days=1, hours=12), timedelta(hours=3))]


def generate(staging, days, interval_s, filler_columns, seed):
    """Write one CSV per day; return {location: {time_ns: value}} as the server will parse it."""
    rng = random.Random(seed)
    raw = {location: {} for location in LOCATIONS}
    header = ["time"] + [f"turbidity-{i}" for i in range(len(LOCATIONS))]
    header += [f"filler-{i}" for i in range(filler_columns)]
    rows = 0
    for day in range(days):
        lines = [",".join(header)]
        for step in range(0, 86_400, interval_s):
            offset = timedelta(days=day, seconds=step)
            if any(start <= offset < start + length for start, length in GAPS):
                continue
            hours = offset.total_seconds() / 3600
            # Excursions: 40 minutes every 6 hours, and 3 minutes every 2 hours.
            excursion = (hours % 6) < 40 / 60 or (hours % 2) < 3 / 60
            cells = [(START + offset).strftime("%Y-%m-%dT%H:%M:%SZ")]
            for location in LOCATIONS:
                level, spike = LEVELS[location]
                value = level * (1 + 0.1 * math.sin(hours) + 0.05 * rng.random())
                text = f"{value + (spike if excursion else 0):.4f}"
                raw[location][(day * 86_400 + step) * SECOND_NS + int(START.timestamp()) * SECOND_NS] = float(text)
                cells.append(text)
            cells += [f"{rng.random():.4f}" for _ in range(filler_columns)]
            lines.append(",".join(cells))
            rows += 1
        (staging / f"plant-{day:03d}.csv").write_text("\n".join(lines) + "\n")
    return raw, rows, rows * (len(header) - 1)


def write_config(root, incoming):
    path = root / "acquirium.toml"
    path.write_text(f"""
[server]
host = "127.0.0.1"
exact_only = true
data_dir = "{root / 'state'}"

[[drivers]]
spec      = "{DIRECTORY / 'driver.py'}:BulkIngestDriver"
driver_id = "bulk-ingest"
interval  = 0.5
source_id = "{SOURCE}"
watch_dir = "{incoming}"
glob      = "*.csv"
format    = "wide"
time_col  = "time"
timezone  = "UTC"
""")
    return path


def expected_outputs(raw):
    """Brute-force results of every app, keyed like the server's output streams."""

    def snap(readings):
        buckets = {}
        for t, v in readings.items():
            buckets.setdefault(t // MINUTE_NS * MINUTE_NS, []).append(v)
        return {t: sum(vs) / len(vs) for t, vs in buckets.items()}

    def fill(snapped):
        times, out = sorted(snapped), dict(snapped)
        for t0, t1 in zip(times, times[1:]):
            if t1 - t0 < MAX_GAP:
                for t in range(t0 + MINUTE_NS, t1, MINUTE_NS):
                    out[t] = snapped[t0] + (snapped[t1] - snapped[t0]) * (t - t0) / (t1 - t0)
        return out

    expected, filled, sustained = {}, {}, {}
    for location in LOCATIONS:
        snapped = snap(raw[location])
        filled[location] = fill(snapped)
        high = {t: float(v > THRESHOLDS_NTU[location]) for t, v in filled[location].items()}
        minutes = SUSTAINED_MINUTES[location]
        sustained[location] = {
            t: float(all(high.get(t - k * MINUTE_NS) == 1.0 for k in range(minutes + 1)))
            for t in high
        }
        expected[("bulk-turbidity-snap", location, "turbidity")] = snapped
        expected[("bulk-turbidity-fill", location, "turbidity")] = filled[location]
        expected[("bulk-turbidity-high", location, "alarm")] = high
        expected[("bulk-turbidity-sustained", location, "alarm")] = sustained[location]
    plant, alarms = "bulk-plant-turbidity", "bulk-plant-turbidity-alarms"
    at = {}
    for values in filled.values():
        for t, v in values.items():
            at.setdefault(t, []).append(v)
    expected[(plant, plant, "mean")] = {t: sum(vs) / len(vs) for t, vs in at.items()}
    expected[(plant, plant, "max")] = {t: max(vs) for t, vs in at.items()}
    expected[(plant, plant, "reporting")] = {t: float(len(vs)) for t, vs in at.items()}
    counts = {}
    for values in sustained.values():
        for t, v in values.items():
            counts[t] = counts.get(t, 0.0) + v
    expected[(alarms, alarms, "locations")] = counts
    return expected


def verify(client, expected):
    mismatches = {}
    for (app, group, port), values in expected.items():
        frame = client.client.timeseries_df(output_reference(app, group, port))
        actual = dict(zip(frame["ts"].cast(int).to_list(), frame["value"].to_list()))
        wrong = len(values.keys() ^ actual.keys()) + sum(
            abs(values[t] - actual[t]) > 1e-9 for t in values.keys() & actual.keys()
        )
        if wrong:
            mismatches[f"{app} {group} {port}"] = wrong
    return mismatches


def wait_for(condition, timeout, message):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError(message)
        time.sleep(0.5)


def soft_sensors(batch):
    if not batch:
        return SOFT_SENSORS
    # The same apps, with each Python function swapped for its batch version.
    return [
        aq.App(
            app.name,
            app.inputs,
            app.outputs,
            aq.PythonFunction.from_file(DIRECTORY / "batch_transforms.py", app.function.entrypoint, batch=True),
            app.window,
        )
        if isinstance(app.function, aq.PythonFunction)
        else app
        for app in SOFT_SENSORS
    ]


def run(args):
    root = args.output.resolve()
    root.mkdir(parents=True)
    staging, incoming = root / "staging", root / "incoming"
    staging.mkdir()
    incoming.mkdir()
    raw, rows, readings = generate(staging, args.days, args.interval, args.filler_columns, args.seed)
    print(f"generated {args.days} day(s): {rows} rows, {readings} readings")
    client = aq.init(write_config(root, incoming), timeout=180)
    try:
        query = SOFT_SENSORS[0].inputs[0].query
        wait_for(lambda: len(client.client.sparql_query(query)["rows"]) == len(LOCATIONS),
                 60, "driver did not register the turbidity points")
        started = time.monotonic()
        for app in soft_sensors(args.batch):
            state = client.deploy_app(app)["app"]
            if state["status"] != "active" or state["error"] or state["warnings"]:
                raise RuntimeError(f"{app.name}: {state}")
        deploy_s = time.monotonic() - started

        before = client.engine_status()["stats"]
        started = time.monotonic()
        for path in sorted(staging.iterdir()):
            path.rename(incoming / path.name)

        def done():
            status = client.engine_status()
            ingested = status["stats"]["source_mutations"] - before["source_mutations"]
            return ingested >= readings and status["engine"]["pending_requests"] == 0

        wait_for(done, args.timeout, "bulk insert did not finish")
        insert_s = time.monotonic() - started
        status = client.engine_status()
        after = status["stats"]
        epochs = after["completed_epochs"] - before["completed_epochs"]
        requests = after["requests"] - before["requests"]
        mismatches = verify(client, expected_outputs(raw))
        summary = {
            "batch": args.batch,
            "days": args.days,
            "rows": rows,
            "readings": readings,
            "deploy_s": deploy_s,
            "insert_s": insert_s,
            "readings_per_s": readings / insert_s,
            "epochs": epochs,
            "requests": requests,
            "requests_per_epoch": requests / epochs,
            "compute_ms": after["compute_ms"] - before["compute_ms"],
            "publish_ms": after["publish_ms"] - before["publish_ms"],
            "apps": status["apps"],
            "mismatches": mismatches,
        }
    finally:
        aq.shutdown()
    (root / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"insert: {insert_s:.1f} s, {summary['readings_per_s']:,.0f} readings/s, "
          f"{epochs} epochs, {summary['requests_per_epoch']:.2f} requests/epoch")
    print(f"deploy: {deploy_s:.1f} s")
    print(f"{'app':32} {'groups':>9} {'input rows':>12} {'changed':>9} {'compute s':>10}")
    for name, totals in sorted(status["apps"].items()):
        print(f"{name:32} {totals['groups']:9} {totals['input_rows']:12} "
              f"{totals['changed_rows']:9} {totals['compute_ms'] / 1000:10.2f}")
    print("outputs match recomputation" if not mismatches else f"MISMATCHES: {mismatches}")
    return 0 if not mismatches else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", type=Path, required=True, help="new directory for data, state and summary.json")
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--interval", type=int, default=10, help="seconds between rows")
    parser.add_argument("--filler-columns", type=int, default=60)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=3600)
    parser.add_argument("--batch", action="store_true", help="use the batch calculations")
    raise SystemExit(run(parser.parse_args()))


if __name__ == "__main__":
    main()
