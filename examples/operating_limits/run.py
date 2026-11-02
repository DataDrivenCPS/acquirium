"""Memory and restart cost against history size, through the native engine.

Run with the locked Acquirium environment; no HTTP server is involved. Each
history size loads in one fresh process and reopens in another, so the reopen
measures only what the engine rebuilds from its log.
"""

import argparse
import importlib.metadata
import json
import os
import platform
import random
import resource
import subprocess
import sys
import time
from pathlib import Path

import pyarrow  # noqa: F401  (imported before the timed reads)
from escapement import (
    CapacityError,
    Engine,
    FixedBucket,
    Input,
    Mutation,
    Native,
    Operator,
    Plan,
    build_info,
)

SECOND = 1_000_000_000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
START = 1_767_225_600 * SECOND  # 2026-01-01T00:00:00Z
REQUEST_MUTATIONS = 10_000
# Acquirium's server defaults (Server/manager.py).
WORKERS = 2
EPOCH_REQUESTS = 128
MAX_PENDING = 100_000


def source(index):
    return f"urn:limits:sensor:{index}"


def derived(index):
    return f"urn:limits:sensor:{index}:mean-15min"


def rss_mib():
    with open("/proc/self/status") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    raise RuntimeError("VmRSS not found")


def peak_mib():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def database_mib(path):
    return sum(item.stat().st_size for item in path.parent.glob(path.name + "*")) / 2**20


def engine(path):
    return Engine(path, workers=WORKERS, epoch_requests=EPOCH_REQUESTS,
                  max_pending_requests=MAX_PENDING)


def plan(streams):
    return Plan("limits-means", [
        Operator(f"mean-{index}", [Input(source(index), "value")],
                 {"mean": derived(index)}, FixedBucket(15 * MINUTE), Native.mean("mean"))
        for index in range(streams)
    ])


def requests(readings, streams, interval_ns, seed):
    """Time-major requests of about REQUEST_MUTATIONS: every stream's readings
    for consecutive instants, as a driver sends them. Values random-walk at a
    sensor's three-decimal resolution."""
    rng = random.Random(seed)
    values = [rng.uniform(10, 100) for _ in range(streams)]
    instants = readings // streams
    per_request = max(1, REQUEST_MUTATIONS // streams)
    for first in range(0, instants, per_request):
        batch = []
        for instant in range(first, min(first + per_request, instants)):
            time_ns = START + instant * interval_ns
            for index in range(streams):
                values[index] += rng.uniform(-0.05, 0.05)
                batch.append(Mutation.put(source(index), time_ns, round(values[index], 3)))
        yield batch


def submit(target, request_id, batch):
    """Ingest, waiting out a full inbox; returns the receipt and seconds blocked."""
    blocked = 0.0
    while True:
        try:
            return target.ingest("limits", request_id, batch), blocked
        except CapacityError:
            started = time.perf_counter()
            time.sleep(0.05)
            blocked += time.perf_counter() - started


def load(args):
    path = Path(args.database)
    baseline = rss_mib()
    started = time.perf_counter()
    blocked = 0.0
    with engine(path) as target:
        if args.derived:
            receipt, _ = target.activate("plan", plan(args.streams)), 0
        processor = target.start()
        receipt = None
        for number, batch in enumerate(requests(args.readings, args.streams,
                                                args.interval * SECOND, args.seed)):
            receipt, waited = submit(target, str(number), batch)
            blocked += waited
        processor.read_after(receipt, timeout=3600)
        elapsed = time.perf_counter() - started
        status = target.status()
        processor.stop()
        result = {
            "phase": "load",
            "readings": args.readings,
            "derived_rows": sum((target.snapshot().stats(derived(index)) or {"count": 0})["count"]
                                for index in range(args.streams)) if args.derived else 0,
            "seconds": elapsed,
            "readings_per_second": args.readings / elapsed,
            "blocked_seconds": blocked,
            "epochs": status.get("epoch"),
            "rss_mib": rss_mib() - baseline,
            "peak_mib": peak_mib() - baseline,
        }
    result["database_mib"] = database_mib(path)
    return result


def timed(call):
    started = time.perf_counter()
    value = call()
    return value, time.perf_counter() - started


def reopen(args):
    path = Path(args.database)
    baseline = rss_mib()
    target, open_seconds = timed(lambda: engine(path))
    with target:
        rss = rss_mib() - baseline
        snapshot = target.snapshot()
        first = source(0)
        stats = snapshot.stats(first)
        hour, hour_seconds = timed(lambda: snapshot.query_arrow(
            first, start=stats["last_ns"] - HOUR, end=stats["last_ns"] + 1, limit=10**9))
        whole, whole_seconds = timed(lambda: snapshot.query_arrow(first, limit=10**9))
        processor = target.start()
        middle = stats["first_ns"] + (stats["last_ns"] - stats["first_ns"]) // 2
        middle -= middle % (args.interval * SECOND)
        corrected, correction_seconds = timed(lambda: processor.read_after(
            target.ingest("limits", "correction", [Mutation.put(first, middle, -1.0)]),
            timeout=600))
        processor.stop()
        verified = corrected.value(first, middle) == -1.0
        if args.derived:
            bucket = middle - (middle % (15 * MINUTE))
            rows = corrected.query(first, start=bucket, end=bucket + 15 * MINUTE, limit=10**6)
            mean = sum(row.value for row in rows) / len(rows)
            verified = verified and abs(corrected.value(derived(0), bucket) - mean) < 1e-9
        return {
            "verified": verified,
            "phase": "reopen",
            "open_seconds": open_seconds,
            "rss_mib": rss,
            "peak_mib": peak_mib() - baseline,
            "hour_rows": hour.num_rows,
            "hour_read_seconds": hour_seconds,
            "stream_rows": whole.num_rows,
            "stream_read_seconds": whole_seconds,
            "correction_seconds": correction_seconds,
        }


def child(phase, args, size, database):
    command = [sys.executable, "-m", "examples.operating_limits.run", "--phase", phase,
               "--database", str(database), "--readings", str(size),
               "--streams", str(args.streams), "--interval", str(args.interval),
               "--seed", str(args.seed)]
    if args.derived:
        command.append("--derived")
    output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    return json.loads(output.strip().splitlines()[-1])


def summary(results, args):
    lines = [
        f"{args.streams} streams, one reading per stream every {args.interval} s"
        + (", a native 15-minute mean of each" if args.derived else "") + ".",
        "",
        "| Readings | Derived rows | Load s | Readings/s | Load RSS MiB | Load peak MiB "
        "| Bytes/row loaded | DB MiB | Reopen s | Reopen RSS MiB | Bytes/row reopened "
        "| 1 h read ms | Stream read ms | Correction ms |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: "
        "| ---: | ---: | ---: |",
    ]
    for item in results:
        loaded, opened = item["load"], item["reopen"]
        rows = loaded["readings"] + loaded["derived_rows"]
        lines.append(
            f"| {loaded['readings']:,} | {loaded['derived_rows']:,} | {loaded['seconds']:.1f} "
            f"| {loaded['readings_per_second']:,.0f} | {loaded['rss_mib']:,.0f} "
            f"| {loaded['peak_mib']:,.0f} | {loaded['rss_mib'] * 2**20 / rows:.0f} "
            f"| {loaded['database_mib']:,.1f} | {opened['open_seconds']:.2f} "
            f"| {opened['rss_mib']:,.0f} | {opened['rss_mib'] * 2**20 / rows:.0f} "
            f"| {opened['hour_read_seconds'] * 1000:.1f} "
            f"| {opened['stream_read_seconds'] * 1000:.1f} "
            f"| {opened['correction_seconds'] * 1000:.1f} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="A new directory for results")
    parser.add_argument("--sizes", type=float, nargs="+", default=(1e6, 4e6, 16e6),
                        help="Source readings per case")
    parser.add_argument("--streams", type=int, default=100)
    parser.add_argument("--interval", type=int, default=1, help="Seconds between readings")
    parser.add_argument("--derived", action="store_true",
                        help="Also maintain a native 15-minute mean of every stream")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--phase", choices=("load", "reopen"), help=argparse.SUPPRESS)
    parser.add_argument("--database", help=argparse.SUPPRESS)
    parser.add_argument("--readings", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.phase:
        print(json.dumps((load if args.phase == "load" else reopen)(args)))
        return
    if args.output is None:
        parser.error("--output is required")
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    for size in (int(size) for size in args.sizes):
        size -= size % args.streams
        database = args.output / f"history-{size}.duckdb"
        item = {"load": child("load", args, size, database)}
        item["reopen"] = child("reopen", args, size, database)
        if not item["reopen"]["verified"]:
            raise SystemExit(f"{size:,} readings: the correction was not applied after reopening")
        results.append(item)
        print(json.dumps(item), flush=True)
        for path in database.parent.glob(database.name + "*"):
            path.unlink()
    environment = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "acquirium": importlib.metadata.version("acquirium"),
        "escapement": build_info(),
    }
    (args.output / "results.json").write_text(json.dumps(
        {"arguments": {key: value for key, value in vars(args).items()
                       if key not in ("output", "phase", "database", "readings")},
         "environment": environment, "results": results}, indent=2, default=str))
    text = summary(results, args)
    (args.output / "summary.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
