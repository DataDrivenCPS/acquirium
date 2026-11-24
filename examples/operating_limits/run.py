"""Memory and restart cost against history size, through Acquirium's engine core.

Run with the locked Acquirium environment; no HTTP server is involved. Each
history size loads in one fresh process and reopens in another, so the reopen
measures only what the engine reads back from its store.
"""

import argparse
from hashlib import sha256
import importlib.metadata
import json
import os
import platform
import random
import shutil
import resource
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import pyarrow as pa
from escapement import Capacity

from acquirium.core import Core

SECOND = 1_000_000_000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
BUCKET = 15 * MINUTE
START = 1_767_225_600 * SECOND  # 2026-01-01T00:00:00Z
REQUEST_READINGS = 10_000
# Acquirium's server defaults (Server/manager.py).
WORKERS = 2
EPOCH_REQUESTS = 128
MAX_PENDING = 100_000
# What Native.mean compiles to: one call per batch of 15-minute buckets.
MEAN_MODULE = """from acquirium.functions import _builtin_batch_function
calculate = _builtin_batch_function({"kind": "mean", "port": "mean"}, ["mean"])
"""


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


def modules(path):
    """The directory beside the store that holds the mean's module."""
    directory = path.parent / "modules"
    directory.mkdir(exist_ok=True)
    (directory / "limits_mean.py").write_text(MEAN_MODULE)
    return directory


def engine(path):
    return Core(path, workers=WORKERS, epoch_requests=EPOCH_REQUESTS, max_pending=MAX_PENDING,
                module_paths=[*sys.path, str(modules(path))])


def plan(streams):
    """A 15-minute mean of each stream, as Acquirium compiles Native.mean."""
    digest = sha256(MEAN_MODULE.encode()).hexdigest()
    return {
        "templates": [{
            "id": "limits-mean",
            "definition": {
                "function": {"module": "limits_mean", "name": "calculate", "digest": digest,
                             "mode": "vectorized"},
                "window": {"anchors": {"grid": {"origin": 0, "step": BUCKET}}, "body": "bucket"},
                "roles": {"value": {"reach": {"span": {"before": 0, "after": 0}},
                                    "reads": "bound", "driving": True}},
                "ports": ["mean"],
                "freshness": "synchronous",
            },
            "annotations": None,
        }],
        "instances": [{"template": "limits-mean", "key": str(index), "outputs": {"mean": derived(index)},
                       "parameters": {}, "annotations": None} for index in range(streams)],
        "bindings": [{"template": "limits-mean", "instance": str(index), "stream": source(index),
                      "role": "value", "valid_from": None, "valid_to": None, "context": ""}
                     for index in range(streams)],
    }


def frame(streams, times, values):
    return pl.DataFrame({
        "ref_uri": streams,
        "ts": pl.Series(times, dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
        "value": values,
    })


def requests(readings, streams, interval_ns, seed):
    """Time-major requests of about REQUEST_READINGS: every stream's readings
    for consecutive instants, as a driver sends them. Values random-walk at a
    sensor's three-decimal resolution."""
    rng = random.Random(seed)
    values = [rng.uniform(10, 100) for _ in range(streams)]
    instants = readings // streams
    per_request = max(1, REQUEST_READINGS // streams)
    for first in range(0, instants, per_request):
        names, times, batch = [], [], []
        for instant in range(first, min(first + per_request, instants)):
            for index in range(streams):
                values[index] += rng.uniform(-0.05, 0.05)
                names.append(source(index))
                times.append(START + instant * interval_ns)
                batch.append(round(values[index], 3))
        yield frame(names, times, batch)


def submit(target, request_id, rows):
    """Ingest, waiting out a full inbox; returns the receipt and seconds blocked."""
    blocked = 0.0
    while True:
        try:
            return target.ingest("limits", request_id, rows), blocked
        except Capacity:
            started = time.perf_counter()
            time.sleep(0.05)
            blocked += time.perf_counter() - started


def counts(snapshot, streams):
    return {row["stream"]: row["count"] for row in snapshot.extent(streams)}


def read(snapshot, stream, **bounds):
    with pa.RecordBatchReader.from_stream(snapshot.read([stream], **bounds)) as reader:
        return reader.read_all()


def value_at(snapshot, stream, time_ns):
    rows = read(snapshot, stream, start=time_ns, end=time_ns + 1)["number"].to_pylist()
    return rows[0] if rows else None


def bucket_mean_matches(snapshot, bucket):
    """Whether stream 0's stored mean for ``bucket`` is the mean of its readings."""
    values = read(snapshot, source(0), start=bucket, end=bucket + BUCKET)["number"].to_pylist()
    stored = value_at(snapshot, derived(0), bucket)
    return stored is not None and abs(stored - sum(values) / len(values)) < 1e-9


def engine_version():
    """The installed escapement-db, and the revision it was built from."""
    distribution = importlib.metadata.distribution("escapement-db")
    origin = json.loads(distribution.read_text("direct_url.json") or "{}")
    return {"version": distribution.version, "url": origin.get("url"),
            "revision": origin.get("vcs_info", {}).get("commit_id")}


def load(args):
    path = Path(args.database)
    baseline = rss_mib()
    started = time.perf_counter()
    blocked = 0.0
    target = engine(path)
    try:
        if args.derived:
            target.activate("plan", plan(args.streams))
        receipt = None
        for number, batch in enumerate(requests(args.readings, args.streams,
                                                args.interval * SECOND, args.seed)):
            receipt, waited = submit(target, str(number), batch)
            blocked += waited
        snapshot = target.wait(receipt, timeout=3600)
        elapsed = time.perf_counter() - started
        derived_rows = (sum(counts(snapshot, [derived(index) for index in range(args.streams)]).values())
                        if args.derived else 0)
        result = {
            "phase": "load",
            "readings": args.readings,
            "derived_rows": derived_rows,
            "seconds": elapsed,
            "readings_per_second": args.readings / elapsed,
            "blocked_seconds": blocked,
            "epochs": snapshot.epoch,
            "rss_mib": rss_mib() - baseline,
            "peak_mib": peak_mib() - baseline,
        }
    finally:
        target.close()
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
    try:
        rss = rss_mib() - baseline
        snapshot = target.engine.snapshot()
        first = source(0)
        (extent,) = snapshot.extent([first])
        hour, hour_seconds = timed(lambda: read(snapshot, first, start=extent["last"] - HOUR,
                                                end=extent["last"] + 1))
        whole, whole_seconds = timed(lambda: read(snapshot, first))
        middle = extent["first"] + (extent["last"] - extent["first"]) // 2
        middle -= middle % (args.interval * SECOND)
        corrected, correction_seconds = timed(lambda: target.wait(
            target.ingest("limits", "correction", frame([first], [middle], [-1.0])), timeout=600))
        verified = value_at(corrected, first, middle) == -1.0
        if args.derived:
            verified = verified and bucket_mean_matches(corrected, middle - middle % BUCKET)
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
    finally:
        target.close()


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
        + (", a 15-minute mean of each" if args.derived else "") + ".",
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
                        help="Also maintain a 15-minute mean of every stream")
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
            shutil.rmtree(path) if path.is_dir() else path.unlink()
        shutil.rmtree(args.output / "modules", ignore_errors=True)
    environment = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "acquirium": importlib.metadata.version("acquirium"),
        "escapement": engine_version(),
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
