"""Sustained native soft-sensor load, independent oracle, and worker telemetry.

Run with the locked Acquirium environment; no HTTP server is involved.
"""

import argparse
import csv
import importlib.metadata
import json
import math
import os
import platform
import statistics
import sys
import time
from collections import deque
from pathlib import Path

from escapement import (
    CapacityError,
    Engine,
    Input,
    Mutation,
    Operator,
    Plan,
    Pointwise,
    PythonFunction,
    build_info,
)

ROLES = ("temperature", "conductivity", "flow", "pressure")
SCALES = (40.0, 1000.0, 100.0, 10.0)


def stream(unit, role):
    return f"urn:model-load:{unit}:{role}"


def scalar_prediction(values):
    """Independent scalar oracle: no worker imports, NumPy, or repeated arrays."""
    point = [value / scale for value, scale in zip(values, SCALES, strict=True)]
    terms = []
    weights = []
    for index in range(128):
        center = (
            index % 8 / 7,
            index // 8 % 4 / 3,
            index // 32 % 4 / 3,
            index * 37 % 128 / 127,
        )
        weight = index % 11 + 1
        terms.append(
            weight
            * math.exp(-sum((x - y) ** 2 for x, y in zip(point, center, strict=True)))
        )
        weights.append(weight)
    return 50.0 + 100.0 * math.fsum(terms) / sum(weights)


def mutations(tick, units):
    changes = [
        (
            stream(unit, role),
            tick,
            scale * (0.25 + (tick * 7 + unit * 11 + index) % 50 / 100),
        )
        for unit in range(units)
        for index, (role, scale) in enumerate(zip(ROLES, SCALES, strict=True))
    ]
    # Old event times exercise replacement and output retraction during streaming.
    if tick >= 13 and tick % 13 == 0:
        changes.append((stream(0, "pressure"), tick - 5, 8.0))
    if tick >= 17 and tick % 17 == 0:
        changes.append((stream(0, "conductivity"), tick - 3, None))
    return changes


def verify(engine, raw, units):
    expected = {
        stream(unit, role): {}
        for unit in range(units)
        for role in (*ROLES, "prediction")
    }
    for (reference, stamp), value in raw.items():
        expected[reference][stamp] = value
    for unit in range(units):
        stamps = set.intersection(
            *(set(expected[stream(unit, role)]) for role in ROLES)
        )
        expected[stream(unit, "prediction")] = {
            stamp: scalar_prediction(
                [expected[stream(unit, role)][stamp] for role in ROLES]
            )
            for stamp in stamps
        }
    snapshot = engine.snapshot()
    actual_rows = snapshot.readings()
    actual = {reference: {} for reference in expected}
    for row in actual_rows:
        assert row.sample_key == 0 and row.quality == 0
        actual[row.stream][row.event_time_ns] = row.value
    for reference, wanted in expected.items():
        got = actual[reference]
        if got.keys() != wanted.keys() or any(
            not math.isclose(got[stamp], value, rel_tol=1e-11, abs_tol=1e-11)
            for stamp, value in wanted.items()
        ):
            raise AssertionError(f"independent verification failed: {reference}")
    return {
        "epoch": snapshot.epoch,
        "applied": snapshot.applied,
        "rows": len(actual_rows),
    }


def write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def run_case(
    directory,
    *,
    pool_size=2,
    units=4,
    workers=None,
    copies=2048,
    duration=10.0,
    rates=(2.0, 40.0, 2.0),
    max_jobs=10_000,
    restart=False,
    max_pending=128,
    epoch_requests=8,
):
    """Each phase lasts duration seconds; source requests affect every operator.

    Rate generation skips missed slots instead of issuing catch-up bursts. A full
    inbox keeps one request for retry and counts capacity rejections separately.
    The JSONL journal is diagnostic; it is not a durable source recovery protocol.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    workers = units if workers is None else workers
    if (
        min(pool_size, units, workers, copies, max_jobs, max_pending, epoch_requests)
        < 1
    ):
        raise ValueError("worker, model, and inbox limits must be positive")
    if duration <= 0 or len(rates) != 3 or any(rate <= 0 for rate in rates):
        raise ValueError("specify a positive phase duration and three positive rates")
    model = directory / "model.py"
    model.write_text(
        Path(__file__)
        .with_name("model.py")
        .read_text()
        .replace("COPIES = 2048", f"COPIES = {copies}")
    )
    function = PythonFunction.from_file(model, "predict")
    plan = Plan(
        "model-load",
        [
            Operator(
                id=f"model-{unit}",
                inputs=[Input(stream(unit, role), role) for role in ROLES],
                outputs={"prediction": stream(unit, "prediction")},
                window=Pointwise(),
                function=function,
            )
            for unit in range(units)
        ],
    )
    options = {
        "workers": workers,
        "python_pool_size": pool_size,
        "python_max_worker_jobs": max_jobs,
        "epoch_requests": epoch_requests,
        "max_pending_requests": max_pending,
    }
    config = {
        "pool_size": pool_size,
        "units": units,
        "workers": workers,
        "copies": copies,
        "duration": duration,
        "rates": rates,
        "max_jobs": max_jobs,
        "restart": restart,
        "max_pending": max_pending,
        "epoch_requests": epoch_requests,
    }
    write(
        directory / "environment.json",
        {
            "config": config,
            "python": sys.version,
            "executable": sys.executable,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "load_average": os.getloadavg(),
            "native": build_info(),
            "native_source": json.loads(
                importlib.metadata.distribution("escapement-db").read_text(
                    "direct_url.json"
                )
                or "null"
            ),
            "model_sha256": function.sha256,
            "model_arrays_bytes_per_worker": 128 * copies * 5 * 8,
        },
    )
    engine = Engine(directory / "state.duckdb", **options)
    processor = None
    raw, samples, sessions, checks, epochs, phases = {}, [], [], [], [], []
    pending = deque()
    latencies, all_pids = [], set()
    admitted = capacity_retries = missed_slots = 0
    tick = -1
    started = time.monotonic()

    def observe(phase):
        status = processor.status()
        if status["error"]:
            raise RuntimeError(status["error"])
        workers = engine.python_worker_stats()
        if workers["resident"] > pool_size:
            raise AssertionError("resident worker limit exceeded")
        now = time.monotonic()
        while pending and pending[0][0] <= status["engine"]["applied"]:
            _, accepted_at = pending.popleft()
            latencies.append((now - accepted_at) * 1000)
        all_pids.update(worker["pid"] for worker in workers["telemetry"]["workers"])
        sample = {
            "seconds": now - started,
            "phase": phase,
            "admitted": admitted,
            "engine": status["engine"],
            "python_workers": workers,
        }
        samples.append(sample)
        epochs.extend(
            dict(session=len(sessions), **report) for report in processor.take_reports()
        )
        return status

    def accept(changes, request_id):
        nonlocal admitted, capacity_retries
        values = [
            Mutation.delete(reference, stamp)
            if value is None
            else Mutation.put(reference, stamp, value)
            for reference, stamp, value in changes
        ]
        try:
            receipt = engine.ingest("model-source", request_id, values)
        except CapacityError:
            capacity_retries += 1
            return False
        pending.append((receipt.sequence, time.monotonic()))
        for reference, stamp, value in changes:
            if value is None:
                raw.pop((reference, stamp), None)
            else:
                raw[reference, stamp] = value
        admitted += 1
        processor.wake()
        journal.write(
            json.dumps(
                {
                    "sequence": receipt.sequence,
                    "request_id": request_id,
                    "changes": changes,
                }
            )
            + "\n"
        )
        return True

    def drain(phase):
        deadline = time.monotonic() + 180
        while observe(phase)["engine"]["pending_requests"]:
            if time.monotonic() > deadline:
                raise TimeoutError("model workload did not drain within 180s")
            time.sleep(0.05)
        checks.append(verify(engine, raw, units))

    def stop_session():
        processor.stop(drain=True)
        sessions.append(
            {
                "python_workers": engine.python_worker_stats(),
                "processor": processor.status(),
            }
        )
        if sessions[-1]["processor"]["stats"]["reports_dropped"]:
            raise AssertionError("benchmark lost epoch reports")

    try:
        engine.activate("activate", plan)
        engine.step()
        processor = engine.start(interval=0.01, report_capacity=2048)
        with (directory / "requests.jsonl").open("w") as journal:
            accept(mutations(tick, units), "warmup")
            drain("warmup")
            for phase_index, (phase, rate) in enumerate(
                zip(("low", "burst", "recovery"), rates, strict=True)
            ):
                phase_start = time.monotonic()
                phase_end = phase_start + duration
                before = admitted
                blocked = 0
                next_offer = next_sample = phase_start
                retry = None
                while time.monotonic() < phase_end:
                    now = time.monotonic()
                    if now >= next_sample:
                        observe(phase)
                        next_sample = time.monotonic() + 0.05
                    if now >= next_offer:
                        if retry is None:
                            tick += 1
                            retry = (mutations(tick, units), f"tick-{tick}")
                        if accept(*retry):
                            retry = None
                            next_offer += 1 / rate
                            skipped = max(
                                0, math.floor((time.monotonic() - next_offer) * rate)
                            )
                            missed_slots += skipped
                            next_offer += skipped / rate
                        else:
                            blocked += 1
                            time.sleep(0.005)
                    else:
                        time.sleep(
                            min(
                                0.01,
                                max(
                                    0,
                                    min(next_offer, next_sample, phase_end)
                                    - time.monotonic(),
                                ),
                            )
                        )
                # A request blocked at the boundary is retried before continuing.
                while retry is not None:
                    if accept(*retry):
                        retry = None
                    else:
                        observe(phase)
                        time.sleep(0.01)
                phases.append(
                    {
                        "phase": phase,
                        "offered_rate": rate,
                        "admitted": admitted - before,
                        "seconds": time.monotonic() - phase_start,
                        "capacity_retries": blocked,
                        "pending_at_end": engine.status()["pending_requests"],
                    }
                )
                print(
                    json.dumps(
                        {
                            "phase": phase,
                            "admitted": admitted,
                            "pending": engine.status()["pending_requests"],
                        }
                    ),
                    flush=True,
                )
                if restart and phase_index == 1:
                    drain("before_restart")
                    stop_session()
                    processor = (
                        None  # Release the native Engine Arc before reopening DuckDB.
                    )
                    engine.close()
                    engine = Engine(directory / "state.duckdb", **options)
                    checks.append(verify(engine, raw, units))
                    processor = engine.start(interval=0.01, report_capacity=2048)
            drain("drain")
            # Ensure both replacement and retraction are exercised even in tiny smoke runs.
            for stamp, value, role in ((0, 9.0, "pressure"), (0, None, "conductivity")):
                while not accept([(stream(0, role), stamp, value)], f"final-{role}"):
                    observe("final")
                    time.sleep(0.01)
                drain("final")
            stop_session()
        result = {
            "config": config,
            "verified": True,
            "checks": checks,
            "phases": phases,
            "admitted": admitted,
            "missed_offer_slots": missed_slots,
            "capacity_retries": capacity_retries,
            "publication_observed_ms": {
                "count": len(latencies),
                "mean": statistics.mean(latencies),
                "p95": sorted(latencies)[math.ceil(len(latencies) * 0.95) - 1],
                "max": max(latencies),
            },
            "max_pending": max(s["engine"]["pending_requests"] for s in samples),
            "max_queue_age_ms": max(
                s["python_workers"]["oldest_queue_age_ms"] for s in samples
            ),
            "sampled_worker_pids": sorted(all_pids),
            "sessions": sessions,
        }
        write(directory / "result.json", result)
        write(directory / "epochs.json", epochs)
        with (directory / "samples.jsonl").open("w") as output:
            for sample in samples:
                output.write(json.dumps(sample) + "\n")
        with (directory / "samples.csv").open("w", newline="") as output:
            fields = (
                "seconds",
                "phase",
                "admitted",
                "applied",
                "pending",
                "resident",
                "active",
                "queued",
                "queue_age_ms",
                "rss_sampled_bytes",
                "warm_service_mean_ms",
                "cold_start_mean_ms",
            )
            writer = csv.DictWriter(output, fields, lineterminator="\n")
            writer.writeheader()
            for sample in samples:
                pool = sample["python_workers"]
                writer.writerow(
                    {
                        "seconds": sample["seconds"],
                        "phase": sample["phase"],
                        "admitted": sample["admitted"],
                        "applied": sample["engine"]["applied"],
                        "pending": sample["engine"]["pending_requests"],
                        "resident": pool["resident"],
                        "active": pool["active"],
                        "queued": pool["queued"],
                        "queue_age_ms": pool["oldest_queue_age_ms"],
                        "rss_sampled_bytes": pool["telemetry"]["rss_sampled_bytes"],
                        "warm_service_mean_ms": pool["telemetry"]["warm_service"][
                            "mean_ms"
                        ],
                        "cold_start_mean_ms": pool["telemetry"]["cold_start"][
                            "mean_ms"
                        ],
                    }
                )
        return result
    finally:
        if processor is not None:
            processor.stop(drain=False)
            processor = None
        engine.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New directory; existing runs are never overwritten",
    )
    parser.add_argument("--pool-size", type=int, default=2)
    parser.add_argument("--units", type=int, default=4)
    parser.add_argument(
        "--workers", type=int, help="Native scheduler limit; defaults to --units"
    )
    parser.add_argument("--copies", type=int, default=2048)
    parser.add_argument("--duration", type=float, default=10, help="Seconds per phase")
    parser.add_argument("--rates", type=float, nargs=3, default=(2, 40, 2))
    parser.add_argument("--max-jobs", type=int, default=10_000)
    parser.add_argument("--max-pending", type=int, default=128)
    parser.add_argument("--epoch-requests", type=int, default=8)
    parser.add_argument("--restart", action="store_true")
    args = vars(parser.parse_args())
    directory = args.pop("output")
    result = run_case(directory, **args)
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "verified",
                    "admitted",
                    "max_pending",
                    "max_queue_age_ms",
                    "publication_observed_ms",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
