"""Profile native epochs and the actual Python worker with paired controls.

uv run --locked python -m examples.profiling.run --output /tmp/acquirium-profile
"""

import argparse
import csv
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

from escapement import (
    Engine,
    FixedBucket,
    Input,
    Mutation,
    Operator,
    Plan,
    Pointwise,
    PythonFunction,
    build_info,
)

DIRECTORY = Path(__file__).resolve().parent
ROOT = DIRECTORY.parents[1]
FUNCTIONS = DIRECTORY.parent / "soft_sensors" / "transforms.py"
WIDTH = 10_000_000_000
SCENARIOS = {
    "tiny": {"tanks": 2, "history": 1, "samples": 1, "requests": 1},
    "batch8": {"tanks": 2, "history": 8, "samples": 1, "requests": 8},
    "wide8": {"tanks": 8, "history": 1, "samples": 1, "requests": 1},
    "dense500": {"tanks": 2, "history": 1, "samples": 500, "requests": 1},
    "history128": {"tanks": 2, "history": 128, "samples": 1, "requests": 1},
}
STAGES = (
    "stdlib_import",
    "polars_import",
    "pyarrow_import",
    "function_setup",
    "input",
    "function",
    "output",
    "finalize",
    "wrapper_prepare",
)


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True))


def stream(tank, probe):
    return f"urn:profile:tank:{tank}:probe:{probe}"


def mean_stream(tank):
    return f"urn:profile:tank:{tank}:mean"


def plan(tanks):
    factors = {
        "from_multiplier": "1",
        "from_offset": "273.15",
        "to_multiplier": "1",
        "to_offset": "273.15",
    }
    operators = [
        Operator(
            id=f"tank-{tank}",
            inputs=[
                Input(stream(tank, probe), "temperature", context=factors)
                for probe in range(2)
            ],
            outputs={"mean": mean_stream(tank)},
            window=FixedBucket(WIDTH),
            function=PythonFunction.from_file(FUNCTIONS, "tank_mean"),
        )
        for tank in range(tanks)
    ]
    operators.extend(
        [
            Operator(
                id="hottest",
                inputs=[Input(mean_stream(tank), "mean") for tank in range(tanks)],
                outputs={"hottest": "urn:profile:hottest"},
                window=Pointwise(),
                function=PythonFunction.from_file(FUNCTIONS, "hottest_tank"),
            ),
            Operator(
                id="alarm",
                inputs=[Input("urn:profile:hottest", "hottest")],
                outputs={"alarm": "urn:profile:alarm"},
                window=Pointwise(),
                function=PythonFunction.from_file(FUNCTIONS, "high_temperature"),
                parameters={"threshold": 27.0},
            ),
        ]
    )
    return Plan("profile-plan", operators)


def verify(engine, raw, tanks):
    """Independent recomputation of every current source and derived relation."""
    expected = defaultdict(dict)
    buckets = defaultdict(list)
    for (reference, timestamp), value in raw.items():
        expected[reference][timestamp] = value
        tank = int(reference.split(":")[3])
        buckets[tank, timestamp // WIDTH * WIDTH].append(value)
    hottest = {}
    for (tank, timestamp), values in buckets.items():
        value = statistics.mean(values)
        expected[mean_stream(tank)][timestamp] = value
        hottest[timestamp] = max(value, hottest.get(timestamp, -math.inf))
    expected["urn:profile:hottest"] = hottest
    expected["urn:profile:alarm"] = {
        stamp: value for stamp, value in hottest.items() if value > 27
    }
    snapshot = engine.snapshot()
    for reference, wanted in expected.items():
        actual = {
            row.event_time_ns: row.value
            for row in snapshot.query(reference, limit=100_000)
        }
        if actual.keys() != wanted.keys() or any(
            not math.isclose(actual[stamp], value, rel_tol=1e-10, abs_tol=1e-10)
            for stamp, value in wanted.items()
        ):
            raise AssertionError(f"incorrect profile result: {reference}")


def run_case(config):
    directory = Path(config["directory"])
    scenario = SCENARIOS[config["scenario"]]
    options = {"workers": config["workers"], "epoch_requests": 128}
    options["python_pool_size"] = (
        0 if config.get("execution", "pooled") == "fresh" else config["workers"]
    )
    if config["instrumented"]:
        launcher = directory / "python-worker"
        launcher.write_text(
            f"#!{sys.executable}\n" + (DIRECTORY / "worker.py").read_text()
        )
        launcher.chmod(0o755)
        options["python"] = str(launcher)
    raw = {}

    def accept(engine, request, values):
        for reference, stamp, value in values:
            raw[reference, stamp] = value
        return engine.ingest(
            "profile-source", request, [Mutation.put(*value) for value in values]
        )

    reports = []
    with (
        tempfile.TemporaryDirectory(prefix="acquirium-profile-db-") as database,
        Engine(Path(database) / "state.duckdb", **options) as engine,
    ):
        engine.activate("activate", plan(scenario["tanks"]))
        engine.step()
        seed = [
            (
                stream(tank, probe),
                bucket * WIDTH + sample,
                30.0 + tank * 3 + probe + sample % 7 * 0.1 + bucket * 0.01,
            )
            for tank in range(scenario["tanks"])
            for probe in range(2)
            for bucket in range(scenario["history"])
            for sample in range(scenario["samples"])
        ]
        # Native admission is bounded; seed history with explicit requests.
        for offset in range(0, len(seed), 10_000):
            accept(engine, f"seed-{offset}", seed[offset : offset + 10_000])
        engine.step()  # Warm caches and initialize every retained bucket.
        verify(engine, raw, scenario["tanks"])
        for iteration in range(config["samples"]):
            for bucket in range(scenario["requests"]):
                values = [
                    (
                        stream(tank, probe),
                        bucket * WIDTH,
                        32.0 + tank * 3 + probe + bucket * 0.01 + iteration * 2,
                    )
                    for tank in range(scenario["tanks"])
                    for probe in range(2)
                ]
                accept(engine, f"correction-{iteration}-{bucket}", values)
            report = engine.step()
            if report is None or report["requests"] != scenario["requests"]:
                raise AssertionError("incorrect measured request prefix")
            reports.append(report)
            verify(engine, raw, scenario["tanks"])
        pool_stats = engine.python_worker_stats()
    workers = []
    by_epoch = {report["epoch"]: report for report in reports}
    for path in sorted((directory / "workers").glob("*.json")):
        worker = json.loads(path.read_text())
        if worker["epoch"] not in by_epoch:
            continue  # Seed/warmup is retained in raw files, not summarized.
        operator_ms = by_epoch[worker["epoch"]]["operator_ms"][worker["operator"]]
        worker["native_operator_ms"] = operator_ms
        measured_ns = sum(worker[f"{stage}_ns"] for stage in STAGES)
        worker["external_and_unattributed_ms"] = operator_ms - measured_ns / 1e6
        workers.append(worker)
    if config["instrumented"] and len(workers) != config["samples"] * (
        scenario["tanks"] + 2
    ):
        raise AssertionError("missing actual worker profiles")
    result = {
        "config": config,
        "scenario": scenario,
        "epochs": reports,
        "workers": workers,
        "verified": True,
        "python_worker_stats": pool_stats,
        "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
    }
    write(directory / "result.json", result)
    return result


def process_baselines(samples):
    env = dict(
        os.environ,
        POLARS_MAX_THREADS="1",
        OMP_NUM_THREADS="1",
        OPENBLAS_NUM_THREADS="1",
        MKL_NUM_THREADS="1",
    )
    result = {}
    for name, code in (
        ("interpreter_lifetime", "pass"),
        ("interpreter_and_numeric_imports", "import polars, pyarrow"),
    ):
        values = []
        for _ in range(samples + 1):
            started = time.perf_counter_ns()
            subprocess.run(
                [sys.executable, "-c", code],
                env=env,
                check=True,
                timeout=30,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            values.append((time.perf_counter_ns() - started) / 1e6)
        result[name] = {
            "samples_ms": values[1:],
            "median_ms": statistics.median(values[1:]),
        }
    return result


def summarize(results, baselines):
    cases = []
    for result in results:
        reports = result["epochs"]
        case = {
            "scenario": result["config"]["scenario"],
            "execution": result["config"].get("execution", "pooled"),
            "workers": result["config"]["workers"],
            "instrumented": result["config"]["instrumented"],
            "samples": len(reports),
            "requests_per_epoch": result["scenario"]["requests"],
            "peak_workers": max(report["peak_workers"] for report in reports),
            "python_worker_stats": result["python_worker_stats"],
        }
        for stage in ("prepare_ms", "compute_ms", "publish_ms", "total_ms"):
            values = [report["timings"][stage] for report in reports]
            case[stage] = statistics.median(values)
            case[f"{stage}_min"] = min(values)
            case[f"{stage}_max"] = max(values)
        case["total_ms_per_request"] = case["total_ms"] / case["requests_per_epoch"]
        if result["workers"]:
            # Additive means per invocation; these cannot be summed to obtain
            # epoch latency when independent operators run concurrently.
            profile = {
                f"{stage}_ms": statistics.mean(
                    worker[f"{stage}_ns"] / 1e6 for worker in result["workers"]
                )
                for stage in STAGES
            }
            profile["external_and_unattributed_ms"] = statistics.mean(
                worker["external_and_unattributed_ms"] for worker in result["workers"]
            )
            profile["native_operator_ms"] = statistics.mean(
                worker["native_operator_ms"] for worker in result["workers"]
            )
            profile["input_rows"] = statistics.mean(
                worker["input_rows"] for worker in result["workers"]
            )
            profile["groups"] = statistics.mean(
                worker["groups"] for worker in result["workers"]
            )
            case["worker_mean"] = profile
        cases.append(case)
    cases.sort(
        key=lambda case: (case["scenario"], case["workers"], case["instrumented"])
    )
    controls = {
        (case["scenario"], case["workers"]): case
        for case in cases
        if not case["instrumented"]
    }
    for case in cases:
        if case["instrumented"]:
            control = controls[case["scenario"], case["workers"]]
            case["instrumentation_compute_delta_pct"] = 100 * (
                case["compute_ms"] / control["compute_ms"] - 1
            )
    return {
        "cases": cases,
        "process_baselines": baselines,
        "verified": all(result["verified"] for result in results),
    }


def markdown(summary):
    execution = summary["cases"][0].get("execution", "fresh")
    lines = [
        "# Execution profile",
        "",
        f"Execution: {execution}; paired fresh-owner controls; median milliseconds per measured epoch.",
        "",
        "| Scenario | Workers | Peak | Requests/epoch | Unprofiled compute | Profiled compute | Delta | Total/request |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    cases = summary["cases"]
    for case in cases:
        if case["instrumented"]:
            continue
        profile = next(
            item
            for item in cases
            if item["instrumented"]
            and item["scenario"] == case["scenario"]
            and item["workers"] == case["workers"]
        )
        lines.append(
            f"| {case['scenario']} | {case['workers']} | {case['peak_workers']} | {case['requests_per_epoch']} | "
            f"{case['compute_ms']:.2f} | {profile['compute_ms']:.2f} | {profile['instrumentation_compute_delta_pct']:+.1f}% | {case['total_ms_per_request']:.2f} |"
        )
    lines += [
        "",
        "## Actual worker stage means",
        "",
        (
            "Milliseconds per worker invocation, including all groups in that invocation. "
            "Parallel worker times are not additive epoch latency."
        ),
        "",
        "| Scenario | Workers | Imports | Function setup | Arrow/JSON input | Function | Validation/Arrow output | Wrapper setup | External/unattributed |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for case in cases:
        if not case["instrumented"]:
            continue
        worker = case["worker_mean"]
        imports = sum(
            worker[f"{name}_ms"]
            for name in ("stdlib_import", "polars_import", "pyarrow_import")
        )
        lines.append(
            f"| {case['scenario']} | {case['workers']} | {imports:.2f} | {worker['function_setup_ms']:.2f} | "
            f"{worker['input_ms']:.2f} | {worker['function_ms']:.2f} | {worker['output_ms']:.2f} | "
            f"{worker['wrapper_prepare_ms']:.2f} | {worker['external_and_unattributed_ms']:.2f} |"
        )
    lines += [
        "",
        (
            "External/unattributed includes Rust grouping/serialization/diff, OS process startup/exit, "
            "polling, and uninstrumented/reporting overhead; it is not a pure interpreter-startup measurement."
        ),
        "",
    ]
    return "\n".join(lines)


def export_workers(output, results):
    fields = [
        "scenario",
        "workers",
        "epoch",
        "operator",
        "groups",
        "input_rows",
        "cold_worker",
        "pid",
        *[f"{stage}_ms" for stage in STAGES],
        "native_operator_ms",
        "external_and_unattributed_ms",
    ]
    with (Path(output) / "workers.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for result in results:
            for worker in result["workers"]:
                writer.writerow(
                    {
                        "scenario": result["config"]["scenario"],
                        "workers": result["config"]["workers"],
                        **{
                            key: worker[key]
                            for key in (
                                "epoch",
                                "operator",
                                "groups",
                                "input_rows",
                                "cold_worker",
                                "pid",
                                "native_operator_ms",
                                "external_and_unattributed_ms",
                            )
                        },
                        **{
                            f"{stage}_ms": worker[f"{stage}_ns"] / 1e6
                            for stage in STAGES
                        },
                    }
                )


def run(output, workers, scenarios, samples, execution="pooled"):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    distribution = importlib.metadata.distribution("escapement-db")
    origin = json.loads(distribution.read_text("direct_url.json") or "{}")
    environment = {
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "native": build_info(),
        "native_commit": origin.get("vcs_info", {}).get("commit_id"),
        "polars": importlib.metadata.version("polars"),
        "pyarrow": importlib.metadata.version("pyarrow"),
        "function_sha256": hashlib.sha256(FUNCTIONS.read_bytes()).hexdigest(),
        "worker_launcher_sha256": hashlib.sha256(
            (DIRECTORY / "worker.py").read_bytes()
        ).hexdigest(),
        "samples": samples,
        "execution": execution,
        "workers": workers,
        "scenarios": scenarios,
        "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
    }
    write(output / "environment.json", environment)
    baselines = process_baselines(samples)
    configs = []
    for scenario in scenarios:
        for count in workers:
            for instrumented in (False, True):
                directory = (
                    output
                    / f"{scenario}-w{count}-{'profile' if instrumented else 'control'}"
                )
                directory.mkdir()
                configs.append(
                    {
                        "scenario": scenario,
                        "workers": count,
                        "instrumented": instrumented,
                        "samples": samples,
                        "execution": execution,
                        "directory": str(directory),
                    }
                )
    random.Random(20261018).shuffle(configs)
    results = []
    for config in configs:
        directory = Path(config["directory"])
        write(directory / "config.json", config)
        print(f"Profiling {directory.name}", flush=True)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "examples.profiling.run",
                "--case",
                str(directory / "config.json"),
            ],
            cwd=ROOT,
            check=True,
            timeout=300,
        )
        results.append(json.loads((directory / "result.json").read_text()))
    summary = summarize(results, baselines)
    write(output / "summary.json", summary)
    (output / "summary.md").write_text(markdown(summary))
    with (output / "epochs.csv").open("w", newline="") as handle:
        fields = [
            "scenario",
            "workers",
            "instrumented",
            "epoch",
            "requests",
            "peak_workers",
            "prepare_ms",
            "compute_ms",
            "publish_ms",
            "total_ms",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for result in results:
            for report in result["epochs"]:
                writer.writerow(
                    {
                        "scenario": result["config"]["scenario"],
                        "workers": result["config"]["workers"],
                        "instrumented": result["config"]["instrumented"],
                        **{
                            key: report[key]
                            for key in ("epoch", "requests", "peak_workers")
                        },
                        **report["timings"],
                    }
                )
    export_workers(output, results)
    print(markdown(summary), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--workers", default="1,2,4")
    parser.add_argument("--scenarios", default=",".join(SCENARIOS))
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--execution", choices=("fresh", "pooled"), default="pooled")
    parser.add_argument("--case", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.case:
        run_case(json.loads(args.case.read_text()))
        return
    workers = [int(value) for value in args.workers.split(",")]
    scenarios = args.scenarios.split(",")
    if (
        args.output is None
        or args.samples < 1
        or min(workers) < 1
        or not set(scenarios) <= SCENARIOS.keys()
    ):
        parser.error(
            "provide a fresh output directory, positive sample/worker counts, and known scenarios"
        )
    run(args.output, workers, scenarios, args.samples, args.execution)


if __name__ == "__main__":
    main()
