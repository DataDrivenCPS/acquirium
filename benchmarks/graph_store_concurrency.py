#!/usr/bin/env python3
"""Benchmark concurrent reads and driver-like writes through OxigraphGraphStore.

This is an operational benchmark, not a test. It uses a disposable on-disk
Acquirium graph store and measures end-to-end query/write latency, including
derived-query-cache rebuilds.

Examples:
    uv run python benchmarks/graph_store_concurrency.py
    uv run python benchmarks/graph_store_concurrency.py --readers 1,4,8 --reader-operations 50
    uv run python benchmarks/graph_store_concurrency.py --profiles readers,append,replace --json /tmp/graph-store.json
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from rdflib import Graph, Literal, RDF, URIRef

from acquirium.Storage.graph_store import OxigraphGraphStore


EXAMPLE = "urn:acquirium:benchmark:"
THING = URIRef(f"{EXAMPLE}Thing")
VALUE = URIRef(f"{EXAMPLE}value")
QUERY = f"SELECT ?subject ?value WHERE {{ ?subject a <{THING}> ; <{VALUE}> ?value . }}"

PROFILE_WRITERS = {
    "readers": 0,
    "append": 1,
    "replace": 1,
    "stale-read": 1,
}

# Keeping both values in the default sweep makes a contention run directly
# comparable: a reader can use the last complete published view, or wait for
# the rebuild triggered by a concurrent write to finish.
CONSISTENCIES = {
    "published": False,
    "fresh": True,
}


@dataclass(frozen=True)
class LatencySummary:
    count: int
    min_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    mean_ms: float


@dataclass
class WorkerResult:
    kind: str
    latencies_ms: list[float]
    errors: list[str]


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    index = (len(values) - 1) * fraction
    lower, upper = math.floor(index), math.ceil(index)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (index - lower)


def summarize(latencies_ms: list[float]) -> LatencySummary:
    values = sorted(latencies_ms)
    if not values:
        return LatencySummary(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    return LatencySummary(
        count=len(values),
        min_ms=values[0],
        p50_ms=percentile(values, 0.50),
        p95_ms=percentile(values, 0.95),
        p99_ms=percentile(values, 0.99),
        max_ms=values[-1],
        mean_ms=sum(values) / len(values),
    )


def parse_positive_csv(value: str) -> list[int]:
    try:
        values = [int(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if not values or any(item < 1 for item in values):
        raise argparse.ArgumentTypeError("expected comma-separated positive integers")
    return values


def data_graph(count: int, *, prefix: str) -> Graph:
    graph = Graph()
    for index in range(count):
        subject = URIRef(f"{EXAMPLE}{prefix}/{index}")
        graph.add((subject, RDF.type, THING))
        graph.add((subject, VALUE, Literal(index)))
    return graph


def run_profile(
    store: OxigraphGraphStore,
    *,
    profile: str,
    reader_count: int,
    reader_operations: int,
    writer_operations: int,
    include_dependencies: bool,
    wait_for_fresh: bool,
) -> dict[str, object]:
    """Run readers with zero or one source-owned writer from a shared barrier."""
    writer_count = PROFILE_WRITERS[profile]
    barrier = threading.Barrier(reader_count + writer_count + 1)
    results: list[WorkerResult] = []
    results_lock = threading.Lock()
    refresh_latencies_ms: list[float] = []
    refresh_lock = threading.Lock()
    publish_latencies_ms: list[float] = []
    publish_lock = threading.Lock()
    source_graph = store.source_graph_uri("benchmark-driver")
    original_build = store._build_query_views
    original_publish = store._publish_query_views

    def timed_build(data: Graph, shapes: Graph):
        began = time.perf_counter_ns()
        try:
            return original_build(data, shapes)
        finally:
            with refresh_lock:
                refresh_latencies_ms.append((time.perf_counter_ns() - began) / 1_000_000)

    def timed_publish(inferred: Graph, shapes: Graph):
        began = time.perf_counter_ns()
        try:
            return original_publish(inferred, shapes)
        finally:
            with publish_lock:
                publish_latencies_ms.append((time.perf_counter_ns() - began) / 1_000_000)

    # The private hook is instrumentation only. It measures actual derived-view
    # rebuilds without changing locking or query behavior.
    store._build_query_views = timed_build  # type: ignore[method-assign]
    store._publish_query_views = timed_publish  # type: ignore[method-assign]

    def reader() -> WorkerResult:
        result = WorkerResult("reader", [], [])
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            result.errors.append("start barrier broke")
            return result
        for _ in range(reader_operations):
            began = time.perf_counter_ns()
            try:
                rows = store.sparql_query(
                    QUERY,
                    include_dependencies=include_dependencies,
                    wait_for_fresh=wait_for_fresh,
                )["rows"]
                if not rows:
                    raise RuntimeError("query unexpectedly returned no rows")
            except Exception as exc:
                result.errors.append(f"{type(exc).__name__}: {exc}")
            finally:
                result.latencies_ms.append((time.perf_counter_ns() - began) / 1_000_000)
        return result

    def writer() -> WorkerResult:
        result = WorkerResult("writer", [], [])
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            result.errors.append("start barrier broke")
            return result
        for index in range(writer_operations):
            began = time.perf_counter_ns()
            try:
                incoming = data_graph(1, prefix=f"driver-write/{profile}/{index}")
                store.insert_graph(
                    incoming,
                    graph_uri=source_graph,
                    replace=profile == "replace",
                )
            except Exception as exc:
                result.errors.append(f"{type(exc).__name__}: {exc}")
            finally:
                result.latencies_ms.append((time.perf_counter_ns() - began) / 1_000_000)
        return result

    def collect(worker) -> None:
        outcome = worker()
        with results_lock:
            results.append(outcome)

    workers = [threading.Thread(target=collect, args=(reader,)) for _ in range(reader_count)]
    if writer_count:
        workers.append(threading.Thread(target=collect, args=(writer,)))
    for worker in workers:
        worker.start()
    began = time.perf_counter()
    barrier.wait()
    for worker in workers:
        worker.join()
    elapsed_s = time.perf_counter() - began
    store._build_query_views = original_build  # type: ignore[method-assign]
    store._publish_query_views = original_publish  # type: ignore[method-assign]

    reader_latencies = [
        latency for result in results if result.kind == "reader" for latency in result.latencies_ms
    ]
    writer_latencies = [
        latency for result in results if result.kind == "writer" for latency in result.latencies_ms
    ]
    errors = [error for result in results for error in result.errors]
    operations = len(reader_latencies) + len(writer_latencies)
    return {
        "profile": profile,
        "readers": reader_count,
        "writers": writer_count,
        "include_dependencies": include_dependencies,
        "wait_for_fresh": wait_for_fresh,
        "elapsed_s": elapsed_s,
        "operations_per_second": operations / elapsed_s if elapsed_s else 0.0,
        "reader_latency_ms": asdict(summarize(reader_latencies)),
        "writer_latency_ms": asdict(summarize(writer_latencies)),
        "derived_refresh_latency_ms": asdict(summarize(refresh_latencies_ms)),
        "derived_publish_latency_ms": asdict(summarize(publish_latencies_ms)),
        "errors": errors,
    }


def run_stale_read_profile(
    store: OxigraphGraphStore,
    *,
    reader_count: int,
    stale_hold_ms: float,
    include_dependencies: bool,
    wait_for_fresh: bool,
) -> dict[str, object]:
    """Measure one query per reader while publication of a rebuild is held.

    The held rebuild creates a known stale window.  This avoids relying on the
    scheduler to happen to overlap a normal, short rebuild with a query: false
    returns the prior complete view, while true cannot complete before release.
    """
    start_readers = threading.Barrier(reader_count + 1)
    rebuild_started = threading.Event()
    release_rebuild = threading.Event()
    results: list[WorkerResult] = []
    results_lock = threading.Lock()
    refresh_latencies_ms: list[float] = []
    refresh_lock = threading.Lock()
    publish_latencies_ms: list[float] = []
    publish_lock = threading.Lock()
    source_graph = store.source_graph_uri("benchmark-stale-read-driver")
    original_build = store._build_query_views
    original_publish = store._publish_query_views

    def held_build(data: Graph, shapes: Graph):
        rebuild_started.set()
        if not release_rebuild.wait(timeout=10):
            raise TimeoutError("stale-read benchmark did not release the rebuild")
        began = time.perf_counter_ns()
        try:
            return original_build(data, shapes)
        finally:
            with refresh_lock:
                refresh_latencies_ms.append((time.perf_counter_ns() - began) / 1_000_000)

    def timed_publish(inferred: Graph, shapes: Graph):
        began = time.perf_counter_ns()
        try:
            return original_publish(inferred, shapes)
        finally:
            with publish_lock:
                publish_latencies_ms.append((time.perf_counter_ns() - began) / 1_000_000)

    # This is deliberately a test hook: it holds publication before inference,
    # without changing the store's locking or read-selection behavior.
    store._build_query_views = held_build  # type: ignore[method-assign]
    store._publish_query_views = timed_publish  # type: ignore[method-assign]

    def reader() -> WorkerResult:
        result = WorkerResult("reader", [], [])
        try:
            start_readers.wait()
            began = time.perf_counter_ns()
            rows = store.sparql_query(
                QUERY,
                include_dependencies=include_dependencies,
                wait_for_fresh=wait_for_fresh,
            )["rows"]
            if not rows:
                raise RuntimeError("query unexpectedly returned no rows")
        except Exception as exc:
            result.errors.append(f"{type(exc).__name__}: {exc}")
        else:
            result.latencies_ms.append((time.perf_counter_ns() - began) / 1_000_000)
        return result

    def collect() -> None:
        outcome = reader()
        with results_lock:
            results.append(outcome)

    workers = [threading.Thread(target=collect) for _ in range(reader_count)]
    writer = WorkerResult("writer", [], [])
    try:
        for worker in workers:
            worker.start()

        began_write = time.perf_counter_ns()
        try:
            store.insert_graph(
                data_graph(1, prefix="driver-write/stale-read"),
                graph_uri=source_graph,
            )
        except Exception as exc:
            writer.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            writer.latencies_ms.append((time.perf_counter_ns() - began_write) / 1_000_000)

        if not rebuild_started.wait(timeout=10):
            writer.errors.append("Timed out waiting for the derived rebuild to start")
        began = time.perf_counter()
        start_readers.wait()
        # The barrier lets every reader attempt its sole query while the old
        # view remains published.  Keep the interval explicit in the result.
        time.sleep(stale_hold_ms / 1_000)
        release_rebuild.set()
        for worker in workers:
            worker.join()
        elapsed_s = time.perf_counter() - began
        # The reader interval above is the measurement. Drain the held rebuild
        # afterwards so its build metric is complete and cannot affect a later
        # row; this synchronization is intentionally outside elapsed_s.
        store.sparql_query(QUERY, include_dependencies=include_dependencies, wait_for_fresh=True)
    finally:
        release_rebuild.set()
        for worker in workers:
            worker.join()
        store._build_query_views = original_build  # type: ignore[method-assign]
        store._publish_query_views = original_publish  # type: ignore[method-assign]

    reader_latencies = [latency for result in results for latency in result.latencies_ms]
    errors = [error for result in results for error in result.errors] + writer.errors
    operations = len(reader_latencies) + len(writer.latencies_ms)
    return {
        "profile": "stale-read",
        "readers": reader_count,
        "writers": 1,
        "include_dependencies": include_dependencies,
        "wait_for_fresh": wait_for_fresh,
        "stale_hold_ms": stale_hold_ms,
        "elapsed_s": elapsed_s,
        "operations_per_second": operations / elapsed_s if elapsed_s else 0.0,
        "reader_latency_ms": asdict(summarize(reader_latencies)),
        "writer_latency_ms": asdict(summarize(writer.latencies_ms)),
        "derived_refresh_latency_ms": asdict(summarize(refresh_latencies_ms)),
        "derived_publish_latency_ms": asdict(summarize(publish_latencies_ms)),
        "errors": errors,
    }


def print_result(result: dict[str, object]) -> None:
    reader = result["reader_latency_ms"]
    writer = result["writer_latency_ms"]
    refresh = result["derived_refresh_latency_ms"]
    publish = result["derived_publish_latency_ms"]
    assert isinstance(reader, dict) and isinstance(writer, dict) and isinstance(refresh, dict)
    assert isinstance(publish, dict)
    print(
        f"{result['profile']:8} readers={result['readers']:>3} writers={result['writers']} "
        f"dependencies={result['include_dependencies']} ops/s={result['operations_per_second']:>8.1f} "
        f"wait_for_fresh={result['wait_for_fresh']!s:5} "
        f"read p50/p95={reader['p50_ms']:.2f}/{reader['p95_ms']:.2f} ms "
        f"write p95={writer['p95_ms']:.2f} ms "
        f"refreshes={refresh['count']} infer p95={refresh['p95_ms']:.2f} ms "
        f"publish p95={publish['p95_ms']:.2f} ms "
        f"errors={len(result['errors'])}",
        flush=True,
    )
    if "stale_hold_ms" in result:
        print(f"  held stale window={result['stale_hold_ms']:.1f} ms", flush=True)


def write_results(path: Path, document: dict[str, object]) -> None:
    """Checkpoint completed rows so an interrupted long sweep remains useful."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n")
    temporary.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readers", default="1,4,8", help="reader concurrency levels")
    parser.add_argument(
        "--profiles",
        default="readers,append,replace,stale-read",
        help="comma-separated profiles: " + ", ".join(PROFILE_WRITERS),
    )
    parser.add_argument("--reader-operations", type=int, default=20, help="queries per reader")
    parser.add_argument("--writer-operations", type=int, default=5, help="writes in writer profiles")
    parser.add_argument("--seed-triples", type=int, default=1_000, help="initial plant subjects")
    parser.add_argument(
        "--stale-hold-ms",
        type=float,
        default=50.0,
        help="how long stale-read holds the rebuild before publication",
    )
    parser.add_argument(
        "--include-dependencies",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="include ontology and shape dependencies in reader queries",
    )
    parser.add_argument(
        "--consistencies",
        default=None,
        help="comma-separated read contracts: " + ", ".join(CONSISTENCIES),
    )
    parser.add_argument(
        "--wait-for-fresh",
        action="store_true",
        help="run only strict readers that wait for each current inferred generation",
    )
    parser.add_argument("--store-path", type=Path, help="retain the disposable store at this path")
    parser.add_argument("--json", type=Path, help="write machine-readable results")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    readers = parse_positive_csv(args.readers)
    profiles = [name.strip() for name in args.profiles.split(",") if name.strip()]
    invalid = sorted(set(profiles) - PROFILE_WRITERS.keys())
    if invalid:
        raise SystemExit(f"unknown profile(s): {', '.join(invalid)}")
    if args.wait_for_fresh and args.consistencies is not None:
        raise SystemExit("--wait-for-fresh cannot be combined with --consistencies")
    requested_consistencies = args.consistencies or "published,fresh"
    consistencies = [name.strip() for name in requested_consistencies.split(",") if name.strip()]
    if args.wait_for_fresh:
        consistencies = ["fresh"]
    invalid = sorted(set(consistencies) - CONSISTENCIES.keys())
    if invalid:
        raise SystemExit(f"unknown consistency contract(s): {', '.join(invalid)}")
    if not consistencies:
        raise SystemExit("--consistencies must select at least one contract")
    if min(args.reader_operations, args.writer_operations, args.seed_triples) < 1:
        raise SystemExit("operation counts and --seed-triples must be positive")
    if args.stale_hold_ms <= 0:
        raise SystemExit("--stale-hold-ms must be positive")

    temporary_root: str | None = None
    if args.store_path is None:
        temporary_root = tempfile.mkdtemp(prefix="acquirium-graph-benchmark-")
        root = Path(temporary_root)
    else:
        root = args.store_path
        root.mkdir(parents=True, exist_ok=True)

    store: OxigraphGraphStore | None = None
    try:
        store = OxigraphGraphStore(store_path=root / "store", env_root=root / "ontoenv")
        store.insert_graph(data_graph(args.seed_triples, prefix="plant"), replace=True)
        # Warm the same derived cache that readers will query; setup time is
        # intentionally excluded from measured profiles.
        store.sparql_query(
            QUERY,
            include_dependencies=args.include_dependencies,
            wait_for_fresh=True,
        )

        document: dict[str, object] = {
            "seed_triples": args.seed_triples,
            "reader_operations": args.reader_operations,
            "writer_operations": args.writer_operations,
            "include_dependencies": args.include_dependencies,
            "consistencies": consistencies,
            "results": [],
        }
        print(
            f"seed={args.seed_triples} reader_operations={args.reader_operations} "
            f"writer_operations={args.writer_operations} "
            f"include_dependencies={args.include_dependencies}",
            flush=True,
        )
        for profile in profiles:
            for reader_count in readers:
                for consistency in consistencies:
                    if profile == "stale-read":
                        result = run_stale_read_profile(
                            store,
                            reader_count=reader_count,
                            stale_hold_ms=args.stale_hold_ms,
                            include_dependencies=args.include_dependencies,
                            wait_for_fresh=CONSISTENCIES[consistency],
                        )
                    else:
                        result = run_profile(
                            store,
                            profile=profile,
                            reader_count=reader_count,
                            reader_operations=args.reader_operations,
                            writer_operations=args.writer_operations,
                            include_dependencies=args.include_dependencies,
                            wait_for_fresh=CONSISTENCIES[consistency],
                        )
                    # Do not let a still-running asynchronous rebuild bleed
                    # into the next row. This synchronization is not timed.
                    store.sparql_query(
                        QUERY,
                        include_dependencies=args.include_dependencies,
                        wait_for_fresh=True,
                    )
                    document["results"].append(result)
                    print_result(result)
                    if args.json:
                        write_results(args.json, document)
        if args.json:
            write_results(args.json, document)
    finally:
        if store is not None:
            store.close()
        if temporary_root is not None:
            shutil.rmtree(temporary_root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
