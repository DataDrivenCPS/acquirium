"""Sweep the microbatch materialization control path on a local DuckDB store.

The benchmark intentionally keeps the application body close to a no-op.  It
still reads through the normal epoch snapshot and declares/publishes the
configured number of output streams, so the measured work is dominated by
publication, topology, scheduling, claims, and durable epoch transitions.
An independent driver thread offers raw input batches while the materializer
drains them concurrently, making requested publish rates an open offered load.

Run a small sweep with::

    uv run python benchmarks/microbatch_materialization.py \
        --input-streams 1,8 --app-input-width 1,8 \
        --app-output-width 1,4 --app-count 1,4 --chain-depth 1,2 \
        --publish-rate-hz 0,10 --microbatches 5

Each invocation creates ``benchmarks/microbatch_YYYYmmddTHHMMSSZ/`` unless
``--output-dir`` is supplied.  The directory contains CSV data, PNG figures,
the generated Markdown report, and the exact run configuration.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
import itertools
import json
import math
import os
from pathlib import Path
import platform
import re
import sys
import tempfile
from threading import Event, Lock, Thread
import time
import traceback
from typing import Any, Callable, Iterable, Sequence, TypeVar

import pyarrow as pa

from acquirium.Materialization.api import Transformation, outputs
from acquirium.Materialization.definitions import definition_for
from acquirium.Materialization.epoch_reconciler import TopologyEpochReconciler
from acquirium.Materialization.executor import LocalExecutorPool
from acquirium.Materialization.impact import pointwise
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.epoch_duckdb import TopologyEpochDuckDB
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest


UTC = timezone.utc
T = TypeVar("T")


@dataclass(frozen=True)
class SweepCase:
    """One point in the Cartesian product of benchmark dimensions."""

    input_streams: int
    app_input_width: int
    app_output_width: int
    publish_rate_hz: float
    app_count: int
    chain_depth: int
    repeat: int


@dataclass
class TopologyPlan:
    definitions: list[Any]
    selector_refs: dict[str, tuple[str, ...]]
    output_refs: tuple[str, ...]
    effective_input_widths: list[int]


@dataclass(frozen=True)
class DriverBatch:
    batch: int
    scheduled_at: float
    publish_started_at: float
    publish_finished_at: float
    row_count: int

    @property
    def publish_seconds(self) -> float:
        return self.publish_finished_at - self.publish_started_at

    @property
    def publish_lateness_seconds(self) -> float:
        return self.publish_started_at - self.scheduled_at


class OpenLoadDriver:
    """Publish input batches on a schedule without waiting for materialization."""

    def __init__(
        self,
        publisher: PublicationDuckDB,
        refs: Sequence[str],
        *,
        case_id: str,
        microbatches: int,
        rows_per_stream: int,
        publish_rate_hz: float,
        start: datetime,
    ) -> None:
        self._publisher = publisher
        self._refs = refs
        self._case_id = case_id
        self._microbatches = microbatches
        self._rows_per_stream = rows_per_stream
        self._publish_rate_hz = publish_rate_hz
        self._start = start
        self._stop = Event()
        self.done = Event()
        self._lock = Lock()
        self.batches: list[DriverBatch] = []
        self.error: BaseException | None = None
        self.started_at: float | None = None
        self.finished_at: float | None = None

    @property
    def published_batches(self) -> int:
        with self._lock:
            return len(self.batches)

    def request_stop(self) -> None:
        self._stop.set()

    def _wait_until(self, deadline: float) -> bool:
        while not self._stop.is_set():
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                return True
            self._stop.wait(min(remaining, 0.25))
        return False

    def run(self) -> None:
        self.started_at = time.perf_counter()
        try:
            for batch in range(1, self._microbatches + 1):
                if self._publish_rate_hz > 0:
                    scheduled_at = self.started_at + (batch - 1) / self._publish_rate_hz
                    if not self._wait_until(scheduled_at):
                        return
                else:
                    scheduled_at = time.perf_counter()
                if self._stop.is_set():
                    return
                publish_started_at = time.perf_counter()
                mutations = _mutations(
                    self._refs, batch, self._rows_per_stream, self._start
                )
                self._publisher.publish(PublicationRequest(
                    f"{self._case_id}:raw:{batch}", mutations
                ))
                publish_finished_at = time.perf_counter()
                with self._lock:
                    self.batches.append(DriverBatch(
                        batch=batch,
                        scheduled_at=scheduled_at,
                        publish_started_at=publish_started_at,
                        publish_finished_at=publish_finished_at,
                        row_count=mutations.num_rows,
                    ))
        except BaseException as error:
            self.error = error
        finally:
            self.finished_at = time.perf_counter()
            self.done.set()


class SyntheticGraph:
    """Minimal graph-query surface used during immutable topology resolution.

    The real graph store returns these same query-result columns.  A selector
    URI is embedded in each generated query, allowing each synthetic app to
    resolve precisely its assigned input streams without loading an ontology.
    """

    _selector_pattern = re.compile(r"<(urn:acq:bench:selector:[^>]+)>")

    def __init__(self, selector_refs: dict[str, tuple[str, ...]]) -> None:
        self._selector_refs = selector_refs

    def sparql_query(self, query: str, **_: Any) -> dict[str, Any]:
        match = self._selector_pattern.search(query)
        if match is None:
            raise ValueError(f"synthetic graph query did not contain an app selector: {query}")
        selector = match.group(1)
        try:
            refs = self._selector_refs[selector]
        except KeyError as error:
            raise KeyError(f"unknown synthetic graph selector {selector!r}") from error
        columns = ["v0", "v1", "ext1", "unit1", "extunit1"]
        rows = [[selector, f"{ref}:point", ref, None, None] for ref in refs]
        return {"columns": columns, "rows": rows}


def _cheap_transform(self: Transformation, _inputs: Any, context: Any) -> None:
    """Emit one row per output stream; application computation is negligible."""

    if context.outputs is None:
        raise RuntimeError("benchmark transformation did not receive output handles")
    for output_name in self.output_names:
        context.outputs.declare(output_name).add(context.write_interval.start, 0.0)


def _make_transformation(
    *,
    class_name: str,
    selector: str,
    output_names: tuple[str, ...],
    output_prefix: str,
) -> type[Transformation]:
    """Create an importable class so the normal executor identity checks run."""

    def build_query(self: Transformation, aq: Any) -> Any:
        return aq.query().entity(uri=self.selector, alias="benchmark_app").measurement(
            frm="benchmark_app", alias="benchmark_input"
        )

    namespace = {
        "__module__": __name__,
        "__qualname__": class_name,
        "selector": selector,
        "output_names": output_names,
        "outputs": {name: outputs.stream(prefix=output_prefix) for name in output_names},
        "impact": pointwise(),
        "invocation": "whole_query",
        "build_query": build_query,
        "transform": _cheap_transform,
    }
    target = type(class_name, (Transformation,), namespace)
    # load_entrypoint() resolves a module:qualname path.  Keep this generated
    # class at module scope so both ``python file.py`` and ``python -m`` work.
    globals()[class_name] = target
    return target


def _planned_output_refs(name: str, prefix: str, width: int) -> tuple[str, ...]:
    return tuple(f"{prefix}:{name}:out{index}" for index in range(width))


def _build_topology(case: SweepCase, run_id: str, case_id: str) -> TopologyPlan:
    raw_refs = tuple(f"urn:acq:bench:raw:{run_id}:{index}" for index in range(case.input_streams))
    previous_refs = raw_refs
    definitions: list[Any] = []
    selector_refs: dict[str, tuple[str, ...]] = {}
    output_refs: list[str] = []
    effective_widths: list[int] = []

    for stage in range(case.chain_depth):
        stage_outputs: list[str] = []
        for app in range(case.app_count):
            # Wrapping makes app count and requested input width independent:
            # fan-out is valid, and a narrow preceding stage naturally limits
            # the number of distinct streams available to a downstream app.
            assigned = tuple(
                previous_refs[(app * case.app_input_width + offset) % len(previous_refs)]
                for offset in range(min(case.app_input_width, len(previous_refs)))
            )
            effective_widths.append(len(assigned))
            selector = f"urn:acq:bench:selector:{run_id}:{case_id}:s{stage}:a{app}"
            selector_refs[selector] = assigned

            name = f"{case_id}_s{stage}_a{app}"
            prefix = f"urn:acq:bench:output:{run_id}:s{stage}:a{app}"
            output_names = tuple(f"out{index}" for index in range(case.app_output_width))
            target = _make_transformation(
                class_name=f"Benchmark_{case_id}_S{stage}_A{app}",
                selector=selector,
                output_names=output_names,
                output_prefix=prefix,
            )
            definition = definition_for(
                target,
                name=name,
                outputs=target.outputs,
                impact=pointwise(),
                invocation="whole_query",
            )
            definitions.append(definition)
            refs = _planned_output_refs(name, prefix, case.app_output_width)
            stage_outputs.extend(refs)
            output_refs.extend(refs)
        previous_refs = tuple(stage_outputs)

    return TopologyPlan(definitions, selector_refs, tuple(output_refs), effective_widths)


def _mutations(
    refs: Sequence[str], batch_index: int, rows_per_stream: int, start: datetime
) -> pa.Table:
    timestamps: list[datetime] = []
    ref_uris: list[str] = []
    values: list[float] = []
    for ref_index, ref in enumerate(refs):
        for row_index in range(rows_per_stream):
            timestamps.append(start + timedelta(seconds=batch_index * rows_per_stream + row_index))
            ref_uris.append(ref)
            values.append(float(batch_index * len(refs) + ref_index + row_index))
    return pa.table(
        {
            "operation": ["upsert"] * len(timestamps),
            "ref_uri": ref_uris,
            "ts": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
            "numeric_value": values,
            "text_value": [None] * len(timestamps),
        },
        schema=MUTATION_SCHEMA,
    )


def _parse_values(raw: str, cast: Callable[[str], T], name: str) -> tuple[T, ...]:
    values: list[T] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = cast(item)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"invalid {name} value {item!r}") from error
        values.append(value)
    if not values:
        raise argparse.ArgumentTypeError(f"{name} must contain at least one value")
    return tuple(values)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError("must be positive")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError("must be finite and non-negative")
    return parsed


def _unique_output_dir(requested: Path | None) -> Path:
    if requested is not None:
        requested.mkdir(parents=True, exist_ok=False)
        return requested
    root = Path(__file__).resolve().parent
    stamp = datetime.now(UTC).strftime("microbatch_%Y%m%dT%H%M%SZ")
    candidate = root / stamp
    suffix = 1
    while candidate.exists():
        candidate = root / f"{stamp}_{suffix}"
        suffix += 1
    candidate.mkdir(parents=True)
    return candidate


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        writer.writerows({field: row.get(field) for field in fields} for row in rows)


RESULT_FIELDS = (
    "case_id", "repeat", "status", "error", "input_streams", "app_input_width",
    "effective_input_width_mean", "effective_input_width_max", "app_output_width",
    "publish_rate_hz", "app_count", "chain_depth", "total_apps", "total_output_streams",
    "workers", "microbatches", "rows_per_stream", "topology_bindings", "topology_components",
    "topology_setup_seconds", "initial_publish_seconds", "initial_materialization_seconds",
    "driver_scheduled_duration_seconds", "driver_publish_duration_seconds",
    "driver_publish_rate_hz", "end_to_end_seconds", "drain_seconds",
    "end_to_end_seconds_per_batch", "end_to_end_batches_per_second", "drain_batches_per_second",
    "driver_publish_rows", "output_rows", "planned_work_items", "transitions",
    "initial_transitions", "max_pending_work_items", "max_pending_components",
    "materializer_errors",
)

CYCLE_FIELDS = (
    "case_id", "repeat", "batch", "input_streams", "app_input_width", "app_output_width",
    "publish_rate_hz", "app_count", "chain_depth", "scheduled_offset_seconds",
    "publish_start_offset_seconds", "publish_end_offset_seconds", "publish_seconds",
    "publish_lateness_seconds", "row_count",
)

PROGRESS_FIELDS = (
    "case_id", "repeat", "elapsed_seconds", "driver_done", "driver_published_batches",
    "planned_work_items", "transitions", "pending_work_items", "pending_components",
)


def _status_metrics(status: dict[str, Any]) -> dict[str, int]:
    work = status.get("work", {})
    pending_work_items = sum(
        int(work.get(name, 0)) for name in ("pending", "claimed", "committed")
    )
    pending_components = sum(
        int(component.get("status") != "sealed")
        for component in status.get("components", [])
    )
    return {
        "pending_work_items": pending_work_items,
        "pending_components": pending_components,
    }


def _runtime_idle(runtime: TopologyEpochDuckDB, status: dict[str, Any]) -> bool:
    work = status.get("work", {})
    components = status.get("components", [])
    return (
        runtime.active_epoch_id() is not None
        and not any(int(work.get(name, 0)) for name in ("pending", "claimed", "committed"))
        and bool(components)
        and all(component.get("status") == "sealed" for component in components)
    )


def _run_case(
    case: SweepCase,
    *,
    case_id: str,
    run_id: str,
    workers: int,
    microbatches: int,
    rows_per_stream: int,
    max_transitions: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    plan = _build_topology(case, run_id, case_id)
    graph = SyntheticGraph(plan.selector_refs)
    raw_refs = tuple(f"urn:acq:bench:raw:{run_id}:{index}" for index in range(case.input_streams))
    start = datetime(2026, 1, 1, tzinfo=UTC)
    result: dict[str, Any] = {
        "case_id": case_id,
        "repeat": case.repeat,
        "status": "ok",
        "error": "",
        "input_streams": case.input_streams,
        "app_input_width": case.app_input_width,
        "effective_input_width_mean": sum(plan.effective_input_widths) / len(plan.effective_input_widths),
        "effective_input_width_max": max(plan.effective_input_widths),
        "app_output_width": case.app_output_width,
        "publish_rate_hz": case.publish_rate_hz,
        "app_count": case.app_count,
        "chain_depth": case.chain_depth,
        "total_apps": case.app_count * case.chain_depth,
        "total_output_streams": len(plan.output_refs),
        "workers": workers,
        "microbatches": microbatches,
        "rows_per_stream": rows_per_stream,
    }
    cycles: list[dict[str, Any]] = []
    progress: list[dict[str, Any]] = []
    publisher: PublicationDuckDB | None = None
    reconciler: TopologyEpochReconciler | None = None
    store: DuckDBStore | None = None
    driver: OpenLoadDriver | None = None
    driver_thread: Thread | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="acquirium-microbatch-") as directory:
            store = DuckDBStore(Path(directory) / "benchmark.duckdb", recreate=True)
            publisher = PublicationDuckDB(store)
            runtime = TopologyEpochDuckDB(store)
            executor = LocalExecutorPool(workers=workers)
            reconciler = TopologyEpochReconciler(runtime, graph, executor)

            initial_publish_start = time.perf_counter()
            publisher.publish(PublicationRequest(
                f"{case_id}:raw:initial", _mutations(raw_refs, 0, rows_per_stream, start)
            ))
            initial_publish_seconds = time.perf_counter() - initial_publish_start

            topology_start = time.perf_counter()
            for definition in plan.definitions:
                runtime.register_definition(definition)
            for definition in plan.definitions:
                runtime.deploy_definition(definition.name, definition.definition_id, graph)
            epoch = runtime.ensure_epoch(1, f"{run_id}:{case_id}:graph")
            summary = runtime.construct_epoch(epoch, graph)
            topology_setup_seconds = time.perf_counter() - topology_start

            initial_materialization_start = time.perf_counter()
            initial_transitions = reconciler.run_until_idle(
                owner=f"benchmark-{case_id}-initial", limit=max_transitions
            )
            initial_materialization_seconds = time.perf_counter() - initial_materialization_start
            if runtime.active_epoch_id() != epoch:
                raise RuntimeError(f"initial epoch did not activate: {runtime.status()}")

            driver = OpenLoadDriver(
                publisher,
                raw_refs,
                case_id=case_id,
                microbatches=microbatches,
                rows_per_stream=rows_per_stream,
                publish_rate_hz=case.publish_rate_hz,
                start=start,
            )
            driver_thread = Thread(
                target=driver.run,
                name=f"benchmark-driver-{case_id}",
                daemon=True,
            )
            monitor_start = time.perf_counter()
            driver_thread.start()
            planned_total = 0
            transition_total = 0
            materializer_errors = 0
            max_pending_work_items = 0
            max_pending_components = 0
            last_progress_at = 0.0
            last_error: BaseException | None = None
            owner = f"benchmark-{case_id}-materializer"

            while True:
                planned = runtime.plan_data_changes()
                planned_total += planned
                transitioned = False
                try:
                    transitioned = reconciler.run_once(owner=owner)
                except Exception as error:
                    # A publication can legitimately race a snapshot in an
                    # open-load run. The durable work remains retryable; let
                    # the next loop observe the newer frontier.
                    materializer_errors += 1
                    last_error = error

                transition_total += int(transitioned)
                if transition_total > max_transitions:
                    raise RuntimeError(
                        f"materialization transitions exceeded --max-transitions={max_transitions}"
                    )
                now = time.perf_counter()
                should_sample = (
                    now - last_progress_at >= 0.05
                    or transitioned
                    or driver.done.is_set()
                )
                if should_sample:
                    status = runtime.status()
                    status_metrics = _status_metrics(status)
                    max_pending_work_items = max(
                        max_pending_work_items, status_metrics["pending_work_items"]
                    )
                    max_pending_components = max(
                        max_pending_components, status_metrics["pending_components"]
                    )
                    progress.append({
                        "case_id": case_id,
                        "repeat": case.repeat,
                        "elapsed_seconds": now - monitor_start,
                        "driver_done": driver.done.is_set(),
                        "driver_published_batches": driver.published_batches,
                        "planned_work_items": planned_total,
                        "transitions": transition_total,
                        **status_metrics,
                    })
                    last_progress_at = now
                    if driver.done.is_set() and _runtime_idle(runtime, status):
                        break
                if not transitioned and not planned:
                    time.sleep(0.001)

            driver_thread.join()
            if driver.error is not None:
                raise RuntimeError(f"open-load driver failed: {driver.error}") from driver.error
            if not driver.batches or driver.started_at is None or driver.finished_at is None:
                raise RuntimeError("open-load driver published no batches")
            if last_error is not None and materializer_errors:
                status = runtime.status()
                if status.get("failed_work"):
                    raise RuntimeError(
                        f"materializer reported failed work after {materializer_errors} errors: "
                        f"{last_error}"
                    )

            first_batch = driver.batches[0]
            last_batch = driver.batches[-1]
            materializer_finished_at = time.perf_counter()
            driver_scheduled_duration = last_batch.scheduled_at - first_batch.scheduled_at
            driver_publish_duration = last_batch.publish_finished_at - first_batch.publish_started_at
            end_to_end_seconds = materializer_finished_at - driver.started_at
            drain_seconds = materializer_finished_at - driver.finished_at

            with store._own_conn() as conn:
                output_rows = int(conn.execute(
                    """SELECT count(*) FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                       WHERE r.ref_uri IN (SELECT * FROM UNNEST(?)) AND NOT t.deleted""",
                    [list(plan.output_refs)],
                ).fetchone()[0])

            result.update({
                "topology_bindings": len(runtime.epoch_bindings(epoch)),
                "topology_components": summary.component_count,
                "topology_setup_seconds": topology_setup_seconds,
                "initial_publish_seconds": initial_publish_seconds,
                "initial_materialization_seconds": initial_materialization_seconds,
                "driver_scheduled_duration_seconds": driver_scheduled_duration,
                "driver_publish_duration_seconds": driver_publish_duration,
                "driver_publish_rate_hz": len(driver.batches) / driver_publish_duration if driver_publish_duration else None,
                "end_to_end_seconds": end_to_end_seconds,
                "drain_seconds": drain_seconds,
                "end_to_end_seconds_per_batch": end_to_end_seconds / len(driver.batches),
                "end_to_end_batches_per_second": len(driver.batches) / end_to_end_seconds if end_to_end_seconds else None,
                "drain_batches_per_second": len(driver.batches) / drain_seconds if drain_seconds > 0 else None,
                "driver_publish_rows": sum(batch.row_count for batch in driver.batches),
                "output_rows": output_rows,
                "planned_work_items": planned_total,
                "transitions": transition_total,
                "initial_transitions": initial_transitions,
                "max_pending_work_items": max_pending_work_items,
                "max_pending_components": max_pending_components,
                "materializer_errors": materializer_errors,
            })
            for batch in driver.batches:
                cycles.append({
                    "case_id": case_id,
                    "repeat": case.repeat,
                    "batch": batch.batch,
                    "input_streams": case.input_streams,
                    "app_input_width": case.app_input_width,
                    "app_output_width": case.app_output_width,
                    "publish_rate_hz": case.publish_rate_hz,
                    "app_count": case.app_count,
                    "chain_depth": case.chain_depth,
                    "scheduled_offset_seconds": batch.scheduled_at - driver.started_at,
                    "publish_start_offset_seconds": batch.publish_started_at - driver.started_at,
                    "publish_end_offset_seconds": batch.publish_finished_at - driver.started_at,
                    "publish_seconds": batch.publish_seconds,
                    "publish_lateness_seconds": batch.publish_lateness_seconds,
                    "row_count": batch.row_count,
                })
    except Exception as error:
        result.update({
            "status": "error",
            "error": f"{type(error).__name__}: {error}",
            "topology_bindings": len(plan.definitions),
            "topology_components": None,
        })
        # Keep the sweep useful when one large point exposes a limit, while
        # retaining the traceback in the terminal for immediate diagnosis.
        traceback.print_exc()
    finally:
        if driver is not None and not driver.done.is_set():
            driver.request_stop()
        if driver_thread is not None and driver_thread.is_alive():
            driver_thread.join()
        if reconciler is not None:
            reconciler.close()
        if store is not None:
            store.close()
    return result, cycles, progress


def _successful(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("status") == "ok"]


def _plot_results(output_dir: Path, rows: Sequence[dict[str, Any]]) -> list[str]:
    successful = _successful(rows)
    if not successful:
        return []
    os.environ.setdefault("MPLCONFIGDIR", str(output_dir / ".matplotlib"))
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figure_dir = output_dir / "figures"
    figure_dir.mkdir()
    dimensions = (
        ("input_streams", "Input streams"),
        ("app_input_width", "Requested app input width"),
        ("app_output_width", "App output width"),
        ("publish_rate_hz", "Publish rate (Hz; 0 = unpaced)"),
        ("app_count", "Apps per stage"),
        ("chain_depth", "Chain depth"),
    )
    paths: list[str] = []

    fig, axes = plt.subplots(2, 3, figsize=(16, 9), constrained_layout=True)
    for axis, (field, label) in zip(axes.flat, dimensions):
        grouped: dict[float, list[float]] = {}
        for row in successful:
            value = float(row[field])
            grouped.setdefault(value, []).append(float(row["end_to_end_seconds_per_batch"]))
        x_values = sorted(grouped)
        y_values = [sorted(grouped[value])[len(grouped[value]) // 2] for value in x_values]
        axis.plot(x_values, y_values, marker="o")
        axis.set_xlabel(label)
        axis.set_ylabel("Median end-to-end seconds / offered batch")
        axis.grid(True, alpha=0.3)
    scaling_path = figure_dir / "scaling_dimensions.png"
    fig.savefig(scaling_path, dpi=150)
    plt.close(fig)
    paths.append(str(scaling_path.relative_to(output_dir)))

    widths = sorted({int(row["app_input_width"]) for row in successful})
    streams = sorted({int(row["input_streams"]) for row in successful})
    matrix = []
    for width in widths:
        values = []
        for stream_count in streams:
            samples = [
                float(row["end_to_end_seconds_per_batch"])
                for row in successful
                if int(row["app_input_width"]) == width and int(row["input_streams"]) == stream_count
            ]
            values.append(
                sorted(samples)[len(samples) // 2] if samples else float("nan")
            )
        matrix.append(values)
    fig, axis = plt.subplots(figsize=(10, 6), constrained_layout=True)
    image = axis.imshow(matrix, aspect="auto", interpolation="nearest")
    axis.set_xticks(range(len(streams)), labels=[str(value) for value in streams])
    axis.set_yticks(range(len(widths)), labels=[str(value) for value in widths])
    axis.set_xlabel("Input streams")
    axis.set_ylabel("Requested app input width")
    axis.set_title("Median end-to-end seconds / offered batch")
    fig.colorbar(image, ax=axis, label="seconds")
    heatmap_path = figure_dir / "input_width_vs_streams.png"
    fig.savefig(heatmap_path, dpi=150)
    plt.close(fig)
    paths.append(str(heatmap_path.relative_to(output_dir)))

    fig, axis = plt.subplots(figsize=(10, 6), constrained_layout=True)
    for depth in sorted({int(row["chain_depth"]) for row in successful}):
        grouped: dict[float, list[float]] = {}
        for row in successful:
            if int(row["chain_depth"]) == depth:
                grouped.setdefault(float(row["publish_rate_hz"]), []).append(
                    float(row["max_pending_work_items"])
                )
        x_values = sorted(grouped)
        y_values = [sorted(grouped[value])[len(grouped[value]) // 2] for value in x_values]
        axis.plot(x_values, y_values, marker="o", label=f"chain depth {depth}")
    axis.set_xlabel("Requested offered rate (Hz; 0 = unpaced)")
    axis.set_ylabel("Median maximum pending work items")
    axis.set_title("Open-load backlog envelope")
    axis.grid(True, alpha=0.3)
    axis.legend()
    backlog_path = figure_dir / "backlog_envelope.png"
    fig.savefig(backlog_path, dpi=150)
    plt.close(fig)
    paths.append(str(backlog_path.relative_to(output_dir)))
    return paths


def _write_report(
    output_dir: Path,
    *,
    run_id: str,
    config: dict[str, Any],
    rows: Sequence[dict[str, Any]],
    figures: Sequence[str],
) -> None:
    successful = _successful(rows)
    failed = len(rows) - len(successful)
    lines = [
        "# Microbatch materialization benchmark",
        "",
        f"Run `{run_id}` generated on {config['started_at']}.",
        "",
        "This is an in-process DuckDB benchmark with an independent synthetic input driver. Each case offers "
        "raw-stream microbatches at the requested rate while a concurrent materializer plans, executes, and "
        "seals the topology epoch. The transformation body declares one constant row per configured output "
        "stream, so application computation is intentionally cheap.",
        "",
        "## Run summary",
        "",
        f"- Cases: {len(rows)} ({len(successful)} successful, {failed} failed)",
        f"- CSV summary: [results.csv](results.csv)",
        f"- Per-microbatch timings: [cycles.csv](cycles.csv)",
        f"- Materializer progress/backlog samples: [progress.csv](progress.csv)",
        f"- Configuration: [config.json](config.json)",
        "",
    ]
    if successful:
        fastest = min(successful, key=lambda row: float(row["end_to_end_seconds_per_batch"]))
        slowest = max(successful, key=lambda row: float(row["end_to_end_seconds_per_batch"]))
        lines.extend([
            "## Observed range",
            "",
            f"- Fastest median case: `{fastest['case_id']}` at "
            f"`{float(fastest['end_to_end_seconds_per_batch']):.6f}` end-to-end seconds/offered batch.",
            f"- Slowest median case: `{slowest['case_id']}` at "
            f"`{float(slowest['end_to_end_seconds_per_batch']):.6f}` end-to-end seconds/offered batch.",
            "",
        ])
    if failed:
        lines.extend(["## Failed cases", ""])
        for row in rows:
            if row.get("status") == "error":
                lines.append(f"- `{row['case_id']}`: {row.get('error', 'unknown error')}")
        lines.append("")
    if figures:
        lines.extend(["## Figures", ""])
        for figure in figures:
            title = Path(figure).stem.replace("_", " ").title()
            lines.extend([f"### {title}", "", f"![{title}]({figure})", ""])
    lines.extend([
        "## Reproduction",
        "",
        "```text",
        "uv run python benchmarks/microbatch_materialization.py "
        + " ".join(
            f"--{key.replace('_', '-')} {value}"
            for key, value in config["arguments"].items()
            if value is not None
        ),
        "```",
        "",
        "The CSVs contain the raw measurements for re-plotting or fitting a platform-overhead model. "
        "Compare `publish_rate_hz` with `driver_publish_rate_hz` and inspect the pending-work columns "
        "when looking for overload.",
        "",
    ])
    (output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-streams", default="1,8,32", help="input stream counts")
    parser.add_argument("--app-input-width", default="1,4", help="streams selected by each app")
    parser.add_argument("--app-output-width", default="1,4", help="output streams declared by each app")
    parser.add_argument("--publish-rate-hz", default="0,10", help="raw microbatch publish rates; 0 is unpaced")
    parser.add_argument("--app-count", default="1,4", help="apps per chain stage")
    parser.add_argument("--chain-depth", default="1,2", help="number of app stages")
    parser.add_argument("--microbatches", type=_positive_int, default=3)
    parser.add_argument("--rows-per-stream", type=_positive_int, default=1)
    parser.add_argument("--workers", type=_positive_int, default=2)
    parser.add_argument("--repeats", type=_positive_int, default=1)
    parser.add_argument("--max-cases", type=_positive_int, default=None, help="truncate the deterministic sweep")
    parser.add_argument("--max-transitions", type=_positive_int, default=1_000_000)
    parser.add_argument("--output-dir", type=Path, default=None, help="new output directory instead of benchmarks/<timestamp>")
    return parser


def main() -> None:
    parser = _parser()
    args = parser.parse_args()
    try:
        input_streams = _parse_values(args.input_streams, _positive_int, "input-streams")
        app_input_width = _parse_values(args.app_input_width, _positive_int, "app-input-width")
        app_output_width = _parse_values(args.app_output_width, _positive_int, "app-output-width")
        publish_rate_hz = _parse_values(args.publish_rate_hz, _nonnegative_float, "publish-rate-hz")
        app_count = _parse_values(args.app_count, _positive_int, "app-count")
        chain_depth = _parse_values(args.chain_depth, _positive_int, "chain-depth")
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))

    combinations = itertools.product(
        input_streams, app_input_width, app_output_width,
        publish_rate_hz, app_count, chain_depth,
    )
    cases: list[SweepCase] = []
    for values in combinations:
        for repeat in range(1, args.repeats + 1):
            if args.max_cases is not None and len(cases) >= args.max_cases:
                break
            cases.append(SweepCase(*values, repeat=repeat))
        if args.max_cases is not None and len(cases) >= args.max_cases:
            break
    if not cases:
        parser.error("the sweep contains no cases")

    output_dir = _unique_output_dir(args.output_dir)
    run_id = output_dir.name
    started_at = datetime.now(UTC).isoformat()
    arguments = {
        "input_streams": args.input_streams,
        "app_input_width": args.app_input_width,
        "app_output_width": args.app_output_width,
        "publish_rate_hz": args.publish_rate_hz,
        "app_count": args.app_count,
        "chain_depth": args.chain_depth,
        "microbatches": args.microbatches,
        "rows_per_stream": args.rows_per_stream,
        "workers": args.workers,
        "repeats": args.repeats,
        "max_cases": args.max_cases,
        "max_transitions": args.max_transitions,
    }
    config = {
        "started_at": started_at,
        "run_id": run_id,
        "python": sys.version,
        "platform": platform.platform(),
        "arguments": arguments,
        "case_count": len(cases),
        "cases": [asdict(case) for case in cases],
    }
    (output_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")

    results: list[dict[str, Any]] = []
    cycles: list[dict[str, Any]] = []
    progress: list[dict[str, Any]] = []
    for index, case in enumerate(cases):
        case_id = f"case_{index:05d}_r{case.repeat}"
        print(
            f"[{index + 1}/{len(cases)}] {case_id}: streams={case.input_streams} "
            f"input_width={case.app_input_width} output_width={case.app_output_width} "
            f"rate={case.publish_rate_hz:g} apps={case.app_count} chain={case.chain_depth}",
            flush=True,
        )
        result, case_cycles, case_progress = _run_case(
            case,
            case_id=case_id,
            run_id=run_id,
            workers=args.workers,
            microbatches=args.microbatches,
            rows_per_stream=args.rows_per_stream,
            max_transitions=args.max_transitions,
        )
        results.append(result)
        cycles.extend(case_cycles)
        progress.extend(case_progress)

    _write_csv(output_dir / "results.csv", results, RESULT_FIELDS)
    _write_csv(output_dir / "cycles.csv", cycles, CYCLE_FIELDS)
    _write_csv(output_dir / "progress.csv", progress, PROGRESS_FIELDS)
    figures = _plot_results(output_dir, results)
    _write_report(output_dir, run_id=run_id, config=config, rows=results, figures=figures)
    print(f"\nWrote benchmark artifacts to {output_dir}")


if __name__ == "__main__":
    main()
