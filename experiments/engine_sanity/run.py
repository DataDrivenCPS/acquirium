"""Sanity and performance run for the time-aware engine.

A synthetic plant of ``--sensors`` temperature points, one sample per minute
each. Four views: ``double`` (per match), ``rolling-mean`` (per match, 10 min
lookback), ``plant-mean`` (all matches over double's generated outputs) and
``ewma`` (a loop on its own named output, one-minute delay, over the plant
mean). The run:

1. loads ``--hours`` of history, then deploys every view over all history and
   times the first step, which computes everything;
2. streams ``--live`` one-minute batches, one step each;
3. writes a late sample hours in the past (it enters the loop);
4. removes one sensor from the model over an hour in the past (a backdated
   model change);
5. compares every stored output with a from-scratch computation in polars.

    python -m experiments.engine_sanity.run --backend duckdb --sensors 50 --hours 6 --live 30
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa
from rdflib import Graph, URIRef

import acquirium.Storage.graph_store as graph_store_module
from acquirium.internals.intervals import NEG_INF, from_micros, to_micros
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.graph_registry import PLANT_GRAPH_URI
from acquirium.Storage.metadata_outbox import execute
from acquirium.Storage.model_history import ModelHistory
from acquirium.Storage.publication.types import MUTATION_SCHEMA

from experiments.engine_sanity.apps import Double, Ewma, PlantMean, RollingMean, TEMPERATURE

UTC = timezone.utc
REF = "https://brickschema.org/schema/Brick/ref#hasExternalReference"
QK = "http://qudt.org/schema/qudt/hasQuantityKind"


class _NoOntologies:
    def __init__(self, *args, **kwargs):
        self.add = MagicMock()
        self.close = MagicMock()

    @classmethod
    def connect(cls, *args, **kwargs):
        return cls()

    def import_dependencies(self, graph):
        return None


def open_world(args, root: Path):
    if not args.ontologies:
        graph_store_module.OntoEnv = _NoOntologies
        graph_store_module.load_bundled_graph = lambda fname, canonical: Graph()
    if args.backend == "timescale":
        from acquirium.Storage.timescale_store import TimescaleStore
        store = TimescaleStore(dsn=args.dsn, recreate=True)
    else:
        from acquirium.Storage.duckdb_store import DuckDBStore
        store = DuckDBStore(root / "ts.duckdb")
    graphs = graph_store_module.OxigraphGraphStore(store_path=root / "graph", env_root=root / "env",
                                                   history=ModelHistory(store))
    return store, graphs, Materializer(store, graphs)


def point(n: int) -> str:
    return (f"<urn:ex/p{n}> <{REF}> <urn:ex/r{n}> .\n"
            f"<urn:ex/p{n}> <{QK}> <{TEMPERATURE}> .")


def batch(sensors: int, minutes: list[datetime], seed: int) -> pa.Table:
    rows = [(f"urn:ex/r{n}", m, 20.0 + ((n * 7 + int(m.timestamp() // 60) * 13 + seed) % 50) / 10.0)
            for m in minutes for n in range(sensors)]
    return pa.table({
        "operation": ["upsert"] * len(rows), "ref_uri": [r[0] for r in rows],
        "ts": pa.array([r[1] for r in rows], pa.timestamp("us", tz="UTC")),
        "numeric_value": pa.array([r[2] for r in rows], pa.float64()),
        "text_value": pa.array([None] * len(rows), pa.string()),
    }, schema=MUTATION_SCHEMA)


def summary(times: list[float]) -> dict:
    if not times:
        return {}
    ordered = sorted(times)
    return {"n": len(times), "mean": statistics.mean(times), "p50": ordered[len(ordered) // 2],
            "p95": ordered[int(len(ordered) * 0.95) - 1 if len(ordered) > 1 else 0], "max": ordered[-1]}


def stored(store) -> dict[str, pl.DataFrame]:
    with store._own_conn() as conn:
        rows = execute(conn,
            "SELECT s.source_id, s.ref_uri, t.ts, t.numeric_value FROM timeseries t "
            "JOIN ref_ids r ON r.ref_id = t.ref_id JOIN streams s ON s.ref_uri = r.ref_uri "
            "WHERE s.source_id LIKE 'derived:%' AND NOT t.deleted").fetchall()
    frame = pl.DataFrame({"view": [r[0].removeprefix("derived:") for r in rows], "ref": [r[1] for r in rows],
                          "ts": [r[2].replace(tzinfo=UTC) for r in rows], "value": [r[3] for r in rows]},
                         schema={"view": pl.String, "ref": pl.String, "ts": pl.Datetime("us", "UTC"), "value": pl.Float64})
    return {view: frame.filter(pl.col("view") == view) for view in frame["view"].unique().to_list()}


def raw(store, sensors: int) -> pl.DataFrame:
    with store._own_conn() as conn:
        rows = execute(conn,
            "SELECT r.ref_uri, t.ts, t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
            "WHERE r.ref_uri LIKE 'urn:ex/r%' AND NOT t.deleted").fetchall()
    return pl.DataFrame({"ref": [r[0] for r in rows], "ts": [r[1].replace(tzinfo=UTC) for r in rows],
                         "value": [r[2] for r in rows]},
                        schema={"ref": pl.String, "ts": pl.Datetime("us", "UTC"), "value": pl.Float64})


def expected(data: pl.DataFrame, gaps: dict[str, tuple[datetime, datetime]], ewma_start: datetime) -> dict[str, dict]:
    """Every output from scratch. ``gaps`` holds the interval each sensor is
    out of the model; outside it every sensor is in the model all the time."""
    def present(ref, ts):
        gap = gaps.get(ref)
        return gap is None or not (gap[0] <= ts < gap[1])

    double, rolling = {}, {}
    plant: dict[datetime, list[float]] = {}
    for (ref,), group in data.sort("ts").group_by(["ref"], maintain_order=True):
        samples = list(zip(group["ts"].to_list(), group["value"].to_list()))
        gap = gaps.get(ref)
        for ts, value in samples:
            if not present(ref, ts):
                continue
            double[(ref, ts)] = 2 * value
            plant.setdefault(ts, []).append(2 * value)
            # the run containing ts starts after the gap if ts is after it
            run_start = gap[1] if gap is not None and ts >= gap[1] else datetime.min.replace(tzinfo=UTC)
            window = [v for t, v in samples if ts - timedelta(minutes=10) < t <= ts and t >= run_start and present(ref, t)]
            rolling[(ref, ts)] = sum(window) / len(window)
    mean = {ts: sum(values) / len(values) for ts, values in plant.items()}
    ewma, previous = {}, {}
    for ts in sorted(mean):
        if ts < ewma_start:
            continue
        x = mean[ts]
        y = 0.3 * x + 0.7 * previous.get(ts - timedelta(minutes=1), x)
        previous[ts] = y
        ewma[ts] = y
    return {"double": double, "rolling-mean": rolling, "plant-mean": mean, "ewma": ewma}


def compare(store, views, want: dict[str, dict], tolerance: float = 1e-9) -> dict[str, int]:
    got = stored(store)
    plan = views.current_plan()[0]
    ref_of_output = {}
    for instance in plan.instances:
        for port in instance.outputs.values():
            for row in instance.rows:
                for descriptors in row.streams.values():
                    for d in descriptors:
                        if d.ref_uri.startswith("urn:ex/r"):
                            ref_of_output[port.ref_uri] = d.ref_uri
    mismatches = {}
    for view in want:
        frame = got.get(view, pl.DataFrame({"ref": [], "ts": [], "value": []}))
        have = {}
        for ref, ts, value in zip(frame["ref"].to_list(), frame["ts"].to_list(), frame["value"].to_list()):
            key = (ref_of_output.get(ref, ref), ts) if view in ("double", "rolling-mean") else ts
            have[key] = value
        need = want[view]
        bad = sum(1 for key in set(have) | set(need)
                  if key not in have or key not in need or abs(have[key] - need[key]) > tolerance)
        mismatches[view] = bad
    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("duckdb", "timescale"), default="duckdb")
    parser.add_argument("--dsn")
    parser.add_argument("--sensors", type=int, default=50)
    parser.add_argument("--hours", type=float, default=6)
    parser.add_argument("--live", type=int, default=30)
    parser.add_argument("--ontologies", action="store_true", help="load the real ontologies (inference per version)")
    parser.add_argument("--out")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="engine-sanity-"))
    store, graphs, views = open_world(args, root)
    result: dict = {"args": vars(args)}
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    start = now - timedelta(minutes=int(args.hours * 60) + args.live + 5)
    history = [start + timedelta(minutes=m) for m in range(int(args.hours * 60))]

    t0 = time.perf_counter()
    graphs.insert_graph("\n".join(point(n) for n in range(args.sensors)), format="nt",
                        graph_uri=URIRef(PLANT_GRAPH_URI), effective_from="-infinity")
    views.model_changed()
    result["model_load_s"] = time.perf_counter() - t0
    t0 = time.perf_counter()
    for hour in range(0, len(history), 60):
        views.engine.submit("readings", batch(args.sensors, history[hour:hour + 60], 0))
    result["history_load_s"] = time.perf_counter() - t0
    result["history_rows"] = len(history) * args.sensors

    t0 = time.perf_counter()
    views.engine.deferred = True
    for app in (Double, RollingMean, PlantMean):
        views.deploy(Deployment.from_class(app), start=NEG_INF)
    views.deploy(Deployment.from_class(Ewma), start=NEG_INF)
    result["deploy_s"] = time.perf_counter() - t0
    ewma_start = from_micros([c for c in views._log.changes() if c.name == "ewma"][-1].start)
    t0 = time.perf_counter()
    report = views.step()
    result["initial_step_s"] = time.perf_counter() - t0
    result["initial_step"] = {"timings": dict(report.timings) if report else {},
                              "calls": views.evaluator.last_report.calls,
                              "slabs": views.evaluator.last_report.slabs,
                              "points_changed": views.evaluator.last_report.points_changed}

    live_times, live_phases = [], []
    for m in range(args.live):
        minute = start + timedelta(minutes=len(history) + m)
        views.engine.submit("readings", batch(args.sensors, [minute], 0))
        t0 = time.perf_counter()
        report = views.step()
        live_times.append(time.perf_counter() - t0)
        live_phases.append(dict(report.timings) if report else {})
    result["live_step_s"] = summary(live_times)
    result["live_phases_mean"] = {k: statistics.mean(p.get(k, 0.0) for p in live_phases) for k in live_phases[0]} if live_phases else {}

    late = start + timedelta(minutes=len(history) // 2 + 3, seconds=30)
    views.engine.submit("readings", batch(1, [late], 99))
    t0 = time.perf_counter()
    views.step()
    result["late_sample_step_s"] = time.perf_counter() - t0
    result["late_sample"] = {"calls": views.evaluator.last_report.calls, "slabs": views.evaluator.last_report.slabs,
                             "points_changed": views.evaluator.last_report.points_changed}

    length = max(1, min(60, len(history) // 3))
    gap = (start + timedelta(minutes=len(history) // 3), start + timedelta(minutes=len(history) // 3 + length))
    t0 = time.perf_counter()
    graphs.sparql_update(f"DELETE DATA {{ {point(0)} }}", graph_uri=URIRef(PLANT_GRAPH_URI),
                         effective_from=gap[0], effective_until=gap[1])
    views.model_changed()
    result["backdated_model_plan_s"] = time.perf_counter() - t0
    t0 = time.perf_counter()
    views.step()
    result["backdated_model_step_s"] = time.perf_counter() - t0
    result["backdated_model"] = {"calls": views.evaluator.last_report.calls, "slabs": views.evaluator.last_report.slabs,
                                 "points_changed": views.evaluator.last_report.points_changed}

    t0 = time.perf_counter()
    want = expected(raw(store, args.sensors), {"urn:ex/r0": gap}, ewma_start)
    result["mismatches"] = compare(store, views, want)
    result["reference_s"] = time.perf_counter() - t0
    result["failures"] = views.failures()
    text = json.dumps(result, indent=2, default=str)
    print(text)
    if args.out:
        Path(args.out).write_text(text)
    graphs.close()
    store.close()
    return 0 if not any(result["mismatches"].values()) and not result["failures"] else 1


if __name__ == "__main__":
    sys.exit(main())
