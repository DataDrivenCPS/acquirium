"""Planning cost as the model gains versions.

A plant of ``--sensors`` points loaded over all time, four views deployed
(two per match, one selecting generated outputs, one loop). Then
``--writes`` backdated model writes, each removing one random sensor over a
random hour in the past; each write is planned and admitted before it holds
(``model_changed``), and a step follows. Reports the planning time per write
against the number of model versions, and the step time.

    python -m experiments.engine_sanity.planning --sensors 50 --writes 40
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from rdflib import URIRef

from acquirium.internals.intervals import NEG_INF
from acquirium.Materialization.planner import Deployment
from acquirium.Storage.graph_registry import PLANT_GRAPH_URI

from experiments.engine_sanity.apps import Double, Ewma, PlantMean, RollingMean
from experiments.engine_sanity.run import batch, open_world, point

UTC = timezone.utc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("duckdb", "timescale"), default="duckdb")
    parser.add_argument("--dsn")
    parser.add_argument("--sensors", type=int, default=50)
    parser.add_argument("--hours", type=float, default=6)
    parser.add_argument("--writes", type=int, default=40)
    parser.add_argument("--ontologies", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="engine-planning-"))
    store, graphs, views = open_world(args, root)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    start = now - timedelta(minutes=int(args.hours * 60) + 5)
    minutes = [start + timedelta(minutes=m) for m in range(int(args.hours * 60))]
    graphs.insert_graph("\n".join(point(n) for n in range(args.sensors)), format="nt",
                        graph_uri=URIRef(PLANT_GRAPH_URI), effective_from="-infinity")
    views.model_changed()
    for hour in range(0, len(minutes), 60):
        views.engine.submit("readings", batch(args.sensors, minutes[hour:hour + 60], 0))
    views.engine.deferred = True
    for app in (Double, RollingMean, PlantMean, Ewma):
        views.deploy(Deployment.from_class(app), start=NEG_INF)
    views.step()
    rng = random.Random(1)
    rows = []
    for write in range(args.writes):
        sensor = rng.randrange(args.sensors)
        first = rng.randrange(0, len(minutes) - 60)
        gap = (minutes[first], minutes[first] + timedelta(minutes=rng.randint(5, 60)))
        t0 = time.perf_counter()
        graphs.sparql_update(f"DELETE DATA {{ {point(sensor)} }}", graph_uri=URIRef(PLANT_GRAPH_URI),
                             effective_from=gap[0], effective_until=gap[1])
        recorded = time.perf_counter() - t0
        t0 = time.perf_counter()
        views.model_changed()
        planned = time.perf_counter() - t0
        t0 = time.perf_counter()
        views.step()
        stepped = time.perf_counter() - t0
        report = views.evaluator.last_report
        rows.append({"write": write, "versions": len(graphs.model_versions()), "record_s": recorded,
                     "plan_s": planned, "step_s": stepped, "calls": report.calls, "slabs": report.slabs,
                     "points_changed": report.points_changed})
        print(json.dumps(rows[-1]), flush=True)
    result = {"args": vars(args), "writes": rows, "failures": views.failures()}
    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=2, default=str))
    graphs.close()
    store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
