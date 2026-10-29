"""Writers and steps at the same time, as in a server.

``--writers`` threads submit one-minute batches for their own sensors (with
late samples now and then) while another thread steps continuously. When
the writers are done the inbox is drained and every output is compared with a
from-scratch computation.

    python -m experiments.engine_sanity.concurrency --backend timescale --dsn ...
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from rdflib import URIRef

from acquirium.internals.intervals import NEG_INF, from_micros, to_micros
from acquirium.Materialization.planner import Deployment
from acquirium.Storage.graph_registry import PLANT_GRAPH_URI
from acquirium.Storage.publication.types import MUTATION_SCHEMA

from experiments.engine_sanity.apps import Double, Ewma, PlantMean, RollingMean
from experiments.engine_sanity.run import compare, expected, open_world, point, raw

import pyarrow as pa

UTC = timezone.utc


def rows_for(sensors, minutes, seed):
    rows = [(f"urn:ex/r{n}", m, 20.0 + ((n * 7 + int(m.timestamp()) // 7 + seed) % 50) / 10.0)
            for m in minutes for n in sensors]
    return pa.table({
        "operation": ["upsert"] * len(rows), "ref_uri": [r[0] for r in rows],
        "ts": pa.array([r[1] for r in rows], pa.timestamp("us", tz="UTC")),
        "numeric_value": pa.array([r[2] for r in rows], pa.float64()),
        "text_value": pa.array([None] * len(rows), pa.string()),
    }, schema=MUTATION_SCHEMA)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("duckdb", "timescale"), default="duckdb")
    parser.add_argument("--dsn")
    parser.add_argument("--sensors", type=int, default=40)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--minutes", type=int, default=240)
    parser.add_argument("--ontologies", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="engine-concurrency-"))
    store, graphs, views = open_world(args, root)
    start = (datetime.now(UTC) - timedelta(minutes=args.minutes + 10)).replace(second=0, microsecond=0)
    graphs.insert_graph("\n".join(point(n) for n in range(args.sensors)), format="nt",
                        graph_uri=URIRef(PLANT_GRAPH_URI), effective_from="-infinity")
    views.model_changed()
    views.engine.deferred = True
    for app in (Double, RollingMean, PlantMean):
        views.deploy(Deployment.from_class(app), start=NEG_INF)
    views.deploy(Deployment.from_class(Ewma), start=to_micros(start))
    done = threading.Event()
    steps, errors = [], []

    def stepper():
        while not done.is_set():
            try:
                t0 = time.perf_counter()
                report = views.step()
                if report is not None:
                    steps.append(time.perf_counter() - t0)
            except Exception as error:  # noqa: BLE001
                errors.append(repr(error))

    def writer(index):
        mine = [n for n in range(args.sensors) if n % args.writers == index]
        rng = random.Random(index)
        for m in range(args.minutes):
            minute = start + timedelta(minutes=m)
            views.engine.submit("readings", rows_for(mine, [minute], 0))
            if rng.random() < 0.2:
                late = minute - timedelta(minutes=rng.randint(1, 60), seconds=rng.randint(1, 59))
                if late >= start:
                    views.engine.submit("readings", rows_for([rng.choice(mine)], [late], 3))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(args.writers)]
    stepping = threading.Thread(target=stepper)
    t0 = time.perf_counter()
    stepping.start()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    written = time.perf_counter() - t0
    done.set()
    stepping.join()
    while views.engine.pending():
        views.step()
    views.step()
    ewma_start = from_micros([c for c in views._log.changes() if c.name == "ewma"][-1].start)
    want = expected(raw(store, args.sensors), {}, ewma_start)
    result = {"args": vars(args), "write_s": written, "steps": len(steps),
              "step_s_mean": sum(steps) / len(steps) if steps else None, "errors": errors[:5],
              "mismatches": compare(store, views, want), "failures": views.failures()}
    print(json.dumps(result, indent=2, default=str))
    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=2, default=str))
    graphs.close()
    store.close()
    return 0 if not any(result["mismatches"].values()) and not errors else 1


if __name__ == "__main__":
    sys.exit(main())
