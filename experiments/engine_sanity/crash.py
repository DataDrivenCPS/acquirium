"""Kill the process in the middle of steps, then check what a restart recovers.

The parent starts a child that loads a plant, deploys the four sanity views
over all history and streams one-minute batches through the inbox, stepping
in the background. The parent kills the child with SIGKILL at a random moment,
repeats that ``--kills`` times on the same database and graph store, then
opens them once more, steps until the inbox is empty, and compares every
output with a from-scratch computation.

    python -m experiments.engine_sanity.crash --backend duckdb --kills 4
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from rdflib import URIRef

from acquirium.internals.intervals import NEG_INF, from_micros
from acquirium.Materialization.planner import Deployment
from acquirium.Storage.graph_registry import PLANT_GRAPH_URI

from experiments.engine_sanity.apps import Double, Ewma, PlantMean, RollingMean
from experiments.engine_sanity.run import batch, compare, expected, open_world, point, raw

UTC = timezone.utc


def child(args) -> None:
    root = Path(args.root)
    store, graphs, views = open_world(args, root, recreate=args.fresh)
    start = datetime.fromisoformat(args.start)
    if args.fresh:
        graphs.insert_graph("\n".join(point(n) for n in range(args.sensors)), format="nt",
                            graph_uri=URIRef(PLANT_GRAPH_URI), effective_from="-infinity")
        views.model_changed()
        views.engine.submit("readings", batch(args.sensors, [start + timedelta(minutes=m) for m in range(60)], 0))
        views.engine.deferred = True
        for app in (Double, RollingMean, PlantMean, Ewma):
            views.deploy(Deployment.from_class(app), start=NEG_INF)
    views.engine.deferred = True
    # Stream minutes after the ones already written, stepping every second batch.
    with store._own_conn() as conn:
        from acquirium.Storage.metadata_outbox import execute
        last = execute(conn, "SELECT max(t.ts) FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
                             "WHERE r.ref_uri = 'urn:ex/r0'").fetchone()[0]
    pending = views.engine.pending()
    print(json.dumps({"event": "started", "last": str(last), "pending": pending}), flush=True)
    minute = (last.replace(tzinfo=UTC) if last else start) + timedelta(minutes=1)
    rng = random.Random(os.getpid())
    for i in range(10_000):
        if minute >= datetime.now(UTC) - timedelta(minutes=1):
            break
        views.engine.submit("readings", batch(args.sensors, [minute], 0))
        if rng.random() < 0.3:
            # a late sample, somewhere in the past
            views.engine.submit("readings", batch(1, [minute - timedelta(minutes=rng.randint(1, 30), seconds=20)], 7))
        minute += timedelta(minutes=1)
        if i % 2:
            views.step()
    views.step()
    print(json.dumps({"event": "finished"}), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("duckdb", "timescale"), default="duckdb")
    parser.add_argument("--dsn")
    parser.add_argument("--sensors", type=int, default=20)
    parser.add_argument("--hours", type=float, default=4)
    parser.add_argument("--kills", type=int, default=4)
    parser.add_argument("--ontologies", action="store_true")
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--fresh", action="store_true")
    parser.add_argument("--root")
    parser.add_argument("--start")
    parser.add_argument("--out")
    args = parser.parse_args()
    if args.child:
        child(args)
        return 0
    root = Path(tempfile.mkdtemp(prefix="engine-crash-"))
    start = (datetime.now(UTC) - timedelta(hours=args.hours)).replace(second=0, microsecond=0)
    base = [sys.executable, "-m", "experiments.engine_sanity.crash", "--child", "--backend", args.backend,
            "--sensors", str(args.sensors), "--root", str(root), "--start", start.isoformat()]
    if args.dsn:
        base += ["--dsn", args.dsn]
    rng = random.Random(3)
    log = []
    for kill in range(args.kills):
        command = base + (["--fresh"] if kill == 0 else [])
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        # let it get going, then kill it at a random moment
        time.sleep(rng.uniform(8, 20) if kill == 0 else rng.uniform(3, 10))
        process.send_signal(signal.SIGKILL)
        output = process.communicate()[0]
        log.append({"kill": kill, "returncode": process.returncode, "tail": output.strip().splitlines()[-3:]})
        print(json.dumps(log[-1]), flush=True)
    # a final process that runs to the end
    final = subprocess.run(base, capture_output=True, text=True)
    log.append({"final": final.returncode, "tail": final.stdout.strip().splitlines()[-3:] + final.stderr.strip().splitlines()[-5:]})
    print(json.dumps(log[-1]), flush=True)
    store, graphs, views = open_world(args, root, recreate=False)
    views.step()
    ewma_start = from_micros([c for c in views._log.changes() if c.name == "ewma"][-1].start)
    want = expected(raw(store, args.sensors), {}, ewma_start)
    result = {"args": vars(args), "log": log, "pending": views.engine.pending(),
              "mismatches": compare(store, views, want), "failures": views.failures(),
              "steps": views.engine.last_step()}
    print(json.dumps(result, indent=2, default=str))
    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=2, default=str))
    graphs.close()
    store.close()
    return 0 if not any(result["mismatches"].values()) and result["pending"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
