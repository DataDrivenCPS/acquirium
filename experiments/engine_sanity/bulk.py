"""One step over many small inbox entries, as concurrent writers leave them.

``--writers`` entries per minute (one per writer, its own sensors) for
``--minutes``, with late samples now and then, all queued and then taken by
one step. Prints the step's timings and compares every output with a
from-scratch computation.

    python -m experiments.engine_sanity.bulk --minutes 60 [--profile]
"""
import argparse, json, random, sys, tempfile, time, cProfile, pstats
from datetime import datetime, timedelta, timezone
from pathlib import Path
from rdflib import URIRef
from acquirium.internals.intervals import NEG_INF, to_micros
from acquirium.Materialization.planner import Deployment
from acquirium.Storage.graph_registry import PLANT_GRAPH_URI
from experiments.engine_sanity.apps import Double, Ewma, PlantMean, RollingMean
from experiments.engine_sanity.run import open_world, point, compare, expected, raw
from experiments.engine_sanity.concurrency import rows_for
from acquirium.internals.intervals import from_micros
UTC = timezone.utc
p = argparse.ArgumentParser(); p.add_argument("--sensors", type=int, default=80); p.add_argument("--writers", type=int, default=16)
p.add_argument("--minutes", type=int, default=120); p.add_argument("--late", type=float, default=0.2); p.add_argument("--profile", action="store_true")
p.add_argument("--backend", default="duckdb"); p.add_argument("--dsn")
args = p.parse_args(); args.ontologies = False
root = Path(tempfile.mkdtemp(prefix="bulk-probe-"))
store, graphs, views = open_world(args, root)
start = (datetime.now(UTC) - timedelta(minutes=args.minutes + 10)).replace(second=0, microsecond=0)
graphs.insert_graph("\n".join(point(n) for n in range(args.sensors)), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI), effective_from="-infinity")
views.model_changed()
views.engine.deferred = True
for app in (Double, RollingMean, PlantMean):
    views.deploy(Deployment.from_class(app), start=NEG_INF)
views.deploy(Deployment.from_class(Ewma), start=to_micros(start))
report = views.step()
entries = 0
for m in range(args.minutes):
    minute = start + timedelta(minutes=m)
    for w in range(args.writers):
        mine = [n for n in range(args.sensors) if n % args.writers == w]
        views.engine.submit("readings", rows_for(mine, [minute], 0)); entries += 1
        rng = random.Random(m * 100 + w)
        if rng.random() < args.late:
            late = minute - timedelta(minutes=rng.randint(1, 60), seconds=rng.randint(1, 59))
            if late >= start:
                views.engine.submit("readings", rows_for([rng.choice(mine)], [late], 3)); entries += 1
t0 = time.perf_counter()
if args.profile:
    prof = cProfile.Profile(); prof.enable()
report = views.step()
if args.profile:
    prof.disable()
elapsed = time.perf_counter() - t0
r = views.evaluator.last_report
print(json.dumps({"entries": entries, "step_s": round(elapsed, 2), "engine": {k: round(v, 3) if isinstance(v, float) else v for k, v in report.timings.items()},
                  "eval": {k: round(v, 3) if isinstance(v, float) else v for k, v in r.timings.items()}, "calls": r.calls, "slabs": r.slabs, "points": r.points_changed}))
ewma_start = from_micros([c for c in views._log.changes() if c.name == "ewma"][-1].start)
print("mismatches", compare(store, views, expected(raw(store, args.sensors), {}, ewma_start)))
if args.profile:
    pstats.Stats(prof).sort_stats("cumulative").print_stats(35)
graphs.close(); store.close()
