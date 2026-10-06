"""RQ2: how do planning and maintenance cost grow with views and matches?

For every combination of plant replicas, deployed views and workers, a
fresh server loads a few hours of data, deploys the views (a backfill over
history), then takes one incremental burst. The event log gives the time
to quiescence, the transform seconds, rows read, and the replan cost
(query compilation and lineage publication) per binding count.

    python -m experiments.rq2_scalability --replicas 1,2 --views 1,3,6 --workers 1,2
"""
from __future__ import annotations

import argparse
import time

import polars as pl

from experiments import benicia, metrics, plots, views as V
from experiments.common import Run, derived_nodes, wait_quiescent


def _views(count: int) -> list[type]:
    return V.VIEWS[:count]


def one(replicas: int, view_count: int, workers: int, *, hours: int, burst: int, poll: float, seed: int) -> dict:
    run = Run("rq2", poll_seconds=poll, workers=workers)
    client = run.start()
    try:
        reps = [benicia.load_replica(i) for i in range(replicas)]
        frames = {}
        for replica in reps:
            benicia.install(client, replica)
            frames[replica.index] = benicia.generate(replica, hours * 60 + burst, seed=seed + replica.index)
            benicia.insert(client, replica, benicia.melt(frames[replica.index].head(hours * 60)))
        marker = len(run.events())
        started = time.monotonic()
        V.deploy(client, _views(view_count), parameters={"conc-high": {"threshold": 60.0}})
        backfill_seconds = wait_quiescent(client, timeout=1800) + (time.monotonic() - started)
        bindings = len(derived_nodes(client))
        backfill = run.events().slice(marker)
        marker = len(run.events())
        started = time.monotonic()
        for replica in reps:
            benicia.insert(client, replica, benicia.melt(frames[replica.index].slice(hours * 60, burst)))
        incremental_seconds = wait_quiescent(client, timeout=1800) + (time.monotonic() - started)
        incremental = run.events().slice(marker)
    finally:
        run.stop()
        run.discard_data()
    b, i = metrics.totals(backfill), metrics.totals(incremental)
    plan = metrics.plans(backfill)
    return {"replicas": replicas, "views": view_count, "workers": workers, "bindings": bindings,
            "streams": 100 * replicas,
            "backfill_seconds": backfill_seconds, "backfill_transform_seconds": b["transform_seconds"],
            "backfill_rows_read": b["rows_read"], "backfill_rows_written": b["rows_written"],
            "plan_compile_seconds": float(plan["compile_seconds"].sum()) if not plan.is_empty() else 0.0,
            "plan_lineage_seconds": float(plan["lineage_seconds"].sum()) if not plan.is_empty() else 0.0,
            "plans": plan.height,
            "incremental_seconds": incremental_seconds, "incremental_transform_seconds": i["transform_seconds"],
            "incremental_rows_read": i["rows_read"], "incremental_rows_written": i["rows_written"],
            "incremental_invocations": i["invocations"]}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replicas", default="1,2,4")
    parser.add_argument("--views", default="1,3,6")
    parser.add_argument("--workers", default="1,2")
    parser.add_argument("--hours", type=int, default=2)
    parser.add_argument("--burst", type=int, default=10, help="simulated minutes in the incremental burst")
    parser.add_argument("--poll", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args(argv)
    grid = [(int(r), int(v), int(w)) for r in args.replicas.split(",") for v in args.views.split(",")
            for w in args.workers.split(",")]

    summary = Run("rq2-summary")
    rows = []
    for replicas, view_count, workers in grid:
        print(f"replicas={replicas} views={view_count} workers={workers}")
        rows.append(one(replicas, view_count, workers, hours=args.hours, burst=args.burst, poll=args.poll, seed=args.seed))
        print(f"  bindings={rows[-1]['bindings']} backfill={rows[-1]['backfill_seconds']:.1f}s "
              f"incremental={rows[-1]['incremental_seconds']:.1f}s compile={rows[-1]['plan_compile_seconds']:.2f}s")
    table = pl.DataFrame(rows)
    summary.save("summary", table)
    summary.save("config.json", vars(args))
    print(table)

    fig, axes = plots.plt.subplots(1, 3, figsize=(9, 2.6))
    for workers in sorted(table["workers"].unique()):
        sub = table.filter(pl.col("workers") == workers).sort("bindings")
        axes[0].plot(sub["bindings"], sub["backfill_seconds"], "o-", label=f"{workers} worker(s)")
        axes[1].plot(sub["bindings"], sub["incremental_seconds"], "o-", label=f"{workers} worker(s)")
    axes[0].set_xlabel("bindings"); axes[0].set_ylabel("seconds to quiescence"); axes[0].set_title("backfill of history", fontsize=9)
    axes[1].set_xlabel("bindings"); axes[1].set_title(f"{args.burst}-minute burst", fontsize=9)
    axes[0].legend(frameon=False, fontsize=7)
    one_worker = table.filter(pl.col("workers") == table["workers"].min()).sort("bindings")
    axes[2].plot(one_worker["bindings"], one_worker["plan_compile_seconds"] / one_worker["plans"], "o-", label="query compilation")
    axes[2].plot(one_worker["bindings"], one_worker["plan_lineage_seconds"] / one_worker["plans"], "s-", label="lineage publication")
    axes[2].set_xlabel("bindings"); axes[2].set_ylabel("seconds per replan"); axes[2].set_title("replan cost", fontsize=9)
    axes[2].legend(frameon=False, fontsize=7)
    print("figure:", plots.save(fig, summary.directory / "rq2_scalability.png"))
    print("run directory:", summary.directory)


if __name__ == "__main__":
    main()
