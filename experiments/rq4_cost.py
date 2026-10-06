"""RQ4: what does incremental maintenance cost against recomputation?

One replay of Benicia data with late arrivals and corrections, one graph
change (a decommissioned sensor) and one dataflow change (a re-parameterized
alarm view with reprocessing). The incremental runtime's cost comes from
the event log. Two recomputation baselines run the same six views with the
oracle's from-scratch code over the canonical data: ``naive`` after every
ingest batch (sampled every ``--naive-every`` batches and scaled up), and
``periodic`` once per simulated hour.

Feldera and Flink baselines are not part of this script; see
paper/EXPERIMENTS_PLAN.md, RQ4.

    python -m experiments.rq4_cost --hours 4
"""
from __future__ import annotations

import argparse
import random
import time
from datetime import timedelta

import polars as pl

from experiments import benicia, changes, metrics, plots, views as V
from experiments.common import Run, wait_quiescent
from experiments.oracle import World, compare, expected
from experiments.replay import Replay


def recompute_cost(world: World, truth: dict[str, pl.DataFrame]) -> dict[str, float]:
    """CPU seconds, rows read and rows written for one from-scratch recomputation."""
    started = time.process_time()
    out = expected(world, truth)
    seconds = time.process_time() - started
    return {"cpu_seconds": seconds, "rows_read": float(sum(f.height for f in truth.values())),
            "rows_written": float(sum(f.height for f in out.values()))}


def attribute_by_kind(events: pl.DataFrame) -> pl.DataFrame:
    """Split transform seconds over the kinds of ingest each invocation consumed."""
    ingests = events.filter(pl.col("kind") == "ingest").select(
        "revision", pl.col("publication_id").str.split(":").list.first().alias("change"))
    invocations = events.filter(pl.col("kind") == "invocation").select("from_revision", "to_revision", "seconds")
    rows = []
    for inv in invocations.iter_rows(named=True):
        consumed = ingests.filter((pl.col("revision") > inv["from_revision"]) & (pl.col("revision") <= inv["to_revision"]))
        if consumed.is_empty():
            rows.append({"change": "repair", "seconds": inv["seconds"]})
            continue
        share = inv["seconds"] / consumed.height
        rows.extend({"change": change, "seconds": share} for change in consumed["change"])
    if not rows:
        return pl.DataFrame({"change": [], "seconds": []})
    seconds = pl.DataFrame(rows).group_by("change").agg(pl.col("seconds").sum())
    counts = ingests.group_by("change").agg(pl.len().alias("ingests"))
    return seconds.join(counts, on="change", how="left").sort("change")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=4)
    parser.add_argument("--late", type=float, default=0.05)
    parser.add_argument("--corrections", type=float, default=0.01)
    parser.add_argument("--naive-every", type=int, default=20, help="recompute the naive baseline every N batches")
    parser.add_argument("--periodic-minutes", type=int, default=60)
    parser.add_argument("--poll", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args(argv)
    rng = random.Random(args.seed)

    run = Run("rq4", poll_seconds=args.poll)
    client = run.start()
    naive = {"cpu_seconds": 0.0, "rows_read": 0.0, "rows_written": 0.0, "samples": 0}
    periodic = {"cpu_seconds": 0.0, "rows_read": 0.0, "rows_written": 0.0, "samples": 0}
    try:
        replica = benicia.load_replica(0)
        benicia.install(client, replica)
        rows_total = args.hours * 60
        wide = benicia.generate(replica, rows_total, seed=args.seed)
        replay = Replay(client, replica, wide, late_fraction=args.late, correction_fraction=args.corrections, seed=args.seed)
        world = World({replica.ref_uri(p.ref_name): p for p in replica.points})
        for view in V.VIEWS:
            changes.deploy_view(client, world, view, {"threshold": 60.0} if view is V.ConcentrationHigh else None)
        wait_quiescent(client, timeout=600)
        # Deployment and the empty-server replans are setup, not maintenance.
        marker = len(run.events())
        graph_change_at, dataflow_change_at = rows_total // 2, (2 * rows_total) // 3
        batches = 0
        for row in range(1, rows_total + 1):
            replay.run(rows=row, pace=False)
            batches += 1
            if row == graph_change_at:
                ref = rng.choice([r for r in world.refs_with(V.CONCENTRATION) if r in replay.truth])
                changes.remove_point(client, world, ref)
                print(f"row {row}: removed {ref.rsplit('/', 1)[-1]}")
            if row == dataflow_change_at:
                times = sorted(next(iter(replay.truth.values())))
                changes.deploy_view(client, world, V.ConcentrationHigh, {"threshold": 75.0})
                changes.reprocess(client, V.ConcentrationHigh, times[0] - timedelta(days=1), times[-1] + timedelta(days=1))
                print(f"row {row}: re-parameterized {V.ConcentrationHigh.name}")
            if batches % args.naive_every == 0:
                cost = recompute_cost(world, replay.truth_frames())
                for key in ("cpu_seconds", "rows_read", "rows_written"):
                    naive[key] += cost[key] * args.naive_every
                naive["samples"] += 1
            if row % args.periodic_minutes == 0:
                cost = recompute_cost(world, replay.truth_frames())
                for key in ("cpu_seconds", "rows_read", "rows_written"):
                    periodic[key] += cost[key]
                periodic["samples"] += 1
        replay.flush_late(wide["timestamp"][-1])
        wait_quiescent(client, timeout=900)
        problems = compare(client, expected(world, replay.truth_frames()))
        events = run.events().slice(marker)
        run.save("sent", replay.sent_frame())
    finally:
        run.stop()
        run.discard_data()

    ours = metrics.totals(events)
    per_app = metrics.cost(events)
    by_kind = attribute_by_kind(events)
    table = pl.DataFrame([
        {"system": "incremental views", "cpu_seconds": ours["transform_seconds"] + ours["plan_seconds"],
         "transform_seconds": ours["transform_seconds"], "plan_seconds": ours["plan_seconds"],
         "rows_read": ours["rows_read"], "rows_written": ours["rows_written"]},
        {"system": f"periodic ({args.periodic_minutes} min)", "cpu_seconds": periodic["cpu_seconds"],
         "transform_seconds": periodic["cpu_seconds"], "plan_seconds": 0.0,
         "rows_read": periodic["rows_read"], "rows_written": periodic["rows_written"]},
        {"system": "naive recompute", "cpu_seconds": naive["cpu_seconds"], "transform_seconds": naive["cpu_seconds"],
         "plan_seconds": 0.0, "rows_read": naive["rows_read"], "rows_written": naive["rows_written"]},
    ])
    run.save("summary", table); run.save("per_view", per_app); run.save("by_change_kind", by_kind)
    run.save("config.json", {**vars(args), "oracle_problems": problems, "ingest_batches": batches,
                             "naive_samples": naive["samples"], "periodic_samples": periodic["samples"]})
    print(table); print(by_kind)
    if problems:
        print(f"WARNING: {len(problems)} oracle mismatches; see config.json")

    fig, axes = plots.plt.subplots(1, 3, figsize=(9, 2.6))
    for ax, column, title in zip(axes, ["cpu_seconds", "rows_read", "rows_written"],
                                 ["CPU seconds", "rows read", "rows written"]):
        ax.bar(table["system"], table[column], color=["#4c72b0", "#dd8452", "#c44e52"])
        ax.set_yscale("log"); ax.set_title(title, fontsize=9); ax.tick_params(axis="x", labelsize=7, rotation=15)
    print("figure:", plots.save(fig, run.directory / "rq4_cost.png"))
    print("run directory:", run.directory)


if __name__ == "__main__":
    main()
