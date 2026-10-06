"""RQ3: how long after a reading arrives is every dependent view updated?

Warm the server with history, deploy the six views, then replay paced data
with a few late arrivals. Latency is taken from the server's event log:
the commit that first consumed an ingest revision, chained through each
view's output revision for the four-deep chain.

    python -m experiments.rq3_latency --minutes 60 --rate 300
"""
from __future__ import annotations

import argparse

import polars as pl

from experiments import benicia, metrics, plots, views as V
from experiments.common import Run, wait_quiescent
from experiments.replay import Replay


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=60, help="paced simulated minutes to replay")
    parser.add_argument("--warmup", type=int, default=180, help="simulated minutes loaded before deployment")
    parser.add_argument("--rate", type=float, default=300.0, help="simulated seconds per wall second")
    parser.add_argument("--poll", type=float, default=0.1)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--late", type=float, default=0.02)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args(argv)

    run = Run("rq3", poll_seconds=args.poll, workers=args.workers)
    client = run.start()
    try:
        replica = benicia.load_replica(0)
        benicia.install(client, replica)
        wide = benicia.generate(replica, args.warmup + args.minutes, seed=args.seed)
        replay = Replay(client, replica, wide, rate=args.rate, late_fraction=args.late, seed=args.seed)
        replay.run(rows=args.warmup, pace=False)
        V.deploy(client, parameters={"conc-high": {"threshold": 60.0}})
        wait_quiescent(client, timeout=900)
        paced_from = len(run.events())
        replay.run(pace=True)
        wait_quiescent(client, timeout=900)
        events = run.events().slice(paced_from)
        run.save("sent", replay.sent_frame())
    finally:
        run.stop()
        run.discard_data()

    chain = metrics.latency(events, [v.name for v in V.CHAIN])
    singles = pl.concat([metrics.latency(events, [v.name]) for v in (V.FlowTotalFiveMinute, V.AcidityDailyRange)])
    table = pl.concat([chain, singles])
    run.save("latency", table)
    summary = (table.group_by("app", "depth").agg(
        pl.len().alias("n"), pl.col("latency_s").median().alias("p50"),
        pl.col("latency_s").quantile(0.95).alias("p95"), pl.col("latency_s").quantile(0.99).alias("p99"),
        pl.col("latency_s").max().alias("max")).sort("depth", "app"))
    run.save("summary", summary)
    run.save("config.json", vars(args))
    print(summary)

    fig, axes = plots.plt.subplots(1, 2, figsize=(7, 2.6))
    for depth in sorted(chain["depth"].unique()):
        app = chain.filter(pl.col("depth") == depth)["app"][0]
        plots.cdf(axes[0], chain.filter(pl.col("depth") == depth)["latency_s"], f"depth {depth}: {app}")
    axes[0].set_xlabel("arrival to visibility (s)"); axes[0].set_ylabel("CDF"); axes[0].legend(frameon=False, fontsize=7)
    axes[0].set_title("four-deep chain", fontsize=9)
    for app in singles["app"].unique().sort():
        plots.cdf(axes[1], singles.filter(pl.col("app") == app)["latency_s"], app)
    plots.cdf(axes[1], chain.filter(pl.col("depth") == 1)["latency_s"], V.ConcentrationFiveMinute.name)
    axes[1].set_xlabel("arrival to visibility (s)"); axes[1].legend(frameon=False, fontsize=7)
    axes[1].set_title("single views", fontsize=9)
    print("figure:", plots.save(fig, run.directory / "rq3_latency_cdf.png"))
    print("run directory:", run.directory)


if __name__ == "__main__":
    main()
