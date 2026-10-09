"""RQ4 baselines: the same replay and the same six views on four systems, one at a time.

Every system receives the identical sequence of batches (fresh readings,
late arrivals, corrections) from the replay driver, builds the six views
in its own SQL, and is read back into the oracle's shape. Feldera runs as
it would be deployed: base rows in TimescaleDB, view deltas applied back to
TimescaleDB tables by a writer process, views read from TimescaleDB. Cost is CPU
seconds from the container's cgroup (for the incremental runtime: the
server process plus its share of the TimescaleDB container), wall time of
the replay, and output rows. All
systems run at per-batch cadence: TimescaleDB refreshes its aggregates
after every batch, Feldera and Flink are continuous, and the incremental
runtime polls at 0.1 s.

    python -m experiments.rq4_baselines --hours 3 --systems acquirium,timescale,feldera,flink
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import polars as pl

from experiments import benicia, metrics, plots, views as V
from experiments.baselines import sinks
from experiments.baselines.sinks import (EMPTY, VIEWS, PER_STREAM, FelderaPipelineSink, FelderaSink, FlinkSink,
                                         TimescaleSink)
from experiments.common import BACKEND, TIMESCALE_CONTAINER, Run, wait_quiescent
from experiments.oracle import World, _same, compare, expected
from experiments.replay import Replay

BASELINES = Path(__file__).resolve().parent / "baselines"
KIND_OF = {V.CONCENTRATION: "conc", V.FLOW: "flow", V.ACIDITY: "ph"}


def kinds_of(replica: benicia.Replica) -> dict[str, str]:
    return {point.ref_name: KIND_OF.get(point.quantity_kind, "other") for point in replica.points}


def expected_views(truth: dict[str, pl.DataFrame], kinds: dict[str, str]) -> dict[str, dict[str | None, pl.DataFrame]]:
    """The six views from the canonical data, keyed like the sinks read them back."""
    conc = {s: f for s, f in truth.items() if kinds.get(s) == "conc"}
    v1 = {s: V.ConcentrationFiveMinute.oracle(f) for s, f in conc.items()}
    v2 = {s: V.ConcentrationRollingHour.oracle(f) for s, f in v1.items()}
    v3 = {s: V.ConcentrationHigh.oracle(f, sinks.THRESHOLD) for s, f in v2.items()}
    return {"v1": v1, "v2": v2, "v3": {s: f for s, f in v3.items() if not f.is_empty()},
            "v4": {None: V.AlarmCountHour.oracle(list(v3.values()))},
            "v5": {None: V.FlowTotalFiveMinute.oracle([f for s, f in truth.items() if kinds.get(s) == "flow"])},
            "v6": {s: V.AcidityDailyRange.oracle(f) for s, f in truth.items() if kinds.get(s) == "ph"}}


def save_outputs(run: Run, system: str, want: dict, got: dict) -> None:
    """Keep every view's rows, expected and actual, for inspection after the run."""
    for label, tables in (("expected", want), ("actual", got)):
        frames = []
        for view, by_key in tables.items():
            for key, frame in by_key.items():
                if frame.is_empty():
                    continue
                frames.append(frame.with_columns(pl.col("value").cast(pl.Utf8), pl.lit(view).alias("view"),
                                                 pl.lit(key or "").alias("sid")))
        if frames:
            run.save(f"{system}_{label}", pl.concat(frames))


def mismatches(want: dict, got: dict) -> dict[str, dict[str, int]]:
    """Per view: streams compared and streams whose rows differ from the oracle."""
    report = {}
    for view in VIEWS:
        keys = set(want.get(view, {})) | set(got.get(view, {}))
        bad = 0
        for key in keys:
            if _same(want.get(view, {}).get(key, EMPTY), got.get(view, {}).get(key, EMPTY), 1e-6):
                bad += 1
        report[view] = {"streams": len(keys), "mismatched": bad}
    return report


def run_baseline(name: str, sink, replica, wide, args) -> dict:
    print(f"== {name}: reset")
    started = time.monotonic()
    sink.reset()
    setup_seconds = time.monotonic() - started
    before = sink.counters()
    replay = Replay(None, replica, wide, late_fraction=args.late, correction_fraction=args.corrections,
                    seed=args.seed, sink=sink.write)
    started = time.monotonic()
    replay.run(pace=False)
    replay.flush_late(wide["timestamp"][-1])
    sink.finish()
    wall = time.monotonic() - started
    after = sink.counters()
    truth = {replay._name_of(ref): frame for ref, frame in replay.truth_frames().items()}
    want, got = expected_views(truth, kinds_of(replica)), sink.outputs()
    save_outputs(args.run, name, want, got)
    report = mismatches(want, got)
    record = {"system": name, "setup_seconds": setup_seconds, "wall_seconds": wall,
              "batches": len(replay.sent), "output_rows": sink.output_rows(),
              "mismatched_streams": sum(r["mismatched"] for r in report.values()),
              "compared_streams": sum(r["streams"] for r in report.values()), "per_view": report}
    for key in after:
        record[key] = after[key] - before.get(key, 0.0)
    if "cpu_seconds" not in after:
        record["cpu_seconds"] = record.get("cpu_engine_seconds") or record.get("cpu_container_seconds")
    if hasattr(sink, "stop"):
        sink.stop()
    print(f"   wall {wall:.1f}s cpu {record['cpu_seconds']:.1f}s rows {record['output_rows']} "
          f"mismatched {record['mismatched_streams']}/{record['compared_streams']}")
    return record


def run_acquirium(replica, wide, args) -> dict:
    import acquirium.runtime as rt
    print("== acquirium: start")
    run = Run("rq4-baselines-acq", poll_seconds=0.1)
    client = run.start()
    try:
        pid = rt._session[3].pid
        benicia.install(client, replica)
        world = World({replica.ref_uri(p.ref_name): p for p in replica.points})
        from experiments import changes
        for view in V.VIEWS:
            changes.deploy_view(client, world, view, {"threshold": sinks.THRESHOLD} if view is V.ConcentrationHigh else None)
        wait_quiescent(client, timeout=600)
        marker = len(run.events())
        db_cpu = (lambda: sinks.cgroup_cpu_seconds(TIMESCALE_CONTAINER)) if BACKEND == "timescale" else (lambda: 0.0)
        cpu0, db0 = sinks.process_cpu_seconds(pid), db_cpu()
        replay = Replay(client, replica, wide, late_fraction=args.late, correction_fraction=args.corrections, seed=args.seed)
        started = time.monotonic()
        replay.run(pace=False)
        replay.flush_late(wide["timestamp"][-1])
        wait_quiescent(client, timeout=900)
        wall = time.monotonic() - started
        cpu_process, cpu_db = sinks.process_cpu_seconds(pid) - cpu0, db_cpu() - db0
        cpu = cpu_process + cpu_db
        problems = compare(client, expected(world, replay.truth_frames()))
        events = run.events().slice(marker)
        totals = metrics.totals(events)
    finally:
        run.stop()
        run.discard_data()
    record = {"system": "acquirium", "setup_seconds": 0.0, "wall_seconds": wall, "batches": len(replay.sent),
              "output_rows": int(totals["rows_written"]), "mismatched_streams": len(problems),
              "compared_streams": len(expected(world, replay.truth_frames())), "per_view": {},
              "cpu_seconds": cpu, "cpu_process_seconds": cpu_process, "cpu_db_seconds": cpu_db,
              "transform_seconds": totals["transform_seconds"],
              "plan_seconds": totals["plan_seconds"], "rows_written": totals["rows_written"]}
    print(f"   wall {wall:.1f}s cpu {cpu:.1f}s (server {cpu_process:.1f} + database {cpu_db:.1f}) "
          f"rows {record['output_rows']} mismatched {len(problems)}")
    return record

LABELS = {"acquirium": "ours", "timescale": "Timescale\ncont. agg.", "feldera": "Feldera +\nTimescale",
          "feldera-memory": "Feldera\n(in memory)", "flink": "Flink SQL"}


def figure(records: list[dict], path: Path) -> Path:
    """CPU (server and database stacked where both are known), wall time and rows written per system."""
    names = [LABELS.get(r["system"], r["system"]) for r in records]
    fig, axes = plots.plt.subplots(1, 3, figsize=(9, 2.6))
    x = range(len(records))
    process = [r.get("cpu_process_seconds", r["cpu_seconds"]) for r in records]
    database = [r.get("cpu_db_seconds", 0.0) for r in records]
    axes[0].bar(x, process, color="#4c72b0", label="server / engine")
    axes[0].bar(x, database, bottom=process, color="#9ecae1", label="database")
    for i, r in enumerate(records):
        axes[0].text(i, r["cpu_seconds"], "exact" if r["mismatched_streams"] == 0
                     else f"{r['mismatched_streams']}/{r['compared_streams']} wrong", ha="center", va="bottom", fontsize=6)
    axes[0].set_title("CPU seconds", fontsize=9); axes[0].legend(fontsize=6, frameon=False)
    axes[1].bar(x, [r["wall_seconds"] for r in records], color="#4c72b0"); axes[1].set_title("wall seconds", fontsize=9)
    rows = [r.get("rows_written") or r["output_rows"] for r in records]
    axes[2].bar(x, rows, color="#4c72b0"); axes[2].set_yscale("log"); axes[2].set_title("rows written (log)", fontsize=9)
    axes[2].axhline(min(r["output_rows"] for r in records), color="grey", lw=0.8, ls="--")
    for ax in axes:
        ax.set_xticks(list(x)); ax.set_xticklabels(names, fontsize=7)
    return plots.save(fig, path)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=3)
    parser.add_argument("--late", type=float, default=0.05)
    parser.add_argument("--corrections", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--systems", default="acquirium,timescale,feldera,flink",
                        help="feldera is the pipeline (Timescale base table, egress applied to Timescale view "
                             "tables by a writer process); feldera-memory is the engine alone")
    parser.add_argument("--timescale-dsn", default="postgresql://acquirium:acquirium@127.0.0.1:5435/acquirium")
    parser.add_argument("--feldera", default="http://127.0.0.1:8085")
    parser.add_argument("--flink-gateway", default="http://127.0.0.1:8083")
    parser.add_argument("--flink-rest", default="http://127.0.0.1:8081")
    parser.add_argument("--flink-lateness", type=int, default=10, help="watermark delay in minutes")
    parser.add_argument("--flink-data", default=str(BASELINES / "data"))
    parser.add_argument("--replot", help="redraw the figure from a records.json of an earlier run and exit")
    args = parser.parse_args(argv)
    if args.replot:
        import json
        source = Path(args.replot)
        print("figure:", figure(json.loads(source.read_text()), source.parent / "rq4_baselines.png"))
        return

    summary = Run("rq4-baselines")
    args.run = summary
    replica = benicia.load_replica(0)
    wide = benicia.generate(replica, args.hours * 60, seed=args.seed)
    kinds = kinds_of(replica)
    records = []
    for name in args.systems.split(","):
        if name == "acquirium":
            records.append(run_acquirium(replica, wide, args))
        elif name == "timescale":
            records.append(run_baseline(name, TimescaleSink(args.timescale_dsn, kinds), replica, wide, args))
        elif name == "feldera":
            records.append(run_baseline(name, FelderaPipelineSink(args.feldera, args.timescale_dsn, kinds,
                                                                  status_dir=summary.directory), replica, wide, args))
        elif name == "feldera-memory":
            records.append(run_baseline(name, FelderaSink(args.feldera, kinds), replica, wide, args))
        elif name == "flink":
            records.append(run_baseline(name, FlinkSink(args.flink_gateway, args.flink_rest, Path(args.flink_data), kinds,
                                                        lateness_minutes=args.flink_lateness), replica, wide, args))
        else:
            raise SystemExit(f"unknown system {name}")
        summary.save("records.json", records)

    table = pl.DataFrame([{k: v for k, v in r.items() if k != "per_view"} for r in records])
    summary.save("summary", table)
    summary.save("config.json", vars(args))
    print(table.select("system", "cpu_seconds", "wall_seconds", "output_rows", "mismatched_streams", "compared_streams"))

    print("figure:", figure(records, summary.directory / "rq4_baselines.png"))
    print("run directory:", summary.directory)


if __name__ == "__main__":
    main()
