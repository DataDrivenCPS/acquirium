"""RQ1: do views stay correct under interleaved data, graph and dataflow changes?

Each sequence starts a fresh server, loads a few hours of Benicia data with
a share of readings held back, deploys the six views, then applies random
operations drawn from the three change classes. After quiescence every
derived stream is compared with the oracle's from-scratch recomputation.

    python -m experiments.rq1_correctness --sequences 3 --ops 15
"""
from __future__ import annotations

import argparse
import random
import time
from datetime import timedelta

import polars as pl
import rdflib

from experiments import benicia, changes, plots, views as V
from experiments.common import Run, wait_quiescent
from experiments.oracle import World, compare, expected
from experiments.replay import Replay

S223 = "http://data.ashrae.org/standard223#"
OPS = {
    "append": 3, "late": 2, "correction": 2, "delete": 1,                  # ΔX
    "remove_point": 1, "add_point": 1, "swap_point": 1,                    # ΔG
    "remove_view": 1, "deploy_view": 1, "reparameterize": 1, "reprocess": 1,  # ΔF
}
CLASS = {"append": "ΔX", "late": "ΔX", "correction": "ΔX", "delete": "ΔX",
         "remove_point": "ΔG", "add_point": "ΔG", "swap_point": "ΔG",
         "remove_view": "ΔF", "deploy_view": "ΔF", "reparameterize": "ΔF", "reprocess": "ΔF"}
KINDS = [V.CONCENTRATION, V.FLOW, V.ACIDITY]


def _connection_points(replica: benicia.Replica) -> list[str]:
    return sorted({str(s) for s, _, o in replica.graph.triples((None, rdflib.RDF.type, None))
                   if str(o).endswith("ConnectionPoint")})


def _apply(op: str, rng: random.Random, client, replica, replay: Replay, world: World, log: list) -> str:
    """Apply one operation; return a short description for the log."""
    if op == "append":
        rows = min(replay.cursor + rng.randint(3, 10), replay.wide.height)
        replay.run(rows=rows, pace=False)
        return f"rows to {rows}"
    if op == "late":
        return f"released {replay.release_late(rng.randint(5, 40))}"
    if op == "correction":
        return f"corrected {replay.correct(rng.randint(1, 10))}"
    if op == "delete":
        ref = rng.choice([r for r in world.refs_with(V.CONCENTRATION) if r in replay.truth] or list(replay.truth))
        start = rng.choice(sorted(replay.truth[ref]))
        changes.delete_window(client, replica, replay, ref, start, start + timedelta(minutes=20))
        return f"deleted 20 min of {ref.rsplit('/', 1)[-1]}"
    if op == "remove_point":
        candidates = [r for kind in KINDS for r in world.refs_with(kind)]
        if len(candidates) < 4:
            return "skipped"
        ref = rng.choice(candidates)
        changes.remove_point(client, world, ref)
        return f"removed {ref.rsplit('/', 1)[-1]}"
    if op == "add_point":
        kind = rng.choice(KINDS)
        name = f"extra-{len(world.points)}-{rng.randrange(1000)}"
        times = sorted(next(iter(replay.truth.values())))[-60:]
        rows = pl.DataFrame({"time": times, "value": [rng.uniform(10, 100) for _ in times]}).with_columns(
            pl.col("time").cast(pl.Datetime("us", "UTC")))
        changes.add_point(client, world, replica, replay, quantity_kind=kind,
                          host=rng.choice(_connection_points(replica)), name=name, rows=rows)
        return f"added {name} ({kind.rsplit('/', 1)[-1]})"
    if op == "swap_point":
        candidates = [r for kind in KINDS for r in world.refs_with(kind) if r in replay.truth]
        if not candidates:
            return "skipped"
        point = changes.swap_point(client, world, replica, replay, rng.choice(candidates), rng)
        return f"swapped in {point.ref_name}"
    if op == "remove_view":
        deployed = [v for v in V.VIEWS if v.name in world.deployed]
        if len(deployed) < 2:
            return "skipped"
        view = rng.choice(deployed)
        changes.remove_view(client, world, view)
        return f"removed {view.name}"
    if op == "deploy_view":
        missing = [v for v in V.VIEWS if v.name not in world.deployed]
        if not missing:
            return "skipped"
        view = rng.choice(missing)
        changes.deploy_view(client, world, view)
        return f"deployed {view.name}"
    if op == "reparameterize":
        if V.ConcentrationHigh.name not in world.deployed:
            return "skipped"
        threshold = rng.choice([50.0, 60.0, 70.0, 80.0])
        times = sorted(next(iter(replay.truth.values())))
        changes.deploy_view(client, world, V.ConcentrationHigh, {"threshold": threshold})
        changes.reprocess(client, V.ConcentrationHigh, times[0] - timedelta(days=1), times[-1] + timedelta(days=1))
        return f"threshold {threshold}"
    if op == "reprocess":
        deployed = [v for v in V.VIEWS if v.name in world.deployed]
        view = rng.choice(deployed)
        times = sorted(next(iter(replay.truth.values())))
        changes.reprocess(client, view, times[len(times) // 2], times[-1])
        return f"reprocessed {view.name}"
    raise ValueError(op)


def run_sequence(index: int, *, ops: int, hours: int, seed: int, poll: float) -> dict:
    rng = random.Random(seed * 1000 + index)
    run = Run("rq1", poll_seconds=poll)
    client = run.start()
    record = {"sequence": index, "seed": seed * 1000 + index, "ops": [], "problems": [], "passed": None}
    try:
        replica = benicia.load_replica(0)
        benicia.install(client, replica)
        wide = benicia.generate(replica, hours * 60 + 120, seed=seed * 1000 + index)
        replay = Replay(client, replica, wide, late_fraction=0.1, seed=seed * 1000 + index)
        replay.run(rows=hours * 60, pace=False)
        world = World({replica.ref_uri(p.ref_name): p for p in replica.points})
        for view in V.VIEWS:
            changes.deploy_view(client, world, view, {"threshold": 60.0} if view is V.ConcentrationHigh else None)
        wait_quiescent(client, timeout=600)
        population = [op for op, weight in OPS.items() for _ in range(weight)]
        for step in range(ops):
            op = rng.choice(population)
            started = time.monotonic()
            note = _apply(op, rng, client, replica, replay, world, record["ops"])
            waited = wait_quiescent(client, timeout=600) if rng.random() < 0.5 else None
            record["ops"].append({"step": step, "op": op, "class": CLASS[op], "note": note,
                                  "seconds": time.monotonic() - started, "waited": waited})
            print(f"  [{index}] {step:3d} {CLASS[op]} {op:15s} {note}")
        wait_quiescent(client, timeout=900)
        problems = compare(client, expected(world, replay.truth_frames()))
        record["problems"] = problems
        record["passed"] = not problems
        record["derived_streams"] = len(expected(world, replay.truth_frames()))
        run.save("sequence.json", record)
    finally:
        run.stop()
        run.discard_data()
    return record


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sequences", type=int, default=3)
    parser.add_argument("--ops", type=int, default=15)
    parser.add_argument("--hours", type=int, default=3)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--poll", type=float, default=0.1)
    args = parser.parse_args(argv)

    summary = Run("rq1-summary")
    records = [run_sequence(i, ops=args.ops, hours=args.hours, seed=args.seed, poll=args.poll)
               for i in range(args.sequences)]
    summary.save("sequences.json", records)
    table = pl.DataFrame([
        {"sequence": r["sequence"], "passed": r["passed"], "ops": len(r["ops"]),
         "ΔX": sum(o["class"] == "ΔX" for o in r["ops"]), "ΔG": sum(o["class"] == "ΔG" for o in r["ops"]),
         "ΔF": sum(o["class"] == "ΔF" for o in r["ops"]), "derived_streams": r.get("derived_streams"),
         "problems": len(r["problems"])} for r in records])
    summary.save("summary", table)
    print(table)

    fig, ax = plots.plt.subplots(figsize=(4.2, 2.6))
    bottom = [0] * len(records)
    for klass, color in [("ΔX", "#4c72b0"), ("ΔG", "#dd8452"), ("ΔF", "#55a868")]:
        counts = [sum(o["class"] == klass for o in r["ops"]) for r in records]
        ax.bar([r["sequence"] for r in records], counts, bottom=bottom, label=klass, color=color)
        bottom = [b + c for b, c in zip(bottom, counts)]
    for r, top in zip(records, bottom):
        ax.text(r["sequence"], top + 0.3, "ok" if r["passed"] else "FAIL", ha="center",
                color="green" if r["passed"] else "red", fontsize=8)
    ax.set_xlabel("sequence"); ax.set_ylabel("operations"); ax.legend(frameon=False, fontsize=8)
    ax.set_title("RQ1: randomized change sequences vs. oracle", fontsize=9)
    print("figure:", plots.save(fig, summary.directory / "rq1_sequences.png"))
    print("run directory:", summary.directory)


if __name__ == "__main__":
    main()
