"""The engine against a pointwise reference of the formalism, on random histories.

``Reference`` keeps the model, the deployments and the data as plain Python
and computes every output point from scratch, the way the formalism defines
it: a model change and a deployment change hold over their interval and the
latest-recorded one decides; an instance is present where its row exists and
its view is deployed; a read is clipped to the run of the row that binds it;
a view reading its own named output does so through its window.

Random operations (backdated model changes, late and out-of-order samples,
deployments from arbitrary starts, removals at arbitrary times, purges) are
applied to both, and after every operation the stored outputs must equal the
reference exactly.
"""
from __future__ import annotations

import os
import random
from collections import defaultdict
from datetime import timedelta

import pytest

from acquirium.internals.intervals import NEG_INF, POS_INF, from_micros, to_micros
from acquirium.Materialization.planner import Deployment
from acquirium.Storage.metadata_outbox import execute

from .test_time_aware_engine import (  # noqa: F401  (the world fixture)
    BASE, Double, Ewma, RollingSum, SumOfDoubles, World, at, point, world,
)
from rdflib import URIRef
from acquirium.Storage.graph_registry import PLANT_GRAPH_URI

APPS = {"double": Double, "rolling-sum": RollingSum, "sum-of-doubles": SumOfDoubles, "ewma": Ewma}
POINTS = (1, 2, 3)
HORIZON = 120  # minutes


class Reference:
    def __init__(self) -> None:
        self.model: list[tuple[int, str, int, int, int]] = []   # (r, op, point, start, end) in minutes
        self.deployments: list[tuple[int, str, bool, int, int]] = []  # (r, view, on, start, end)
        self.data: dict[int, dict[int, float]] = defaultdict(dict)  # point -> minute -> value
        self.r = 0

    # ---- the three logs ----
    def change_model(self, op: str, p: int, start: int, end: int) -> None:
        self.r += 1
        self.model.append((self.r, op, p, start, end))

    def change_deployment(self, view: str, on: bool, start: int, end: int) -> None:
        self.r += 1
        self.deployments.append((self.r, view, on, start, end))

    def present(self, p: int, t: int) -> bool:
        holding = [c for c in self.model if c[2] == p and c[3] <= t < c[4]]
        return bool(holding) and max(holding)[1] == "+"

    def deployed(self, view: str, t: int) -> bool:
        holding = [c for c in self.deployments if c[1] == view and c[3] <= t < c[4]]
        return bool(holding) and max(holding)[2]

    def run_start(self, p: int, t: int) -> int:
        """Start of the run of p's life containing t (p present at t)."""
        while t - 1 >= -10 * HORIZON and self.present(p, t - 1):
            t -= 1
        return t

    # ---- outputs from scratch ----
    def outputs(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        double: dict[int, dict[int, float]] = defaultdict(dict)
        for p in POINTS:
            for t, x in self.data[p].items():
                if self.present(p, t) and self.deployed("double", t):
                    double[p][t] = 2 * x
        out["double"] = {p: dict(v) for p, v in double.items() if v}
        rolling: dict[int, dict[int, float]] = defaultdict(dict)
        for p in POINTS:
            for t in self.data[p]:
                if self.present(p, t) and self.deployed("rolling-sum", t):
                    # the app sums over (t - 10 min, t], clipped to the run
                    first = self.run_start(p, t)
                    rolling[p][t] = sum(x for s, x in self.data[p].items() if s >= first and t - 10 < s <= t)
        out["rolling-sum"] = {p: dict(v) for p, v in rolling.items() if v}
        # sum-of-doubles reads double's outputs while their instances are present
        total: dict[int, float] = defaultdict(float)
        times = set()
        for p, values in double.items():
            for t, y in values.items():
                if self.deployed("sum-of-doubles", t):
                    total[t] += y
                    times.add(t)
        out["sum-of-doubles"] = {t: total[t] for t in times}
        # ewma reads r1 by name (never clipped) and its own previous value
        ewma: dict[int, float] = {}
        for t in sorted(self.data[1]):
            if self.deployed("ewma", t):
                ewma[t] = 0.5 * self.data[1][t] + 0.5 * ewma.get(t - 1, 0.0)
        out["ewma"] = ewma
        return out


def stored(world: World) -> dict[str, dict]:
    """What the engine stored, keyed like Reference.outputs."""
    with world.store._own_conn() as conn:
        rows = execute(conn,
            "SELECT s.source_id, s.ref_name, t.ts, t.numeric_value FROM timeseries t "
            "JOIN ref_ids r ON r.ref_id = t.ref_id JOIN streams s ON s.ref_uri = r.ref_uri "
            "WHERE s.source_id LIKE 'derived:%' AND NOT t.deleted").fetchall()
    plan = world.views.current_plan()[0]
    point_of: dict[str, int] = {}
    for instance in plan.instances:
        for port in instance.outputs.values():
            for row in instance.rows:
                for descriptors in row.streams.values():
                    for d in descriptors:
                        if d.ref_uri.startswith("urn:ex/r"):
                            point_of[port.ref_name] = int(d.ref_uri.removeprefix("urn:ex/r"))
    out: dict[str, dict] = {"double": defaultdict(dict), "rolling-sum": defaultdict(dict),
                            "sum-of-doubles": {}, "ewma": {}}
    for source, ref_name, ts, value in rows:
        view = source.removeprefix("derived:")
        minute = int((ts.replace(tzinfo=BASE.tzinfo) - BASE).total_seconds() // 60)
        if view in ("double", "rolling-sum"):
            out[view][point_of.get(ref_name, ref_name)][minute] = value
        else:
            out[view][minute] = value
    return {k: (dict(v) if k in ("double", "rolling-sum") else v) for k, v in out.items()}


def minute(m: int):
    return BASE + timedelta(minutes=m)


def bound(m: int | None, *, end: bool):
    if m is None:
        return "infinity" if end else "-infinity"
    return from_micros(to_micros(minute(m)))


def run(world: World, seed: int, operations: int) -> None:
    rng = random.Random(seed)
    ref = Reference()
    deployed = set()
    # sum-of-doubles and ewma depend on the others; deploy in any order anyway
    for step in range(operations):
        kind = rng.choice(["model", "model", "data", "data", "data", "deploy", "remove"])
        if kind == "model":
            p = rng.choice(POINTS)
            op = rng.choice(["+", "+", "-"])
            start = rng.choice([None, rng.randrange(0, HORIZON)])
            end = None if rng.random() < 0.5 else (start or 0) + rng.randrange(1, 60)
            lo, hi = (-10 * HORIZON if start is None else start), (10 * HORIZON if end is None else end)
            if op == "+":
                world.graphs.insert_graph(point(p), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI),
                                          effective_from=bound(start, end=False), effective_until=bound(end, end=True))
            else:
                world.graphs.sparql_update(f"DELETE DATA {{ {point(p)} }}", graph_uri=URIRef(PLANT_GRAPH_URI),
                                           effective_from=bound(start, end=False), effective_until=bound(end, end=True))
            world.views.model_changed()
            world.views.step()
            ref.change_model(op, p, lo, hi)
        elif kind == "data":
            p = rng.choice(POINTS)
            samples = [(rng.randrange(0, HORIZON), float(rng.randrange(1, 9))) for _ in range(rng.randint(1, 4))]
            samples = list(dict(samples).items())
            world.write(f"urn:ex/r{p}", [(minute(m), v) for m, v in samples])
            for m, v in samples:
                ref.data[p][m] = v
        elif kind == "deploy":
            name = rng.choice(sorted(APPS))
            start = rng.choice([None, rng.randrange(0, HORIZON)])
            if name == "ewma" and start is None:
                start = 0  # a loop needs a finite start
            world.views.deploy(Deployment.from_class(APPS[name]),
                               start=NEG_INF if start is None else to_micros(minute(start)))
            world.views.step()
            ref.change_deployment(name, True, -10 * HORIZON if start is None else start, 10 * HORIZON)
            deployed.add(name)
        elif kind == "remove" and deployed:
            name = rng.choice(sorted(deployed))
            purge = rng.random() < 0.2
            at_minute = rng.randrange(0, HORIZON)
            try:
                world.views.remove(name, at=to_micros(minute(at_minute)), purge=purge)
            except KeyError:
                continue
            world.views.step()
            ref.change_deployment(name, False, -10 * HORIZON if purge else at_minute, 10 * HORIZON)
        expected, got = ref.outputs(), stored(world)
        for view in APPS:
            assert got[view] == expected[view], f"seed {seed} op {step} ({kind}) view {view}"
        assert not world.views.failures(), world.views.failures()


SEEDS = int(os.getenv("ENGINE_TEST_SEEDS", "6"))
OPERATIONS = int(os.getenv("ENGINE_TEST_OPERATIONS", "25"))


@pytest.mark.parametrize("seed", range(SEEDS))
def test_random_histories_match_the_reference(world, seed):
    run(world, seed, OPERATIONS)
