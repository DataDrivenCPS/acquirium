"""The pure core of the view engine: interval sets, the deployment log, and
admission of a view set.

Each function is checked on the examples of the formalism and against a
brute-force model on random inputs: interval sets as sets of integer points,
the deployment log as "the latest-recorded change covering the time", and the
delay rule as "every simple cycle sums to less than zero".
"""
from __future__ import annotations

import random
from fractions import Fraction

import pytest

from acquirium.internals.intervals import (
    NEG_INF,
    POS_INF,
    expand,
    intersect,
    lives,
    merge,
    pieces_of,
    run_at,
    subtract,
    symmetric_difference,
)
from acquirium.Materialization.admission import (
    AdmissionError,
    ViewShape,
    _loop_without_delay,
    admit,
    edges,
)
from acquirium.Materialization.deployments import (
    DeploymentChange,
    changed,
    definition_at,
    deployment,
)

S = 1_000_000  # one second in microseconds
MIN = 60 * S


# -------------------- interval sets --------------------
def points(intervals) -> set[int]:
    return {p for start, end in intervals for p in range(start, end)}


def random_set(rng: random.Random) -> list[tuple[int, int]]:
    out = []
    for _ in range(rng.randint(0, 4)):
        start = rng.randint(0, 30)
        out.append((start, start + rng.randint(0, 8)))
    return out


def test_set_operations_match_point_sets():
    rng = random.Random(1)
    for _ in range(500):
        a, b = random_set(rng), random_set(rng)
        assert points(merge(a)) == points(a)
        assert points(intersect(a, b)) == points(a) & points(b)
        assert points(subtract(a, b)) == points(a) - points(b)
        assert points(symmetric_difference(a, b)) == points(a) ^ points(b)
        for result in (merge(a), intersect(a, b), subtract(a, b)):
            # an interval set: sorted, disjoint, not touching, not empty
            assert all(start < end for start, end in result)
            assert all(left[1] < right[0] for left, right in zip(result, result[1:]))


def test_run_at_is_the_maximal_interval_containing_the_time():
    assert run_at([(0, 5), (5, 8), (10, 12)], 6) == (0, 8)
    assert run_at([(0, 5), (10, 12)], 7) is None
    assert run_at([(0, 5)], 5) is None


def test_expand_gives_the_outputs_whose_window_touches_the_set():
    rng = random.Random(2)
    for _ in range(300):
        changed_set = random_set(rng)
        lookback = rng.randint(0, 5)
        lookahead = rng.randint(-lookback, 5)  # negative: a delay
        touched = points(changed_set)
        expected = {t for t in range(-20, 60)
                    if any(t - lookback <= p <= t + lookahead for p in touched)}
        got = points(expand(changed_set, lookback=lookback, lookahead=lookahead)) & set(range(-20, 60))
        assert got == expected


def test_expand_keeps_unbounded_ends_and_reads_all_history():
    assert expand([(10, 20)], lookback=POS_INF, lookahead=0) == [(10, POS_INF)]
    assert expand([(NEG_INF, 5)], lookback=0, lookahead=3) == [(NEG_INF, 5)]
    with pytest.raises(ValueError):
        expand([(0, 1)], lookback=1, lookahead=-2)


def test_lives_and_pieces_are_inverse():
    pieces = [(0, 5, {"a"}), (5, 9, {"a", "b"}), (9, 12, {"b"}), (15, 20, {"a"})]
    found = lives(pieces)
    assert found == {"a": [(0, 9), (15, 20)], "b": [(5, 12)]}
    assert pieces_of(found) == [(0, 5, frozenset("a")), (5, 9, frozenset("ab")),
                                (9, 12, frozenset("b")), (15, 20, frozenset("a"))]


# -------------------- deployment log --------------------
def test_deploy_remove_redeploy_and_purge():
    log = [DeploymentChange(1, "v", "A", 100, POS_INF)]
    assert deployment(log, "v") == [(100, POS_INF, "A")]
    log.append(DeploymentChange(2, "v", None, 300, POS_INF))      # removed at 300
    assert deployment(log, "v") == [(100, 300, "A")]
    log.append(DeploymentChange(3, "v", "B", 200, POS_INF))       # redefined from 200
    assert deployment(log, "v") == [(100, 200, "A"), (200, POS_INF, "B")]
    assert definition_at(log, "v", 250) == "B"
    assert definition_at(log, "v", 250, as_of=2) == "A"           # as known before r = 3
    assert definition_at(log, "v", 350, as_of=2) is None
    assert changed(log, 3) == {"v": [(200, POS_INF)]}
    log.append(DeploymentChange(4, "v", None, NEG_INF, POS_INF))  # purge
    assert deployment(log, "v") == []
    assert changed(log, 4) == {"v": [(100, POS_INF)]}


def test_redeploying_the_same_text_changes_nothing():
    log = [DeploymentChange(1, "v", "A", 0, POS_INF), DeploymentChange(2, "v", "A", 50, POS_INF)]
    assert deployment(log, "v") == [(0, POS_INF, "A")]
    assert changed(log, 2) == {}


def test_deployment_log_matches_the_definition():
    rng = random.Random(3)
    for _ in range(200):
        log = []
        for r in range(1, rng.randint(1, 7)):
            start = rng.randint(0, 20)
            log.append(DeploymentChange(r, rng.choice("vw"), rng.choice(["A", "B", None]),
                                        start, start + rng.randint(1, 10)))

        def naive(name, t, as_of):
            covering = [c for c in log if c.name == name and c.r <= as_of and c.start <= t < c.end]
            return max(covering, key=lambda c: c.r).definition if covering else None

        head = len(log)
        for name in "vw":
            for t in range(0, 32):
                assert definition_at(log, name, t) == naive(name, t, head)
        for r in range(1, head + 1):
            regions = changed(log, r)
            for name in "vw":
                expected = {t for t in range(0, 32) if naive(name, t, r - 1) != naive(name, t, r)}
                assert points(regions.get(name, [])) == expected


# -------------------- admission --------------------
def test_views_without_loops_are_admitted_in_selection_order():
    schedule = admit({
        "raw_avg": ViewShape(),
        "efficiency": ViewShape(upstream=frozenset({"raw_avg"}), lookahead=5 * MIN),
        "report": ViewShape(upstream=frozenset({"efficiency", "raw_avg"})),
    })
    assert schedule.order.index("raw_avg") < schedule.order.index("efficiency") < schedule.order.index("report")
    assert schedule.epsilon is None and schedule.loops == frozenset()
    # a 5 min lookahead means efficiency waits 5 min longer than raw_avg
    assert schedule.phi["efficiency"] - schedule.phi["raw_avg"] >= 5 * MIN


def test_selection_must_not_loop():
    with pytest.raises(AdmissionError, match="select each other's outputs in a cycle"):
        admit({"a": ViewShape(upstream=frozenset({"b"})), "b": ViewShape(upstream=frozenset({"a"}))})
    with pytest.raises(AdmissionError, match="a -> a"):
        admit({"a": ViewShape(upstream=frozenset({"a"}))})


def test_a_view_may_read_its_own_earlier_output():
    schedule = admit({"ewma": ViewShape(named=(("ewma", -MIN),), start=0)})
    assert schedule.loops == frozenset({"ewma"})
    assert schedule.epsilon == MIN


def test_a_loop_without_delay_is_rejected():
    with pytest.raises(AdmissionError, match=r"ewma -> ewma .* sum to \+0s"):
        admit({"ewma": ViewShape(named=(("ewma", 0),), start=0)})


def test_a_loop_needs_a_finite_start():
    with pytest.raises(AdmissionError, match="needs a finite start"):
        admit({"ewma": ViewShape(named=(("ewma", -MIN),))})


def test_estimator_and_controller_loop():
    # E reads C over [t - 2 min, t - 1 min] through a named role; C selects
    # E's output with 30 s lookahead.
    views = {
        "E": ViewShape(named=(("C", -MIN),), start=0),
        "C": ViewShape(upstream=frozenset({"E"}), lookahead=30 * S, start=0),
    }
    schedule = admit(views)
    assert schedule.epsilon == 15 * S  # the loop moves 30 s back over 2 edges
    assert 30 * S < schedule.phi["C"] - schedule.phi["E"] < 60 * S
    views["C"] = ViewShape(upstream=frozenset({"E"}), lookahead=90 * S, start=0)
    with pytest.raises(AdmissionError, match=r"sum to \+30s"):
        admit(views)


def test_the_union_over_time_catches_a_contradiction_no_snapshot_shows():
    # A read B with lookahead 1 s before time a; B reads A with lookback 1 s
    # from a on. Admission sees both definitions of the union.
    views = {
        "A": ViewShape(named=(("B", S),), start=0),
        "B": ViewShape(named=(("A", 0),), start=0),
    }
    with pytest.raises(AdmissionError, match=r"sum to \+1s"):
        admit(views)


def simple_cycles(graph):
    nodes = sorted({n for edge in graph for n in edge})
    out = []

    def walk(start, node, path):
        for (source, target), weight in graph.items():
            if source != node:
                continue
            if target == start:
                out.append(path + [target])
            elif target not in path and target > start:
                walk(start, target, path + [target])

    for start in nodes:
        walk(start, start, [start])
    return out


def test_admission_matches_the_delay_rule_on_random_graphs():
    rng = random.Random(4)
    for _ in range(400):
        names = [f"v{i}" for i in range(rng.randint(1, 5))]
        views = {}
        for name in names:
            named = tuple((rng.choice(names), rng.randint(-3, 2)) for _ in range(rng.randint(0, 3)))
            views[name] = ViewShape(named=named, start=0)
        graph = edges(views)
        cycles = simple_cycles(graph)
        sums = [sum(graph[(a, b)] for a, b in zip(c, c[1:])) for c in cycles]
        if all(total < 0 for total in sums):
            schedule = admit(views)
            means = [Fraction(total, len(c) - 1) for c, total in zip(cycles, sums)]
            assert schedule.epsilon == (-max(means) if means else None)
            step = schedule.epsilon or 0
            for (source, target), weight in graph.items():
                assert schedule.phi[target] >= schedule.phi[source] + weight + step
        else:
            with pytest.raises(AdmissionError):
                admit(views)
            cycle = _loop_without_delay(sorted(views), graph)
            assert cycle[0] == cycle[-1]
            assert sum(graph[(a, b)] for a, b in zip(cycle, cycle[1:])) >= 0
