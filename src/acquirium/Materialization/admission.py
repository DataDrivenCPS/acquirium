"""Admission of a view set, and the evaluation schedule it implies.

A deployment change is admitted only if the whole view set, over every
definition each name has ever had, passes three checks:

- stratified selection: no view selects (through its query) the outputs of a
  view that, directly or indirectly, selects its own outputs;
- the delay rule: every loop of the view graph has lookaheads summing to less
  than zero, so going once around a loop moves strictly into the past;
- a finite start for every view on a loop, the base case of its recursion.

The view graph has an edge ``W -> V`` labelled ``l`` when ``V`` may read
``W``'s outputs with lookahead ``l`` (negative: a delay). If the delay rule
holds there are offsets ``phi`` and a step ``epsilon > 0`` with
``phi[V] >= phi[W] + l + epsilon`` on every edge. Then ``t + phi[V]`` drops
by at least ``epsilon`` along every dependency between output points, which
makes the outputs well defined and gives the order to compute them in.

Times and durations are integer microseconds.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from fractions import Fraction

from acquirium.internals.intervals import NEG_INF


class AdmissionError(ValueError):
    """The view set would leave some output without a unique value."""


@dataclass(frozen=True)
class ViewShape:
    """What admission needs to know about one view name, collected over every
    definition the name has had.

    ``upstream`` holds the views whose outputs the query may select.
    ``lookahead`` is the largest lookahead of the roles the query binds;
    ``named`` lists ``(view, lookahead)`` for each named role. ``start`` is
    the earliest time the name is deployed, ``NEG_INF`` for all history.
    """
    upstream: frozenset[str] = frozenset()
    lookahead: int = 0
    named: tuple[tuple[str, int], ...] = ()
    start: int = NEG_INF


@dataclass(frozen=True)
class Schedule:
    """The result of admission.

    ``order`` lists every view after the views it selects from. ``phi`` and
    ``epsilon`` are the offsets and step described in the module docstring;
    ``epsilon`` is ``None`` when the view graph has no loop. ``loops`` holds
    the views that lie on a loop.
    """
    order: tuple[str, ...]
    phi: Mapping[str, Fraction]
    epsilon: Fraction | None
    loops: frozenset[str]

    def sigma(self, view: str, at: int) -> Fraction:
        """The schedule time of ``view``'s output at ``at``."""
        return at + self.phi[view]


def edges(views: Mapping[str, ViewShape]) -> dict[tuple[str, str], int]:
    """The view graph: ``(W, V) -> l`` for the largest lookahead with which
    ``V`` reads ``W``. Views that are not in the set have no edges."""
    out: dict[tuple[str, str], int] = {}

    def add(source: str, target: str, lookahead: int) -> None:
        if source in views:
            key = (source, target)
            out[key] = max(out.get(key, lookahead), lookahead)

    for name, shape in views.items():
        for source in shape.upstream:
            add(source, name, shape.lookahead)
        for source, lookahead in shape.named:
            add(source, name, lookahead)
    return out


def admit(views: Mapping[str, ViewShape]) -> Schedule:
    """Check the view set and return its schedule, or raise ``AdmissionError``."""
    order = _selection_order(views)
    graph = edges(views)
    names = sorted(views)
    cycle = _loop_without_delay(names, graph)
    if cycle is not None:
        total = sum(graph[(a, b)] for a, b in zip(cycle, cycle[1:]))
        raise AdmissionError(
            f"loop {' -> '.join(cycle)} does not move into the past: its lookaheads sum to "
            f"{_seconds(total)}; every loop must sum to less than zero, so give one of its "
            "named inputs a longer delay"
        )
    loops = _on_loops(names, graph)
    for name in sorted(loops):
        if views[name].start == NEG_INF:
            raise AdmissionError(
                f"view {name!r} is on a loop, so it needs a finite start: its first output "
                "is the base case of the recursion"
            )
    mean = _max_cycle_mean(names, graph)
    epsilon = None if mean is None else -mean
    phi = _longest_paths(names, graph, epsilon or Fraction(0))
    return Schedule(order, phi, epsilon, loops)


def _seconds(micros: int) -> str:
    return f"{'+' if micros >= 0 else ''}{micros / 1_000_000:g}s"


def _selection_order(views: Mapping[str, ViewShape]) -> tuple[str, ...]:
    """Topological order of the selection relation; raise on a cycle."""
    order: list[str] = []
    state: dict[str, int] = {}
    path: list[str] = []

    def visit(name: str) -> None:
        if state.get(name) == 2:
            return
        if state.get(name) == 1:
            cycle = path[path.index(name):] + [name]
            raise AdmissionError(
                f"views select each other's outputs in a cycle: {' -> '.join(reversed(cycle))}; "
                "whether a view's inputs exist cannot be delayed, so selection must not loop"
            )
        state[name] = 1
        path.append(name)
        for source in sorted(views[name].upstream):
            if source in views:
                visit(source)
        path.pop()
        state[name] = 2
        order.append(name)

    for name in sorted(views):
        visit(name)
    return tuple(order)


def _loop_without_delay(names: list[str], graph: Mapping[tuple[str, str], int]) -> list[str] | None:
    """A cycle whose lookaheads sum to zero or more, as ``[a, b, ..., a]``.

    Integer weights ``l`` are scaled to ``(n + 1) * l + 1``: a cycle of at most
    ``n`` edges is positive after scaling exactly when its sum is ``>= 0``.
    Bellman-Ford for longest paths then finds such a cycle.
    """
    n = len(names)
    scale = n + 1
    # Every view starts at 0, as if a root reached it by a zero-weight edge.
    dist = {name: 0 for name in names}
    parent: dict[str, str] = {}
    changed = None
    for _ in range(n + 1):
        changed = None
        for (source, target), lookahead in graph.items():
            value = dist[source] + scale * lookahead + 1
            if value > dist[target]:
                dist[target] = value
                parent[target] = source
                changed = target
        if changed is None:
            return None
    # A view relaxed in the last round descends from a positive cycle of the
    # parent graph; walking back n + 1 steps lands on it.
    node = changed
    for _ in range(n + 1):
        node = parent[node]
    cycle = [node]
    current = parent[node]
    while current != node:
        cycle.append(current)
        current = parent[current]
    cycle.append(node)
    cycle.reverse()
    return cycle


def _on_loops(names: list[str], graph: Mapping[tuple[str, str], int]) -> frozenset[str]:
    """The views in a strongly connected component that contains a cycle."""
    children: dict[str, list[str]] = {name: [] for name in names}
    for source, target in graph:
        children[source].append(target)
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    found: set[str] = set()
    counter = 0

    def connect(node: str) -> None:
        nonlocal counter
        index[node] = low[node] = counter
        counter += 1
        stack.append(node)
        on_stack.add(node)
        for child in children[node]:
            if child not in index:
                connect(child)
                low[node] = min(low[node], low[child])
            elif child in on_stack:
                low[node] = min(low[node], index[child])
        if low[node] == index[node]:
            component = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                component.append(member)
                if member == node:
                    break
            if len(component) > 1 or (node, node) in graph:
                found.update(component)

    for name in names:
        if name not in index:
            connect(name)
    return frozenset(found)


def _max_cycle_mean(names: list[str], graph: Mapping[tuple[str, str], int]) -> Fraction | None:
    """Karp's maximum mean cycle, ``None`` if the graph has no cycle.

    A root joined to every view by zero-weight edges makes every view
    reachable; the root has no incoming edge, so it is on no cycle.
    """
    n = len(names) + 1
    walks: list[dict[str, int | None]] = [{name: 0 for name in names}]
    for _ in range(1, n):
        previous = walks[-1]
        current: dict[str, int | None] = {name: None for name in names}
        for (source, target), lookahead in graph.items():
            if previous[source] is not None:
                value = previous[source] + lookahead
                if current[target] is None or value > current[target]:
                    current[target] = value
        walks.append(current)
    # walks[k] holds the heaviest walk of k + 1 edges from the root; walks of
    # length 1 (the root edge alone) weigh 0, which is walks[0].
    best: Fraction | None = None
    last = walks[n - 1]
    for name in names:
        if last[name] is None:
            continue
        worst: Fraction | None = None
        for k in range(n - 1):
            if walks[k][name] is not None:
                value = Fraction(last[name] - walks[k][name], n - 1 - k)
                worst = value if worst is None or value < worst else worst
        if worst is not None and (best is None or worst > best):
            best = worst
    return best


def _longest_paths(names: Iterable[str], graph: Mapping[tuple[str, str], int], epsilon: Fraction) -> dict[str, Fraction]:
    """Longest paths from a zero-weight root with weights ``l + epsilon``.

    Finite because, with ``epsilon`` at most minus the maximum cycle mean, no
    cycle has positive weight.
    """
    names = list(names)
    phi = {name: Fraction(0) for name in names}
    for _ in range(len(names)):
        changed = False
        for (source, target), lookahead in graph.items():
            value = phi[source] + lookahead + epsilon
            if value > phi[target]:
                phi[target] = value
                changed = True
        if not changed:
            break
    return phi
