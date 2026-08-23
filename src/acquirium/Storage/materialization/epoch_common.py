"""Backend-independent topology-epoch construction helpers."""
from __future__ import annotations

import json
from collections import defaultdict, deque
from hashlib import sha256
from typing import Iterable, Mapping, Sequence

from acquirium.Materialization.bindings import BindingSpec
from acquirium.Materialization.epochs import EpochBinding
from acquirium.Materialization.impact import TimeRange, coalesce_ranges


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def epoch_id(graph_revision: int, graph_digest: str, definitions: Sequence[tuple]) -> str:
    """Derive an immutable identity from the complete desired topology input."""
    catalog = [tuple(item) for item in sorted(definitions, key=canonical_json)]
    return sha256(canonical_json({"graph_revision": graph_revision, "graph_digest": graph_digest, "definitions": catalog}).encode()).hexdigest()


def binding_json(binding: BindingSpec) -> dict[str, object]:
    return {
        "logical_key": binding.logical_key,
        "inputs": {role: list(refs) for role, refs in sorted(binding.inputs.items())},
        "outputs": {role: list(refs) for role, refs in sorted(binding.outputs.items())},
        "metadata": dict(binding.metadata),
    }


def epoch_binding(epoch: str, definition_id: str, binding: BindingSpec) -> EpochBinding:
    return EpochBinding(
        epoch_id=epoch,
        binding_id=binding.binding_id(definition_id),
        definition_id=definition_id,
        logical_key=binding.logical_key,
        content_digest=binding.content_digest,
        inputs={role: tuple(refs) for role, refs in sorted(binding.inputs.items())},
        outputs={role: tuple(refs) for role, refs in sorted(binding.outputs.items())},
        metadata=dict(binding.metadata),
    )


def global_dag(bindings: Sequence[EpochBinding]) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...], tuple[tuple[str, ...], ...]]:
    """Validate ownership/cycles and return edges, topo order, weak components.

    Outputs are globally owned, including across different definitions.  A
    directed edge points from the binding producing an input stream to the
    binding consuming it.  Components are weakly connected so one seal cannot
    expose a mixed-version dependency path.
    """
    owners: dict[str, str] = {}
    for binding in bindings:
        for ref in binding.output_refs:
            prior = owners.setdefault(ref, binding.binding_id)
            if prior != binding.binding_id:
                raise ValueError(f"output {ref!r} has ambiguous owners {prior!r} and {binding.binding_id!r}")

    edges: set[tuple[str, str]] = set()
    children: dict[str, set[str]] = {binding.binding_id: set() for binding in bindings}
    indegree = {binding.binding_id: 0 for binding in bindings}
    for binding in bindings:
        for ref in binding.input_refs:
            source = owners.get(ref)
            if source is None or source == binding.binding_id:
                if source == binding.binding_id:
                    raise ValueError("resolved binding topology contains a self-cycle")
                continue
            if (source, binding.binding_id) not in edges:
                edges.add((source, binding.binding_id))
                children[source].add(binding.binding_id)
                indegree[binding.binding_id] += 1

    queue = deque(sorted(node for node, degree in indegree.items() if degree == 0))
    topo: list[str] = []
    while queue:
        node = queue.popleft()
        topo.append(node)
        for child in sorted(children[node]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if len(topo) != len(bindings):
        raise ValueError("resolved binding topology contains a cycle")

    neighbors: dict[str, set[str]] = defaultdict(set)
    for source, target in edges:
        neighbors[source].add(target)
        neighbors[target].add(source)
    components: list[tuple[str, ...]] = []
    unseen = set(children)
    while unseen:
        root = min(unseen)
        component: list[str] = []
        pending = [root]
        unseen.remove(root)
        while pending:
            node = pending.pop()
            component.append(node)
            for neighbor in sorted(neighbors[node], reverse=True):
                if neighbor in unseen:
                    unseen.remove(neighbor)
                    pending.append(neighbor)
        components.append(tuple(sorted(component)))
    return tuple(sorted(edges)), tuple(topo), tuple(sorted(components))


def retained_ranges(ranges: Iterable[tuple[object, object]]) -> tuple[TimeRange, ...]:
    parsed = [TimeRange(start, end) for start, end in ranges]
    return coalesce_ranges(parsed)
