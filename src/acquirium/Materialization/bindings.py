"""Stable, storage-independent materialization binding specifications."""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
from typing import Any, Iterable, Mapping, Protocol, Sequence


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class BindingSpec:
    """Resolved logical identities and metadata for one materialization unit."""

    logical_key: str
    inputs: Mapping[str, tuple[str, ...]]
    outputs: Mapping[str, tuple[str, ...]]
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.logical_key:
            raise ValueError("a binding logical_key is required")
        if not self.inputs:
            raise ValueError("a binding must declare at least one input")
        if not self.outputs:
            raise ValueError("a binding must declare at least one output")
        for values in (*self.inputs.values(), *self.outputs.values()):
            if not values or any(not item for item in values):
                raise ValueError("binding identities must be non-empty")
        _canonical(self.metadata)  # fail early on non JSON-safe metadata

    @property
    def content_digest(self) -> str:
        return sha256(_canonical({"inputs": self.inputs, "outputs": self.outputs, "metadata": self.metadata}).encode()).hexdigest()

    def binding_id(self, definition_id: str) -> str:
        return sha256(f"{definition_id}:{self.logical_key}".encode()).hexdigest()


class GraphView(Protocol):
    """Marker protocol intentionally kept small for custom binding resolvers."""


class BindingResolver(Protocol):
    def resolve(self, graph: GraphView) -> Iterable[BindingSpec]: ...


@dataclass(frozen=True)
class Selector:
    """Declarative selector retained verbatim for server-side graph resolution."""

    criteria: Mapping[str, Any]


@dataclass(frozen=True)
class PerInput:
    selector: Selector


@dataclass(frozen=True)
class ByEntity:
    selectors: Mapping[str, Selector]
    entity_alias: str = "entity"


@dataclass(frozen=True)
class Single:
    inputs: Mapping[str, str]


def per_input(selector: Selector) -> PerInput:
    return PerInput(selector)


def by_entity(selector_map: Mapping[str, Selector], *, entity_alias: str = "entity") -> ByEntity:
    if not selector_map:
        raise ValueError("by_entity requires at least one selector")
    return ByEntity(dict(selector_map), entity_alias)


def single(input_map: Mapping[str, str]) -> Single:
    if not input_map:
        raise ValueError("single requires at least one input")
    return Single(dict(input_map))


@dataclass(frozen=True)
class BindingDiff:
    """Topology change classified by stable binding identity and content."""

    unchanged: tuple[BindingSpec, ...]
    added: tuple[BindingSpec, ...]
    changed: tuple[BindingSpec, ...]
    removed_ids: tuple[str, ...]


def diff_bindings(
    definition_id: str, previous: Sequence[BindingSpec], current: Sequence[BindingSpec]
) -> BindingDiff:
    """Diff resolved bindings without treating a metadata/input change as new."""
    old = {binding.binding_id(definition_id): binding for binding in previous}
    new = {binding.binding_id(definition_id): binding for binding in current}
    if len(old) != len(previous) or len(new) != len(current):
        raise ValueError("a resolver returned duplicate logical binding keys")
    unchanged, added, changed = [], [], []
    for binding_id, binding in new.items():
        prior = old.get(binding_id)
        if prior is None:
            added.append(binding)
        elif prior.content_digest == binding.content_digest:
            unchanged.append(binding)
        else:
            changed.append(binding)
    return BindingDiff(tuple(unchanged), tuple(added), tuple(changed), tuple(sorted(old.keys() - new.keys())))


def validate_binding_topology(
    bindings: Sequence[BindingSpec], *, definition_id: str
) -> None:
    """Reject ambiguous stream owners and cycles within a resolved topology."""
    owner: dict[str, str] = {}
    for binding in bindings:
        binding_id = binding.binding_id(definition_id)
        for refs in binding.outputs.values():
            for ref_uri in refs:
                previous = owner.setdefault(ref_uri, binding_id)
                if previous != binding_id:
                    raise ValueError(f"output {ref_uri!r} has ambiguous owners {previous!r} and {binding_id!r}")
    edges: dict[str, set[str]] = {binding.binding_id(definition_id): set() for binding in bindings}
    for binding in bindings:
        target = binding.binding_id(definition_id)
        for refs in binding.inputs.values():
            for ref_uri in refs:
                source = owner.get(ref_uri)
                if source is not None:
                    edges[source].add(target)
    visiting: set[str] = set()
    visited: set[str] = set()
    def visit(node: str) -> None:
        if node in visiting:
            raise ValueError("resolved binding topology contains a cycle")
        if node not in visited:
            visiting.add(node)
            for child in edges[node]:
                visit(child)
            visiting.remove(node)
            visited.add(node)
    for node in edges:
        visit(node)
