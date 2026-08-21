"""Server-local resolution of durable transformation binding declarations."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from acquirium.Materialization.bindings import BindingSpec
from acquirium.Materialization.impact import ImpactPolicy
from acquirium.Materialization.worker import load_entrypoint


def _roles(value: object, *, default_role: str) -> dict[str, tuple[str, ...]]:
    if isinstance(value, str):
        return {default_role: (value,)}
    if not isinstance(value, Mapping):
        raise ValueError("binding refs must be a URI or a role-to-URI mapping")
    result: dict[str, tuple[str, ...]] = {}
    for role, refs in value.items():
        if isinstance(refs, str):
            result[str(role)] = (refs,)
        elif isinstance(refs, (list, tuple)) and all(isinstance(ref, str) for ref in refs):
            result[str(role)] = tuple(refs)
        else:
            raise ValueError(f"binding role {role!r} must contain URI strings")
    return result


def _binding(value: Mapping[str, object], *, default_key: str = "default") -> BindingSpec:
    return BindingSpec(
        logical_key=str(value.get("logical_key", default_key)),
        inputs=_roles(value["inputs"], default_role="input"),
        outputs=_roles(value["outputs"], default_role="output"),
        metadata=value.get("metadata", {}) if isinstance(value.get("metadata", {}), Mapping) else {},
    )


def resolve_bindings(spec: Mapping[str, object], graph: object) -> tuple[BindingSpec, ...]:
    """Resolve the explicit v1 binding contract against the published graph.

    ``bind`` may contain a trusted ``resolver`` entrypoint returning
    ``BindingSpec`` objects (or their JSON-shaped equivalents), a ``bindings``
    list, or one complete binding.  With no ``bind``, direct ``inputs`` and
    ``outputs`` mappings form one binding.  Selector expansion remains a Phase
    5 concern rather than silently guessing output identities here.
    """
    declaration = spec.get("bind")
    resolved: Iterable[BindingSpec | Mapping[str, object]]
    if isinstance(declaration, Mapping) and isinstance(declaration.get("resolver"), str):
        resolved = load_entrypoint(declaration["resolver"])(graph)
    elif isinstance(declaration, Mapping) and isinstance(declaration.get("bindings"), list):
        resolved = declaration["bindings"]
    elif isinstance(declaration, Mapping) and "inputs" in declaration and "outputs" in declaration:
        resolved = (declaration,)
    elif declaration is None:
        if spec.get("inputs") is None or spec.get("outputs") is None:
            raise ValueError("a direct transformation requires inputs and outputs")
        resolved = ({"inputs": spec["inputs"], "outputs": spec["outputs"]},)
    else:
        raise ValueError("bind must declare explicit bindings or a trusted resolver entrypoint")
    bindings = tuple(item if isinstance(item, BindingSpec) else _binding(item) for item in resolved)
    if not bindings:
        raise ValueError("binding resolver returned no bindings")
    return bindings


@dataclass(frozen=True)
class RebindResult:
    deployment_name: str
    graph_revision: int
    impact: ImpactPolicy


class MaterializationRebinder:
    """Consumes one durable rebind request at a time."""

    def __init__(self, storage, graph: object) -> None:
        self._storage = storage
        self._graph = graph

    def run_once(self, owner: str) -> RebindResult | None:
        request = self._storage.lease_rebind(owner)
        if request is None:
            return None
        deployment_name, graph_revision = request
        try:
            bundle = self._storage.deployment_definition(deployment_name)
            bindings = resolve_bindings(bundle["spec"], self._graph)
            self._storage.persist_bindings(
                deployment_name, int(bundle["generation"]), graph_revision,
                str(bundle["definition_id"]), bindings,
            )
            self._storage.finish_rebind(deployment_name, graph_revision)
            impact = ImpactPolicy.from_json(bundle["spec"]["impact"])
            return RebindResult(deployment_name, graph_revision, impact)
        except Exception as error:
            self._storage.finish_rebind(deployment_name, graph_revision, error={
                "type": type(error).__name__, "message": str(error),
            })
            raise
