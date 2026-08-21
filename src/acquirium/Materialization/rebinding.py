"""Server-local resolution of durable transformation binding declarations."""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
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


def _selector_refs(selector: Mapping[str, object], graph: object) -> tuple[str, ...]:
    return tuple(ref for _, ref in _selector_rows(selector, graph, entity_alias="ref_uri"))


def _selector_rows(selector: Mapping[str, object], graph: object, *, entity_alias: str) -> tuple[tuple[str, str], ...]:
    criteria = selector.get("criteria", selector)
    if not isinstance(criteria, Mapping):
        raise ValueError("selector criteria must be a mapping")
    direct = criteria.get("ref_uris", criteria.get("ref_uri"))
    if isinstance(direct, str):
        return ((direct, direct),)
    if isinstance(direct, (list, tuple)) and all(isinstance(ref, str) for ref in direct):
        return tuple((ref, ref) for ref in sorted(direct))
    query = criteria.get("sparql")
    if not isinstance(query, str):
        raise ValueError("selector requires ref_uri(s) or a SPARQL query selecting ?ref_uri")
    result = graph.sparql_query(query, include_dependencies=True, wait_for_fresh=True)
    try:
        ref_column = result["columns"].index("ref_uri")
        entity_column = result["columns"].index(entity_alias)
    except ValueError as error:
        raise ValueError(f"selector SPARQL must select ?ref_uri and ?{entity_alias}") from error
    return tuple(sorted((str(row[entity_column]), str(row[ref_column])) for row in result["rows"]
                        if row[entity_column] is not None and row[ref_column] is not None))


def _per_input(selector: Mapping[str, object], outputs: Mapping[str, object], graph: object) -> tuple[BindingSpec, ...]:
    name = outputs.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("per_input outputs require a non-empty name")
    prefix = str(outputs.get("prefix", "urn:acquirium:derived"))
    bindings = []
    for ref in _selector_refs(selector, graph):
        token = sha256(ref.encode()).hexdigest()[:20]
        bindings.append(BindingSpec(ref, {"input": (ref,)}, {"output": (f"{prefix}:{name}:{token}",)}, {"input_ref": ref}))
    return tuple(bindings)


def _by_entity(selectors: Mapping[str, object], outputs: Mapping[str, object], graph: object,
               *, entity_alias: str) -> tuple[BindingSpec, ...]:
    name = outputs.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("by_entity outputs require a non-empty name")
    prefix = str(outputs.get("prefix", "urn:acquirium:derived"))
    roles: dict[str, dict[str, list[str]]] = {}
    for role, selector in selectors.items():
        if not isinstance(selector, Mapping):
            raise ValueError(f"selector for role {role!r} must be a mapping")
        grouped: dict[str, list[str]] = {}
        for entity, ref in _selector_rows(selector, graph, entity_alias=entity_alias):
            grouped.setdefault(entity, []).append(ref)
        roles[str(role)] = grouped
    entities = set.intersection(*(set(values) for values in roles.values())) if roles else set()
    bindings = []
    for entity in sorted(entities):
        token = sha256(entity.encode()).hexdigest()[:20]
        bindings.append(BindingSpec(entity, {role: tuple(sorted(values[entity])) for role, values in roles.items()},
            {"output": (f"{prefix}:{name}:{token}",)}, {entity_alias: entity}))
    return tuple(bindings)


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
    if isinstance(declaration, Mapping) and isinstance(declaration.get("selectors"), Mapping) and isinstance(spec.get("outputs"), Mapping):
        return _by_entity(declaration["selectors"], spec["outputs"], graph,
                          entity_alias=str(declaration.get("entity_alias", "entity")))
    if isinstance(declaration, Mapping) and "selector" in declaration and isinstance(spec.get("outputs"), Mapping):
        return _per_input(declaration["selector"], spec["outputs"], graph)
    if declaration is None and isinstance(spec.get("inputs"), Mapping) and "criteria" in spec["inputs"] and isinstance(spec.get("outputs"), Mapping):
        return _per_input(spec["inputs"], spec["outputs"], graph)
    if isinstance(declaration, Mapping) and isinstance(declaration.get("resolver"), str):
        resolved = load_entrypoint(declaration["resolver"])(graph)
    elif isinstance(declaration, Mapping) and isinstance(declaration.get("bindings"), list):
        resolved = declaration["bindings"]
    elif isinstance(declaration, Mapping) and "inputs" in declaration and isinstance(spec.get("outputs"), Mapping):
        resolved = ({"inputs": declaration["inputs"], "outputs": spec["outputs"]},)
    elif isinstance(declaration, Mapping) and "inputs" in declaration and "outputs" in declaration:
        resolved = (declaration,)
    elif declaration is None:
        if spec.get("inputs") is None or spec.get("outputs") is None:
            raise ValueError("a direct transformation requires inputs and outputs")
        resolved = ({"inputs": spec["inputs"], "outputs": spec["outputs"]},)
    else:
        raise ValueError("bind must declare explicit bindings or a trusted resolver entrypoint")
    bindings = tuple(item if isinstance(item, BindingSpec) else _binding(item) for item in resolved)
    if not bindings and not (isinstance(declaration, Mapping) and "selector" in declaration):
        raise ValueError("binding resolver returned no bindings")
    return bindings


@dataclass(frozen=True)
class RebindResult:
    deployment_name: str
    graph_revision: int
    generation: int
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
            generation = self._storage.stage_bindings(
                deployment_name, graph_revision, str(bundle["definition_id"]), bindings
            )
            self._storage.finish_rebind(deployment_name, graph_revision)
            impact = ImpactPolicy.from_json(bundle["spec"]["impact"])
            return RebindResult(deployment_name, graph_revision, generation, impact)
        except Exception as error:
            self._storage.finish_rebind(deployment_name, graph_revision, error={
                "type": type(error).__name__, "message": str(error),
            })
            raise
