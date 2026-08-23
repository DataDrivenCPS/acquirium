"""Server-side construction of immutable resolved binding topologies."""
from __future__ import annotations

from hashlib import sha256
from typing import Iterable, Mapping

from acquirium.Materialization.bindings import BindingSpec


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
    return tuple(
        BindingSpec(
            ref,
            {"input": (ref,)},
            {"output": (f"{prefix}:{name}:{sha256(ref.encode()).hexdigest()[:20]}",)},
            {"input_ref": ref},
        )
        for ref in _selector_refs(selector, graph)
    )


def _selector_refs(selector: Mapping[str, object], graph: object) -> tuple[str, ...]:
    return tuple(ref for _, ref in _selector_rows(selector, graph, entity_alias="ref_uri"))


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
    return tuple(
        BindingSpec(
            entity,
            {role: tuple(sorted(values[entity])) for role, values in roles.items()},
            {"output": (f"{prefix}:{name}:{sha256(entity.encode()).hexdigest()[:20]}",)},
            {entity_alias: entity},
        )
        for entity in sorted(entities)
    )


def resolve_bindings(spec: Mapping[str, object], graph: object) -> tuple[BindingSpec, ...]:
    """Resolve one trusted definition against the published graph exactly once."""
    declaration = spec.get("bind")
    resolved: Iterable[BindingSpec | Mapping[str, object]]
    if isinstance(declaration, Mapping) and isinstance(declaration.get("selectors"), Mapping) and isinstance(spec.get("outputs"), Mapping):
        return _by_entity(declaration["selectors"], spec["outputs"], graph,
                          entity_alias=str(declaration.get("entity_alias", "entity")))
    if isinstance(declaration, Mapping) and "selector" in declaration and isinstance(spec.get("outputs"), Mapping):
        return _per_input(declaration["selector"], spec["outputs"], graph)
    if declaration is None and isinstance(spec.get("inputs"), Mapping) and "criteria" in spec["inputs"] and isinstance(spec.get("outputs"), Mapping):
        return _per_input(spec["inputs"], spec["outputs"], graph)
    if isinstance(declaration, Mapping) and isinstance(declaration.get("bindings"), list):
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
        raise ValueError("bind must use a declarative selector or explicit bindings")
    bindings = tuple(item if isinstance(item, BindingSpec) else _binding(item) for item in resolved)
    if not bindings and not (isinstance(declaration, Mapping) and "selector" in declaration):
        raise ValueError("binding declaration resolved to no bindings")
    return bindings
