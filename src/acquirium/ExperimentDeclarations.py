"""Shared variable declaration validation and legacy document normalization.

Known semantic/index/source fields remain explicit. Other legacy descriptive
fields move under ``metadata``; conflicting duplicate values require repair.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field, replace
import json
from typing import Any

from acquirium.ExperimentValues import KINDS, to_json

FORMAT = "_declaration_format"
FORMAT_VERSION = 1
# These are properties of a declaration, rather than arbitrary user fields.
FIELDS = frozenset({"unit", "quantity_kind", "type", "substance", "process", "role_type",
    "keys", "observed", "observed_by_key", "source_path", "source_unit", "description", "media_type",
    "_resolved_semantics"})


def stream_keys(kind: str, metadata: Mapping) -> tuple[str, ...]:
    from acquirium.Client.explore.attributes import REGISTRY, check_key
    from acquirium.Client.explore.relations import RELATIONS
    from acquirium.Client.metadata import STREAM_FIELDS
    keys = metadata.get("keys", ())
    if not isinstance(keys, (tuple, list)) or any(not isinstance(key, str) for key in keys):
        raise ValueError("keys must be a sequence of index names")
    if keys and kind != "timeseries":
        raise ValueError("only time-series variables accept keys")
    reserved = set(REGISTRY) | set(RELATIONS) | set(STREAM_FIELDS) | {"study_id", "run_id", "variable_id", "observed"}
    for key in keys:
        check_key(key)
        if key in reserved:
            raise ValueError(f"index name {key!r} is reserved stream metadata")
    if len(set(keys)) != len(keys):
        raise ValueError("index names must be unique")
    return tuple(keys)


def stream_key(key: Any, keys: tuple[str, ...]) -> tuple:
    if not isinstance(key, (tuple, list)) or len(key) != len(keys):
        raise ValueError(f"expected a key tuple with {len(keys)} entries")
    key = to_json(list(key), path="key")
    if any(value is None or not isinstance(value, (str, bool, int, float)) for value in key):
        raise ValueError("stream keys must contain strings, numbers, or booleans")
    return tuple(key)


def normalize_metadata(values: Mapping[str, Any]) -> dict:
    """Convert legacy fields losslessly, rejecting conflicting user values."""
    from acquirium.Client.explore.attributes import REGISTRY
    values = deepcopy(dict(values))
    adapter_format = values.pop("_adapter_format", None)
    if adapter_format == 1:
        # Format 1's adapter_metadata was its authoritative user object; the
        # metadata field was a disposable projection, not a second user object.
        values["metadata"] = json.loads(values.pop("adapter_metadata"))
    values.pop(FORMAT, None)
    user = values.pop("metadata", {})
    if not isinstance(user, Mapping):
        raise TypeError("metadata must be a string-keyed JSON object")
    user = dict(user)
    explicit, conflicts = {}, []
    for key, value in values.items():
        if key in FIELDS or key in REGISTRY:
            if value is not None and value != [] and value != {}:
                explicit[key] = value
        elif key in user and json.dumps(to_json(user[key]), sort_keys=True) != json.dumps(to_json(value), sort_keys=True):
            conflicts.append(key)
        else:
            user[key] = value
    if conflicts:
        raise ValueError(f"conflicting legacy declaration metadata fields: {', '.join(sorted(conflicts))}; repair before upgrading")
    return {FORMAT: FORMAT_VERSION, **explicit, "metadata": to_json(user, path="metadata")}


@dataclass(frozen=True)
class Declaration:
    label: str
    kind: str
    unit: str | None = None
    keys: tuple[str, ...] = ()
    observed: Mapping[tuple, str] | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    role: str = "output"
    quantity_kind: str | None = None
    source_path: str | None = None
    description: str | None = None
    source_unit: str | None = None
    variable_id: str | None = None
    resolved: Mapping[str, Any] = field(default_factory=dict)
    media_type: str | None = None
    observed_by_key: tuple[dict, ...] = ()
    semantic_fields: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_document(cls, label, role, kind, document, *, variable_id=None):
        """Decode saved documents, accepting descriptive graph edits as input."""
        from acquirium.Client.explore.attributes import REGISTRY
        values = deepcopy(document)
        if values.keys() - FIELDS - REGISTRY.keys() - {FORMAT, "metadata"}:
            values = normalize_metadata(values)
        return cls(label, kind, role=role, variable_id=variable_id,
            keys=tuple(values.pop("keys", ())), observed=values.pop("observed", None),
            observed_by_key=tuple(values.pop("observed_by_key", ())),
            metadata=values.pop("metadata", {}), resolved=values.pop("_resolved_semantics", {}),
            **{name: values.pop(name, None) for name in _PROPERTIES},
            semantic_fields={k: v for k, v in values.items() if k != FORMAT})

    def document(self):
        """Serialize a declaration without validation or legacy conversion."""
        values = {**self.semantic_fields, **{name: getattr(self, name) for name in _PROPERTIES},
                  "keys": list(self.keys), "observed_by_key": list(self.observed_by_key),
                  "observed": None if isinstance(self.observed, Mapping) else getattr(self.observed, "uri", self.observed),
                  "_resolved_semantics": self.resolved}
        return deepcopy({FORMAT: FORMAT_VERSION,
            **{k: v for k, v in values.items() if v is not None and v != [] and v != {}},
            "metadata": self.metadata})

    def equivalent(self, role, kind, document):
        """Compare declarations independently of server-resolved semantic terms."""
        def identity(values):
            return json.dumps({k: v for k, v in values.items() if k != "_resolved_semantics"}, sort_keys=True)
        return (self.role, self.kind, identity(self.document())) == (role, kind, identity(document))

    def resolve(self, resolve):
        from acquirium.Client.explore.attributes import REGISTRY
        from acquirium.Client.metadata import is_uri
        resolved = dict(self.resolved)
        for name, value in {**self.semantic_fields, **{name: getattr(self, name) for name in _PROPERTIES}}.items():
            attr = REGISTRY.get(name)
            if attr is None or attr.literal or value is None or value == [] or value == {}:
                continue
            def term(item):
                if item is None:
                    return None
                uri = str(item) if is_uri(item) else resolve(str(item), attr.kind)
                if uri is None:
                    raise ValueError(f"could not resolve {name} {item!r}")
                return str(uri)
            resolved[name] = [term(v) for v in value] if isinstance(value, (list, tuple)) else term(value)
        return replace(self, resolved=resolved)


_PROPERTIES = ("unit", "quantity_kind", "source_path", "description", "source_unit", "media_type")


def validate_declaration(component: Declaration, *, model=False, metadata_is_json=False) -> Declaration:
    if not isinstance(component.label, str) or not component.label.strip():
        raise ValueError("a non-empty label is required")
    if component.role not in ({"input", "output"} if model else {"input", "output", "annotation"}):
        raise ValueError("adapter declarations must be inputs or outputs" if model else "a valid role is required")
    allowed = {"scalar", "text", "json", "table", "timeseries"} if model else KINDS
    if component.kind not in allowed:
        raise ValueError(f"unsupported model component kind {component.kind!r}" if model else f"kind must be one of {', '.join(sorted(KINDS))}")
    keys = stream_keys(component.kind, {"keys": component.keys})
    payload = component.metadata if metadata_is_json else to_json(component.metadata, path=f"{component.label}.metadata")
    if not isinstance(payload, dict):
        raise TypeError("metadata must be a string-keyed JSON object")
    mappings = component.observed_by_key
    if not mappings and isinstance(component.observed, Mapping):
        mappings = [{"key": list(stream_key(key, keys)), "observed": str(uri)} for key, uri in component.observed.items()]
        mappings.sort(key=lambda item: json.dumps(item["key"], sort_keys=True))
    else:
        mappings = [{**item, "key": list(stream_key(item.get("key", ()), keys))} for item in mappings]
        for item in mappings:
            if not isinstance(item.get("observed"), str):
                raise ValueError("observed properties must be URI strings")
    observed = component.observed
    if observed is not None and not isinstance(observed, Mapping):
        observed = str(getattr(observed, "uri", observed))
    return replace(component, keys=keys, metadata=payload, observed=observed, observed_by_key=tuple(mappings))


def prepare_declaration(label, role, kind, values, *, variable_id=None, resolve=None):
    """Normalize and validate external input, including legacy declarations."""
    component = validate_declaration(Declaration.from_document(label, role, kind,
        normalize_metadata(values), variable_id=variable_id), metadata_is_json=True)
    return component.resolve(resolve) if resolve is not None else component
