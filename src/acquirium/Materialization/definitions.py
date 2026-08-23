"""Immutable, deterministic transformation definition bundles."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
import inspect
import json
from pathlib import Path
from typing import Any, Literal, Mapping

from acquirium.Materialization.impact import ImpactPolicy


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _persistable(value: object) -> object:
    """Convert definition metadata to stable JSON without losing mappings."""
    if isinstance(value, Mapping):
        return {str(key): _persistable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_persistable(item) for item in value]
    if hasattr(value, "__dataclass_fields__"):
        return _persistable(asdict(value))
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def definition_spec(definition: "MaterializationDefinition") -> dict[str, object]:
    """The durable, JSON-compatible portion of an immutable definition."""
    return {
        "execution": definition.execution,
        "inputs": _persistable(definition.inputs),
        "bind": _persistable(definition.bind),
        "outputs": _persistable(definition.outputs),
        "impact": definition.impact.to_json() if definition.impact else None,
        "parameters_schema": _persistable(definition.parameters_schema),
    }


def source_digest(target: object) -> str:
    """Digest the importable executable module and qualified entrypoint."""
    module_name = getattr(target, "__module__", "")
    qualname = getattr(target, "__qualname__", target.__class__.__qualname__)
    module = inspect.getmodule(target)
    module_file = getattr(module, "__file__", None)
    if module_file:
        try:
            content = Path(module_file).read_bytes()
        except OSError:
            content = b""
        if content:
            return sha256(
                module_name.encode() + b":" + qualname.encode() + b"\0" + content
            ).hexdigest()
    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):
        source = qualname
    return sha256(f"{module_name}:{qualname}\n{source}".encode()).hexdigest()


@dataclass(frozen=True)
class MaterializationDefinition:
    name: str
    source_digest: str
    entrypoint: str
    kind: Literal["transformation", "experiment", "service"] = "transformation"
    execution: Literal["batch", "scalar"] = "batch"
    inputs: object | None = None
    bind: object | None = None
    outputs: object | None = None
    impact: ImpactPolicy | None = None
    parameters_schema: Mapping[str, Any] = field(default_factory=dict)

    @property
    def definition_id(self) -> str:
        payload = asdict(self)
        if self.impact is not None:
            payload["impact"] = self.impact.to_json()
        return sha256(_json(payload).encode()).hexdigest()


def definition_for(target: object, **kwargs: Any) -> MaterializationDefinition:
    name = kwargs.pop("name", getattr(target, "__name__", target.__class__.__name__))
    entrypoint = f"{getattr(target, '__module__', '')}:{getattr(target, '__qualname__', target.__class__.__qualname__)}"
    return MaterializationDefinition(name=name, source_digest=source_digest(target), entrypoint=entrypoint, **kwargs)
