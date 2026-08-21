"""Immutable, deterministic transformation definition bundles."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
import inspect
import json
from typing import Any, Literal, Mapping

from acquirium.Materialization.impact import ImpactPolicy


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def source_digest(target: object) -> str:
    """Digest source rather than a mutable object instance or its address."""
    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):
        source = getattr(target, "__qualname__", repr(target))
    module = getattr(target, "__module__", "")
    qualname = getattr(target, "__qualname__", target.__class__.__qualname__)
    return sha256(f"{module}:{qualname}\n{source}".encode()).hexdigest()


@dataclass(frozen=True)
class MaterializationDefinition:
    name: str
    source_digest: str
    entrypoint: str
    kind: Literal["transformation", "experiment", "service"] = "transformation"
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
