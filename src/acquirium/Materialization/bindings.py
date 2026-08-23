"""Stable, storage-independent materialization binding specifications."""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
from typing import Any, Mapping


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
