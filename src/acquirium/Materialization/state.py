"""Generic artifact production and optional state-revision contracts."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
import json
from typing import Literal, Mapping

from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.artifacts import ArtifactRecord

PromotionMode = Literal["prospective", "recompute_all", "recompute_from"]

@dataclass(frozen=True)
class ArtifactCandidate:
    data: bytes
    media_type: str = "application/octet-stream"
    metadata: Mapping[str, object] = field(default_factory=dict)
    metrics: Mapping[str, object] = field(default_factory=dict)
    @property
    def digest(self) -> str: return sha256(self.data).hexdigest()

@dataclass(frozen=True)
class ArtifactRequest:
    request_id: str
    kind: str
    deployment_name: str
    binding_id: str
    input_versions: Mapping[str, int]
    interval: TimeRange
    previous_revision: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)
    @property
    def semantic_digest(self) -> str:
        value = {"kind": self.kind, "deployment": self.deployment_name, "binding": self.binding_id,
                 "versions": dict(sorted(self.input_versions.items())), "start": self.interval.start.isoformat(),
                 "end": self.interval.end.isoformat(), "previous": self.previous_revision,
                 "metadata": self.metadata}
        return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

@dataclass(frozen=True)
class ArtifactLease:
    request: ArtifactRequest
    owner: str
    attempt: int
    expires_at: datetime

@dataclass(frozen=True)
class StateRevision:
    revision_id: str
    deployment_name: str
    binding_id: str
    artifact: ArtifactRecord
    status: str
    parent_revision: str | None = None
    policy: PromotionMode | None = None
    effective_from: datetime | None = None
    metrics: Mapping[str, object] = field(default_factory=dict)
