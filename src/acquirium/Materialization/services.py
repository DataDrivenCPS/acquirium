"""Durable service declarations and coalesced change hints."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from typing import Literal, Mapping
import pyarrow as pa


ServiceStatus = Literal["registered", "running", "stopped", "failed"]


@dataclass(frozen=True)
class ChangeHint:
    service_name: str
    token: str
    data_versions: Mapping[str, int]
    graph_revision: int | None
    created_at: datetime


@dataclass(frozen=True)
class ServiceRecord:
    name: str
    definition_id: str
    status: ServiceStatus
    health: str
    updated_at: datetime


@dataclass(frozen=True)
class ServiceSnapshot:
    """An Arrow read together with the authoritative version vector it observed."""
    token: str
    data_versions: Mapping[str, int]
    graph_revision: int | None
    inputs: pa.Table


def snapshot_token(data_versions: Mapping[str, int], graph_revision: int | None) -> str:
    payload = json.dumps({"data_versions": dict(sorted(data_versions.items())), "graph_revision": graph_revision},
                         sort_keys=True, separators=(",", ":"))
    return sha256(payload.encode()).hexdigest()
