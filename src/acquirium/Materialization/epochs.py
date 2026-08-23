"""Durable topology-epoch value objects.

An epoch is the unit of desired materialized state.  These objects deliberately
contain resolved stream identities; a worker can execute one without access to
the graph store or a late-binding selector.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping

import pyarrow as pa

from acquirium.Materialization.impact import TimeRange


EpochStatus = Literal["constructing", "ready", "reconciling", "active", "superseded", "failed", "compacted"]
ComponentStatus = Literal["pending", "sealed", "superseded"]
WorkStatus = Literal["pending", "claimed", "committed", "failed", "superseded"]


@dataclass(frozen=True)
class EpochDefinition:
    definition_id: str
    name: str
    source_digest: str
    entrypoint: str
    kind: str
    spec: Mapping[str, Any]


@dataclass(frozen=True)
class EpochBinding:
    epoch_id: str
    binding_id: str
    definition_id: str
    logical_key: str
    content_digest: str
    inputs: Mapping[str, tuple[str, ...]]
    outputs: Mapping[str, tuple[str, ...]]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    state_revision: str | None = None

    @property
    def input_refs(self) -> tuple[str, ...]:
        return tuple(sorted({ref for refs in self.inputs.values() for ref in refs}))

    @property
    def output_refs(self) -> tuple[str, ...]:
        return tuple(sorted({ref for refs in self.outputs.values() for ref in refs}))


@dataclass(frozen=True)
class EpochComponent:
    epoch_id: str
    component_id: str
    binding_ids: tuple[str, ...]
    status: ComponentStatus = "pending"


@dataclass(frozen=True)
class EpochWork:
    work_id: str
    epoch_id: str
    component_id: str
    binding_id: str
    interval: TimeRange
    input_versions: Mapping[str, int]
    upstream_frontier: Mapping[str, str]
    binding_digest: str
    status: WorkStatus = "pending"
    attempt: int = 0


@dataclass(frozen=True)
class EpochClaim:
    claim_id: str
    kind: str
    target_id: str
    owner: str
    attempt: int
    expires_at: datetime


@dataclass(frozen=True)
class EpochSnapshot:
    work: EpochWork
    binding: EpochBinding
    definition: EpochDefinition
    inputs: pa.Table
    input_versions: Mapping[str, int]


@dataclass(frozen=True)
class EpochSummary:
    epoch_id: str
    graph_revision: int
    graph_digest: str
    status: EpochStatus
    component_count: int
    sealed_component_count: int


class StaleEpochError(RuntimeError):
    """The desired topology or one of its immutable inputs is no longer current."""


class EpochClaimError(RuntimeError):
    """A control-plane claim is absent, expired, or owned by another attempt."""


def table_from_rows(rows: list[dict[str, Any]]) -> pa.Table:
    """Return the canonical transform input shape, including empty batches."""
    schema = pa.schema([
        ("operation", pa.string()),
        ("ref_uri", pa.string()),
        ("ts", pa.timestamp("us", tz="UTC")),
        ("numeric_value", pa.float64()),
        ("text_value", pa.string()),
    ])
    return pa.Table.from_pylist(rows, schema=schema)
