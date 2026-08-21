"""Immutable data supplied to materialization compute code."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping
import pyarrow as pa
from acquirium.Materialization.impact import TimeRange

@dataclass(frozen=True)
class TransformContext:
    binding_id: str
    execution_id: str
    interval: TimeRange
    input_versions: Mapping[str, int]
    metadata: Mapping[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class ComputeRequest:
    inputs: pa.Table
    context: TransformContext
    output_refs: frozenset[str]
    scalar: bool = False
