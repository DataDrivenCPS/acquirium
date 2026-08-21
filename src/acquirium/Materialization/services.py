"""Durable service declarations and coalesced change hints."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping


@dataclass(frozen=True)
class ChangeHint:
    service_name: str
    token: str
    data_versions: Mapping[str, int]
    graph_revision: int | None
    created_at: datetime
