"""Durable idempotent external-effect intents."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import Mapping


@dataclass(frozen=True)
class EffectIntent:
    effect_id: str
    execution_id: str
    kind: str
    destination: str
    payload: Mapping[str, object]
    idempotency_key: str
    status: str = "pending"
    attempts: int = 0
    next_attempt_at: datetime | None = None
    error: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if not all((self.effect_id, self.execution_id, self.kind, self.destination, self.idempotency_key)):
            raise ValueError("effect intent identity fields are required")
        try: json.dumps(dict(self.payload), sort_keys=True)
        except (TypeError, ValueError) as error: raise ValueError("effect payload must be JSON-serializable") from error
