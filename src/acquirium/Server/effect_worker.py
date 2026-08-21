"""Server-owned delivery adapter for durable external effects."""
from __future__ import annotations

from urllib.parse import urlparse

import requests

from acquirium.Materialization.effects import EffectIntent


def deliver_effect(intent: EffectIntent) -> None:
    """Deliver the currently supported external effect types.

    The idempotency key is sent on every attempt.  Destinations should retain
    it long enough to collapse retries after a client-side timeout.
    """
    if intent.kind != "webhook":
        raise ValueError(f"unsupported effect kind {intent.kind!r}")
    parsed = urlparse(intent.destination)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("webhook destination must be an absolute http(s) URL")
    payload = dict(intent.payload)
    headers = {str(key): str(value) for key, value in dict(payload.pop("headers", {})).items()}
    headers["Idempotency-Key"] = intent.idempotency_key
    timeout = float(payload.pop("timeout", 5))
    response = requests.post(intent.destination, json=payload, headers=headers, timeout=timeout)
    response.raise_for_status()
