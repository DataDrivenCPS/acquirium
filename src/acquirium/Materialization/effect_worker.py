"""Bounded durable delivery loop for external-effect intents."""
from __future__ import annotations

from datetime import timedelta
from typing import Callable, Protocol

from acquirium.Materialization.effects import EffectIntent


class EffectSender(Protocol):
    def __call__(self, intent: EffectIntent) -> None: ...


class EffectDispatcher:
    """Deliver one leased effect at a time with durable retry state.

    The destination must honour ``intent.idempotency_key``.  This provides
    at-least-once transport while making retries safe for the common HTTP and
    queue destinations that expose an idempotency header/key.
    """
    def __init__(self, storage: object, sender: EffectSender, *, max_attempts: int = 8,
                 initial_backoff: timedelta = timedelta(seconds=1)) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if initial_backoff < timedelta(0):
            raise ValueError("initial_backoff must not be negative")
        self._storage = storage
        self._sender = sender
        self._max_attempts = max_attempts
        self._initial_backoff = initial_backoff

    def deliver_once(self, owner: str) -> bool:
        intent = self._storage.lease_effect_intent(owner)
        if intent is None:
            return False
        try:
            self._sender(intent)
        except Exception as error:
            payload = {"type": type(error).__name__, "message": str(error)}
            if intent.attempts >= self._max_attempts:
                self._storage.fail_effect_intent(intent.effect_id, owner, payload)
            else:
                self._storage.fail_effect_intent(
                    intent.effect_id, owner, payload,
                    retry_after=self._initial_backoff * (2 ** (intent.attempts - 1)),
                )
            return True
        self._storage.complete_effect_intent(intent.effect_id, owner)
        return True
