"""Dedicated in-process runtime for persistent service definitions."""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from acquirium.Materialization.effects import EffectIntent
from acquirium.Materialization.services import ChangeHint
from acquirium.Materialization.worker import load_entrypoint

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServiceContext:
    """Restricted service facade for one coalesced change notification.

    Services receive an effect-only write capability.  They deliberately do
    not receive a transformation output writer, so a persistent dashboard or
    controller cannot publish into materializer-owned derived streams.
    """
    service_name: str
    change: ChangeHint
    _storage: Any
    _snapshot: Callable[..., object]

    def snapshot(self, refs: tuple[str, ...] | list[str], *, since: "datetime | None" = None) -> object:
        """Read authoritative rows for each stream through the snapshot API.

        By default returns only the latest row of each stream. Pass ``since``
        to read every live row at or after that event time instead — a rolling
        window or the full retained history.
        """
        return self._snapshot(tuple(refs), since=since)

    def emit_effect(self, *, effect_id: str, kind: str, destination: str,
                    payload: Mapping[str, object], idempotency_key: str) -> str:
        return self._storage.create_effect_intent(EffectIntent(
            effect_id, self.change.token, kind, destination, payload, idempotency_key,
        ))


class ServiceSupervisor:
    """Run each logical service on a dedicated, bounded service executor."""
    def __init__(self, storage: Any, snapshot: Callable[[tuple[str, ...]], object], *, workers: int = 2) -> None:
        self._storage = storage
        self._snapshot = snapshot
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="acquirium-service")
        self._instances: dict[str, tuple[str, object]] = {}

    def start(self, name: str):
        record = self._storage.service(name)
        self._storage.service_definition(record.definition_id)
        return self._storage.set_service_status(name, "running", "healthy")

    def stop(self, name: str):
        self._instances.pop(name, None)
        return self._storage.set_service_status(name, "stopped", "unknown")

    def restore(self) -> None:
        """Discard process-local instances; durable running records resume work."""
        self._instances.clear()

    def run_once(self, name: str) -> bool:
        record = self._storage.service(name)
        if record.status != "running":
            return False
        hint = self._storage.next_service_hint(name)
        if hint is None:
            return False
        try:
            instance = self._instance(record.name, record.definition_id)
            context = ServiceContext(name, hint, self._storage, self._snapshot)
            callback = getattr(instance, "on_change", None)
            if callback is None:
                raise TypeError("service definition must provide on_change(change, context)")
            result = self._executor.submit(callback, hint, context).result()
            if asyncio.iscoroutine(result):
                asyncio.run(result)
        except Exception as error:
            # A crashing on_change is a bug in the service, not a transient
            # delivery, so stop it loudly instead of re-running it every tick.
            # The hint stays un-acked, so an explicit restart redelivers it;
            # durable external side effects belong in effect intents, which
            # already back off and retry.
            log.exception("service %s on_change failed; marking it failed", name)
            self._storage.set_service_status(name, "failed", f"on_change raised {type(error).__name__}")
            return False
        self._storage.acknowledge_service_hint(name, hint.token)
        self._storage.set_service_status(name, "running", "healthy")
        return True

    def run_next(self) -> bool:
        for record in self._storage.services(status="running"):
            if self.run_once(record.name):
                return True
        return False

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _instance(self, name: str, definition_id: str) -> object:
        existing = self._instances.get(name)
        if existing is not None and existing[0] == definition_id:
            return existing[1]
        definition = self._storage.service_definition(definition_id)
        target = load_entrypoint(definition["entrypoint"])
        instance = target() if isinstance(target, type) else target
        self._instances[name] = (definition_id, instance)
        return instance
