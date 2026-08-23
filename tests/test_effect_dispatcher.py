from __future__ import annotations

from datetime import timedelta

from acquirium.Materialization.effect_worker import EffectDispatcher
from acquirium.Materialization.effects import EffectIntent
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.support_duckdb import MaterializationSupportDuckDB


def test_effect_dispatcher_retries_with_backoff_then_dead_letters(tmp_path):
    store = MaterializationSupportDuckDB(DuckDBStore(tmp_path / "effects.duckdb", recreate=True))
    store.create_effect_intent(EffectIntent("effect", "execution", "webhook", "https://example.test", {}, "key"))
    attempts: list[int] = []

    def fail(intent):
        attempts.append(intent.attempts)
        raise RuntimeError("unavailable")

    dispatcher = EffectDispatcher(store, fail, max_attempts=2, initial_backoff=timedelta(0))
    assert dispatcher.deliver_once("worker")
    assert store.effect_intent("effect").status == "pending"
    assert dispatcher.deliver_once("worker")
    terminal = store.effect_intent("effect")
    assert attempts == [1, 2]
    assert terminal.status == "dead_letter"
    assert terminal.error == {"type": "RuntimeError", "message": "unavailable"}


def test_expired_effect_lease_is_recovered_and_old_owner_cannot_complete(tmp_path):
    store = MaterializationSupportDuckDB(DuckDBStore(tmp_path / "effects.duckdb", recreate=True))
    store.create_effect_intent(EffectIntent("effect", "execution", "webhook", "https://example.test", {}, "key"))
    first = store.lease_effect_intent("lost", duration=-timedelta(microseconds=1))
    assert first is not None
    replacement = store.lease_effect_intent("replacement")
    assert replacement is not None and replacement.attempts == 2
    try:
        store.complete_effect_intent(first.effect_id, "lost")
    except ValueError:
        pass
    else:
        raise AssertionError("expired lease owner completed an effect")
    store.complete_effect_intent(replacement.effect_id, "replacement")
