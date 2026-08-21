"""Shared durable effect-intent lifecycle contract."""
from datetime import timedelta
from uuid import uuid4

import pytest

from acquirium.Materialization.effects import EffectIntent
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.materialization.postgres import MaterializationPostgres


@pytest.fixture(params=["duckdb", "postgres"])
def effect_store(request, tmp_path, pg_dsn):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "effects.duckdb", recreate=True)
        try: yield MaterializationDuckDB(store)
        finally: store.close()
    else:
        try: runtime = MaterializationPostgres(pg_dsn)
        except Exception as error: pytest.skip(f"PostgreSQL unavailable: {error}")
        try: yield runtime
        finally: runtime.close()


def test_effect_intents_deduplicate_retry_and_dead_letter(effect_store):
    marker = uuid4().hex
    intent = EffectIntent(f"effect:{marker}", "execution", "webhook", "https://example.test/hook",
                          {"message": "hello"}, f"idempotency:{marker}")
    assert effect_store.create_effect_intent(intent) == intent.effect_id
    assert effect_store.create_effect_intent(EffectIntent(f"duplicate:{marker}", "other", "webhook",
        "https://example.test/hook", {}, intent.idempotency_key)) == intent.effect_id
    lease = effect_store.lease_effect_intent("worker")
    assert lease is not None and lease.attempts == 1
    effect_store.fail_effect_intent(lease.effect_id, {"message": "temporary"}, retry_after=timedelta(0))
    retry = effect_store.lease_effect_intent("replacement")
    assert retry is not None and retry.attempts == 2
    effect_store.complete_effect_intent(retry.effect_id)
    assert effect_store.lease_effect_intent("worker") is None

    dead = EffectIntent(f"dead:{marker}", "execution", "webhook", "https://example.test/dead", {}, f"dead:{marker}")
    effect_store.create_effect_intent(dead)
    leased_dead = effect_store.lease_effect_intent("worker")
    effect_store.fail_effect_intent(leased_dead.effect_id, {"message": "permanent"})
    assert effect_store.lease_effect_intent("worker") is None
