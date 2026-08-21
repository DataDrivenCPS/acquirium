"""Shared durable service hint contract."""
from datetime import datetime, timezone

import pytest

from acquirium.Materialization.services import ChangeHint
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.materialization.postgres import MaterializationPostgres


@pytest.fixture(params=["duckdb", "postgres"])
def service_store(request, tmp_path, pg_dsn):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "services.duckdb", recreate=True)
        try: yield MaterializationDuckDB(store)
        finally: store.close()
    else:
        try: runtime = MaterializationPostgres(pg_dsn)
        except Exception as error: pytest.skip(f"PostgreSQL unavailable: {error}")
        try: yield runtime
        finally: runtime.close()


def test_service_hints_coalesce_and_are_at_least_once(service_store):
    service_store.register_service("dashboard", "definition")
    service_store.set_service_status("dashboard", "running")
    now = datetime.now(timezone.utc)
    service_store.coalesce_service_hint(ChangeHint("dashboard", "first", {"urn:in": 1}, 4, now))
    service_store.coalesce_service_hint(ChangeHint("dashboard", "latest", {"urn:in": 3, "urn:other": 2}, 5, now))
    hint = service_store.next_service_hint("dashboard")
    assert hint is not None and hint.token == "latest" and hint.data_versions == {"urn:in": 3, "urn:other": 2}
    assert service_store.next_service_hint("dashboard").token == "latest"
    service_store.acknowledge_service_hint("dashboard", "wrong-token")
    assert service_store.next_service_hint("dashboard") is not None
    service_store.acknowledge_service_hint("dashboard", "latest")
    assert service_store.next_service_hint("dashboard") is None
