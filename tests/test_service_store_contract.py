"""Shared durable service hint contract."""
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pyarrow as pa

from acquirium.Materialization.services import ChangeHint
from acquirium.Materialization.definitions import MaterializationDefinition
from acquirium.Storage.publication.types import PublicationRequest, MUTATION_SCHEMA
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.support_duckdb import MaterializationSupportDuckDB
from acquirium.Storage.materialization.support_postgres import MaterializationSupportPostgres


@pytest.fixture(params=["duckdb", "postgres"])
def service_store(request, tmp_path, pg_dsn):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "services.duckdb", recreate=True)
        try: yield MaterializationSupportDuckDB(store)
        finally: store.close()
    else:
        try: runtime = MaterializationSupportPostgres(pg_dsn)
        except Exception as error: pytest.skip(f"PostgreSQL unavailable: {error}")
        try: yield runtime
        finally: runtime.close()


def test_service_hints_coalesce_and_are_at_least_once(service_store):
    name = f"dashboard-{uuid4()}"
    definition = MaterializationDefinition(name, "digest", "tests.test_service_store_contract:Dashboard", kind="service")
    service_store.register_definition(definition)
    service_store.register_service(name, definition.definition_id)
    service_store.set_service_status(name, "running")
    now = datetime.now(timezone.utc)
    service_store.coalesce_service_hint(ChangeHint(name, "first", {"urn:in": 1}, 4, now))
    service_store.coalesce_service_hint(ChangeHint(name, "latest", {"urn:in": 3, "urn:other": 2}, 5, now))
    hint = service_store.next_service_hint(name)
    assert hint is not None and hint.token == "latest" and hint.data_versions == {"urn:in": 3, "urn:other": 2}
    assert service_store.next_service_hint(name).token == "latest"
    service_store.acknowledge_service_hint(name, "wrong-token")
    assert service_store.next_service_hint(name) is not None
    service_store.acknowledge_service_hint(name, "latest")
    assert service_store.next_service_hint(name) is None
    assert name not in service_store.services_needing_hint({"urn:in": 3, "urn:other": 2}, 5)
    assert name in service_store.services_needing_hint({"urn:in": 4, "urn:other": 2}, 5)


def test_service_hint_coalescing_merges_disjoint_stream_versions(service_store):
    name = f"dashboard-{uuid4()}"
    definition = MaterializationDefinition(name, "digest", "tests.test_service_store_contract:Dashboard", kind="service")
    service_store.register_definition(definition)
    service_store.register_service(name, definition.definition_id)
    now = datetime.now(timezone.utc)
    service_store.coalesce_service_hint(ChangeHint(name, "a", {"urn:a": 3}, 1, now))
    service_store.coalesce_service_hint(ChangeHint(name, "b", {"urn:b": 4}, 2, now))
    hint = service_store.next_service_hint(name)
    assert hint is not None
    assert hint.token == "b" and hint.data_versions == {"urn:a": 3, "urn:b": 4} and hint.graph_revision == 2


def test_service_input_snapshot_reads_current_canonical_values(tmp_path):
    store = DuckDBStore(tmp_path / "service-snapshot.duckdb", recreate=True)
    try:
        runtime = MaterializationSupportDuckDB(store)
        mutations = pa.table({"operation": ["upsert"], "ref_uri": ["urn:input"],
            "ts": pa.array([datetime(2026, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
            "numeric_value": [42.0], "text_value": [None]}, schema=MUTATION_SCHEMA)
        PublicationDuckDB(store).publish(PublicationRequest("service-snapshot", mutations))
        versions, values = runtime.service_input_snapshot(("urn:input",))
        assert versions == {"urn:input": 1}
        assert values.to_pylist() == [{"ref_uri": "urn:input", "ts": datetime(2026, 1, 1, tzinfo=timezone.utc),
                                       "numeric_value": 42.0, "text_value": None}]
    finally:
        store.close()
