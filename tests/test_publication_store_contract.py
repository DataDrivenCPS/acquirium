"""Canonical publication contract after removal of the legacy app runtime."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from uuid import uuid4

import pyarrow as pa
import pytest

from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.support_duckdb import MaterializationSupportDuckDB
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.postgres import PublicationPostgres
from acquirium.Storage.publication import ids
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationConflict, PublicationRequest


def test_publication_is_idempotent_and_emits_range_manifest(tmp_path):
    store = DuckDBStore(tmp_path / "publication.duckdb", recreate=True)
    try:
        materialization = MaterializationSupportDuckDB(store)
        publisher = PublicationDuckDB(store)
        mutations = pa.table({"operation": ["upsert"], "ref_uri": ["urn:input"],
            "ts": pa.array([datetime(2026, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
            "numeric_value": [1.0], "text_value": [None]}, schema=MUTATION_SCHEMA)
        first = publisher.publish(PublicationRequest("publication", mutations))
        assert first.versions == {"urn:input": 1}
        assert publisher.publish(PublicationRequest("publication", mutations)).deduplicated
        assert materialization.change_ranges("urn:input", after_version=0, through_version=1)
        changed = mutations.set_column(3, "numeric_value", pa.array([2.0], type=pa.float64()))
        try:
            publisher.publish(PublicationRequest("publication", changed))
        except PublicationConflict:
            pass
        else:
            raise AssertionError("conflicting retry was accepted")
    finally:
        store.close()


def test_payload_hash_preserves_null_text_and_operation_identity():
    base = {"operation": "delete", "ref_uri": "urn:input",
            "ts": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "numeric_value": None, "text_value": None}
    null_text = pa.Table.from_pylist([base], schema=MUTATION_SCHEMA)
    empty_text = pa.Table.from_pylist([{**base, "text_value": ""}], schema=MUTATION_SCHEMA)
    distinct_operation = pa.Table.from_pylist([{**base, "operation": "tombstone"}], schema=MUTATION_SCHEMA)
    assert len({ids.payload_hash(null_text), ids.payload_hash(empty_text),
                ids.payload_hash(distinct_operation)}) == 3


def test_postgres_serializes_concurrent_retries_by_publication_id(pg_dsn):
    publisher = None
    try:
        publisher = PublicationPostgres(pg_dsn, min_size=2, max_size=2)
        publisher._pool.wait(timeout=1)
    except Exception as error:
        if publisher is not None:
            publisher.close()
        pytest.skip(f"PostgreSQL unavailable: {error}")
    publication_id = f"concurrent-publication-{uuid4()}"
    mutations = pa.Table.from_pylist([{
        "operation": "upsert", "ref_uri": f"urn:{publication_id}",
        "ts": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "numeric_value": 1.0, "text_value": None,
    }], schema=MUTATION_SCHEMA)
    barrier = Barrier(2)
    def publish_once():
        barrier.wait()
        return publisher.publish(PublicationRequest(publication_id, mutations))
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            receipts = [future.result() for future in (executor.submit(publish_once), executor.submit(publish_once))]
        assert sorted(receipt.deduplicated for receipt in receipts) == [False, True]
        assert receipts[0].versions == receipts[1].versions
    finally:
        publisher.close()


def test_duckdb_schema_does_not_create_retired_app_runtime_tables(tmp_path):
    store = DuckDBStore(tmp_path / "publication.duckdb", recreate=True)
    try:
        with store._own_conn() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
            }
        assert not any(table.startswith("app_") for table in tables)
    finally:
        store.close()
