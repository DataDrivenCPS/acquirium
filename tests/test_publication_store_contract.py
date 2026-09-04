"""Revision-frontier publication contract."""
from datetime import datetime, timezone

import pyarrow as pa

from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.publication.revision import RevisionPublisher
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest


def test_publication_assigns_one_new_global_revision_per_write(tmp_path):
    store = DuckDBStore(tmp_path / "publication.duckdb", recreate=True)
    try:
        publisher = RevisionPublisher(store)
        mutations = pa.table({"operation": ["upsert"], "ref_uri": ["urn:input"],
            "ts": pa.array([datetime(2026, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
            "numeric_value": [1.0], "text_value": [None]}, schema=MUTATION_SCHEMA)
        assert publisher.publish(PublicationRequest("publication", mutations)).row_count == 1
        assert publisher.publish(PublicationRequest("publication", mutations)).row_count == 1
        with store._own_conn() as conn:
            assert conn.execute("SELECT current_revision FROM system_state").fetchone() == (2,)
            assert conn.execute("SELECT last_revision FROM timeseries").fetchone() == (2,)
    finally:
        store.close()


def test_publication_rejects_deletion(tmp_path):
    base = {"operation": "delete", "ref_uri": "urn:input",
            "ts": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "numeric_value": None, "text_value": None}
    store = DuckDBStore(tmp_path / "delete.duckdb")
    try:
        try:
            RevisionPublisher(store).publish(PublicationRequest("delete", pa.Table.from_pylist([base], schema=MUTATION_SCHEMA)))
        except ValueError as error:
            assert "deletion" in str(error)
        else:
            raise AssertionError("deletion was accepted")
    finally:
        store.close()


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
