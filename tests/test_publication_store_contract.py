"""Revision-frontier publication contract."""
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.publication.revision import RevisionPublisher
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest
from tests.test_timeseries_store_contract import _snapshot


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


def test_duckdb_schema_contains_revision_frontier_tables(tmp_path):
    store = DuckDBStore(tmp_path / "publication.duckdb", recreate=True)
    try:
        with store._own_conn() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
            }
        assert {"binding_progress", "system_state", "timeseries", "streams"} <= tables
    finally:
        store.close()


def test_replacement_publication_validation_and_empty(materialization_store):
    store = materialization_store
    publisher = RevisionPublisher(store)
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = dict(operation='upsert', ref_uri='urn:input', ts=stamp, numeric_value=2., text_value=None)
    def request(rows):
        return PublicationRequest('replacement', pa.Table.from_pylist(rows, schema=MUTATION_SCHEMA))
    assert publisher.replace(request([row, dict(row, numeric_value=3.)]), 'urn:input').row_count == 1
    original = _snapshot(store)
    for invalid in [dict(row, operation='delete'), dict(row, ref_uri='urn:other'),
                    dict(row, ts=None), dict(row, text_value='invalid'), dict(row, operation=None)]:
        with pytest.raises(ValueError):
            publisher.replace(request([invalid, row]), 'urn:input')
        assert _snapshot(store) == original
    receipt = publisher.replace(request([]), 'urn:input')
    assert receipt.publication_id == 'replacement'
    assert receipt.row_count == 0
    assert not list(store.timeseries('urn:input'))
    assert _snapshot(store)[0] == original[0] + 1


def test_http_replacement_preserves_ingestion_contract(materialization_store, monkeypatch):
    from fastapi.testclient import TestClient
    from acquirium.Server.app import app
    from acquirium.Server.manager import Manager

    store = materialization_store
    ref = str(store.ensure_stream_ref(None, 'source', 'sensor', value_kind='numeric'))
    manager = Manager.__new__(Manager)
    manager.timeseries_store = store
    manager.publication = RevisionPublisher(store)
    monkeypatch.setattr(app.state, 'manager', manager, raising=False)
    client = TestClient(app)
    def send(values, replace=False):
        return client.post('/insert_timeseries', json=[dict(source_id='source', ref_name='sensor',
                          values=values, replace=replace)])
    a, b = '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z'
    assert send([[a, 1.], [b, 2.]], True).json() == {'ok': True, 'rows_inserted': 2}
    assert send([[a, 3.]]).status_code == 200
    assert store.timeseries_info(ref).row_count == 2
    assert send([[b, 'Manual Control'], [b, 4.]], True).json() == {'ok': True, 'rows_inserted': 1}
    original = _snapshot(store)
    assert send([['invalid timestamp', 1.]], True).status_code == 422
    assert _snapshot(store) == original
    assert send([], True).json() == {'ok': True, 'rows_inserted': 0}
    assert store.timeseries_info(ref).row_count == 0
