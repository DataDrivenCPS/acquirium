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


def test_batch_reset_uses_one_revision_and_joins_caller_transaction(materialization_store):
    from acquirium.Storage.metadata_outbox import execute
    store = materialization_store
    publisher = RevisionPublisher(store)
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    refs = [str(store.ensure_stream_ref(None, "reset-test", name)) for name in ("a", "b", "other")]
    for ref in refs:
        store.upsert_rows(ref, [(stamp, ref)])
    # Tombstones must be physically cleared, although deletion counts exclude them.
    with store._lock, store._write_conn() as conn:
        execute(conn, "UPDATE timeseries SET deleted=TRUE WHERE ref_id=(SELECT ref_id FROM ref_ids WHERE ref_uri=?)", [refs[1]])
    before = _snapshot(store)
    with store._lock, store._write_conn() as conn:
        assert publisher.reset(conn, []) is None
    assert _snapshot(store) == before
    with pytest.raises(RuntimeError, match="caller failure"):
        with store._lock, store._write_conn() as conn:
            revision = publisher.reset(conn, [*refs[:2], refs[0]])
            assert revision == before[0] + 1
            assert execute(conn, "SELECT count(*) FROM timeseries").fetchone()[0] == 1
            raise RuntimeError("caller failure")
    assert _snapshot(store) == before
    with store._lock, store._write_conn() as conn:
        revision = publisher.reset(conn, [*refs[:2], refs[0]])
    after = _snapshot(store)
    assert after[0] == before[0] + 1 == revision
    assert after[2] == sorted((ref, revision) for ref in refs[:2])
    assert len(after[1]) == 1
    assert store.timeseries_info(refs[2]).row_count == 1
    with store._own_conn() as conn:
        assert execute(conn, "SELECT count(*) FROM streams WHERE source_id=?", ["reset-test"]).fetchone()[0] == 3


def test_publication_preserves_instants_in_non_utc_session(materialization_store):
    store = materialization_store
    if not isinstance(store, DuckDBStore):
        store.conn.execute("SET TIME ZONE 'America/Denver'")
    # Both sides of the spring transition and the repeated autumn hour.
    stamps = [datetime(2026, 3, 8, hour, tzinfo=timezone.utc) for hour in (8, 9)]
    stamps += [datetime(2026, 11, 1, hour, tzinfo=timezone.utc) for hour in (7, 8)]
    rows = [dict(operation="upsert", ref_uri="urn:timezone", ts=stamp,
                 numeric_value=float(index), text_value=None) for index, stamp in enumerate(stamps)]
    publisher = RevisionPublisher(store)
    request = PublicationRequest("timezone", pa.Table.from_pylist(rows, schema=MUTATION_SCHEMA))
    for write in (lambda: publisher.publish(request), lambda: publisher.replace(request, "urn:timezone")):
        assert write().row_count == 4
        actual = [stamp for batch in store.timeseries("urn:timezone") for stamp in batch.column("ts").to_pylist()]
        assert sorted(actual) == stamps


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
