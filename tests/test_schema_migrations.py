"""Versioned schema upgrades preserve data on both storage backends."""
from datetime import datetime, timedelta, timezone

import pytest

from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.migrations import Migration, migrate
from acquirium.Storage.timescale_store import TimescaleStore


def reopen(store, *, recreate=False):
    if isinstance(store, DuckDBStore):
        return DuckDBStore(store.db_path, recreate=recreate)
    return TimescaleStore(dsn=store.dsn, recreate=recreate)


def test_upgrade_integer_keyed_database_preserves_samples(materialization_store):
    store = materialization_store
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ref = str(store.ensure_stream_ref(None, "source", "flow", value_kind="numeric"))
    store.upsert_rows(ref, [(stamp, 3.)], value_kind="numeric")
    # Reproduce the unversioned schema from main, retaining its stream and row.
    with store._lock, store._own_conn() as conn:
        conn.execute("DROP VIEW timeseries_streams")
        conn.execute("DROP INDEX idx_timeseries_last_revision")
        for column in ("deleted", "last_revision"):
            conn.execute(f"ALTER TABLE timeseries DROP COLUMN {column}")
        if isinstance(store, DuckDBStore):
            conn.execute("ALTER TABLE timeseries DROP COLUMN last_stream_version")
        for table in ("stream_resets", "binding_progress", "system_state", "schema_migrations"):
            conn.execute(f"DROP TABLE {table}")
        conn.execute("""CREATE VIEW timeseries_streams AS
            SELECT r.ref_uri, s.point_uri, s.source_id, s.ref_name,
                   COALESCE(s.value_kind, 'text') AS value_kind,
                   t.ts, t.numeric_value AS value_numeric, t.text_value AS value_text
            FROM timeseries t JOIN ref_ids r USING (ref_id)
            LEFT JOIN streams s ON r.ref_uri=s.ref_uri""")
    store.close()
    for attempt in range(2):
        upgraded = reopen(store)
        try:
            with upgraded._own_conn() as conn:
                assert conn.execute("SELECT numeric_value, deleted FROM timeseries ORDER BY ts LIMIT 1").fetchone() == (3., False)
                assert conn.execute("SELECT count(*) FROM timeseries_streams").fetchone()[0] == attempt + 1
                assert conn.execute("SELECT version, name FROM schema_migrations WHERE component='timeseries'").fetchall() == [(1, "publication_columns")]
            upgraded.upsert_rows(ref, [(stamp + timedelta(hours=attempt + 1), 4. + attempt)], value_kind="numeric")
            assert upgraded.timeseries_info(ref).row_count == attempt + 2
        finally:
            upgraded.close()


def test_adopt_already_upgraded_unversioned_schema(materialization_store):
    store = materialization_store
    with store._lock, store._write_conn() as conn:
        conn.execute("DROP TABLE schema_migrations")
    adopted = reopen(store)
    try:
        with adopted._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 1
    finally:
        adopted.close()


def test_failed_migration_rolls_back_schema_and_history(materialization_store):
    store = materialization_store
    backend = "duckdb" if isinstance(store, DuckDBStore) else "postgres"
    first = Migration(1, "create_marker", ("CREATE TABLE migration_marker (value INTEGER)",))
    failed = Migration(2, "populate_marker", ("INSERT INTO missing_migration_table VALUES (1)",))
    with pytest.raises(Exception):
        with store._lock, store._write_conn() as conn:
            migrate(conn, backend=backend, component="test", migrations=(first, failed))
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM schema_migrations WHERE component='test'").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='migration_marker'").fetchone()[0] == 0
    corrected = Migration(2, "populate_marker", ("INSERT INTO migration_marker VALUES (1)",))
    for _ in range(2):
        with store._lock, store._write_conn() as conn:
            migrate(conn, backend=backend, component="test", migrations=(first, corrected))
    with store._own_conn() as conn:
        assert conn.execute("SELECT * FROM migration_marker").fetchall() == [(1,)]


def test_recreate_resets_migration_history(materialization_store):
    store = materialization_store
    backend = "duckdb" if isinstance(store, DuckDBStore) else "postgres"
    with store._lock, store._write_conn() as conn:
        migrate(conn, backend=backend, component="test", migrations=(Migration(1, "marker", ()),))
    recreated = reopen(store, recreate=True)
    try:
        with recreated._own_conn() as conn:
            assert conn.execute("SELECT component, version FROM schema_migrations").fetchall() == [("timeseries", 1)]
    finally:
        recreated.close()
