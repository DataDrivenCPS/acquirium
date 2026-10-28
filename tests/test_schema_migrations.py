"""Versioned schema upgrades of the experiment ledger on both SQL backends."""
import pytest

from acquirium.Storage.ledger import DuckDBLedger, open_ledger
from acquirium.Storage.migrations import Migration, migrate


def backend(store):
    return "duckdb" if isinstance(store, DuckDBLedger) else "postgres"


def test_failed_migration_rolls_back_schema_and_history(experiment_ledger):
    store = experiment_ledger
    first = Migration(1, "create_marker", ("CREATE TABLE migration_marker (value INTEGER)",))
    failed = Migration(2, "populate_marker", ("INSERT INTO missing_migration_table VALUES (1)",))
    with pytest.raises(Exception):
        with store._lock, store._write_conn() as conn:
            migrate(conn, backend=backend(store), component="test", migrations=(first, failed))
    with store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name IN ('schema_migrations', 'migration_marker')").fetchone()[0] == 0
    corrected = Migration(2, "populate_marker", ("INSERT INTO migration_marker VALUES (1)",))
    for _ in range(2):
        with store._lock, store._write_conn() as conn:
            migrate(conn, backend=backend(store), component="test", migrations=(first, corrected))
    with store._own_conn() as conn:
        assert conn.execute("SELECT * FROM migration_marker").fetchall() == [(1,)]


def test_recreate_resets_migration_history(experiment_ledger):
    store = experiment_ledger
    with store._lock, store._write_conn() as conn:
        migrate(conn, backend=backend(store), component="test", migrations=(Migration(1, "marker", ()),))
    recreated = open_ledger(store.db_path if isinstance(store, DuckDBLedger) else store.dsn, recreate=True)
    try:
        with recreated._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='schema_migrations'").fetchone()[0] == 0
    finally:
        recreated.close()
