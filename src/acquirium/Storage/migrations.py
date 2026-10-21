"""Ordered schema migrations shared by DuckDB and PostgreSQL.

Call ``migrate`` inside the store's write transaction, before using the new
schema. Each component owns an append-only sequence of numbered migrations.
DDL and its ledger entries commit together; a failure rolls both back.
"""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
from typing import Any


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    upgrade: tuple[str, ...] | Callable[[Any], None]


def migrate(conn, *, backend: str, component: str, migrations: tuple[Migration, ...]) -> None:
    """Apply pending migrations on a connection with an open transaction."""
    if [migration.version for migration in migrations] != list(range(1, len(migrations) + 1)):
        raise ValueError("migrations must have consecutive versions starting at 1")
    if backend == "postgres":
        # Serialize startup across workers, including creation of the ledger.
        # The schema is part of the key so independent stores do not block.
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':acquirium:migrations', 0))")
    elif backend != "duckdb":
        raise ValueError(f"unsupported migration backend: {backend}")
    conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
        component VARCHAR NOT NULL,
        version INTEGER NOT NULL,
        name VARCHAR NOT NULL,
        applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (component, version)
    )""")
    placeholder = "%s" if backend == "postgres" else "?"
    applied = dict(conn.execute(
        f"SELECT version, name FROM schema_migrations WHERE component={placeholder}", [component]
    ).fetchall())
    known = {migration.version: migration.name for migration in migrations}
    if any(known.get(version) != name for version, name in applied.items()):
        raise RuntimeError(f"incompatible {component} schema migration history")
    for migration in migrations:
        if migration.version in applied:
            continue
        if callable(migration.upgrade):
            migration.upgrade(conn)
        else:
            for statement in migration.upgrade:
                conn.execute(statement)
        conn.execute(
            f"INSERT INTO schema_migrations (component, version, name) VALUES ({placeholder}, {placeholder}, {placeholder})",
            [component, migration.version, migration.name],
        )


# IF NOT EXISTS also adopts databases upgraded before the migration ledger
# existed. Keep these definitions unchanged; append future versions instead.
TIMESERIES_MIGRATIONS = (
    Migration(1, "publication_columns", (
        "ALTER TABLE timeseries ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE timeseries ALTER COLUMN deleted SET NOT NULL",
        "ALTER TABLE timeseries ADD COLUMN IF NOT EXISTS last_revision BIGINT DEFAULT 0",
        "ALTER TABLE timeseries ALTER COLUMN last_revision SET NOT NULL",
    )),
)


def _duckdb_publication_columns(conn) -> None:
    columns = {row[0] for row in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='timeseries'"
    ).fetchall()}
    added = {"deleted": "FALSE", "last_revision": "0", "last_stream_version": "0"}
    if added.keys() <= columns:
        return
    # DuckDB cannot add constrained columns to populated tables atomically.
    # Rebuild inside the migration transaction to retain NOT NULL constraints;
    # a failure restores the original table and its indexes. Startup recreates
    # secondary indexes after migrations, and the view still uses this name.
    conn.execute("""CREATE TABLE timeseries_migration_v1 (
        ref_id INTEGER NOT NULL, ts TIMESTAMP NOT NULL,
        numeric_value DOUBLE, text_value VARCHAR,
        deleted BOOLEAN NOT NULL DEFAULT FALSE,
        last_revision BIGINT NOT NULL DEFAULT 0,
        last_stream_version BIGINT NOT NULL DEFAULT 0,
        CHECK (numeric_value IS NULL OR text_value IS NULL),
        UNIQUE (ref_id, ts)
    )""")
    expressions = [name if name in columns else default for name, default in added.items()]
    conn.execute("INSERT INTO timeseries_migration_v1 SELECT ref_id, ts, numeric_value, text_value, "
                 + ", ".join(expressions) + " FROM timeseries")
    conn.execute("DROP TABLE timeseries")
    conn.execute("ALTER TABLE timeseries_migration_v1 RENAME TO timeseries")


DUCKDB_TIMESERIES_MIGRATIONS = (
    Migration(1, "publication_columns", _duckdb_publication_columns),
)
EXPERIMENT_MIGRATIONS = (
    Migration(1, "observation_keys", (
        "ALTER TABLE experiment_observations ADD COLUMN IF NOT EXISTS key_json VARCHAR",
    )),
)
