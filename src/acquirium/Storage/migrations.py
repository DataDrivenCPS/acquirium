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
EXPERIMENT_MIGRATIONS = (
    Migration(1, "observation_keys", (
        "ALTER TABLE experiment_observations ADD COLUMN IF NOT EXISTS key_json VARCHAR",
    )),
)

# Imported last: the migration imports experiment records only when it runs.
from acquirium.Storage.metadata_outbox import migrate_documents
EXPERIMENT_MIGRATIONS += (Migration(2, "graph_metadata_outbox", migrate_documents),)

EXPERIMENT_MIGRATIONS += (Migration(3, "declaration_upgrade_frontier", (
    "ALTER TABLE experiment_metadata_state ADD COLUMN declaration_format INTEGER DEFAULT 0",
    "ALTER TABLE experiment_metadata_state ADD COLUMN declaration_upgrade_version BIGINT",
)),)

# Samples moved to Escapement. The ledger lists every stream a run may have
# written: a row is added before the first sample and removed only after the
# stream's samples are cleared.
EXPERIMENT_MIGRATIONS += (Migration(4, "experiment_streams", (
    "CREATE TABLE IF NOT EXISTS experiment_streams (ref_uri VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, source_id VARCHAR NOT NULL, ref_name VARCHAR NOT NULL)",
    # The Escapement request sequence each time-series observation's data
    # was accepted at; its epoch is looked up from it. A separate table: DuckDB cannot alter experiment_observations in the
    # transaction where migration 2 updates it.
    "CREATE TABLE IF NOT EXISTS experiment_observation_accepted (run_id VARCHAR NOT NULL, sequence BIGINT NOT NULL, accepted BIGINT NOT NULL, PRIMARY KEY (run_id, sequence))",
)),)
