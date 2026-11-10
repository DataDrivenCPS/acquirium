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
        raise RuntimeError(
            f"incompatible {component} schema migration history: this database was written "
            "by another version of Acquirium and cannot be upgraded; use a fresh data directory"
        )
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


def _baseline(conn) -> None:
    from uuid import uuid4

    from acquirium.Storage.metadata_outbox import execute
    for statement in _BASELINE:
        execute(conn, statement)
    execute(conn, "INSERT INTO experiment_metadata_state VALUES (?, 0, 0)", [str(uuid4())])


_BASELINE = (
    "CREATE TABLE experiment_templates (template_id VARCHAR PRIMARY KEY, name VARCHAR UNIQUE NOT NULL, created_at TIMESTAMP NOT NULL)",
    "CREATE TABLE experiment_variables (variable_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, label VARCHAR NOT NULL, role VARCHAR NOT NULL, kind VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, UNIQUE(template_id, label))",
    "CREATE TABLE experiment_runs (run_id VARCHAR PRIMARY KEY, template_id VARCHAR NOT NULL, status VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, error_json VARCHAR)",
    "CREATE TABLE experiment_observations (run_id VARCHAR NOT NULL, variable_id VARCHAR, sequence BIGINT NOT NULL, recorded_at TIMESTAMP NOT NULL, occurred_at TIMESTAMP, value_json VARCHAR, artifact_digest VARCHAR, ref_uri VARCHAR, range_start TIMESTAMP, range_end TIMESTAMP, key_json VARCHAR, PRIMARY KEY(run_id, sequence))",
    # The Escapement request sequence each time-series observation's data was
    # accepted at; its epoch is looked up from it.
    "CREATE TABLE experiment_observation_accepted (run_id VARCHAR NOT NULL, sequence BIGINT NOT NULL, accepted BIGINT NOT NULL, PRIMARY KEY (run_id, sequence))",
    "CREATE TABLE experiment_artifacts (digest VARCHAR PRIMARY KEY, filename VARCHAR NOT NULL, media_type VARCHAR, byte_length BIGINT NOT NULL, created_at TIMESTAMP NOT NULL)",
    # Artifact bytes commit with the rows that reference them.
    "CREATE TABLE experiment_artifact_chunks (digest VARCHAR NOT NULL, seq INTEGER NOT NULL, data BYTEA NOT NULL, PRIMARY KEY(digest, seq))",
    # Every stream a run may have written: a row is added before the first
    # sample and removed only after the stream's samples are cleared.
    "CREATE TABLE experiment_streams (ref_uri VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, source_id VARCHAR NOT NULL, ref_name VARCHAR NOT NULL)",
    # Resolved graph writes, delivered in version order.
    "CREATE TABLE experiment_metadata_state (generation VARCHAR NOT NULL, version BIGINT NOT NULL, published_version BIGINT NOT NULL)",
    "CREATE TABLE experiment_metadata_outbox (version BIGINT PRIMARY KEY, update_text VARCHAR NOT NULL)",
    "CREATE INDEX experiment_variables_template_idx ON experiment_variables(template_id)",
    "CREATE INDEX experiment_runs_template_idx ON experiment_runs(template_id, status, started_at)",
    "CREATE INDEX experiment_observations_variable_idx ON experiment_observations(variable_id, run_id, sequence)",
    "CREATE INDEX experiment_observations_ref_idx ON experiment_observations(ref_uri, range_start, range_end)",
)

# Version 1 creates the whole schema. Ledgers from releases before it are not
# upgraded. Keep these definitions unchanged; append future versions instead.
EXPERIMENT_MIGRATIONS = (Migration(1, "baseline", _baseline),)
