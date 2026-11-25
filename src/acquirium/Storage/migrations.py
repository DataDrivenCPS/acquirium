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


def _duckdb_drop_timeseries_unique(conn) -> None:
    unique = conn.execute(
        "SELECT count(*) FROM duckdb_constraints() WHERE schema_name=current_schema() "
        "AND table_name='timeseries' AND constraint_type='UNIQUE'"
    ).fetchone()[0]
    if not unique:
        return
    # DuckDB cannot drop a constraint in place, so the table is rebuilt inside
    # the migration transaction. Startup recreates secondary indexes after
    # migrations, and the view still uses this name.
    columns = "ref_id, ts, numeric_value, text_value, deleted, last_stream_version, last_revision"
    conn.execute("""CREATE TABLE timeseries_migration_v2 (
        ref_id INTEGER NOT NULL, ts TIMESTAMP NOT NULL,
        numeric_value DOUBLE, text_value VARCHAR,
        deleted BOOLEAN NOT NULL DEFAULT FALSE,
        last_stream_version BIGINT NOT NULL DEFAULT 0,
        last_revision BIGINT NOT NULL DEFAULT 0,
        CHECK (numeric_value IS NULL OR text_value IS NULL)
    )""")
    conn.execute(f"INSERT INTO timeseries_migration_v2 ({columns}) SELECT {columns} FROM timeseries")
    conn.execute("DROP TABLE timeseries")
    conn.execute("ALTER TABLE timeseries_migration_v2 RENAME TO timeseries")


def _columns(conn, table: str, placeholder: str) -> set[str]:
    return {row[0] for row in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() "
        f"AND table_name={placeholder}", [table]
    ).fetchall()}


# ``ref`` became ``stream`` and ``point`` became ``property``. Each entry is a
# table and the columns to rename in it. Index names are kept, so startup
# finds the indexes that already exist.
_NAME_RENAMES = (
    ("stream_ids", (("ref_id", "stream_id"), ("ref_uri", "stream_uri"))),
    ("timeseries", (("ref_id", "stream_id"),)),
    ("streams", (("ref_uri", "stream_uri"), ("point_uri", "property_uri"), ("ref_name", "stream_name"))),
    ("logs", (("point_uri", "property_uri"),)),
    ("stream_resets", (("ref_uri", "stream_uri"),)),
    ("materialization_lineage", (("input_ref_uri", "input_stream_uri"), ("output_ref_uri", "output_stream_uri"))),
)


def _rename_stream_and_property_columns(conn, placeholder: str) -> None:
    # The view is dropped because it names the old columns; startup recreates
    # it after migrations.
    conn.execute("DROP VIEW IF EXISTS timeseries_streams")
    if _columns(conn, "ref_ids", placeholder):
        # Startup has just created an empty ``stream_ids``; the populated
        # ``ref_ids`` takes its place.
        if conn.execute("SELECT count(*) FROM stream_ids").fetchone()[0]:
            raise RuntimeError("both ref_ids and stream_ids hold rows; cannot rename ref_ids")
        conn.execute("DROP TABLE stream_ids")
        conn.execute("ALTER TABLE ref_ids RENAME TO stream_ids")
    for table, renames in _NAME_RENAMES:
        columns = _columns(conn, table, placeholder)
        for old, new in renames:
            if old in columns and new not in columns:
                conn.execute(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")


def _duckdb_stream_and_property_names(conn) -> None:
    # DuckDB cannot alter a table that has secondary indexes. They are dropped
    # here and startup recreates them after migrations.
    tables = [table for table, _ in _NAME_RENAMES] + ["ref_ids"]
    marks = ", ".join("?" for _ in tables)
    for (index,) in conn.execute(
        f"SELECT index_name FROM duckdb_indexes() WHERE schema_name=current_schema() AND table_name IN ({marks})", tables
    ).fetchall():
        conn.execute(f'DROP INDEX "{index}"')
    _rename_stream_and_property_columns(conn, "?")


def _postgres_stream_and_property_names(conn) -> None:
    _rename_stream_and_property_columns(conn, "%s")


def _experiment_stream_uri_column(conn) -> None:
    postgres = conn.__class__.__module__.startswith("psycopg")
    columns = _columns(conn, "experiment_observations", "%s" if postgres else "?")
    if "ref_uri" not in columns or "stream_uri" in columns:
        return
    # DuckDB cannot alter a table that has secondary indexes; startup
    # recreates them after migrations.
    for index in ("experiment_observations_ref_idx", "experiment_observations_variable_idx"):
        conn.execute(f"DROP INDEX IF EXISTS {index}")
    conn.execute("ALTER TABLE experiment_observations RENAME COLUMN ref_uri TO stream_uri")


TIMESERIES_MIGRATIONS += (Migration(2, "stream_and_property_names", _postgres_stream_and_property_names),)
DUCKDB_TIMESERIES_MIGRATIONS = (
    Migration(1, "publication_columns", _duckdb_publication_columns),
    Migration(2, "drop_timeseries_unique", _duckdb_drop_timeseries_unique),
    Migration(3, "stream_and_property_names", _duckdb_stream_and_property_names),
)
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

EXPERIMENT_MIGRATIONS += (Migration(4, "stream_uri_column", _experiment_stream_uri_column),)
