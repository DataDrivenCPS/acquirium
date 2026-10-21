---
title: Storage migrations
---

Schema upgrades run automatically when a store opens with `recreate=False`.
The shared runner in `src/acquirium/Storage/migrations.py` records each applied
migration in `schema_migrations`, keyed by component and version. Timeseries
and experiment schemas have independent version sequences.

To change a schema:

1. Append a `Migration` with the next consecutive version and a descriptive name
   to that component's migration list. Keep applied versions unchanged.
2. Supply SQL statements, or an upgrade function when the backend needs schema
   inspection or a table rebuild. Keep database work on the supplied connection.
3. Run the migration before creating views or indexes that depend on its changes.
4. Add contract tests for a populated previous schema, repeated opening, and new
   writes after upgrade on both backends.

The caller holds the store's writer lock and runs the pending migrations and
ledger inserts in one transaction. PostgreSQL also takes a transaction-scoped
advisory lock to serialize migration runners across workers in the same schema.
Failures roll back schema changes and migration history together. Opening a
schema with unknown migration versions or changed names raises an error.
`recreate=True` resets the migration history along with the managed tables.

The first migrations also adopt databases upgraded before this ledger existed.
For DuckDB, adding the constrained publication columns to a populated table
requires rebuilding that table inside the migration transaction. Existing
samples and reference IDs are retained; allow space for the replacement table
when upgrading a large database. Subsequent openings skip this migration.
