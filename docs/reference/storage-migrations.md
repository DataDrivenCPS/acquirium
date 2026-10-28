---
title: Storage migrations
---

These migrations cover the experiment ledger: the SQL store, DuckDB or
PostgreSQL, that holds Studies, runs, observations, and artifact bytes
(`experiments_path` in the [server configuration](server-config.md)). Time
series live in Escapement, which versions its own schema.

Schema upgrades run automatically when the ledger opens with `recreate=False`.
The shared runner in `src/acquirium/Storage/migrations.py` records each applied
migration in `schema_migrations`, keyed by component and version.

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

Experiment schema version 2 moves variable, run, and annotation metadata to the
graph. It first converts existing SQL snapshots and pinned resolutions into
resolved outbox operations in the schema transaction, then removes the snapshot
columns, annotation metadata payloads, and the resolution table. A failed
conversion rolls back the migration. Existing observations, IDs, sample ranges,
and artifact associations are retained.

The next metadata read delivers the outbox. Entries are removed only after their
graph update and source flush succeed; retries do not re-resolve saved units. Backups must retain
both the SQL ledger and its authoritative graph. SQL alone can recover pending
updates, but cannot reconstruct metadata whose updates were already acknowledged.
The experiment ledger runs only inside the server, against the server's graph;
clients reach it through `aq.init()`.

Experiment schema version 3 adds a declaration format and scheduled delivery
frontier to the existing outbox state. Before normalized reads, the metadata
component checks the graph generation and drains older operations. It reads
complete variable documents outside the SQL writer transaction, while holding
the shared metadata delivery lock. Known semantic, index, and source fields
stay explicit. Namespaced JSON is preserved; other descriptive fields move
under `metadata`. Identical duplicates merge. Conflicting duplicates abort
before scheduling any normalized mutation and identify fields requiring repair.
For legacy SQL snapshots this check also precedes schema conversion, preserving
the original data if repair is needed.

Explicit metadata-source overrides are normalized in their own graph; semantic
assertions remain in their original sources. Existing semantic RDF terms are reused without a resolver, and original or
unresolved spellings remain in documents. Resolved projection changes and a
scheduled frontier commit atomically in SQL. The format is marked delivered
only after graph flush and acknowledgment reach that frontier. A restart
replays pending changes before returning normalized declarations. Pending
older outbox formats remain readable. Missing acknowledged graphs require
restoring the matching graph backup; pending new updates cannot replace it.

Experiment schema version 4 adds `experiment_streams`, the streams a run's
recordings own. A recording commits stream ownership, artifacts, observations,
and graph intent in one SQL transaction, and writes its samples to Escapement
as the transaction's last step: a recording that fails before then leaves
nothing behind. Escapement cannot roll back with SQL, so if the commit itself
fails after the samples were published, a retry rewrites the same sample keys.
Deleting runs clears their samples the same way, last. Outbox events may target a run-owned source graph;
older events retain the experiment graph as their default target. Source
clears are ordering barriers. Delivery does not perform another SQL reference
registration. Artifact bytes are stored in the SQL store, content-addressed
and split into chunks, so they commit and delete with the rows that reference
them. Ledgers from releases that kept samples in SQL are not migrated; recreate
them.
