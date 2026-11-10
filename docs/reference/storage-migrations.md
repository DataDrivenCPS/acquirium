---
title: Storage upgrades and recovery
---

The experiment ledger stores Studies, run state, observations, and artifact
bytes in DuckDB or PostgreSQL. Its location is `experiments_path` in the
[server configuration](server-config.md). Recorded samples live in Escapement;
descriptive metadata lives in the graph.

## Schema upgrades

With `recreate=False`, the ledger upgrades its schema automatically when it
opens. Applied versions are recorded in `schema_migrations`. Each upgrade runs
in a transaction with its ledger entry; a failure rolls both back. Unknown
versions or changed migration names raise an error.

`recreate=True` erases the configured data directory and experiment ledger.
Use it only when those records can be discarded.

## Back up and restore

Stop the server, then back up the graph, experiment ledger, and Escapement store
together. Include any configured paths or databases outside `data_dir`.

Restore the matching stores together. The ledger's outbox can replay pending
graph writes, but acknowledged metadata lives only in the graph. A ledger copy
cannot reconstruct a missing graph. Recorded sample references also depend on
the matching Escapement store.

On startup, pending graph operations are delivered again, and sample cleanup
resumes for streams belonging to deleted runs. The
[experiment storage explanation](../explanation/experiments.md#storage) describes
the recording and deletion order.

## Adding a ledger upgrade

Append a `Migration` in `src/acquirium/Storage/migrations.py` with the next
consecutive version and a descriptive name. Keep applied versions unchanged.
Use the supplied connection for SQL or an upgrade function, and apply the
upgrade before creating dependent views or indexes.

The runner holds the store's writer lock. PostgreSQL also uses a
transaction-scoped advisory lock. Check populated-schema upgrades, repeated
opening, rollback, and new writes on both backends.
