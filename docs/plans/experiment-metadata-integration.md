# Experiment metadata integration with PR #120

Status: implemented, including shared lossless documents and graph-authoritative
metadata. The public Study, variable, run, and recording APIs are retained.

## Storage boundary

The shared metadata layer stores complete documents under private RDF predicates
and writes queryable attribute projections alongside them. Its internal typed
JSON encoding preserves arbitrary object keys, null positions, empty containers,
numbers, dates, datetimes, and URI values. Namespaced `metadata` objects support
arbitrary JSON keys; valid paths are also queryable under `metadata`.

Ordinary metadata patches retain PR #120's top-level removal semantics. Nested
nulls and empty containers survive in the complete document. Semantic keys keep
their predicates and resolved values, while the document retains the original
spelling and ordering. Native query tables expose resolved values and shared
schemas; saved experiment metadata reads complete documents directly through the
same SPARQL seam. `metadata_objects()` assembles selected user values directly from documents,
using RDF only for unsaved fields and semantic attributes, then applies casts.

Explicit edits in the reserved metadata source override complete user fields
in the derived query view as well as document reads, preserving the original
source assertions. Semantic predicates remain additive. Object selections and
exclusions traverse both implicit list elements and explicit numeric positions
without shifting list positions or reintroducing dropped fields.

The graph owns descriptive variable, run, and annotation metadata. SQL owns
Study identity, declaration identity/kind/role, run lifecycle, recorded values,
observation ordering, stream references and intervals, and artifact ownership.
There are no permanent SQL metadata snapshots, SQL resolution table, shape
restoration fallback, or SQL metadata-filter implementation. The older
`metadata={...}` filters compare hydrated complete documents for exact saved-field
matching, including ordered lists and nested objects. Ordinary attribute filters
continue to use the shared query compiler.

Model adapters now namespace their JSON metadata directly. Their earlier
serialized `adapter_metadata` representation is converted during migration;
serialization belongs to the shared metadata layer.

## Durable handoff

Each metadata operation enters `experiment_metadata_outbox` in the SQL transaction
that creates or changes its lifecycle row. Operations contain resolved RDF terms,
so replay does not depend on a resolver or repeat a historical resolution.
Lifecycle transitions patch only their fields, preserving saved metadata.

The next metadata read or general graph query delivers pending operations in
version order. Delivery coalesces replacements and field patches into one update
per target graph while preserving source-clear barriers and operation order,
avoiding parser depth limits on large sweeps. Delivery is serialized across ledger instances sharing a graph. Stream
source deletions name the sources captured by their deletion transaction.

Graph delivery and flushing source writes to disk precede SQL acknowledgment. A crash or failure before
acknowledgment retains the outbox entries; replay is idempotent. An SQL rollback
also rolls back its outbox entry. General graph queries log delivery failures;
experiment metadata reads propagate them instead of returning stale snapshots.

## Existing databases and recovery

Experiment migration 2 converts SQL metadata snapshots and pinned resolutions to
resolved outbox operations before dropping the old metadata columns and
resolution table. Annotation metadata leaves the observation payload column;
recorded scalar/JSON values and artifact associations stay intact. Legacy UUID
identifiers keep their SQL IDs and absolute graph URIs. Invalid legacy records
abort the schema transaction, preserving the original SQL data for repair.

A graph marker identifies the SQL store generation. The SQL delivery frontier
records whether any metadata writes have already been acknowledged, so a new
pending write cannot initialize a replacement for a lost authoritative graph. Recreating a store clears
its old experiment graph on first delivery. Reopening a populated store with a
missing or mismatched authoritative graph and no pending initialization data
raises a restoration error instead of silently returning empty metadata.

After acknowledgment, the graph is the only durable copy of metadata. Backups
and restoration must include both graph and SQL state. The outbox recovers
pending writes; it is not an archive of already acknowledged documents.
Standalone ledger callers use the same documents in a small persistent Oxigraph
store in a directory they name through the explicit `ExperimentStore.standalone()` factory. The ordinary
constructor requires an injected backend. See the current
[declaration and recording refinement](experiment-refinement.md) and
[migration/recovery reference](../reference/storage-migrations.md).

## Validation

Contract tests run against DuckDB and isolated TimescaleDB schemas. They cover
legacy upgrades, migration rollback, SQL/outbox atomicity, resolver-independent
restart, failure after graph commit but before acknowledgment, and deletion
replay. Shared metadata tests cover arbitrary JSON keys and nested values, typed
dates, null positions, empty containers, projections, replacements, and casts.
Real-server model tests exercise sampling, metadata queries, and process restart.
