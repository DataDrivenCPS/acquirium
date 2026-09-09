# Revision-frontier materialization for DuckDB and TimescaleDB

Apps select input streams with semantic queries and maintain derived streams
from the latest available readings. Each invocation replaces an explicit output
window, including results that disappear after corrections. Complete buckets
and declared input dependencies make results independent of ingestion batch
boundaries. Output mutations and consumed progress commit atomically.

Grouping (`per_match` or `all_matches`) is independent of generated or named
output identity. One coordinator uses a bounded executor, isolates failures by
dependency branch, and rejects obsolete work at publication. Durable cursors
support long finite backfills and explicit reprocessing. Query-match changes
schedule retained output repair; code changes preserve the consumed frontier.

The [app guide](docs/apps.md) covers authoring and operation; the
[reference](docs/reference/apps.md) defines the interface and window semantics.
The [backend guide](docs/materialization-implementation.md) describes storage,
recovery, and verification. Tests cover batching invariance, corrections,
removal propagation, concurrent-ingestion snapshots, bounded execution,
deployment validation, and recovery on DuckDB and PostgreSQL/TimescaleDB.
