---
title: Materialization internals
---

Acquirium stores both derived values and processing progress in the timeseries
database. Each row records the revision that last wrote it, and each binding
records how far it has processed its inputs. These records allow the runtime
to resume after a restart without relying on worker memory.

This page describes how the storage backends support that behavior and which
settings affect server operation. For an introduction to writing an app, see
[Your first app](../tutorials/first-app.md). For the calculation model and
processing guarantees, see [Apps](apps.md).

## Scheduling and recovery

The timeseries database stores current values and assigns each write a
monotonically increasing revision. Each binding records the last input revision
it has processed, called its *consumed frontier*. To prepare the next invocation,
the runtime opens a consistent database snapshot, finds input timestamps that
changed after that frontier, and loads the required windows into Arrow tables.
It closes the read transaction before running user code.

Before publishing the result, the runtime checks that the binding generation
and consumed frontier still match those used to prepare the work. If the
binding has been replaced or its progress has changed, the result is discarded.
Otherwise, output changes and progress commit together. Deleted output rows
leave revisioned tombstones so downstream apps can observe the removal;
reinserting a timestamp clears its tombstone.

One coordinator schedules independent bindings through a bounded thread pool.
The next dependency layer reads its inputs after predecessor work has completed.
A failed binding keeps its previous frontier and blocks its descendants, while
unrelated branches can continue processing.

Long, finite work ranges are divided into output intervals of approximately one
day, rounded to complete buckets where necessary. A durable cursor records which
intervals have finished, and the input frontier advances when the entire range
is complete. Corrections newer than the range's captured revision are processed
afterward. Explicit reprocessing uses the same durable work mechanism while
preserving the existing input frontier. Whole-history calculations still load
their complete retained input and are not bounded by these daily intervals.

## The storage contract

The timeseries backend stores `system_state` (one global `current_revision`)
and `binding_progress` (`progress_key` → `consumed_revision`). The materializer
owns `materialization_deployments`, `materialization_lineage`, and
`materialization_work`. Lineage records output ownership, input references,
and a query-context fingerprint. Work rows store bounded output cursors for
long backfills and explicit reprocessing. Derived rows live beside raw rows in
the ordinary `timeseries` table.

The scheduling algorithm is shared between backends. Each backend supplies the
following connection and write hooks:

| hook | purpose |
|---|---|
| `_connect()` | An independent connection for a coherent batch snapshot. |
| `_own_conn()` | A short-lived read connection. |
| `_write_conn()` | A write connection, taken under the store's lock. |
| `_next_revision(conn)` | Allocate the next global revision inside the caller's transaction. |
| `_insert_frame(conn, frame, revision)` | Upsert rows keyed by `(stream, ts)` at that revision. |

`RevisionStore` coordinates snapshot reads and publication through these hooks,
adapting SQL parameter syntax, UTC conversion, and the join used to resolve
stream keys for each backend.

## DuckDB and PostgreSQL/TimescaleDB

| concern | DuckDB | PostgreSQL / TimescaleDB |
|---|---|---|
| Stream key in `timeseries` | integer `ref_id`, joined to `ref_ids` | integer `ref_id`, joined to `ref_ids` |
| Timestamp storage | UTC-normalized `TIMESTAMP` (naive in SQL) | `TIMESTAMPTZ` |
| Revisioned write | registered Polars frame, delete+insert keyed by `(ref_id, ts)` | Polars CSV into a temporary `COPY` staging table, then ordered `INSERT ... SELECT ... ON CONFLICT` keyed by `(ref_id, ts)` |
| Read connection | a new connection to the shared embedded database | a new psycopg connection |
| Snapshot boundary | `conn.begin()` | `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` |
| Write transaction | in-process store lock around the store's write connection | in-process store lock around a transaction on the store's persistent write connection |
| Backend SQL | `?` parameters, `INSERT OR REPLACE` where needed | `%s` parameters, `ON CONFLICT` |

TimescaleDB's hypertable is the normal `timeseries` table. It omits
TimescaleDB's default timestamp index and uses a unique `(ref_id, ts)` index
for reads and idempotent upserts. Compression segments by `ref_id`, orders
each segment by descending timestamp, and has a seven-day compression policy.
Materialization needs no separate hypertable or continuous aggregate.

## Operational notes

Run one server process for each embedded Oxigraph graph store. That process
owns the graph files and schedules app execution; the timeseries backend stores
the readings and durable processing state separately. On startup, the runtime
creates the materialization tables if needed and restores deployments,
frontiers, work cursors, and query-context fingerprints.

The `[server]` settings control execution and diagnostics.
`materialization_poll_seconds` (default `0.25`) sets how often an idle runtime
checks for work. `materialization_workers` (default `2`) limits concurrent
execution, and `materialization_error_log_seconds` (default `30`) limits how
frequently repeated failures are logged. A failing deployment does not prevent
ingestion or independent apps from progressing.

Apps must produce the same result for a given batch. The runtime can repeat
uncommitted work, but it cannot undo external side effects performed by a
transform. Each write to an existing `(stream, timestamp)` replaces the current
value and advances its `last_revision`; previous values are not retained as a
version history. Output removals retain revisioned tombstones so downstream
calculations can detect them.

A persistent thread pool bounds the number of concurrent invocations. Finite
work ranges are divided into roughly one-day output intervals, expanded to
complete buckets and supplied with the declared input context. This limits the
size of individual work items, but is not a fixed memory bound: memory use also
depends on data density and context size. Apps using `lookback="all"` load their
complete retained history.

## Tests

`tests/integration/test_materialization_store_contract.py` runs the same
revision-frontier scenario against DuckDB and TimescaleDB: initial
materialization, a correction, output visibility, revision progression, and
exactly-once frontier advancement. The Timescale target creates a private
schema so it cannot disturb the API integration server's database. It needs
`ACQUIRIUM_TEST_PG_DSN`; without it the Timescale half is skipped.

`tests/unit/test_incremental_materialization.py` is the unit-level contract:
output identities and grouping, window construction, progress-key continuity,
unit conversion, alignment, and DAG validation.

The processing contract is checked across ingestion order, corrections, and
recovery:

- Together and split ingestion produce equal fixed-bucket and rolling results.
- Late corrections repair old results without corrupting boundary context.
- Empty replacement removes rows and propagates through an application chain.
- Restart preserves progress and pending reprocessing.
- Failed deployment leaves the active definition intact.
- Failed transforms leave healthy branches runnable.
- Worker capacity bounds transformations and loaded batches.
- PostgreSQL batch reads share a Repeatable Read snapshot.

## Implementation boundaries

| Module | Responsibility |
|---|---|
| `Materialization/models.py` | App declarations, stream sets, windows, resolved bindings, DAG validation, and dataframe helpers |
| `Materialization/planner.py` | Deployment serialization and query compilation into bindings |
| `Materialization/revision_store.py` | Snapshot reads, durable work cursors, and atomic replacement/progress writes |
| `Materialization/scheduler.py` | Bounded execution and per-binding failure tracking |
| `Materialization/runtime.py` | Deployment activation, graph refresh, scheduling cadence, and status |
| `Materialization/checks.py` | Shared dry-run result rendering and window clipping |
| `Materialization/local.py` | Caller-process checks using data fetched from the server |
| `Materialization/worker.py` | Importing and verifying application entrypoints |

The top-level `acquirium` package exports the authoring API. Embedders can
import runtime types from `acquirium.Materialization`; implementation modules
import their dependencies directly.
