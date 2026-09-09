# Incremental materialization: backends and operations

Acquirium keeps derived streams up to date with a revision frontier: the
timeseries database is the recovery authority, every row records the revision
that last wrote it, and a durable frontier says which revision each binding
has consumed.

The model, the algorithms, and the reasoning behind them now live in one
place — [How it works](reference/apps.md#how-it-works) in the app reference.
This page covers what is specific to running it: how each storage backend
implements the contract, and what an operator needs to know.

- Writing and deploying an app: [Apps](apps.md)
- The runtime contract, algorithms and design decisions:
  [App reference](reference/apps.md)

## The storage contract

For read-only operational inspection, use `acquirium app list` and
`acquirium app inspect NAME` (both accept `--json`). The corresponding
`GET /apps` and `GET /apps/{name}` endpoints join the durable deployment registry
with the latest in-memory plan and stored progress. They do not trigger graph
refresh or execution. Progress is fetched once per request, not once per binding.
Declarations remain inspectable when code cannot be loaded or queries match
nothing. See the [inspection guide](apps.md#inspect-deployed-apps) for status
and plan-freshness semantics.

The timeseries backend stores `system_state` (one global `current_revision`)
and `binding_progress` (`progress_key` → `consumed_revision`). The materializer
owns `materialization_deployments`, `materialization_lineage`, and
`materialization_work`. Lineage records output ownership, input references,
and a query-context fingerprint. Work rows store bounded output cursors for
long backfills and explicit reprocessing. Derived rows live beside raw rows in
the ordinary `timeseries` table.

A backend supplies connection and write hooks and inherits the entire scheduler:

| hook | purpose |
|---|---|
| `_connect()` | An independent connection for a coherent batch snapshot. |
| `_own_conn()` | A short-lived read connection. |
| `_write_conn()` | A write connection, taken under the store's lock. |
| `_next_revision(conn)` | Allocate the next global revision inside the caller's transaction. |
| `_insert_frame(conn, frame, revision)` | Upsert rows keyed by `(stream, ts)` at that revision. |

`RevisionStore` owns the algorithm and adapts only the parameter spelling, the
UTC conversion, and the stream-key join.

## DuckDB and PostgreSQL/TimescaleDB

| concern | DuckDB | PostgreSQL / TimescaleDB |
|---|---|---|
| Stream key in `timeseries` | integer `ref_id`, joined to `ref_ids` | `ref_uri` text directly |
| Timestamp storage | UTC-normalized `TIMESTAMP` (naive in SQL) | `TIMESTAMPTZ` |
| Revisioned write | registered Polars frame, delete+insert keyed by `(ref_id, ts)` | cursor `executemany` upsert keyed by `(ref_uri, ts)` |
| Read connection | a new connection to the shared embedded database | a new psycopg connection |
| Snapshot boundary | `conn.begin()` | `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` |
| Write serialization | in-process store lock; DuckDB has one writer | in-process store lock; the transaction also provides isolation |
| Backend SQL | `?` parameters, `INSERT OR REPLACE` where needed | `%s` parameters, `ON CONFLICT` |

TimescaleDB's hypertable is the normal `timeseries` table. Materialization
needs no separate hypertable, continuous aggregate, or Timescale job.

## Operational notes

- **Run one server process.** The embedded Oxigraph graph store has a single
  owning process; the timeseries backend stores its values separately.
- **Tuning.** `[server] materialization_poll_seconds` (default `0.25`) sets
  the idle polling cadence, `materialization_workers` (default `2`) bounds
  concurrent execution, and `materialization_error_log_seconds` (default `30`)
  rate-limits repeated failure logs. A failing deployment is isolated: it
  cannot stop ingestion or the other durable workers.
- **Apps must be deterministic for a given batch.** The runtime can safely
  recompute uncommitted work, but it cannot roll back side effects performed
  by user code.
- **Corrections keep the current value.** A re-written `(stream, timestamp)`
  overwrites that row and advances its `last_revision`; the store keeps
  current values, not a history of prior ones.
- **Initialization.** Startup creates the materialization tables with the
  complete schema. Restart uses the stored deployments, frontiers, work cursors,
  and query-context fingerprints to resume processing.
- **Replacement propagates.** Rows removed from an assigned output interval
  retain revisioned tombstones so downstream calculations observe removals.
- **Bounded execution.** One coordinator uses a persistent thread pool. Finite
  work ranges are partitioned into roughly one-day output intervals, with
  complete buckets and the declared input context. Whole-history apps remain
  bounded by their retained history, not by a fixed memory limit.

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

## Performance probe

Run `.venv/bin/python scripts/benchmark_materialization.py` for a temporary
DuckDB workload with eight precompiled bindings, 48,000 rows, two workers,
a backfill, a correction, and idle polling. It checks the output row count.
On the development machine on 2026-09-09, this took 0.79 seconds for backfill,
0.19 seconds for the correction cycle, and 0.52 milliseconds per idle tick.
These are local measurements, not capacity guarantees or a before/after comparison.
Graph compilation and module startup are excluded.
