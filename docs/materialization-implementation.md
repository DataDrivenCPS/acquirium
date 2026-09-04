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

Materialization adds two tables to the selected timeseries backend —
`system_state` (one global `current_revision`) and `binding_progress`
(`progress_key` → `consumed_revision`) — plus two control-plane tables owned
by the materializer, `materialization_deployments` and
`materialization_lineage`. Derived rows live in the ordinary `timeseries`
table beside raw ones.

A backend supplies four private hooks and inherits the entire scheduler:

| hook | purpose |
|---|---|
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
| Snapshot boundary | `conn.begin()` | `BEGIN` issued as SQL |
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
- **Schema changes are not migrated.** Development databases created before a
  materialization schema change must be dropped and recreated.

## Tests

`tests/integration/test_materialization_store_contract.py` runs the same
revision-frontier scenario against DuckDB and TimescaleDB: initial
materialization, a correction, output visibility, revision progression, and
exactly-once frontier advancement. The Timescale target creates a private
schema so it cannot disturb the API integration server's database. It needs
`ACQUIRIUM_TEST_PG_DSN`; without it the Timescale half is skipped.

`tests/unit/test_incremental_materialization.py` is the unit-level contract:
output flavors and grouping, window construction, progress-key continuity,
unit conversion, alignment, and DAG validation.
