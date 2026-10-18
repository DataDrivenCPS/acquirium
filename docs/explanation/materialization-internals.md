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

## Ingestion and steps

A write request does not reach `timeseries` directly. The server appends it
to `materialization_inbox` in its own transaction and acknowledges it then;
the entry carries the request's canonical mutations as an Arrow stream and
its kind, `readings` or `replace`. The materialization loop applies the
inbox as a *step*: every pending entry in sequence order, inside one write
transaction, at one revision, and then every compiled binding, layer by
layer, on the same connection. Derived rows therefore commit together with
the raw rows that caused them, and a dependent layer reads its producers'
outputs from the step's own transaction instead of waiting for a later
tick. The step is recorded in `materialization_steps`. A step commits as a
whole or not at all: a database failure anywhere rolls back raw rows,
derived rows and progress together and leaves the inbox for the next
attempt, while a failing transform only skips that binding and its
descendants for the step. `POST /materialization/step` forces a step. An
embedder that constructs the engine without marking it deferred gets a step
on every submission.

The step holds the backend's write lock for its whole transaction. A write
that arrives meanwhile is appended to the inbox and lands above every
frontier the step advanced, so the next step sees it. On PostgreSQL the
append runs on a connection of its own, so the publisher is acknowledged
while a step is in progress; on DuckDB it waits for the store's lock. The
inbox row keeps the time it was received, which travels with the entry
into the step's ingest event. Transforms run in a bounded pool on data
already loaded; reads and writes stay on the step's single connection.

### A layer

A step evaluates the plan one dependency layer at a time, and a layer is one
unit of work. Rows read for the same streams over the same window are read
once for the layer and shared by the bindings that need them, as one Polars
frame. Transforms run in the pool. An eager transform returns tables; a lazy
one returns plans, built from `inputs[alias].lazy()` or
`aq.align(..., lazy=True)`. When every transform of the layer has run, its
lazy outputs are collected in one call with common-subplan elimination, so
a resample two views share is computed once. Polars shares a plan only
between dependents that hold the same plan object, so the layer keeps a
memo that hands two views the same object for the same loaded frame,
stream, bucket and aggregate. A plan that fails at collection fails only
its binding.

The layer then publishes in one wave with one statement per kind. Progress,
work rows and resets are read once. One read gives each output's stream id
and last timestamp, deleted rows included; outputs never written before get
their ids in one `INSERT ... RETURNING`. A window that starts after its
stream's last row is a fresh step's window: it is appended, with no probe
and no tombstone. The remaining windows are corrections: they are probed in
one `VALUES` query and tombstoned in one `UPDATE`, a semi-join on DuckDB,
where a row matched by two windows must be updated once, and a join driven
by the windows on PostgreSQL, where the `EXISTS` form scanned the table once
a wave had about thirty windows. All rows are then inserted as one frame
keyed by stream id, and progress advances in one `UPDATE`. Two bindings
writing one `(stream, timestamp)` in a wave is an error; the check runs
only on streams with two writers. Full rebuilds, which replace a stream
physically, stay per output. The step's timings split into read, transform,
collect and publish.

## Change feed and triggers

After a step commits, what it wrote to each derived stream (the replaced
output window and its new rows) goes to an in-memory, bounded feed read with
`GET /materialization/changes`, and to the URL of every output declared with
`aq.output.trigger(url)`. Nothing is published for a step that rolls back,
so a reader never sees a value that was not committed. The feed is
process-local; a reader that falls behind it, or a restart, is told so and
rereads the committed streams.

## Model changes and effective times

Every write to a data graph (`insert_graph`, `sparql_update`, a stream
registration) is computed as a delta against the live graph and logged in
`graph_history` and `graph_history_triples`, in the timeseries database,
with the time the author says the change took effect: `effective_at`,
default now. The log row is committed before the graph is written. The two
stores share no transaction, so a crash between them leaves a recorded
change the graph does not have, and the materializer applies those on
startup; the reverse, a graph write without a record, cannot happen. A write
dated earlier than a recorded change to one of the same triples is rejected
(HTTP 409): the live graph applies writes in arrival order, and undoing
changes in effective-time order has to give back the same graph.

Two kinds of write are logged but make no version of the model. A write
that removes nothing from an empty graph is that graph's initial load: the
model before it is not empty but unknown, so the load holds for all time
however it is dated. `insert_graph(versioned=False)` marks a write as
bookkeeping about the model rather than a change to it; `register_streams`
uses it, because where a stream's data lives is not a change to the plant.
Neither is ever undone, and neither conflicts with a dated write.

The model is one version between two consecutive change points, the
effective times of the versioned changes. `GraphHistory.as_of(t)` rebuilds
the inferred model as of `t` by undoing the later-dated changes over the
live data graphs, in an in-memory store that answers SPARQL like the live
one; up to eight versions are cached. Live queries do not go through it.
`POST /sparql_json` with `as_of`, `GET /graph_change_points`, the client's
time-range reads and the planner are its readers.

A versioned change is also recorded in `materialization_graph_changes`,
under the plan lock and together with the graph write, so no refresh can
plan the bindings a write creates before their time is known, and a change
is consumed only by a plan whose graph revision includes it. The next
refresh consumes those rows and applies the earliest time to the bindings
the change created or retired. `materialization_bindings` keeps one row per
interval in which the model said a binding existed. A binding that appears
for an app that already had bindings opens a row at the effective time and
carries that time as its validity, so it reads and writes nothing before
it, even for later corrections to earlier data. A binding that disappears
closes its row at the effective time and its derived streams are tombstoned
from that time on at a new revision, so a consumer retracts its own results
while rows before the time stay as the record of what the model said then.
A closed binding stays in the plan over its own interval: a late reading or
correction before the change still reaches the result as the model was
then, for instance a fleet total over the members it had. Each closed
interval of a binding is its own instance with its own frontier. A named
output can therefore have several owners, one per interval, and the
intervals never overlap. A bucketed view switches at the start of the
bucket containing the effective time. A change without an effective time
takes effect when it is written.

A view deployed after the model changed is planned against each version of
the model: once for the version before the first change point, then once
per change point. A binding's interval is the run of versions that produce
it, floored to the view's bucket, so a sensor removed before the view
existed is in the fleet total for the time it was in the fleet and absent
after. With no change points the live plan covers all retained history. A
consumer of a derived stream is bound only while its producer holds, in the
live plan and in each past version alike, so its intervals follow the
producer's.

Only a recorded model change gives bindings an interval. Bindings that
appear because the dataflow changed (a view deployed, an upstream view
removed) cover retained history, and bindings that disappear for that
reason are dropped, along with any closed interval below the removed view.
A binding whose match context changed is closed and reopened, and its
repair starts at the effective time. Removing an app deletes its rows, so a
later deployment under the same name starts without history. The lineage
table lists the derived streams of open and closed bindings; the dependency
graph is built from it.

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
Otherwise, output changes and progress commit together. Incremental output
removals leave revisioned tombstones so downstream apps can observe the removal;
reinserting a timestamp clears its tombstone.

Whole-stream ingestion replacement (`replace=True`) instead physically deletes
old rows, inserts the replacement, and records the latest reset revision for the
stream in the same transaction. A binding whose input has reset since its
consumed frontier rebuilds from all current inputs, including unchanged sibling
streams. Its assigned outputs are physically replaced and receive reset markers,
so descendants also rebuild. Only one reset marker per stream is retained;
repeated replacements overwrite it rather than accumulate deleted keys.

A reset rebuild runs as one unpartitioned invocation. Existing output timestamps
help bound the rebuild, including when all inputs were cleared. Output writes,
output reset markers, removal of superseded backfill work, and progress advance
commit atomically. A transform or write failure leaves the prior work available
for retry. A reset committed after a batch's snapshot invalidates that batch at
publication, preventing old work from restoring removed values. Replacement is
intended for infrequent reloads: full input loading and recomputation can consume
substantially more memory and time than incremental work.

Each layer of a step starts with one change index: a single read of the
streams written or reset above the oldest consumed frontier in the layer,
with their latest revisions, on the step's connection. A binding whose
inputs are absent from the index has nothing to read, so its frontier
advances to the current revision in one batched update. Only bindings with
a changed input, pending work, or a reset read a batch. The index is
rebuilt after a layer publishes, so the next dependency layer sees its
predecessors' outputs. A failed binding keeps its previous frontier and
blocks its descendants, while unrelated branches continue in the same step.

Long, finite work ranges are divided into output intervals of approximately one
day, rounded to complete buckets where necessary. A durable cursor records which
intervals have finished, and the input frontier advances when the entire range
is complete. Corrections newer than the range's captured revision are processed
afterward. Explicit reprocessing uses the same durable work mechanism while
preserving the existing input frontier. Whole-history calculations still load
their complete retained input and are not bounded by these daily intervals.

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
and `binding_progress` (`progress_key` → `consumed_revision`), plus
`stream_resets` (`ref_uri` → latest reset `last_revision`). The engine owns
`materialization_inbox` (pending write requests) and `materialization_steps`
(one row per applied step: revision, sequence range, entries, rows). The
materializer owns `materialization_bindings` (binding validity intervals)
and `materialization_graph_changes` (effective times awaiting a refresh).
The graph history owns `graph_history` (one row per data-graph write: graph,
effective and recorded time, whether applied, whether timeless) and
`graph_history_triples` (the triples each write added and removed). Opening a store creates
the reset table if it is absent; no existing timeseries columns change. The materializer
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
| Stream key in a written frame | `ref_id` from a wave; `ref_uri` from ingestion, joined to `ref_ids` | the same |
| Tombstones of a wave | one `UPDATE` with a semi-join over the windows | one `UPDATE ... FROM (VALUES ...) JOIN ref_ids`, an index walk per window |
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
`materialization_poll_seconds` (default `0.25`) sets how long an idle runtime
waits before the next step. `materialization_workers` (default `2`) limits concurrent
execution, and `materialization_error_log_seconds` (default `30`) limits how
frequently repeated failures are logged. A failing deployment does not prevent
ingestion or independent apps from progressing. `materialization_event_log`
(or `ACQUIRIUM_EVENT_LOG`) names a JSON-lines file that records every
ingestion, step, invocation, publication, failure and replan with
timestamps, revisions, rows and timings; ingest and step lines join on the
revision. Without it every hook is one attribute lookup.

Apps must produce the same result for a given batch. The runtime can repeat
uncommitted work, but it cannot undo external side effects performed by a
transform. Each write to an existing `(stream, timestamp)` replaces the current
value and advances its `last_revision`; previous values are not retained as a
version history. Incremental output removals retain revisioned tombstones so
downstream calculations can detect them. Whole-stream replacement and its
resulting rebuilds physically remove old rows and use stream reset markers.

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
`tests/unit/test_materialization_lazy.py` checks that a lazy transform
publishes what the eager one would, that two views share one resample plan
per stream, and that a failing plan fails only its binding.

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
| `Materialization/engine.py` | The inbox and the step that applies it at one revision |
| `Materialization/changes.py` | The change feed and trigger posts of committed steps |
| `Materialization/graph_history.py` | The log of data-graph writes with their effective times, the model as of a time, and recovery of recorded writes the graph lacks |
| `Materialization/revision_store.py` | Snapshot reads, durable work cursors, and atomic replacement/progress writes |
| `Materialization/scheduler.py` | Layer-by-layer evaluation of a plan on a step's connection, bounded transform execution, per-binding failure tracking |
| `Materialization/runtime.py` | Deployment activation, graph refresh, readiness cadence, and status; the engine's evaluator |
| `Materialization/checks.py` | Shared dry-run result rendering and window clipping |
| `Materialization/local.py` | Caller-process checks using data fetched from the server |
| `Materialization/worker.py` | Importing and verifying application entrypoints |

The top-level `acquirium` package exports the authoring API. Embedders can
import runtime types from `acquirium.Materialization`; implementation modules
import their dependencies directly.
