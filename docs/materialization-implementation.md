# Incremental materialization implementation

This document describes how Acquirium turns a Python `Transformation` into a
derived time series that keeps up with its inputs. For example, an app can read
temperature measurements, publish a rolling average, and resume after a restart
without forgetting which measurements it has already processed. Use
[apps.md](apps.md) to write and deploy an app; use this page to understand what
happens after it is deployed.

The implementation is deliberately small. The timeseries database is the
recovery authority: a durable frontier says which input revision a binding has
consumed, and normal timeseries rows carry the revision that last wrote them.

## The model

A **transformation** is a Python class that describes one kind of calculation.
Its semantic query is compiled against the current plant graph into one or more
**bindings**. A binding is the concrete version of that calculation: it fixes
the input stream references to read and the derived output references to write.
The binding signature is a stable hash of that declaration.

```text
Transformation class
  build_query() + outputs
          │ compile against graph revision
          ▼
  Binding(s) ────────────────► application DAG
  inputs:  source → [stream refs]    edges: output ref → dependent binding
  outputs: result → derived ref
          │
          ▼
  RevisionStore + Scheduler
```

`Transformation` creates one binding for the complete query result.
`RowWiseTransformation` creates one binding per query-result row. The scheduler
does not need to know which form produced a binding.

## Durable state

Materialization adds two small tables to the selected timeseries backend.

```text
system_state
┌──────────────────┐
│ current_revision │  42
└──────────────────┘

binding_progress
┌──────────────────────────────────────────────────────────────────┬───────────────────┐
│ binding_signature                                                │ consumed_revision │
├──────────────────────────────────────────────────────────────────┼───────────────────┤
│ 7d4d…                                                            │ 40                │
│ b102…                                                            │ 42                │
└──────────────────────────────────────────────────────────────────┴───────────────────┘

timeseries
┌──────────────┬─────────────────────┬───────────────┬─────────┬───────────────┐
│ stream key   │ ts                  │ value         │ deleted │ last_revision │
├──────────────┼─────────────────────┼───────────────┼─────────┼───────────────┤
│ input:zone-a │ 2026-01-01 12:00:00 │ 21.5          │ false   │ 41            │
│ derived:avg  │ 2026-01-01 12:00:00 │ 21.1          │ false   │ 42            │
└──────────────┴─────────────────────┴───────────────┴─────────┴───────────────┘
```

Every non-empty public write allocates one global revision. A batch may update
many rows and streams, but they all receive that revision. Materialized output
rows and the corresponding `binding_progress` update are committed together:
after a crash, the database contains both the output and its advanced frontier,
or contains neither. It never records progress without the output it represents.

The graph, app definitions, and lineage projection are durable too:
`materialization_deployments` stores deployment JSON and
`materialization_lineage` stores the current compiled input/output relation.
Together, these tables provide the materializer's durable control-plane state.

## One invocation

The scheduler handles a binding in five steps.

```text
             read transaction                         write transaction
┌───────────────────────────────────────┐    ┌────────────────────────────────────┐
│ 1. read consumed revision: 40          │    │ 5. re-check frontier is still 40    │
│ 2. snapshot current revision: 41       │    │    allocate output revision: 42     │
│ 3. find relevant rows with             │    │    register derived streams         │
│    40 < last_revision <= 41            │    │    upsert output rows @ 42          │
│ 4. read windowed input StreamSets      │    │    advance frontier: 40 → 41        │
└───────────────────┬───────────────────┘    └───────────────────┬────────────────┘
                    │                                            │
                    └──── sealed InputBatch ──► transform() ─────┘
```

The app author chooses a built-in window-policy object on the transformation;
the backend applies that choice when it constructs an `InputBatch`. For
example:

```python
window = aq.Changed()                    # the default
window = aq.AroundChange(before="5m")
window = aq.AllAvailable()
```

These are built-in Acquirium objects that an app assigns to its `window`
attribute. They tell Acquirium which range of input timestamps to include when
it runs the app after a change. The backend always begins by finding relevant
input rows written since the binding's previous frontier. Their earliest and latest timestamps are
the **changed extent**. It then uses the app's policy to choose the data the
app receives:

```text
new input rows ──► changed extent ─────────────────────────► InputBatch.changed_window
                         │
                         └──► app's window policy ──► read window ─► InputBatch.read_window
                                   ├─ Changed()
                                   ├─ AroundChange(...)
                                   └─ AllAvailable()
```

`Changed()` reads exactly the changed extent. `AroundChange(before="5m")`
expands it with context. `AllAvailable()` reads the complete retained input
extent. The resulting batch records both: `changed_window` says what
caused the run, while `read_window` says what data was supplied to
`transform()`. Even when the read window contains extra context,
`StreamSet.changes` identifies only rows written in the selected
input-revision interval.

For example, imagine that newly written temperature rows have timestamps from
`10:02` through `10:04`. Acquirium records that range as
`changed_window = 10:02 … 10:04`: it explains why this run happened. If the
app declares `window = aq.AroundChange(before="5m")`, Acquirium also uses the
same changed extent to calculate `read_window = 09:57 … 10:04`. The app's
`transform()` method receives all rows in the read window, and
`inputs["temperature"].changes` identifies just the rows from `10:02` through
`10:04`. With `Changed()`, the two windows are equal; with `AllAvailable()`,
the changed window still identifies the trigger while the read window contains
all retained input rows.

The compare-and-advance check in step 5 makes concurrent attempts harmless.
Only the invocation whose `from_revision` still matches the durable frontier
is accepted. Unrelated revisions are skipped by advancing the frontier without
running user code.

## Graph planning and execution

The server recompiles deployments when the graph's published revision changes.
It atomically replaces the in-memory DAG and application instances, then
publishes structural lineage for the new bindings. A run takes a snapshot of
that plan, so graph recompilation never changes the inputs of an invocation
already executing.

Bindings run by dependency layer. Work in the same layer may execute in
parallel; the next layer starts only after the preceding layer commits.

```text
raw streams ──► [A] ──► derived:a ──► [C]
       └─────► [B] ──► derived:b ──┘

layer 1: A and B may execute together
layer 2: C executes after accepted layer-1 commits
```

The server uses an in-process executor. `Scheduler` can also use a Ray executor
when embedded by an application. `InputBatch` and output tables travel as sealed
Arrow data, while the database transaction records durable progress.

## DuckDB and PostgreSQL/TimescaleDB

Both backends provide the same materialization contract. The schema layout and
connection mechanics differ.

| Concern | DuckDB | PostgreSQL / TimescaleDB |
| --- | --- | --- |
| Stream key in `timeseries` | Integer `ref_id`, joined to `ref_ids` | `ref_uri` text directly |
| Timestamp storage | UTC-normalized `TIMESTAMP` (naive in SQL) | `TIMESTAMPTZ` |
| Revisioned write | Registered Polars frame, delete+insert keyed by `(ref_id, ts)` | Cursor `executemany` upsert keyed by `(ref_uri, ts)` |
| Read connection | A new connection to the shared embedded database | A new psycopg connection |
| Write serialization | In-process store lock; DuckDB has one writer | In-process store lock; PostgreSQL transaction also provides database isolation |
| Backend SQL | `?` parameters and `INSERT OR REPLACE` where needed | `%s` parameters and `ON CONFLICT` |

The common `RevisionStore` owns the scheduling algorithm. It adapts only the
parameter spelling, UTC conversion, and stream-key join for each backend.
`TimescaleStore` supplies the same private transaction hooks as `DuckDBStore`
(`_own_conn`, `_write_conn`, `_next_revision`, and `_insert_frame`), so both
backends use the same scheduler and materialization runtime.

TimescaleDB's hypertable is still the normal `timeseries` table. Materialization
does not require a separate hypertable, continuous aggregate, or Timescale job.

## Operational notes

- Run one server worker. The embedded Oxigraph graph store has one owning server
  process, while either timeseries backend stores its values separately.
- The materializer polls durably. `materialization_poll_seconds` controls the
  recovery/idle polling cadence, and `materialization_workers` bounds execution
  within a dependency layer.
- A transformation must be deterministic for a given `InputBatch`. The runtime
  can safely retry uncommitted computation, but it cannot roll back external
  side effects performed by user code.
- Revisions identify newly written or corrected current rows. A corrected
  `(stream, timestamp)` overwrites that current row and advances its
  `last_revision`; this feature keeps the current value rather than a history
  of prior values.

## Tests

`tests/integration/test_materialization_store_contract.py` runs the same
revision-frontier scenario against DuckDB and TimescaleDB. It verifies initial
materialization, a correction, output visibility, revision progression, and
exactly-once frontier advancement. The Timescale target creates a private
schema so it cannot alter the API integration server's shared database.
