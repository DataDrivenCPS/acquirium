---
title: App reference
---

This page is the complete reference for Acquirium's app platform — the
*incremental materialization* runtime that keeps derived streams up to date.
It has two halves:

- **[How it works](#how-it-works)** — the components, the durable state, the
  algorithms, and the design decisions behind them. Read this when you need to
  predict what the system will do, debug something surprising, or embed the
  runtime in another program.
- **[API reference](#api-reference)** — every class, attribute and method an
  app author touches, plus the CLI, HTTP and configuration surfaces.

For a guided introduction with worked examples, start with
[Apps](../apps.md). For the storage-backend contract and operational
settings, see
[Backends and operations](../materialization-implementation.md).

---

# How it works

## The pieces

```text
   deploy / check                          acquirium.toml  [[apps]]
        │                                          │
        ▼                                          ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ Materializer                     durable registry + orchestration
 │   materialization_deployments    what is deployed
 │   materialization_lineage        what each binding reads and writes
 └──────┬─────────────────────────────────────────┬─────────────┘
        │ compile, when the graph revision changes│ once per tick
        ▼                                         ▼
 ┌────────────────┐                        ┌──────────────────┐
 │ BindingPlanner │ ─► ApplicationGraph ─► │    Scheduler     │
 │ runs build_query│    bindings + edges   │ waves, locking   │
 └───────┬────────┘                        └────────┬─────────┘
         │ reads                            batch in │ rows out
         ▼                                           ▼
 ┌────────────────┐                        ┌──────────────────┐
 │  plant model   │                        │  RevisionStore   │
 │   (Oxigraph)   │                        │ read one batch,  │
 └────────────────┘                        │ commit atomically│
                                           └────────┬─────────┘
                                                    ▼
                                            timeseries store
                                          (DuckDB / TimescaleDB)

                     transform() runs in an Executor:
                     InProcessExecutor (the server) or RayExecutor
```

| component | module | responsibility |
|---|---|---|
| `Materializer` | `Materialization/runtime.py` | Durable deployment registry, graph-driven recompilation, lineage publication, per-tick throttling. The server's entry point. |
| `Deployment` | `Materialization/planner.py` | The durable record of one deployed app: entrypoint, digest, outputs, windows, throttles, parameters. JSON round-trips over HTTP and into the database. |
| `BindingPlanner` | `Materialization/planner.py` | Runs each app's `build_query` against one pinned graph revision and compiles the matches into bindings. |
| `Binding` | `Materialization/incremental.py` | One concrete calculation: fixed input references, fixed output references, a window policy, and two identities. |
| `ApplicationGraph` | `Materialization/incremental.py` | The validated DAG of bindings: single ownership of every output, no cycles, dependency layers. |
| `RevisionStore` | `Materialization/incremental.py` | All durable read/write logic: build a coherent batch, commit results and progress in one transaction. Backend-independent. |
| `Scheduler` | `Materialization/incremental.py` | Claims work, runs a dependency wave through an executor, commits it. Holds no durable state. |
| `Executor` | `Materialization/incremental.py` | Where `transform` actually runs: `InProcessExecutor` (server default) or `RayExecutor`. |
| `local.check_app` | `Materialization/local.py` | The same compile-and-run path in the caller's process, reading inputs over the client API. |

## Vocabulary

| term | meaning |
|---|---|
| **app** | The `App` subclass an author writes: a query, an output declaration, and a `transform`. |
| **deployment** | The durable record of one app being deployed, including its parameters. |
| **match / input group** | One row of the app's query result. The CLI calls it an *input group*. |
| **binding** | The compiled form of one input group: the exact stream references to read and derived references to write. Whenever a binding has unconsumed input, it produces one call to `transform`. |
| **port** | A key of the `outputs` mapping — the name used in `output["…"] = …`. |
| **derived stream** | The timeseries a port publishes, registered under `derived:<app name>`. |
| **revision** | A monotonic counter incremented once per non-empty write to the timeseries store. Every row records the revision that last wrote it. |
| **frontier** | The highest revision a binding has consumed. Stored in `binding_progress`. |
| **changed extent / read window** | The timestamp range of newly written rows, and that range widened by `lookback` and `lookahead`. |

## Durable state

Materialization keeps everything it needs to recover in the timeseries
database. There is no queue, no lease table, and no in-memory state that
matters after a restart.

```text
system_state                     one row, the global write counter
┌──────────────────┐
│ current_revision │  42
└──────────────────┘

binding_progress                 how far each binding has consumed
┌──────────────┬───────────────────┐
│ progress_key │ consumed_revision │
├──────────────┼───────────────────┤
│ 7d4d…        │ 40                │
│ b102…        │ 42                │
└──────────────┴───────────────────┘

timeseries                       raw and derived rows, side by side
┌──────────────┬─────────────────────┬───────┬─────────┬───────────────┐
│ stream       │ ts                  │ value │ deleted │ last_revision │
├──────────────┼─────────────────────┼───────┼─────────┼───────────────┤
│ input:zone-a │ 2026-01-01 12:00:00 │ 21.5  │ false   │ 41            │
│ derived:avg  │ 2026-01-01 12:00:00 │ 21.1  │ false   │ 42            │
└──────────────┴─────────────────────┴───────┴─────────┴───────────────┘

materialization_deployments      name → deployment JSON
materialization_lineage          binding → (input alias, input ref, port, output ref)
```

Two properties do most of the work:

- **Every write allocates one revision.** A batch touching many rows and many
  streams gets a single revision number, so "what changed since 40" is one
  indexed predicate rather than a per-stream cursor.
- **Output rows and the frontier advance commit together.** After a crash the
  database holds either the output *and* its advanced frontier, or neither. It
  never records progress for work it did not publish.

## The life of one invocation

```text
             read transaction                         write transaction
┌───────────────────────────────────────┐    ┌────────────────────────────────────┐
│ 1. read consumed revision: 40         │    │ 5. re-check frontier is still 40   │
│ 2. snapshot current revision: 41      │    │    allocate output revision: 42    │
│ 3. find rows with                     │    │    register derived streams        │
│      40 < last_revision <= 41         │    │    upsert output rows @ 42         │
│ 4. read the windowed input StreamSets │    │    advance frontier: 40 → 41       │
└───────────────────┬───────────────────┘    └───────────────────┬────────────────┘
                    │                                            │
                    └──── sealed Batch ──► transform() ──────────┘
                           (no database access in between)
```

`transform` runs entirely outside both transactions, on Arrow data that has
already been read. It cannot hold a database connection open, and a slow app
cannot block a writer.

## Algorithms

### Planning: from query to bindings

`BindingPlanner.compile(deployments, graph_revision)` turns deployment records
into an `ApplicationGraph`. It is deliberately optimistic: it reads the
graph's published version before and after, and throws away any plan that was
built while the model changed underneath it.

```text
for each deployment:
  1. load_entrypoint(entrypoint, digest)      import module:qualname, verify digest
  2. app = target(**parameters)               construct with deployment parameters
  3. query = app.build_query(plant facade)    a narrow facade: query + text resolution
  4. rows = execute(query)                    SPARQL over the pinned graph revision
  5. dedupe rows by {alias: sorted ref_uris}  a SPARQL join can repeat a match
  6. group rows into bindings:
        any per_row output   → one binding per row
        all named outputs    → one binding over the whole table
        named + several rows → planning error
  7. for each binding: derive output refs, build Binding
validate: single ownership of each output ref, no self-consumption, acyclic
```

Step 5 matters more than it looks. Unrelated triples in the plant model can
produce the same alias-to-stream combination several times; binding each
distinct combination once means the number of derived streams follows the
plant, not the shape of the query's joins.

Step 6 is the rule the guide states as "the outputs decide how matches become
calls". A binding built for the whole table keeps every matched row in
`context.result` but has no single `context.row`; a lone match is treated as a
row, so an app whose query resolves to exactly one group may declare both
kinds of output.

### Batch construction: what one call reads

`RevisionStore._build_batch(conn, binding, previous, target)`:

```text
refs        = every input stream reference of the binding
changed     = SELECT min(ts), max(ts) WHERE ref IN refs
                AND previous < last_revision <= target
if changed is empty                       → no batch (nothing relevant changed)

read window = lookback is "all"
                ? SELECT min(ts), max(ts) WHERE ref IN refs AND NOT deleted
                : [changed.start - lookback, changed.end + lookahead]

for each alias:
  rows      = SELECT ref, ts, numeric_value, text_value, last_revision
                WHERE ref IN alias refs AND NOT deleted
                  AND ts BETWEEN read window
  table     = the whole read window
  changes   = rows where previous < last_revision <= target
```

Both tables are handed to the app inside one `StreamSet`: `.collect()`/`.df()`
give the read window, `.changes` gives only the rows that triggered the call.
An alias is read as strings when one of its rows carries a text value and no
numeric one; otherwise it is numeric.

When a revision touched none of the binding's inputs, `next_batch` returns
nothing and advances the frontier anyway — with a compare-and-set on the
previous value, so it cannot race an invocation already in flight. Unrelated
writes therefore cost one query, not one invocation.

### Commit: exactly-once frontier advance

`RevisionStore.commit_wave(commits)` is where the exactly-once guarantee is
enforced.

```text
in one write transaction:
  accepted = [c for c in commits
              if binding_progress[c.progress_key] == c.batch.from_revision]
  if any accepted output has rows:
      revision = next_revision()               one revision for the whole wave
      for each output table:
          INSERT INTO streams (…) ON CONFLICT DO NOTHING     register the stream
          upsert rows keyed by (stream, ts) at that revision
  for each accepted commit:
      binding_progress[progress_key] = batch.to_revision
```

That equality check is the whole safety property. Two processes that both
computed revisions `(40, 41]` will both offer a commit; the first advances the
frontier to 41 and the second is rejected, because the frontier no longer
matches what it started from, so its work is discarded rather than
double-counted. Rows are keyed by `(stream, timestamp)`, so even the rejected
duplicate would only have overwritten identical values — the check exists to
keep the *frontier* honest, not the values.

A binding whose transform assigned nothing is still accepted: its frontier
advances, publishing no rows. That is how an alarm app with nothing to report
makes progress.

### Scheduling: waves and throttles

The compiled DAG is executed in layers. Every binding in a layer is
independent of the others in it, so they may run concurrently; a layer starts
only after the previous one has committed.

```text
raw streams ──► [A] ──► derived:a ──► [C]
       └──────► [B] ──► derived:b ──┘

layer 1: A and B run together
layer 2: C runs after layer 1's accepted commits
```

Each tick of the server's materialization worker (`Materializer.run_once`)
walks the layers and decides, per binding, whether to run it:

```text
consumed = initialise(binding, app.backfill)     create the progress row if new
if current_revision <= consumed:      nothing pending — clear its pending timer
if min_interval set and now - last_run < min_interval:      skip this tick
elapsed = now - first_pending
if elapsed < coalesce and (max_delay is None or elapsed < max_delay):  skip
otherwise: run it
```

`coalesce` waits for a quiet gap in a burst of writes; `max_delay` bounds that
wait so a steady trickle cannot postpone a run indefinitely; `min_interval`
caps the run rate. All three are throttles on *pending input* — none of them
runs an app that has nothing new to read, and none of them is a wall clock.

Within a wave, an executor that exposes `submit`/`resolve` (the Ray executor)
has the entire wave submitted before anything is awaited, so tasks overlap,
and the whole wave is then committed in one transaction. Otherwise the
scheduler runs the wave in a thread pool and each binding commits on its own,
under the same frontier check.

A per-binding in-process lock stops one server from running the same binding
twice concurrently. It is an optimization, not the safety property: the
compare-and-set at commit is what makes concurrency safe across processes and
restarts.

### Recompilation

`Materializer.refresh()` recompiles when the graph's published version has
changed, and swaps the DAG and the app instances together under one lock, so
an invocation already running always sees a coherent plan. It then rewrites
`materialization_lineage` and — only when the set of binding signatures
actually changed — republishes the lineage graph. That condition matters:
publishing lineage advances the graph's published version, so republishing on
every refresh would trigger a perpetual recompile loop.

### Identity: three different names

Three derived names are computed from different inputs, and the differences
are deliberate.

```text
binding signature   = sha256(app, executable digest, inputs, outputs,
                             lookback, lookahead, parameters)
   → names one compiled binding in diagnostics and lineage.
     Changes when the code or configuration changes.

progress key        = sha256(app, {alias: input refs}, {port: output ref})
   → keys binding_progress: what the binding reads and writes.
     Survives code and parameter edits.

derived stream name = per_row: "<port>:<sha256(port, sorted (alias, ref) pairs)>"
                      named:   exactly the declared stream_name
   → the durable identity of the published stream, under source
     derived:<app name>.
```

Splitting the signature from the progress key is what lets you edit an app
without losing its place. If progress were keyed by the signature, fixing a
comment would create a "new" binding starting at the current revision, and —
without `backfill` — every row written between the edit and the redeploy would
be silently skipped. Keying progress by what a binding reads and writes makes
an edit a resumption rather than a reset.

The `per_row` stream name excludes the executable digest for the same reason:
a recompiled app writes the same derived stream instead of orphaning the old
one and starting a new history.

## Design decisions

| decision | why | what it costs |
|---|---|---|
| **Outputs are keyed by (stream, timestamp) and overwrite** | Makes every recomputation idempotent, which removes checkpoints, deduplication, and partial-failure repair from both the runtime and the app author's job. | The store keeps current values, not a history of corrections. An app must be deterministic; non-deterministic code silently rewrites history. |
| **The database is the only recovery authority** | One place to look after a crash, and no queue state that can disagree with the data. | Progress advances only as fast as transactions commit; there is no in-memory fast path. |
| **A revision counter, not per-stream cursors** | One indexed predicate answers "what changed for this binding", however many streams it reads. | A revision is global, so bindings must skip revisions that did not touch their inputs (they do, without running user code). |
| **Progress keyed by what a binding reads and writes** | Editing code or parameters resumes instead of resetting, so a trivial edit cannot silently skip data. | Changing an app's *meaning* while keeping its inputs and outputs does not reprocess history; that needs a removal and a backfill, or a new name. |
| **Apps are bound by query, not by stream ID** | The calculation is written once and follows the plant model; sensors added later are picked up automatically. | Bindings appear and disappear as the model changes, and a query bug is a deployment-wide problem. A check makes the match list visible before deploying. |
| **The output declaration decides the grouping** | One concept fewer: there is no separate "mode" attribute that can disagree with what the outputs say. | Mixing `per_row` and `named` is only meaningful for a single-group query, which is a planning error to explain rather than a silent behavior. |
| **A `named` output must have exactly one owner** | An absolute stream name is a promise that one thing writes it; fan-out would make the writer ambiguous. | Fleet aggregates over a fanned-out calculation need a second, chained app. |
| **Output metadata is declared, never inferred** | A stream's value kind and semantics are fixed before any data exists, exactly as a driver registers a stream — so the first batch cannot define the schema by accident. | More to write in the declaration; `value_kind` is required. |
| **`transform` runs outside the transactions** | A slow or failing app cannot hold a write lock or block ingestion. | Inputs are fully materialized as Arrow before the call; a very wide window costs memory. |
| **Layers run in order; a wave's results land before the next layer reads** | A downstream app never observes a partially published upstream result. | A slow binding delays the layer it is in. |
| **Ray is optional and never durable state** | Object references and retries are not recovery information; the frontier is. The server runs in-process to avoid competing with the Ray driver supervisor. | Multi-node execution needs an embedder to construct `Scheduler` with a `RayExecutor`. |

## Storage backends

Both backends implement the same contract; only the SQL and the connection
mechanics differ.

| concern | DuckDB | PostgreSQL / TimescaleDB |
|---|---|---|
| Stream key in `timeseries` | integer `ref_id`, joined to `ref_ids` | `ref_uri` text directly |
| Timestamps | UTC-normalized `TIMESTAMP` (naive in SQL) | `TIMESTAMPTZ` |
| Revisioned write | registered Polars frame, delete+insert keyed by `(ref_id, ts)` | cursor `executemany` upsert keyed by `(ref_uri, ts)` |
| Parameters | `?` | `%s` |
| Write serialization | in-process lock; DuckDB has one writer | in-process lock plus transaction isolation |

`RevisionStore` owns the algorithm and adapts only those details. A backend
supplies four private hooks — `_own_conn`, `_write_conn`, `_next_revision`,
`_insert_frame` — and inherits the whole scheduler. TimescaleDB needs no
continuous aggregate or background job: derived rows are ordinary rows in the
same hypertable.

## Failure modes

| situation | what happens |
|---|---|
| `transform` raises during a scheduled run | That invocation commits nothing and its frontier does not advance; the error is logged (rate-limited) and it is retried on a later tick. Other bindings in the same wave still commit their own results, but the tick ends early, so later layers wait for the next one. A failing app cannot stop ingestion or the other durable workers. |
| `transform` raises during a check | Reported in that group's `error` field (the CLI exits non-zero). With `--local`, it raises in your terminal with a traceback instead. |
| The server restarts mid-computation | Nothing was committed, so the same window is read again and recomputed. Outputs overwrite themselves. |
| An app's file is edited on disk | A check always runs the new code: a module whose file changed is reloaded before it is used. A **deployed** app keeps running the code the server already imported; the edit surfaces at the next recompilation (a graph publication, another deployment, or a restart), where the file no longer matches the digest recorded at deploy time and loading fails with `entrypoint digest mismatch`. Because compilation covers every deployment at once, that stalls recompilation for all of them until the app is deployed again — which resumes from its kept frontier. |
| A deployed app's module cannot be imported | Deployment fails with an error naming the module; a check can be given a `search_path`, or run with `--local`. |
| The plant model changes | The plan is recompiled; new matches start under their `backfill` setting, and vanished matches simply stop running. Their derived streams remain. |
| Two bindings declare the same output stream | Rejected at planning: `multiple bindings own …`. |
| An app's query selects its own output | Rejected at planning: a binding cannot consume its own output. |

---

# API reference

Everything an app author needs is exported from the top-level `acquirium`
package:

```python
import acquirium as aq

aq.App        aq.output        aq.align        aq.console
```

The runtime types beneath them (`StreamSet`, `InputBatch`, `OutputBuilder`,
`Binding`, `RevisionStore`, `Scheduler`, …) are importable from
`acquirium.Materialization`. An app author reads them as documentation; an
embedder constructs them.

**Durations** appear throughout. Every duration accepts a string with a `ms`,
`s`, `m`, `h` or `d` suffix (`"250ms"`, `"30s"`, `"5m"`, `"2h"`, `"7d"`) or a
`datetime.timedelta`. The number may be fractional (`"1.5d"`), and durations
may not be negative.

## `aq.App`

```python
class MyApp(aq.App):
    name: str | None = None
    lookback: timedelta | str = "0s"
    lookahead: timedelta | str = "0s"
    backfill: bool = False
    coalesce: timedelta | str = "0s"
    max_delay: timedelta | str | None = None
    min_interval: timedelta | str | None = None
    outputs: Mapping[str, OutputSpec] = {}

    def build_query(self, plant) -> Query: ...
    def transform(self, inputs, output, context) -> None: ...
```

The base class for every app. Subclass it, declare `outputs`, and implement
`build_query` and `transform`.

### Attributes

| member | default | meaning |
|---|---|---|
| `name` | the Python class name | Durable app name; also the namespace of its derived streams (`derived:<name>`). Keep it stable across code revisions if the app should keep writing the same streams. |
| `lookback` | `"0s"` | Stored context read *before* the changed extent. `"all"` reads the whole retained extent instead. A windowed calculation needs at least its own window length. |
| `lookahead` | `"0s"` | Context read *after* the changed extent, for corrections that land mid-history. |
| `backfill` | `False` | Whether a newly seen binding starts at revision 0 (processing retained history) or at the current revision. Applies only the first time that binding is seen; after that its stored frontier governs. |
| `coalesce` | `"0s"` | Wait for this long a quiet gap in a burst of writes before running. |
| `max_delay` | `None` | Upper bound on the `coalesce` wait: run once the oldest pending change is this old. |
| `min_interval` | `None` | At most one run per interval for a binding. A throttle, not a schedule. |
| `outputs` | `{}` | Mapping of port name → output declaration. At least one is required. |

`outputs` is validated when the app is deployed or checked, before it runs:
port names must be non-empty strings, each value must come from
`aq.output.per_row(...)` or `aq.output.named(...)` (or be a mapping with the
same fields), and two named outputs may not claim the same stream name.

Declare `name` and `outputs` as **class** attributes — they are read from the
class, not from an instance. Windows and throttles may be set either way, so
`self.lookback = window` in `__init__` is a supported way to make a window
configurable.

### Constructor parameters

An app's `__init__` arguments are its deployment parameters:

```python
class HighTurbidityAlarm(aq.App):
    def __init__(self, threshold: float = 5.0):
        self.threshold = threshold
```

```toml
[[apps]]
spec = "./high_turbidity_alarm.py:HighTurbidityAlarm"
threshold = 3.0
```

```python
client.deploy_app(HighTurbidityAlarm, parameters={"threshold": 3.0})
```

Parameters are stored with the deployment, passed to the constructor on every
recompilation, and included in the binding signature — but not in the progress
key, so changing one resumes rather than restarts.

### `build_query(plant)`

```python
def build_query(self, plant) -> Query
```

Return one Acquirium [`Query`](client-api.md) selecting the app's inputs. It
runs during planning against a pinned graph revision — once per
recompilation, not once per batch.

`plant` is a deliberately narrow facade: it offers `plant.query()` and the
graph and text-resolution operations a query needs. It is not the runtime
client; do not fetch timeseries inside `build_query`.

Every `alias=` that resolves to a stream reference becomes a key of `inputs`
and a column of `context.result`; aliases that name entities appear only in
the match table.

```python
def build_query(self, plant):
    return (
        plant.query()
        .entity("ReverseOsmosis", alias="ro")
        .measurement(frm="ro", alias="feed", direction="upstream",
                     nearest=True, quantity_kind="pressure")
        .measurement(frm="ro", alias="permeate", direction="downstream",
                     nearest=True, quantity_kind="pressure")
    )
```

Two aliases only name different things when the query distinguishes them —
here by walking the topology upstream and downstream. Repeating one filter
under two aliases binds both to the same matches.

If the query matches nothing, the app compiles to no bindings, `transform` is
never called, and no derived stream exists. This is not an error; a check
reports `0 input group(s) matched`.

### `transform(inputs, output, context)`

```python
def transform(
    self,
    inputs: Mapping[str, StreamSet],
    output: OutputBuilder,
    context: InputBatch,
) -> None
```

Called once per binding that has unconsumed input. Receives a fixed,
internally consistent batch; the return value is ignored. Results are
published by assigning them to `output`.

Given the same batch, `transform` must produce the same output: the runtime
may recompute a window after a restart, and non-determinism silently rewrites
stored history. Side effects outside the database cannot be rolled back.

## `aq.output`

Two functions build the declarations that go in `outputs`. Both return an
`OutputSpec`.

### `aq.output.per_row(...)`

```python
aq.output.per_row(*, value_kind, point_uri=None, label=None, unit=None,
                  quantity_kind=None, medium=None, substance=None,
                  data_source=None, properties=None) -> OutputSpec
```

One derived stream per matched row, named after the port and the inputs it was
computed from:

```text
app name + port + sorted (input alias, input ref_uri) pairs
                      │  sha256
                      ▼
   ref_name  = "<port>:<digest>"
   source_id = "derived:<app name>"
   ref_uri   = the usual Acquirium reference URI for that pair
```

Use it whenever the calculation fans out: a thousand matched sensors become a
thousand derived streams, none of them named by hand, and recompiling the same
app, port and inputs reuses the same streams. If a group's bound inputs change
— a sensor joins a pair — that group's derived stream is a new one, and the
old stream keeps its history.

Passing `stream_name` raises; use `named` for that.

### `aq.output.named(stream_name, ...)`

```python
aq.output.named(stream_name, *, value_kind, point_uri=None, label=None, …) -> OutputSpec
```

One derived stream whose reference name is exactly `stream_name`, under
`derived:<app name>`. Use it when the result is a thing the plant refers to
directly — a total, an index, a compliance figure — so it can be found by
name.

A named output is valid only when the app's query resolves to exactly one
input group. Declaring one alongside `per_row` fan-out is a planning error
that names the offending ports and points at a second, chained app.

### Declaration arguments

| argument | meaning |
|---|---|
| `value_kind` | **Required.** `"numeric"` or `"text"`. Fixed at declaration; never inferred from published data. |
| `stream_name` | `named` only, and required there: the exact reference name. |
| `point_uri` | An existing point in the plant model to attach this derived reference to. When omitted, Acquirium creates a derived point. |
| `label` | `rdfs:label` placed on the output point. |
| `unit` | Unit URI placed on the output point. |
| `quantity_kind` | Quantity-kind URI placed on the output point. |
| `medium` | Medium URI placed on the output point. |
| `substance` | Substance URI placed on the output point. |
| `data_source` | A literal data-source tag on the output reference — the simplest handle for a downstream app's query to select on. |
| `properties` | Mapping of predicate URI → tuple of object URIs, added to the output point. |

Every field is explicit: an output's metadata is what its declaration says,
never copied from its inputs. Alongside the point metadata, the runtime
records `isCalculatedFrom` from the binding to each input and `produces` to
each output, which is what makes derived streams selectable by later apps and
forms the DAG.

## `StreamSet`

The value of each `inputs[alias]`: the rows for one alias inside this call's
window, plus a description of the streams that produced them.

| member | signature | returns |
|---|---|---|
| `.df()` | `df(library="polars")` | The read window as a [Polars](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html) dataframe with columns `ref_uri`, `time`, `value`. `library="pandas"` returns the same rows as pandas; any other value raises `ValueError`. |
| `.collect()` | `collect()` | The read window as a [`pyarrow.Table`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html). |
| `.batches()` | `batches()` | Iterator of `pyarrow.RecordBatch` over the read window (65,536 rows per batch by default). |
| `.changes` | attribute | `pyarrow.Table` holding only the rows that are new in this call — those written in the revisions it consumes. Same columns. |
| `.in_unit(unit)` | `in_unit(unit)` | A new `StreamSet` with every value converted into `unit`. |
| `.stream` | property | The single `StreamDescriptor` bound to this alias. Raises `ValueError` when the alias holds any other number of streams. |
| `.streams` | attribute | Tuple of every `StreamDescriptor` bound to this alias. |
| `.alias` | attribute | The query alias this set was bound to. |
| `.window` | attribute | The `TimeWindow` actually read; equal to `context.read_window`. |

`time` is a timezone-aware UTC microsecond timestamp. `value` is `float64`
for a numeric alias and a string for a text alias — an alias is read as text
when a row in the window carries a text value and no numeric one.

The frame's shape never changes with the match: `ref_uri` is present whether
one stream is bound or fifty. To find out which streams those are, ask the
stream set (`.stream` or `.streams`) rather than the frame.

### `.stream` versus `.streams`

| kind of output | streams per alias per call | use |
|---|---|---|
| `per_row` | exactly one, for every alias — a row pairing `feed` and `permeate` gives one of each | `.stream` |
| `named` | every match the query found | `.streams` |

`.stream` raises on a multi-stream alias rather than returning the first, so
an app converted from `per_row` to `named` fails loudly instead of silently
computing on one sensor and ignoring the rest.

### `.in_unit(unit)`

```python
temperature = inputs["temperature"].in_unit("DEG_C")
```

Returns a `StreamSet` whose values — the read window and `.changes` alike —
are converted into `unit`, each stream from its own recorded unit, so an alias
mixing Fahrenheit and Celsius sensors comes out uniform. `unit` may be a QUDT
URI, symbol, or label (`"DEG_C"`, `"http://qudt.org/vocab/unit/DEG_C"`,
`"mg/L"`). See [Units](../explanation/units.md).

The result is an ordinary `StreamSet`, so `.df()`, `.batches()`, `aq.align`
and a further `.in_unit()` all work on it.

Raises rather than producing mis-scaled values when a stream has no recorded
unit (`ValueError`), the units are dimensionally incompatible (`ValueError`),
the values are not numeric (`TypeError`), or the stream set carries no
converter (`RuntimeError` — the server injects one; an embedded scheduler
passes `unit_converter=` to `RevisionStore`).

Unit conversion is linear, so each stream's factor and offset are derived by
converting two sample values; expect floating-point error on the order of
1e-13.

## `StreamDescriptor`

What the compiled query knows about one stream.

| field | meaning |
|---|---|
| `ref_uri` | How the stream is identified in storage. |
| `point_uri` | The point in the plant model it measures, when the query bound one. |
| `label` | Human-readable name from the model. |
| `unit` | The unit URI the stream records in. |
| `value_kind`, `quantity_kind`, `medium`, `substance`, `properties` | Declared for completeness but **not populated** by the planner today. Do not rely on them. |

## `InputBatch`

The `context` argument: what this call is about, and why it happened. It holds
no measurement data.

| member | type | meaning |
|---|---|---|
| `.row` | `Mapping[str, Any]` | The matched row this call is computing. Raises `ValueError` for a `named` output, which is about every row at once. |
| `.result` | `polars.DataFrame` | Every row the query matched — the same table in every call of the app, whichever kind of output it declares. |
| `.changed_window` | `TimeWindow` | Timestamp extent of the unconsumed writes: why this call happened. |
| `.read_window` | `TimeWindow` | The range actually read: the changed extent widened by `lookback` and `lookahead`. Equals every `StreamSet.window`. |
| `.graph_revision` | `int` | The graph version this binding was compiled against. |
| `.from_revision`, `.to_revision` | `int` | The revisions this call consumes: everything after `from_revision`, up to and including `to_revision`. |
| `.binding_signature` | `str` | Identifies this compiled binding in logs and lineage. |

`.row` and `.result` use the column names of
[`Query.metadata()`](client-api.md): a column named for each alias holds the
URI that matched it, and where that alias is a sensor, `<alias>_ref`,
`<alias>.label` and `<alias>.unit` sit beside it.

## `OutputBuilder`

The `output` argument. Assignment is by declared port name, at most once per
call:

```python
output["celsius"] = frame.select("time", "value")
```

Accepted values: a Polars dataframe, a pandas dataframe, a `pyarrow.Table`, a
`pyarrow.RecordBatch`, or a sequence of record batches. Once converted to
Arrow, the table must have exactly two columns:

| column | requirement |
|---|---|
| `time` | Non-null, timezone-aware timestamps, unique within this assignment. Normalized to UTC microseconds and sorted ascending on write. |
| `value` | Non-null. Numbers for a `"numeric"` port (stored as `float64`); strings for a `"text"` port. |

| assignment | result |
|---|---|
| a declared port | accepted after validation |
| a name not in `outputs` | `KeyError`, listing the declared ports |
| the same port twice in one call | `ValueError` |
| wrong columns, wrong types, nulls, or duplicate timestamps | `TypeError`/`ValueError`, prefixed with the port name |

A declared port that is never assigned, or is assigned an empty table,
publishes nothing for that call — the frontier still advances. Everything
assigned across all ports in one call commits in a single transaction together
with that progress.

## `TimeWindow`

```python
TimeWindow(start: datetime, end: datetime)
```

An inclusive range of instants: a read window covers every row with
`start <= time <= end`. Naive datetimes are assumed to be UTC; aware ones are
converted to it. An `end` before `start` raises `ValueError`.

## `aq.align`

```python
aq.align(inputs, every, *, aggregate="mean") -> polars.DataFrame
```

Resample every input stream onto shared time buckets and return one wide
frame: a `time` column plus one column per stream. An alias bound to a single
stream contributes a column named after the alias; an alias bound to several
contributes `alias[label-or-ref]` columns. Buckets a stream never reported in
hold nulls.

- `inputs` — the mapping `transform` was given, or any subset of it.
- `every` — bucket size; a positive duration.
- `aggregate` — one of `mean`, `min`, `max`, `sum`, `first`, `last`, `median`,
  `count`.

```python
frame = aq.align(inputs, every="1m", aggregate="mean")
output["average"] = frame.select(
    "time", pl.mean_horizontal(pl.exclude("time")).alias("value")
).drop_nulls()
```

Raises `ValueError` for a non-positive bucket or an unknown aggregate. With no
rows at all, returns an empty frame with just a `time` column.

## `aq.console`

```python
aq.console(banner=None, *, depth=1) -> None
```

Open an interactive Python console holding the calling frame's variables — its
locals merged over its globals, locals winning. Inside `transform` that is
`inputs`, `output`, `context`, `self`, and whatever you have computed so far.
Ctrl-D or `exit()` closes it and execution resumes.

The namespace is a snapshot: rebinding a name in the console does not change
the variable in the running function, though mutating an object does. Pass
`banner` to replace the default banner, and `depth` to show an outer frame
(`depth=2` from inside a helper shows that helper's caller).

Without an interactive terminal — a deployed app, a server-side check, a test
— it logs a warning naming the call site and returns immediately, so a
forgotten console never blocks a server. To get a console from a check, run it
with `--local`.

## Checking an app

A check compiles the app's query against the live graph, reads **every
retained input row** for each resulting binding, runs `transform`, and returns
what it computed. It writes nothing: the app is not registered, its derived
streams are not created, no progress row is written or advanced, and no
revision is allocated. A deployed app of the same name is unaffected.

Because each binding reads its whole stored history rather than only the
newest rows, the values shown are what a `backfill = True` deployment would
publish.

### CLI

```bash
acquirium app check <module_or_file>:<ClassName> [options]
```

| option | default | meaning |
|---|---|---|
| `--params JSON` | `{}` | Constructor parameters, e.g. `'{"threshold": 3}'`. |
| `-n`, `--limit N` | `5` | Show the first `N` rows of each output; `0` shows every row. |
| `--local` | off | Run the app in this process instead of on the server. |
| `--json` | off | Print the raw result document. |
| `-c`, `--config PATH` | discovered | `acquirium.toml`, for the server address. |
| `--server-url`, `--server-port` | from config | Override the server address. |

Exits non-zero when any binding reported an error, so a check works as a CI
test. A spec naming a file (`./my_app.py:MyApp`) also sends that file's
directory to the server as a search path.

### Python

```python
client.check_app(MyApp, parameters={"offset": 273.15}, limit=None, search_path=None)
```

Returns the full result document; every computed row is included unless
`limit` is given. `search_path` defaults to the directory of the class's own
module (pass `""` to send none).

### HTTP

```text
POST /apps/check?limit=<int>&search_path=<dir>
```

Body is the deployment JSON. `search_path` is a directory **on the server's
filesystem** to look in first, so a file that is not otherwise importable
there can still be checked. Deployment has no such escape hatch: a deployed
app must be importable by the server on its own, since it is loaded again long
after the request that created it.

A module whose file has changed since the server imported it is reloaded, so
editing an app and re-checking runs the new code.

### The result document

```text
{
  "app": "temperature-normalizer",
  "graph_revision": 12,
  "bindings": [
    {
      "inputs": {"temperature": [{"ref_uri": …, "label": …, "unit": …}]},
      "row": {"temperature": …, "temperature_ref": …, "temperature.label": …},
      "input_rows": {"temperature": 288},
      "read_window": ["2026-09-01T00:00:00+00:00", "2026-09-02T00:00:00+00:00"],
      "outputs": {
        "normalized": {"stream": …, "ref_name": …, "value_kind": "numeric",
                       "rows": 288, "truncated": false,
                       "values": [{"time": …, "value": …}, …]}
      },
      "error": null
    }
  ]
}
```

One entry per input group, so an empty `bindings` list means the query matched
nothing. `rows` is always the full count; `values` holds every row unless
`limit` headed it, in which case `truncated` is true. A binding whose inputs
have no stored data reports `"no stored data for these inputs"` and no
outputs. A transform that raises is reported in that binding's `error` rather
than propagating, so one broken group still shows the others.

### Running the check locally

```bash
acquirium app check ./my_app.py:MyApp --local
```

```python
from acquirium.Materialization import local
result = local.check_app(client, MyApp, parameters={"offset": 273.15})
```

The app is compiled and run in the caller's process, with its inputs fetched
over the client API, and returns the same document. Three things follow:

- `breakpoint()` and `aq.console()` open in the calling terminal, and
  debuggers and profilers attach normally.
- A failing `transform` raises where it was called, with its traceback,
  instead of being captured in that binding's `error`. One broken binding
  therefore stops the check.
- The server never imports the app, so nothing needs to be importable there
  and `search_path` is irrelevant.

Everything else matches a server-side check, including the exact derived
stream each output would be written to. Input rows travel over HTTP, so
reading a lot of history is slower this way.

## Deploying

### Configuration

```toml
[[apps]]
spec = "./temperature_normalizer.py:TemperatureNormalizer"
offset = 273.15
```

Every key other than `spec` and `name` is passed to the constructor.
Relative paths resolve against the config file's directory. Apps in the
config are deployed once the server is healthy and its configured drivers have
started, so their queries resolve against the intended graph.

A `spec` may instead name a **registrar** — a callable taking
`(acquirium_client, parameters)` and returning an `App` class, an iterable of
them, or `None` — which is how a family of related apps is deployed from one
entry.

### Python

```python
client.deploy_app(MyApp, parameters={"offset": 273.15})   # PUT /apps/{name}
client.remove_app("temperature-normalizer")               # DELETE /apps/{name}
client.app_dag()                                          # GET /materialization/dag
```

`deploy_app` ships a *reference* to the code — module path, qualified name,
and a source digest — not the code itself, so the server must be able to
import the identical module. A class defined in a script's `__main__` is
rejected at deploy time for that reason.

`remove_app` deletes the deployment and forgets its progress rows, so
redeploying the same name starts fresh under its `backfill` setting. Its
derived streams are left in place.

`app_dag()` returns the active plan as a NetworkX `DiGraph`: one node per
binding (with its inputs, outputs, `lookback`, `backfill`, consumed and
current revisions) and one edge per derived stream a downstream binding reads.

To reprocess history with changed code, remove and redeploy with
`backfill = True`, or deploy the new version under a new name — the old
streams remain until their app is removed.

### Server settings

| key in `[server]` | default | meaning |
|---|---|---|
| `materialization_poll_seconds` | `0.25` | Idle polling cadence of the materialization workers. |
| `materialization_workers` | `2` | Number of concurrent materialization workers. |
| `materialization_error_log_seconds` | `30` | Rate limit for repeated failure logs from one worker. |

Run one server process: the embedded graph store has a single owning process.

## Embedding the runtime

The pieces below are for programs that schedule materialization themselves.
They are importable from `acquirium.Materialization`.

```python
from acquirium.Materialization import (
    ApplicationGraph, Binding, InProcessExecutor, RayExecutor,
    RevisionStore, Scheduler,
)
from acquirium.Materialization.planner import BindingPlanner, Deployment
from acquirium.Materialization.runtime import Materializer
```

| call | what it does |
|---|---|
| `Deployment.from_class(cls, *, parameters=None)` | Build the durable record from an `App` subclass. Rejects a non-`App`, an app with no outputs, and a `__main__`-defined class. |
| `Deployment.to_json()` / `.from_json(text)` | Round-trip the record; durations are whole microseconds and `lookback` may be `"all"`. |
| `BindingPlanner(graph, *, query_resolver=None, record_resolver=None)` | Construct a planner over anything offering `graph_status()` and `sparql_query()`. |
| `.compile(deployments, graph_revision, *, search_path=None)` | Returns `(ApplicationGraph, {binding signature: app instance})`. Raises if the graph moved during planning. |
| `RevisionStore(store, unit_converter=None)` | Durable read/write logic over a `DuckDBStore` or `TimescaleStore`. The converter is injected here and reaches `StreamSet.in_unit`. |
| `.initialise(binding, backfill=False)` | Create the progress row if absent; returns the consumed revision. |
| `.next_batch(binding)` | The next `Batch`, or `None`. Advances past irrelevant revisions. |
| `.preview_batch(binding)` | A batch over all stored input data, touching no durable state. What a check uses. |
| `.commit(binding, batch, results)` / `.commit_wave(commits)` | Publish results and advance frontiers atomically; returns which commits were accepted. |
| `Scheduler(store, executor=None)` | Defaults to `RayExecutor()`; pass `InProcessExecutor()` for deterministic single-process execution. |
| `.run_once(binding, app)` | Claim, execute and commit one binding. Returns whether anything committed. |
| `.run_layer(bindings, apps, *, max_workers=None)` | One dependency wave, committed together. |
| `.run_graph_once(graph, apps)` / `.run_until_idle(graph, apps)` | Every wave once, or until nothing is left to do. |
| `ApplicationGraph(bindings)` | Validates ownership and acyclicity; `.topological()` and `.layers()` give execution order. |
| `Binding.derive_output_ref_name(port, inputs, spec=None)` | The derived stream name a port would publish under. |
| `Materializer(store, graph, *, query_resolver=None, record_resolver=None, unit_converter=None)` | The full facade: `deploy`, `remove`, `check`, `refresh`, `run_once`, `dag`. |

`RayExecutor` runs each batch as a Ray task, putting the sealed Arrow batch in
the object store once. It is optional and holds no recovery state; the
frontier remains the only authority. `StreamSet.in_unit` needs a converter
that can be shipped to a worker, which the server's graph-backed converter is
not — so unit conversion under Ray raises rather than silently returning
unconverted values.

## Error messages

| message | cause | fix |
|---|---|---|
| `an app requires at least one declared output` | `outputs` is empty | Declare a port. |
| `an app class defined in a script's __main__ module cannot be deployed` | The class lives in the script you ran | Move it into an importable module. |
| `output 'x' is not declared in this app's outputs` | Typo, or a missing declaration | Assign a declared port. |
| `output 'x': an output must be an Arrow/Polars/pandas table with exactly time and value columns` | Extra columns, usually `ref_uri` | `select("time", "value")`. |
| `output 'x': output value must be non-null` | Nulls survived a resample or join | `drop_nulls()` or fill them. |
| `output 'x': output timestamps must be unique` | Two rows share a timestamp | Aggregate the duplicates. |
| `output 'x': text output requires string values` | A `"text"` port was given numbers | Format them, or declare the port numeric. |
| `alias 'a' is bound to N streams, not one` | `.stream` on a `named` app's alias | Use `.streams`. |
| `this call covers every matched row, so it has no single row` | `context.row` in a `named` app | Use `context.result`. |
| `app 'x' declares named output(s) […] alongside per-row fan-out` | A `named` port in a fanning-out app | Compute the aggregate in a second, chained app. |
| `multiple bindings own '…'` | Two apps declare the same named stream | Rename one. |
| `application bindings contain a cycle` | Apps read each other's outputs in a loop | Break the cycle. |
| `stream … has no recorded unit to convert from` | `in_unit` on a stream whose point has no unit | Declare the unit on the point. |
| `durations must use ms, s, m, h, or d` | An unrecognised suffix, such as `"2w"`, or a bare number | Spell the unit, or pass a `datetime.timedelta`. |
| `the server could not import '…' for entrypoint '…'` | The server cannot see the app's module | Install it, put it where the server imports from, or check with `--local`. |
| `entrypoint digest mismatch` | The file changed between deployment and load | Redeploy the app. |
| `graph changed while materialization plan was being compiled` | The plant model was published mid-planning | Nothing to do; the plan is recompiled. |
