# Revision-aware materialization runtime: development guide

## Status and authority

This document is the implementation guide for the next Acquirium application
runtime. It is intended to be handed directly to an implementation agent.

This design supersedes `continuous_batch.md` and `continuous_batch_plan.md` for
application execution. Those documents remain useful records of the publication,
versioning, tombstone, and backend work already completed, but their changed-key
delivery model, bootstrap machinery, and one-Ray-actor-per-app architecture are
not the target.

This is a clean replacement. Do not preserve old app endpoints, runtime tables,
serialized app state, or client compatibility. Preserve only components that
still fit the contracts below.

## Outcome

Acquirium will provide one low-level execution substrate and three user-facing
features built on it:

| Feature | Purpose | Lifecycle | Durable result |
| --- | --- | --- | --- |
| Transformation | Maintain derived streams as data or metadata changes | Standing | App-owned canonical streams |
| Experiment | Execute a bounded, attributable computation | Explicit start and finish | Run-scoped streams, artifacts, metrics, and provenance |
| Service | Host a persistent API, UI, controller, or dashboard | Standing process | Service-defined; durable writes use the publication API |

The common substrate is a **revision-aware materialization engine**:

```text
resolve bindings
    -> discover dependency changes
    -> calculate affected output ranges
    -> coalesce and partition work
    -> read one consistent input snapshot
    -> execute Python against Arrow batches
    -> atomically replace output ranges + advance progress + record provenance
```

This is intentionally not an every-event stream processor. It maintains the
latest correct materialized state. Multiple changes may coalesce before a
transformation observes them.

## Product principles

1. **Derived streams must be cheap to declare.** A pointwise unit conversion is
   one function plus declarations, with no cursor, scheduling, retry, deletion,
   or provenance code.
2. **Correctness belongs to storage.** Notifications and worker memory are
   hints and caches. Durable versions, bindings, work, commits, and state
   revisions survive process loss.
3. **Recompute ranges, do not make user code emit perfect deltas.** A worker
   returns the complete replacement for an owned output range. The platform
   retracts obsolete values in that range.
4. **Late data is ordinary data.** A late or corrected point invalidates the
   output range affected by its event time.
5. **Metadata is a versioned dependency.** Selector results and resolved
   metadata are persisted. A graph change can add, remove, or change bindings.
6. **Python is batch-oriented.** Arrow is the compute boundary. Scalar helpers
   are conveniences compiled onto that boundary.
7. **State is explicit.** Stateful classes are supported, but authoritative
   model state is a durable immutable artifact, never an actor's heap.
8. **Execution engines are replaceable.** DuckDB, plain Python, and eventually
   chDB are compute adapters. Ray is an executor. Neither defines semantics.
9. **DuckDB and PostgreSQL behave identically.** One backend contract suite is
   the gate for every storage phase.
10. **Bounded resources are part of correctness.** Work is leased, paged,
    coalesced, retryable, and observable. No logical app gets an unbounded
    in-memory queue.

## Repository starting point

The implementation agent starts from a branch that already contains the prior
continuous-batch implementation. Treat it as working code to mine and replace,
not as the target API:

| Current area | Files | Disposition |
| --- | --- | --- |
| Publication/version storage | `Storage/continuous/{types,ids,duckdb,postgres}.py` | Reuse stable IDs, hashes, tombstones, heads, locks, transactions, and backend tests; replace app-batch and changed-key contracts |
| Canonical stores | `Storage/duckdb_store.py`, `Storage/timescale_store.py` | Retain as authoritative storage and integrate range manifests/materialization tables |
| Graph generations | `Storage/graph_store.py`, `Server/manager.py` | Turn the current source/published versions into durable graph revisions and rebind wake-ups |
| Change router/compactor | `Server/router.py`, `Server/compactor.py` | Replace with materialization scheduler and range-manifest compaction |
| App execution | `Apps/runner.py`, `Apps/supervisor.py` | Replace per-app actors with fixed executors; retain safe source loading ideas only |
| User API | `Apps/base.py`, `Apps/mapped.py`, `Client/acquirium.py` | Replace with transformation/experiment/service APIs; preserve useful output metadata conventions selectively |
| Internal transport | `Server/app.py`, `Client/client.py` | Reuse Arrow IPC patterns; replace app batch endpoints |
| Tests | `tests/test_continuous_store_contract.py`, `tests/unit/test_change_router.py`, app tests | Preserve backend-contract style and behavioral coverage; rewrite around ranges, plans, and pools |

Many current comments cite `continuous_batch.md` or
`continuous_batch_plan.md`. Update or delete those comments as their code is
migrated so the repository does not retain two apparent authorities.

## Non-goals for the first release

- Every intermediate event or correction being delivered exactly once.
- Kafka/Flink-compatible event-time watermarks and triggers.
- Transparent distributed execution across multiple Acquirium servers.
- Arbitrary mutation of Python object state with automatic checkpointing.
- Automatic inference of an arbitrary Python function's temporal impact.
- Reconstructing values removed by canonical timeseries retention.
- Running chDB as an authoritative third storage backend.
- Backward compatibility with the current `App`, `MappedApp`, app endpoints,
  runtime tables, or persisted Ray actors.

## Vocabulary and semantic contract

### Publication

One atomic writer-defined set of canonical timeseries mutations. The existing
stable publication ID, payload hash, per-stream version increment, canonical
tombstone, and receipt concepts remain.

### Definition

An immutable registered version of a transformation, experiment, or service:
source digest, entry point, parameters schema, dependency declaration, output
declaration, impact policy, and execution requirements.

### Binding

One late-resolved materialization instance. A binding contains an arbitrary set
of input streams, an arbitrary set of output streams, and the resolved metadata
needed by the computation. It is the unit of dependency tracking and ownership.

Examples:

- one temperature stream -> one converted stream;
- temperature + humidity -> one comfort stream;
- three phase currents -> magnitude + imbalance streams;
- all sensors on one equipment item -> several model outputs.

### Graph revision

A monotonically increasing identity for a published canonical query graph. It
identifies the graph used to resolve bindings; it does not imply that Acquirium
can query every old graph revision. Each binding stores its resolved metadata
payload and content digest so execution never needs to reread an old graph.

### Invalidation

A durable statement that a binding's output may be wrong over an event-time
range because data, metadata, code, parameters, or model state changed.

### Reconciliation plan

A stable work plan created from one binding generation, graph revision, state
revision, input-version vector, and coalesced set of invalidations. A plan has
one or more independently replaceable range partitions.

### State revision

An immutable, content-addressed model or logical-state artifact produced by a
stateful transformation. A mutable model loaded in a worker is only a cache of a
state revision.

### Execution receipt

The durable record of a plan partition or experiment attempt: definition,
binding, graph revision, state revision, input versions, affected range, output
publication, timing, status, and error summary.

### Latest-state guarantee

For each active binding, Acquirium eventually makes its output equal to applying
the binding's definition and applicable state revision to the latest retained
canonical inputs and resolved metadata. Superseded intermediate input values
need not be observed.

Full history means all event timestamps retained in canonical storage, using the
newest canonical value at each timestamp. It does not mean every past revision
of a corrected timestamp.

## User-facing API

The implementation should offer a minimal function API and a class API. Both
compile to the same internal `MaterializationDefinition`.

### Function transformation

The smallest useful form is pointwise and one-to-one:

```python
@aq.transform(
    name="temperature_to_celsius",
    inputs=aq.select(quantity_kind="Temperature", unit="K"),
    outputs=aq.outputs.per_input(name="celsius", unit="Cel"),
)
def to_celsius(value: float) -> float:
    return value - 273.15
```

Type annotations define scalar input and output types. Internally, Acquirium
adapts the function to an Arrow batch. Built-in expressions should compile to
SQL or Arrow compute kernels when possible; correctness must not depend on that
optimization.

The general batch form is explicit:

```python
@aq.transform(
    name="comfort_index",
    bind=bindings.by_equipment(
        temperature=aq.select(quantity_kind="Temperature"),
        humidity=aq.select(quantity_kind="RelativeHumidity"),
    ),
    outputs=comfort_outputs,
    impact=aq.impact.lookback("5 minutes"),
)
def comfort(batch: pa.Table, ctx: aq.TransformContext) -> pa.Table:
    ...
```

The returned table is the complete desired content for the partition's declared
output streams and replacement range. Returning rows outside that ownership
boundary is an error.

### Class transformation

Class transformations support expensive setup, cached resources, explicit
durable state revisions, training, and model promotion:

```python
class AdaptiveController(aq.StatefulTransformation):
    name = "adaptive_controller"
    bindings = bindings.by_equipment(...)
    outputs = controller_outputs
    impact = aq.impact.lookback("15 minutes")
    update_policy = aq.state.every(hours=6, min_new_rows=10_000)

    def setup_worker(self, ctx: aq.WorkerContext) -> object:
        # Ephemeral cache/resource. Recreated after worker loss.
        return load_runtime_libraries()

    def train(
        self,
        data: pa.Table,
        previous: aq.StateArtifact | None,
        ctx: aq.TrainingContext,
    ) -> aq.StateCandidate:
        ...

    def load_state(
        self,
        artifact: aq.StateArtifact,
        worker: object,
    ) -> object:
        return deserialize_model(artifact)

    def transform(
        self,
        batch: pa.Table,
        model: object,
        ctx: aq.TransformContext,
    ) -> pa.Table:
        ...
```

The framework, not the object, owns training schedules and promotion. `train`
produces a candidate artifact. Promotion creates a state revision and applies
one declared policy:

- `prospective`: existing output remains; subsequent reconciliation plans use
  the new state. Late data arriving later also uses the then-active state.
- `recompute_all`: invalidate all retained history for the binding.
- `recompute_from(event_time)`: invalidate retained history at and after an
  explicit event-time boundary.

Every output receipt records the state revision actually used. A class may keep
the deserialized model in worker memory for speed, but a worker restart must be
able to reproduce behavior by loading the artifact.

Reinforcement-learning applications use the same model:

- observations, rewards, and actions are ordinary declared streams;
- training produces immutable policy state revisions;
- policy promotion is explicit and attributable;
- external actions use a durable idempotent effect intent, never a direct call
  hidden inside `transform`;
- online mutable replay buffers must be either reconstructible from streams or
  stored as explicit state artifacts.

### Binding API

Provide built-in binding helpers for common cases:

- `per_input(selector)`;
- `by_entity(selector_map, entity_alias=...)`;
- `single(input_map)`;
- a class implementing `resolve(graph: GraphView) -> Iterable[BindingSpec]`.

`BindingSpec` contains stable logical input/output identities and JSON-safe
resolved metadata. Binding IDs are derived from the definition ID and stable
logical binding key, not from the current input set. Its `content_digest`
changes when inputs, outputs, or resolved metadata change.

Do not expose storage reference IDs or require users to mint output URIs. Output
helpers own deterministic naming. Advanced users may provide explicit logical
identities, subject to ownership validation.

### Impact policies

Ship these policies first:

```python
aq.impact.pointwise()
aq.impact.lookback(duration)
aq.impact.window(before=..., after=...)
aq.impact.full_history()
```

An impact policy maps changed input event-time ranges to dirty output ranges.
It must be serializable and evaluable without importing user code so the
scheduler can recover work. A custom Python impact planner may be added later;
until it has a durable planning protocol, advanced applications use
`full_history()` conservatively.

Pointwise is the ergonomic default only for the scalar/per-input decorator.
General many-input transformations must declare impact explicitly.

### Experiments

Experiments are separate from transformations:

```python
with aq.experiment(
    LoadShift,
    start=start,
    end=end,
    params=Params(...),
    metadata={"scenario": "summer", "optimizer": {"gap": 0.01}},
) as run:
    result = run.execute()
    run.metric("total_cost", result.cost)
    run.keep("selected scenario")
```

At start, freeze and persist:

- definition and source digest;
- graph revision;
- complete resolved binding payloads;
- input stream version vector;
- requested event-time bounds;
- validated parameters and their JSON schema;
- active state revision or explicit model artifact;
- arbitrary nested JSON metadata.

Experiment outputs are run-owned, not standing transformation outputs. An
experiment always has a run record, including failure and cancellation.

### Services

Services are persistent processes with invalidation hooks:

```python
class Dashboard(aq.Service):
    subscriptions = [aq.select(equipment_type="AHU")]

    async def on_change(self, change: aq.ChangeHint) -> None:
        snapshot = await change.snapshot()
        await self.broadcast(render(snapshot))
```

Change hints are coalesced and at-least-once. They contain no authoritative
data. `change.snapshot()` reads current state using a supplied consistency
token. Services that publish durable data use the normal publication API.
External calls that must survive crashes use effect intents.

Services may use dedicated Ray actors or local ASGI processes. They do not run
through the materialization worker pool because their lifecycle and resource
ownership differ.

## Internal architecture

```text
drivers / app commits / graph publication
                  |
                  v
       canonical storage + revisions
                  |
          disposable wake hints
                  |
                  v
       MaterializationScheduler
       - rebind graph revisions
       - find lagging bindings
       - create/coalesce plans
       - lease partitions
                  |
                  v
       bounded ExecutorPool (Ray or local)
       - load definition/state cache
       - read Arrow snapshot
       - run function/class
                  |
                  v
       atomic range replacement commit
                  |
                  +--> downstream invalidation
                  +--> receipt/provenance
                  +--> durable effect intents
```

### Component boundaries

#### Storage

Storage owns canonical values, graph revision records, definitions, bindings,
dependency progress, manifests, plans, leases, commits, artifacts, experiment
runs, effect intents, and transactions.

#### Scheduler

The server-local scheduler is stateless beyond disposable caches. It asks
storage what is stale, creates durable plans, and sends lease IDs to executors.
A safety scan makes lost wake-ups harmless.

#### Executor pool

Use a fixed number of long-lived workers, not one actor per logical binding or
app. Workers cache imported definitions and loaded state artifacts by digest.
The pool size is configuration, with a low-resource local-process executor as a
valid implementation of the same protocol.

Ray starts and supervises workers and can fan out large backfills. Routine work
must not create new Ray actors or tasks per input publication.

#### Artifact store

The first implementation uses a server-owned filesystem directory with
content-addressed paths and atomic rename. Metadata and the active pointer live
in SQL. The interface must permit a future object-store implementation.

## Durable data model

These are logical schemas. DuckDB may use its existing integer `ref_id` mapping;
PostgreSQL uses `ref_uri`. Backend-specific SQL belongs in the backend modules.

### Retain from the current runtime

Keep, with cleanup and renaming where useful:

- canonical `timeseries`, including tombstones and `last_stream_version`;
- `stream_heads`;
- stable publication receipts and payload hashes;
- one compact durable manifest per affected stream/version;
- deterministic stream-head locking in PostgreSQL;
- the DuckDB server-owned single write coordinator;
- Arrow mutation schemas and backend contract-test pattern.

Change the manifest from one row per changed key to coalescible event-time
ranges. Exact keys are unnecessary when a materialization replaces ranges.

```text
stream_change_ranges(
    ref_uri,
    stream_version,
    publication_id,
    start_ts,
    end_ts,                 # half-open, UTC microsecond precision
    change_kind,            # upsert | delete | mixed
    row_count,
    PRIMARY KEY (ref_uri, stream_version, start_ts, end_ts)
)
```

A publication may emit several disjoint ranges for one stream. Initially,
normalize timestamps into configurable fixed buckets to bound manifest size;
adjacent buckets in the same publication are merged. Preserve the publication
receipt indefinitely or according to a separately documented idempotency
horizon. Range manifests are compactable after dependent progress advances.

All runtime timestamps use UTC at microsecond precision. The exact range for a
single timestamp is `[ts, ts + 1 microsecond)`. Bucket ranges are also half-open.
Centralize this arithmetic and test it identically on both backends.

### Definitions and bindings

```text
definitions(
    definition_id PRIMARY KEY,
    name,
    kind,                   # transformation | experiment | service
    source_digest,
    source_artifact_uri,
    entrypoint,
    spec_json,
    params_schema_json,
    created_at
)

deployments(
    name PRIMARY KEY,
    definition_id,
    generation,
    status,                 # registered | rebinding | active | paused | failed
    current_graph_revision,
    updated_at
)

bindings(
    binding_id,
    deployment_name,
    generation,
    logical_key,
    content_digest,
    graph_revision,
    resolved_metadata_json,
    status,                 # staging | active | retiring | failed
    PRIMARY KEY (binding_id, generation)
)

binding_inputs(
    binding_id,
    generation,
    ref_uri,
    role,
    PRIMARY KEY (binding_id, generation, ref_uri, role)
)

binding_outputs(
    binding_id,
    generation,
    ref_uri,
    role,
    PRIMARY KEY (binding_id, generation, ref_uri, role)
)

binding_progress(
    binding_id,
    generation,
    ref_uri,
    stream_version,
    PRIMARY KEY (binding_id, generation, ref_uri)
)
```

An output stream has exactly one owning active binding generation. Reject
ambiguous ownership during binding activation.

### Graph revisions and rebind requests

```text
graph_revisions(
    graph_revision PRIMARY KEY,
    source_version,
    content_digest,
    published_at
)

rebind_requests(
    deployment_name,
    graph_revision,
    status,
    attempts,
    lease_owner,
    lease_expires_at,
    error_json,
    PRIMARY KEY (deployment_name, graph_revision)
)
```

Create a graph revision only after the canonical inferred query graph has been
published. Rebinding reads that published graph and stores complete resolved
binding payloads. A newer graph revision supersedes queued older rebinds.

Graph storage and runtime SQL storage do not share a transaction. On startup
and after every graph-cache publication, compare the published graph version
and digest with the newest `graph_revisions` row and idempotently insert any
missing revision/rebind requests. A crash between graph publication and SQL
recording must delay rebinding, never lose it.

For v1, re-resolve every dynamic transformation on every published graph
revision. This is deliberately simple. Optimize with selector dependency
indexes only after measurement.

### Plans, partitions, and receipts

```text
materialization_plans(
    plan_id PRIMARY KEY,
    binding_id,
    generation,
    graph_revision,
    state_revision,
    input_vector_json,
    reason_json,
    status,                 # pending | running | finalizing | committed | failed
    created_at,
    completed_at
)

plan_inputs(
    plan_id,
    ref_uri,
    from_version,
    to_version,
    PRIMARY KEY (plan_id, ref_uri)
)

plan_partitions(
    partition_id PRIMARY KEY,
    plan_id,
    start_ts,
    end_ts,
    status,                 # pending | leased | committed | failed
    attempt,
    lease_owner,
    lease_expires_at,
    committed_output_id,
    error_json
)

execution_receipts(
    execution_id PRIMARY KEY,
    partition_id,
    attempt,
    definition_id,
    binding_id,
    generation,
    graph_revision,
    state_revision,
    input_vector_json,
    start_ts,
    end_ts,
    output_publication_id,
    rows_read,
    rows_written,
    started_at,
    finished_at,
    status,
    error_json
)
```

Plan IDs and partition IDs are hashes of their semantic contents. Retrying the
same work is idempotent. Lease IDs and attempts are operational identities and
are not included in output lineage.

### State artifacts and training

```text
state_artifacts(
    artifact_digest PRIMARY KEY,
    uri,
    size_bytes,
    media_type,
    metadata_json,
    created_at
)

state_revisions(
    state_revision PRIMARY KEY,
    deployment_name,
    binding_id,
    parent_state_revision,
    artifact_digest,
    training_execution_id,
    activation_policy,
    activation_input_vector_json,
    effective_from,
    status,                 # candidate | active | rejected | retired
    metrics_json,
    created_at,
    activated_at
)

training_requests(
    training_id PRIMARY KEY,
    deployment_name,
    binding_id,
    previous_state_revision,
    input_vector_json,
    range_start,
    range_end,
    status,
    lease_owner,
    lease_expires_at,
    error_json
)
```

Artifact write protocol:

1. Serialize to a temporary file under the artifact root.
2. `fsync` when configured for durable local operation.
3. Compute the digest over bytes.
4. Atomically rename to the content-addressed final path.
5. Insert `state_artifacts` and candidate `state_revisions` transactionally.
6. Promote the candidate in a separate explicit transaction that changes the
   active pointer and creates the required invalidations.

An orphan sweeper may remove temporary or unreferenced artifacts after a grace
period. Never delete an artifact referenced by a receipt or retained experiment.

### Experiments and effects

```text
experiment_runs(
    run_id PRIMARY KEY,
    definition_id,
    graph_revision,
    status,
    start_ts,
    end_ts,
    params_json,
    params_schema_json,
    metadata_json,
    input_vector_json,
    binding_snapshot_json,
    state_revision,
    started_at,
    finished_at,
    error_json,
    keep_reason,
    collected_at
)

run_metrics(run_id, name, value_json, recorded_at)
run_artifacts(run_id, name, artifact_digest, metadata_json)

effect_intents(
    effect_id PRIMARY KEY,
    execution_id,
    kind,
    destination,
    payload_json,
    idempotency_key,
    status,
    attempts,
    next_attempt_at,
    error_json
)
```

## Core algorithms

### 1. Canonical publication

Reuse the current atomic publication protocol, changing manifest emission:

1. Normalize duplicate `(ref_uri, timestamp)` mutations, last operation wins.
2. Compute the stable payload hash.
3. Return an existing matching receipt or reject a conflicting ID.
4. Lock affected PostgreSQL stream heads in sorted order.
5. Increment each affected stream exactly once.
6. Upsert values or tombstones with the assigned stream version.
7. Summarize affected timestamps into change ranges.
8. Insert range manifests and the receipt.
9. Commit.
10. Wake the scheduler with affected references.

Singleton driver publications remain correct. Driver-side buffering is a
latency/throughput optimization, not a semantic requirement.

### 2. Graph publication and rebinding

After the graph store publishes a complete inferred query graph:

1. Calculate its content digest and insert a graph revision.
2. Enqueue or upsert one rebind request per dynamic deployment.
3. A rebind worker resolves all `BindingSpec`s against that graph.
4. Validate deterministic identities, output ownership, and the resulting DAG.
5. Diff by stable binding ID and content digest.
6. For unchanged bindings, advance their recorded graph revision only.
7. For added bindings, create a staging generation and a full-history plan.
8. For changed bindings, create a staging generation and full-history plan.
9. For removed bindings, create a retiring generation whose replacement is
   empty across all owned output history.
10. Activate the entire new binding topology only after staging generations
    are complete. If validation or computation fails, retain the prior active
    topology and expose the failure.

Topology activation must be atomic from downstream discovery's perspective.
Derived stream declarations and lineage graph writes are published with the
binding generation, not piecemeal during resolution.

### 3. Discovering stale bindings

The scheduler maintains an in-memory index from input `ref_uri` to active
bindings. A publication wake consults the index and marks candidates. A periodic
safety scan asks storage for bindings whose progress trails any input head.

For each candidate with no open plan:

1. Start a repeatable-read transaction.
2. Read binding progress and current heads as the plan input vector.
3. Read change ranges in every `(from_version, to_version]` interval.
4. Apply the binding's declarative impact policy.
5. Union and coalesce overlapping/adjacent output ranges.
6. Partition ranges according to configurable duration and estimated row count.
7. Insert the plan, inputs, and partitions idempotently.
8. Commit and notify the executor pool.

If there are version changes but their normalized range set is empty, create a
zero-partition plan and finalize progress immediately.

Do not advance binding progress when a plan is created. Progress advances only
after every partition commits.

### 4. Snapshot preparation

When a worker leases a partition, it requests its input snapshot from the
server. In one repeatable snapshot:

1. Verify deployment, binding generation, plan, partition, and lease.
2. Read current input heads and capture them as the attempt vector.
3. Expand the partition range into each input's required read range using the
   impact policy.
4. Read every live canonical input row in those ranges.
5. Include tombstone/deletion information only where the transform contract
   explicitly needs it; range replacement otherwise handles disappearance.
6. Materialize Arrow before ending the transaction.
7. Return binding metadata, roles, output ownership, graph revision, state
   artifact identity, attempt vector, and Arrow rows.

The attempt vector may be newer than the plan vector. Latest state wins. Store
it on the execution attempt.

### 5. Transform execution

The worker:

1. Loads the immutable definition by source digest if not cached.
2. Calls `setup_worker` once per loaded class/digest when applicable.
3. Loads the state artifact by digest if not cached.
4. Constructs a read-only `TransformContext` containing only the materialized
   inputs, resolved binding metadata, range, versions, state identity, and
   deterministic execution ID.
5. Executes the scalar adapter, Arrow function, or class `transform` method.
6. Validates schema, timestamps, output ownership, range bounds, and value kind.
7. Submits the complete replacement plus attempt metadata.

Direct live graph/timeseries reads from transformation code are rejected. A
transformation that needs more data must declare it as an input/read range so it
participates in invalidation and provenance.

### 6. Optimistic atomic range commit

Do not hold a database snapshot open while Python computes. At commit, one
transaction:

1. Lock the partition and verify the lease and generation.
2. Return the stored receipt if this execution already committed.
3. Check manifests newer than the attempt vector. If any new input change maps
   through the impact policy to an output range intersecting this partition,
   reject the attempt as stale and return it to pending.
4. Normalize submitted rows and validate ownership/range again.
5. Produce tombstones for existing app-owned output keys in the partition that
   are absent from the replacement.
6. Publish tombstones and replacement rows as one output publication.
7. Mark the partition committed and insert its execution receipt.
8. Insert effect intents.
9. If every partition is committed, advance binding progress to at least the
   plan vector and mark the plan committed.
10. Commit and wake downstream references.

The stale-attempt check requires impact policies to be storage-evaluable. This
is why arbitrary Python impact functions are not in v1.

Concurrent changes outside the partition's dependency range do not invalidate
the attempt. Changes beyond the plan vector remain discoverable after progress
advances only to the plan vector.

### 7. Full-history bootstrap and topology replacement

A new or changed binding creates partitions over all retained canonical event
time. Compute them into generation-scoped staging outputs. Do not expose a
partially rebuilt topology.

After all staging partitions succeed, one activation transaction:

1. Verify the graph revision and deployment generation are still current.
2. Supersede or tombstone every prior-generation owned output not present in
   staging.
3. Publish the staged replacement.
4. Activate new bindings and retire removed/changed bindings.
5. Set progress to each bootstrap plan's captured input vector.
6. Record activation receipts and lineage.
7. Remove staging rows after commit or mark them for asynchronous cleanup.

Large activations must not build one enormous in-memory mutation table. Stage in
backend-native tables and execute set-based replacement inside the transaction.
If a backend cannot safely commit the full generation in one transaction,
implement indirection through an active generation column/view so activation is
one pointer swap.

Writes committed after a bootstrap snapshot remain above binding progress and
become ordinary tail plans.

### 8. Training and state promotion

The scheduler evaluates serialized update policies using durable input progress
and wall-clock state. When due:

1. Create one idempotent training request with its input vector and range.
2. Lease it to the executor pool.
3. Read a consistent training snapshot as Arrow.
4. Load the previous state artifact if declared.
5. Run `train` and persist the candidate artifact.
6. Record training metrics and receipt.
7. Apply automatic promotion policy or await explicit user promotion.
8. Promotion atomically changes the active state revision and creates the
   invalidations required by `prospective`, `recompute_all`, or
   `recompute_from`.

Prospective promotion allows already-created plans to finish with their pinned
old state; their receipts remain truthful, and progress advances only through
their original plan vectors. Full/from-time promotion invalidates leases and
rejects commits from older state revisions in affected ranges so old work cannot
overwrite a completed recomputation.

Only one training request per binding may actively mutate its state lineage at
a time. Parallel hyperparameter trials are experiments producing candidates;
promotion remains a single explicit operation.

### 9. Compaction

For each input stream, the safe manifest floor is the minimum version still
needed by:

- active binding progress;
- open reconciliation plans;
- staging topology generations;
- training requests requiring change-derived ranges.

Paused deployments do not pin manifests forever. If their progress falls below
the retained floor, resume creates a full-history reconciliation generation.

Execution receipts and state artifacts have separate retention policies.
Experiment retention follows `app_proposal.md`: preserve a recent window and
explicitly kept runs; collect large outputs/artifacts before cheap run tombstones.

## Backend rules

### DuckDB

- The server process remains the only owner of the writable database.
- All writes use the existing `DuckDBStore` lock and write coordinator.
- Executor workers access storage only through internal Arrow endpoints.
- Use set-based SQL for range replacement and staging activation.
- Do not open the same writable DuckDB file from Ray workers.

### PostgreSQL/TimescaleDB

- Use the existing bounded psycopg pool.
- Acquire overlapping stream/output locks in deterministic URI order.
- Use repeatable-read for snapshot preparation.
- Use `FOR UPDATE SKIP LOCKED` for work leases where available.
- Keep corrections to historical timestamps supported. Do not re-enable a
  Timescale compression policy that prevents required updates.
- Partition or index by `(ref_uri, ts)` and measure range replacement plans.

### Compute adapters

Define a narrow protocol:

```python
class ComputeAdapter(Protocol):
    def load(self, definition: DefinitionBundle) -> LoadedDefinition: ...
    def execute(self, loaded: LoadedDefinition, request: ComputeRequest) -> pa.Table: ...
    def train(self, loaded: LoadedDefinition, request: TrainingRequest) -> StateCandidate: ...
```

Implement:

1. `PythonArrowAdapter` first.
2. `DuckDBAdapter` for SQL expressions and vectorized Arrow Python UDFs.
3. A chDB experimental adapter only after the core acceptance suite passes.

Do not use chDB materialized views as the invalidation or correctness layer.
ClickHouse incremental materialized views operate on inserted blocks and do not
automatically reconcile arbitrary source mutations or metadata rebinding. chDB
may still be valuable for SQL-heavy scans; measure it as an adapter.

## Proposed module layout

```text
src/acquirium/
  Materialization/
    __init__.py
    api.py                  # decorators and public base classes
    bindings.py             # BindingSpec and built-in binders
    impact.py               # serializable impact policies
    definitions.py          # immutable specs/digests/bundles
    context.py              # Transform/Training/Worker contexts
    state.py                # state candidates/revisions/artifact API
    scheduler.py            # durable plan/rebind/training orchestration
    executor.py             # executor protocol and local pool
    ray_executor.py         # fixed Ray worker pool adapter
    worker.py               # definition/state caches and execution
    validation.py           # output/range/ownership validation
  Experiments/
    api.py
    runs.py
    retention.py
  Services/
    api.py
    supervisor.py
  Storage/materialization/
    types.py                # backend-neutral request/result models
    ids.py                  # deterministic digests and IDs
    artifacts.py            # artifact-store protocol + filesystem impl
    duckdb.py
    postgres.py
  Server/
    materialization.py      # internal/public endpoints
    effect_worker.py
```

Retire after replacement:

- `Storage/continuous/` changed-key app batch APIs and bootstrap tables;
- `Server/router.py` and `Server/compactor.py` in their current forms;
- continuous portions of `Apps/runner.py` and `Apps/supervisor.py`;
- `App`, `MappedApp`, and old start/reset semantics after examples/tests move;
- old internal batch endpoints and stale `run_app`/keep-alive models.

Do not delete reusable publication/version logic before equivalent contract
tests pass through its new location.

## HTTP and Arrow transport

Public endpoints:

```text
POST   /transformations/register
POST   /transformations/{name}/start
POST   /transformations/{name}/pause
POST   /transformations/{name}/rebind
POST   /transformations/{name}/reconcile
GET    /transformations
GET    /transformations/{name}

POST   /transformations/{name}/state/train
POST   /transformations/{name}/state/{revision}/promote
GET    /transformations/{name}/state

POST   /experiments
POST   /experiments/{run_id}/cancel
POST   /experiments/{run_id}/keep
GET    /experiments/{run_id}
GET    /experiments

POST   /services/register
POST   /services/{name}/start
POST   /services/{name}/stop
GET    /services/{name}
```

Internal executor endpoints initially use Arrow IPC over HTTP, matching the
current worker/server isolation:

```text
POST /internal/materializations/lease
POST /internal/materializations/{partition_id}/snapshot
POST /internal/materializations/{partition_id}/commit
POST /internal/materializations/{partition_id}/fail

POST /internal/training/lease
POST /internal/training/{training_id}/snapshot
POST /internal/training/{training_id}/complete
POST /internal/training/{training_id}/fail
```

Put execution metadata in Arrow schema metadata or typed HTTP headers; never
encode it as repeated columns on every data row. All internal requests include
lease owner, attempt, generation, and deterministic work ID.

## Development phases

Each phase must be independently reviewable and leave the tree green. Do not
build all tables first and defer executable behavior until the end.

### Phase 0 — Characterization and deletion map

Deliverables:

- Add a short architecture decision record pointing to this guide.
- Characterize existing publication semantics on both backends.
- Capture baseline ingest throughput, one-row publication latency, historical
  range read throughput, and the existing 1/4/16/64-app benchmark.
- List every old endpoint, table, model field, test, and config key to delete.
- Add feature-level tests describing the new public API as skipped tests only
  if they are immediately enabled in subsequent phases.

Exit gate: baseline artifact committed under `benchmarks/results/` without
credentials or machine-specific paths; publication contract green.

### Phase 1 — Revision and range-manifest storage core

Files:

- introduce `Storage/materialization/types.py`, `ids.py`, `duckdb.py`, and
  `postgres.py`;
- adapt `duckdb_store.py`, `timescale_store.py`, and `Server/manager.py`;
- add range manifests alongside exact changed-key emission temporarily;
- add graph revision publication after inferred graph publication.

Tests:

- singleton and multirow publications;
- corrections and tombstones at old timestamps;
- stable retry and conflicting retry;
- one stream-version increment per affected publication;
- disjoint and adjacent range normalization;
- atomic rows, heads, manifests, and receipts;
- graph revision emitted only for a completely published query graph;
- DuckDB/PostgreSQL parity and concurrent PostgreSQL writers.

Exit gate: no application runtime changes yet; all canonical writes emit the new
range manifest contract. The old path may still dual-write/read changed keys
only to keep the branch executable until Phase 4. No new code may depend on
changed keys, and the old manifest/table is deleted after cutover.

### Phase 2 — Definitions, bindings, graph rebinding, and DAG validation

Files:

- add `Materialization/api.py`, `bindings.py`, `impact.py`, `definitions.py`;
- implement definition packaging/digests without shipping arbitrary mutable
  object instances;
- add definition/deployment/binding tables and store methods;
- implement rebind requests and a server-local rebind worker;
- register derived output declarations and lineage by binding generation.

Tests:

- deterministic function/class definition digests;
- stable binding IDs and changed content digests;
- one-to-one, one-to-many, many-to-one, and many-to-many bindings;
- graph change adds/removes/changes bindings;
- failed rebind leaves old topology active;
- ambiguous output ownership rejected;
- direct and late-bound DAG cycles rejected;
- unchanged graph selection creates no recomputation.

Exit gate: a registered transformation resolves and persists a valid topology,
but does not execute yet.

### Phase 3 — Plans, impact, leases, and optimistic commits

Files:

- implement plan/partition/receipt storage contracts on both backends;
- add `Materialization/scheduler.py` and `validation.py`;
- add internal lease/snapshot/commit endpoints;
- implement pointwise, lookback, window, and full-history impact policies;
- implement backend-native atomic range replacement.

Tests:

- late point creates the correct event-time invalidation;
- overlapping changes coalesce;
- lookback/window expansion is exact at boundaries;
- plans partition without losing dirty ranges;
- lease timeout and retry;
- duplicate commit returns one receipt;
- concurrent intersecting input change rejects stale output;
- nonintersecting change does not reject valid output;
- missing replacement rows become tombstones;
- progress advances only after every plan partition commits;
- crash at every transaction boundary recovers.

Exit gate: storage can drive a fake executor end to end on both backends.

### Phase 4 — Arrow executor pool and public function transformations

Files:

- implement local executor and fixed Ray executor pool;
- implement `PythonArrowAdapter` and optional scalar adapter;
- wire public registration/lifecycle/status endpoints and client methods;
- add preview/debug against the same compute request without committing;
- remove one-actor-per-app execution from the active path.
- stop dual-writing exact changed-key manifests once the old runtime is no
  longer active; remove their tests or rewrite them against ranges.

Tests:

- pointwise unit conversion from the minimal decorator;
- vectorized Arrow function;
- many-to-many batch transformation;
- output schema/range/ownership violations;
- worker crash and definition cache reload;
- server restart with pending and leased work;
- long backfill does not block singleton ingest;
- multi-hop DAG convergence;
- 1,000 logical pointwise bindings do not create 1,000 Ray actors.

Exit gate: stateless transformations are production-complete and meet the
initial performance gates below.

### Phase 5 — Atomic topology bootstrap and reconciliation

Implement generation-scoped staging and pointer-swap activation for added,
changed, removed, and manually reconciled bindings.

Tests:

- app added years after inputs computes all retained history;
- graph change during bootstrap queues a newer rebind without exposing partial
  topology;
- removed binding retracts all owned output;
- selector expansion and contraction;
- code/parameter replacement;
- canonical retention boundary is reported clearly;
- tail changes arriving during bootstrap run afterward;
- activation is atomic to downstream selectors and transformations.

Exit gate: metadata-driven late binding is complete end to end.

### Phase 6 — Stateful classes, artifacts, training, and promotion

Files:

- implement `Materialization/state.py` and filesystem artifact storage;
- implement worker setup/state caches;
- add training leases, receipts, policies, and promotion endpoints;
- implement prospective/full/from-time promotion invalidation;
- add artifact retention and orphan cleanup.

Tests:

- setup resource recreated after worker loss;
- state artifact digest is stable and verified on load;
- training retry produces one candidate revision;
- promotion is atomic;
- prospective promotion does not rewrite existing output;
- full and from-time promotion dirty exactly intended ranges;
- every output receipt cites the actual state revision;
- worker-local mutation cannot become authoritative state;
- previous artifact remains usable after failed training;
- simple online-learning and reinforcement-learning examples recover after
  server and worker restart.

Exit gate: class-based transformations are durable and reproducible without
depending on Ray actor memory.

### Phase 7 — Experiments and provenance

Implement bounded run APIs, frozen bindings, parameters schemas, nested metadata,
metrics, artifacts, run-owned streams, keep/collect retention, and graph lineage.

Tests:

- success/failure/cancel records;
- frozen graph/bindings despite concurrent metadata change;
- params validation and old-schema read fallback;
- arbitrary nested metadata round trip and server-side filtering;
- run output identity isolation;
- keep and collection behavior;
- rerun from a retained run's definition and parameters.

Exit gate: the load-shifting example from `app_proposal.md` contains application
logic rather than hand-written provenance bookkeeping.

### Phase 8 — Services and effect delivery

Implement service packaging/lifecycle, coalesced data and graph change hints,
snapshot tokens, dedicated execution, and durable idempotent effect delivery.

Tests:

- at-least-once coalesced hint delivery;
- lost hints recovered by safety scan;
- snapshot reads current authoritative state;
- service restart and health reporting;
- effect retry, deduplication, backoff, and dead-letter status;
- a service cannot bypass output ownership for derived streams.

Exit gate: a streaming dashboard example updates without polling raw database
internals.

### Phase 9 — Cleanup, tuning, and optional chDB spike

- Remove old runtime tables, endpoints, models, configs, source restoration,
  actors, and tests identified in Phase 0.
- Rename remaining publication modules consistently.
- Add schema/version diagnostics that fail clearly on old databases.
- Benchmark DuckDB SQL/Arrow UDF execution against plain Arrow Python.
- Benchmark chDB only as a compute adapter on historical scans, joins, and
  model feature preparation. Do not integrate it unless it beats the simpler
  adapters materially after import, conversion, and memory costs are included.
- Update `README.md`, examples, CLI help, and deployment documentation.

Exit gate: no production path references the superseded continuous app runtime.

## Test organization

Use these suites:

```text
tests/unit/test_materialization_*.py
tests/test_materialization_store_contract.py
tests/integration/test_materialization_api.py
tests/integration/test_materialization_recovery.py
tests/integration/test_materialization_dag.py
tests/integration/test_stateful_transformations.py
tests/integration/test_experiments.py
tests/integration/test_services.py
```

Service-free unit tests cover IDs, range algebra, impact policies, binding diffs,
DAG validation, schemas, validation, cache behavior, and scheduler state
transitions. The shared backend contract is parameterized over DuckDB and
PostgreSQL. Integration tests use settings from `tests/conftest.py` and never
hard-code localhost ports or credentials.

Property-based tests are strongly recommended for range union/subtraction,
impact expansion, partitioning, and progress advancement. Important invariants:

- normalized ranges are ordered, nonoverlapping, and half-open;
- unioning change sets never loses a timestamp;
- progress never exceeds a plan's captured vector;
- committed output never spans outside owned streams/ranges;
- an intersecting post-snapshot change cannot commit stale output;
- retries do not create duplicate publications, effects, states, or receipts.

## Performance and resource acceptance

Measure publication commit to final derived publication commit, excluding app
registration and initial source packaging.

Initial single-machine gates on the existing benchmark host:

- 64 one-row-per-second pointwise transformations: p50 < 250 ms, p95 < 1 s,
  drain < 2 s, no failed or skipped work;
- 1,000 idle logical bindings: no per-binding Ray actor and less than 50 MiB
  incremental server metadata/cache memory beyond persisted state;
- a 10-million-row backfill uses bounded worker/server memory and does not hold
  the canonical write lock while Python executes;
- singleton ingest latency does not grow linearly with the number of bindings;
- coalescing 1,000 corrections to the same range produces bounded plans rather
  than 1,000 Python executions;
- both backends report ingest, planning, queue, snapshot, compute, commit, and
  end-to-end timings separately.

Do not encode the exact gates as universal product promises until results exist
on representative Raspberry Pi, laptop, and workstation hardware. Preserve the
benchmark shape and report hardware/configuration with results.

## Observability

Expose:

- stream heads and manifest floors;
- graph revision and rebind lag;
- active/staging/failed binding counts;
- open plans and partitions by status;
- oldest pending work age;
- lease expirations and retry counts;
- invalidated/coalesced range duration;
- rows and bytes read/written;
- snapshot, compute, validation, and commit latency;
- stale-attempt rejection rate;
- per-deployment input-version lag;
- active/candidate state revisions and training age;
- artifact bytes and orphan count;
- experiment/service/effect status.

Every log line associated with execution should carry deployment, binding,
generation, plan, partition, execution, and attempt IDs where applicable.

## Security and isolation

- Treat registered source as trusted local code in v1 and say so explicitly.
- Never unpickle an artifact before verifying its digest and expected media
  type. Prefer safe model formats where practical.
- Validate artifact paths remain beneath the configured artifact root.
- Do not expose internal executor endpoints outside the server/Ray network.
- Redact secrets from definition parameters, receipts, error payloads, and
  experiment metadata; secret references are resolved at execution time.
- Effect destinations must pass the same URL policy used for current triggers.
- Bound Arrow body sizes, output row counts, range sizes, execution time, and
  worker memory through configuration.

## Implementation discipline

- Follow `AGENTS.md` and use Python 3.12/type hints.
- Use `uv` commands and the existing make targets.
- Keep backend-neutral algorithms out of backend SQL modules.
- Add no generic utility package; place range/ID logic in focused modules.
- Use one transaction per documented atomic boundary.
- Never make notification delivery or Ray object retention a correctness
  requirement.
- Prefer deleting superseded paths to maintaining adapters.
- Do not add Kafka, Flink, Redis, Celery, or another service.
- Do not add chDB to dependencies before the Phase 9 benchmark justifies it.
- Commit each phase in scoped imperative commits and report schema/configuration
  consequences in the PR.

## Decisions intentionally fixed by this guide

- The platform is an incremental materializer, not an event replay engine.
- Dirty event-time ranges are the durable work representation.
- Transform results replace owned ranges.
- Function and class APIs share one internal definition.
- Stateful correctness uses immutable state artifacts and revisions.
- Worker/class memory is cache only.
- Metadata changes create binding generations and atomic topology activation.
- A bounded executor pool replaces one actor per app/binding.
- Arrow is the compute boundary.
- DuckDB and PostgreSQL remain authoritative backends.
- chDB is an optional future compute adapter, not the semantic substrate.
- Experiments and services are separate lifecycle abstractions built on shared
  revisions, snapshots, receipts, artifacts, and effects.

## Questions that do not block implementation

The following choices can use the defaults below until real applications provide
evidence to change them:

| Question | Default |
| --- | --- |
| Manifest range bucket | 1 minute, configurable |
| Plan coalescing delay | 50 ms |
| Safety scan | 1 second |
| Lease duration | 5 minutes with heartbeat for long work |
| Tail range partition | estimated 100,000 input rows, bounded by time |
| Bootstrap partition | estimated 500,000 input rows |
| Function execution | Arrow batch |
| Stateful promotion | Explicit unless definition opts into metric-based auto-promotion |
| Model update effect | Prospective unless explicitly declared otherwise |
| Paused binding retention | Compact normally; reconcile if it falls behind |
| Graph rebinding | Re-resolve all dynamic deployments |
| Artifact backend | Content-addressed local filesystem |
| Ray deployment | Fixed worker pool; local executor permitted |

The first implementation agent should not pause to redesign these defaults.
Record measurements and revisit them at the phase exit gates.
