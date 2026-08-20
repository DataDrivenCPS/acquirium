# Continuous latest-state microbatch runtime

## Status

This document defines the target execution model for Acquirium applications. It
replaces interval-driven incremental runs with a continuous, event-driven
microbatch runtime built on Ray and either DuckDB or PostgreSQL.

This is a clean replacement, not a compatibility proposal. The old run/cursor
endpoints, periodic keep-alive behavior, and internal completion-event mechanism
are removed rather than adapted.

The first implementation covers page-separable incremental applications without
lookback. It does not redesign bounded experiments, whole-history aggregates,
windowed processing, or mutable stateful operators.

## Motivation

The current path performs a complete distributed orchestration on every timer
tick: cursor lookup, Ray compute task, HTTP change read, Arrow staging, Ray
commit task, database transaction, completion-outbox poll, and downstream
trigger. That fixed work dominates small transforms.

The trivial one-row `value + 1` concurrency benchmark demonstrates saturation:

| Concurrent apps | p50 | p95 | Skipped runs | Drain time |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 44 ms | 714 ms | 0 | 0.52 s |
| 4 | 65 ms | 971 ms | 0 | 0.52 s |
| 16 | 201 ms | 2.83 s | 5 | 2.14 s |
| 64 | 9.25 s | 10.81 s | 608 | 11.33 s |

Registration is deliberately excluded: it is provisioning, not the standing
data path. More shared memory prevents exhaustion but does not remove per-run
scheduling, serialization, and transaction costs.

## Semantic contract

The central guarantee is latest-state convergence:

> An app processes the net keys changed since its last committed input versions,
> using values from one pinned snapshot of the newest canonical state.
> Intermediate values superseded before processing are not delivered.

If one timestamp changes while an app is behind:

```text
temperature v10: 20
temperature v11: 21
temperature v12: 22
```

an app moving from v9 toward v12 processes that key once with `22`. It does not
observe `20`, then `21`, then `22`.

Versions are invalidation and consistency boundaries, not immutable historical
snapshots. Acquirium borrows two ideas from
[BTrDB](https://www.usenix.org/system/files/conference/fast16/fast16-papers-andersen.pdf)
and [DISTIL](https://escholarship.org/uc/item/61w8z66w): a writer publication
advances affected stream versions, and a derived publication records the input
version vector incorporated into its result. It does not reproduce BTrDB's
copy-on-write storage.

The first implementation is not an every-event replay system. A separate event
contract is required when every intermediate value has meaning.

## Goals

- Low latency under light load and natural microbatching under pressure.
- Identical semantics on DuckDB and PostgreSQL without Kafka or Flink.
- Canonical timeseries as the only value authority.
- Writer-defined atomic publications with durable retry identity.
- Coalesced corrections evaluated against the newest consistent state.
- Atomic output, input-version advancement, provenance, and webhook intent.
- Explicit deletion/retraction so downstream state converges.
- Recovery from durable storage rather than notifications or Ray actor memory.
- Ordinary transforms executed inside long-lived actors; extra Ray tasks only
  for declared heavy parallel work.

## Architecture

```text
driver or app publication
          |
          v
DuckDB or PostgreSQL
  canonical rows + stream versions + changed-key manifests
          |
          v
server-local ChangeRouter
          |
          v
long-lived Ray AppRunner actors
          |
          v
atomic derived publication
          |
          +---- output keys re-enter the same path
```

The database owns values, publication identity, versions, consumer progress,
and transactions. Ray owns computation. Router messages are disposable hints.

## Durable data model

These schemas are logical. Implementations use backend-native types, and DuckDB
may retain its internal integer reference IDs.

### Canonical values and version heads

```text
timeseries(
    ref_uri,
    ts,
    numeric_value,
    text_value,
    deleted,
    last_stream_version,
    PRIMARY KEY (ref_uri, ts)
)

stream_heads(
    ref_uri PRIMARY KEY,
    current_version BIGINT NOT NULL,
    retained_from_version BIGINT NOT NULL
)
```

`timeseries` is authoritative. Normal reads hide `deleted=true`; physical
tombstones retain the last version touching a deleted key. Each affected stream
head advances exactly once per writer publication. `retained_from_version`
marks the oldest version still covered by changed-key manifests.

### Publications and changed keys

```text
stream_publications(
    publication_seq BIGINT PRIMARY KEY,
    publication_id TEXT UNIQUE NOT NULL,
    payload_hash TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    versions_json JSON NOT NULL,
    committed_at TIMESTAMP NOT NULL
)

stream_change_keys(
    publication_seq BIGINT NOT NULL,
    publication_row INTEGER NOT NULL,
    ref_uri TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    ts TIMESTAMP NOT NULL,
    PRIMARY KEY (publication_seq, publication_row)
)

INDEX stream_change_keys_ref_version (ref_uri, stream_version)
```

Publication receipts are durable idempotency records and survive manifest
compaction. `publication_seq` is a stable local ordering key, but correctness
does not depend on allocation order matching commit order; PostgreSQL sequences
do not provide that guarantee. `versions_json` records the resulting version of
every affected reference.

The manifest stores changed keys, not old values. A difference read finds keys
touched in `(from_version, to_version]`, then joins canonical values in the same
database snapshot. A multi-stream publication yields a vector such as:

```text
p42 -> {temperature: v18, pressure: v103, flow: v9}
```

The writer defines the atomic mutation set; storage assigns versions.

### App lifecycle, subscriptions, and provenance

```text
app_runtime(
    app_id TEXT PRIMARY KEY,
    generation BIGINT NOT NULL,
    status TEXT NOT NULL,
    topology_version BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL
)

app_subscriptions(
    app_id,
    generation,
    ref_uri,
    stream_version,
    PRIMARY KEY (app_id, generation, ref_uri)
)

app_batch_commits(
    app_id,
    generation,
    batch_id,
    batch_kind,
    rows_inserted,
    committed_at,
    PRIMARY KEY (app_id, generation, batch_id)
)

app_batch_inputs(
    app_id,
    generation,
    batch_id,
    ref_uri,
    from_version,
    to_version,
    PRIMARY KEY (app_id, generation, batch_id, ref_uri)
)
```

Runtime status is `registered`, `bootstrapping`, `active`, `stopping`, `stopped`,
or `failed`, and is authoritative during recovery. A subscription version means
the app's durable effects incorporate canonical input state through that stream
version. Batch inputs preserve precise derivation provenance.

### Historical bootstrap and reconciliation

```text
app_bootstraps(
    bootstrap_id,
    app_id,
    generation,
    status,
    next_ordinal,
    PRIMARY KEY (bootstrap_id)
)

app_bootstrap_streams(
    bootstrap_id,
    ref_uri,
    stream_version,
    PRIMARY KEY (bootstrap_id, ref_uri)
)

app_bootstrap_rows(
    bootstrap_id,
    ordinal,
    ref_uri,
    ts,
    numeric_value,
    text_value,
    PRIMARY KEY (bootstrap_id, ordinal)
)

app_bootstrap_outputs(
    bootstrap_id,
    ordinal,
    output_ref_uri,
    ts,
    numeric_value,
    text_value,
    PRIMARY KEY (bootstrap_id, ordinal)
)
```

These tables durably stage large input snapshots and their computed outputs so
pages can be retried without exposing partial replacement results.

## Publication protocol

Every write accepts a stable `publication_id`. Drivers assign it before handing
a collected batch to the client, and the client preserves it on retry. App
publications derive it from the stable app batch ID.

One transaction:

1. normalizes duplicate `(ref_uri, ts)` mutations to their final operation;
2. hashes the normalized payload;
3. returns the receipt when an existing ID has the same hash;
4. rejects an existing ID with a different hash;
5. locks affected stream heads in sorted `ref_uri` order;
6. increments each affected stream once;
7. upserts canonical values or canonical tombstones;
8. inserts one changed-key row per normalized key;
9. stores the receipt and resulting version vector; and
10. commits before waking the router.

Sorted head locking prevents overlapping PostgreSQL writers from deadlocking.
DuckDB performs the same protocol under its single write coordinator.

A driver publication may contain one record. It creates one receipt and one
changed-key row. Router coalescing and app microbatching are independent of
driver batch size.

## Consistent latest-state batch read

`next_app_batch` uses one repeatable snapshot transaction:

1. read subscribed stream versions;
2. find complete committed publications referenced by pending manifests,
   stopping near the target changed-key count;
3. include one oversized publication whole rather than splitting it;
4. derive actual `to_version` values from selected publications;
5. find distinct keys touched in each `(from_version, to_version]`;
6. left-join those keys to canonical timeseries in the same snapshot;
7. skip a canonical row whose `last_stream_version` exceeds this batch's
   `to_version`, because a later batch will process its newest value;
8. return live rows as `upsert` and tombstones as `delete`; and
9. materialize Arrow fully before closing the transaction.

Concurrent writes after the snapshot become a later batch. A batch may advance
versions while returning no value rows when all its keys were superseded by
later versions already visible in the snapshot. That cursor-only batch must
still commit.

The 50,000-key batch target is soft. Writer publications are never partially
acknowledged.

## Transform contract

Continuous transforms derive durable output only from materialized `ctx.inputs`,
immutable parameters, and immutable model/build state. Unversioned live storage
reads are rejected because they could observe values beyond recorded versions.

Transforms are pure with respect to logical streaming state. A cached trained
model may be reused for inference, but processing cannot mutate hidden actor
state needed by the next batch. Transactional state and windows require a
separate design.

Input and output frames contain:

```text
operation: "upsert" | "delete"
ref_uri
ts
numeric_value
text_value
```

Mapped transformations propagate deletes unless explicitly resolved otherwise.
Output validation rejects mutations outside declared output streams.

## Atomic app commit

Tail batch IDs derive from generation and sorted
`(ref_uri, from_version, to_version)` ranges. Bootstrap page IDs derive from
bootstrap ID and ordinal range.

One tail commit transaction:

1. claims `(app_id, generation, batch_id)` idempotently;
2. verifies submitted input ranges against the batch ID;
3. publishes normalized output mutations using the app batch ID;
4. records consumed version ranges;
5. advances subscription versions monotonically;
6. inserts external webhook intents;
7. records output count; and
8. commits everything together.

An already committed batch returns its stored result. An output-empty batch
advances inputs without creating an empty output publication. External webhook
intents retain the stable batch ID. Internal completion outbox records disappear:
output publications already wake downstream subscriptions.

## Full-history first start

Adding an app long after its input streams began still computes derived output
over the full retained canonical history.

On first `start_app`, one transaction:

1. establishes a repeatable snapshot;
2. captures the stream-version vector in that snapshot;
3. copies every live canonical row for matching inputs into bootstrap staging;
4. creates subscriptions at captured versions; and
5. commits durable bootstrap state.

PostgreSQL uses repeatable-read; DuckDB uses snapshot isolation. Publication
transactions update canonical rows and heads atomically, so captured rows and
versions describe one state without holding writer locks during a potentially
large historical copy. Publications committed after the snapshot have greater
per-stream versions and remain pending for tail processing.

The actor transforms staged history in stable pages and stores results in
`app_bootstrap_outputs`. After all pages succeed, one final transaction replaces
all app-owned output streams from staging, records input provenance, removes
staging, marks the app active, and wakes downstream apps. No partial historical
result becomes visible.

This supports page-separable transforms such as per-stream soft sensors, unit
conversions, filters with deletion support, and other mappings where page output
does not depend on another page. A whole-history aggregate, rolling window, or
transform with mutable cross-page state is not page-separable and belongs to a
bounded/windowed execution contract.

The same reconciliation protocol handles reset, app replacement, selector
expansion, and restart after required manifests were compacted. While staging,
subscriptions retain all versions newer than the snapshot.

Full history means retained canonical history. A separate timeseries retention
policy may have removed old timestamps; the runtime cannot reconstruct data the
canonical store no longer contains. It also means temporal history, not revision
history: if the value at one timestamp was corrected several times, bootstrap
uses its newest canonical value once because superseded revisions are not stored.

## Change router and actor execution

`ChangeRouter` lives in the server beside storage and the supervisor. After
publication commit, `router.wake(changed_refs)` signals that durable work may
exist. The router maps those references to active apps, coalesces for at most 50
ms, and sends one nonblocking `process_pending` call per ready actor. The wake-up
contains no authoritative cursor or data.

Busy apps have one pending bit, not an unbounded queue. A one-second safety scan
compares active subscription versions with stream heads and recovers lost
wake-ups. On startup the router triggers active and bootstrapping apps. App
versions and staging are durable, so router state need not be.

One long-lived `AppRunner` owns each app:

```text
process_pending
  -> next_app_batch
  -> materialize InputBatch
  -> transform locally
  -> validate mutations
  -> commit_app_batch
  -> schedule another turn when has_more or trigger_pending
```

The normal path creates no stateless compute or separate commit task. Only one
batch is in flight per generation, and each turn yields for stop/status/recovery.
Explicitly heavy CPU/GPU or mapped-parallel apps may shard a pinned batch; the
owner commits once after all shards succeed.

## Lifecycle

```python
aq.register_app(app)
aq.start_app(app.name)
aq.stop_app(app.name)
aq.reset_app(app.name)
aq.delete_app(app.name)
```

- `register_app` validates and records status `registered` without computing.
- `start_app` performs first-history bootstrap, resumes retained differences, or
  reconciles when its version is below a retained floor.
- `stop_app` waits for the current transaction boundary and prevents new work.
- `reset_app` creates a new generation and reconciles canonical history.
- replacing code creates a new generation and reconciles.
- `delete_app` removes execution, staging, subscriptions, provenance, outputs,
  and actor state owned by the app.

The old `run_app`, `keep_alive`, `interval`, public cursor APIs, and periodic
incremental loop are removed. Preview/debug remains read-only.

## Compaction

Changed-key rows are temporary invalidation state. Per reference:

```text
safe(ref) = minimum stream_version of active or bootstrapping subscriptions
```

The compactor deletes manifests through `safe(ref)` and advances the retained
floor. With no active or bootstrapping subscriber it may compact through the
head. Publication receipts remain durable.

Stopped apps do not pin manifests forever. On restart, a cursor at or above all
retained floors resumes through normal differences. A cursor below a floor
starts a new generation and reconciles from canonical history. This is valid
because latest-state convergence, not intermediate delivery, is promised.

Metrics expose heads, floors, version lag, manifest rows/bytes, reconciliation,
bootstrap age, and compaction progress.

## Internal transport and backends

Ray actors access storage through private Arrow endpoints:

```text
POST /internal/apps/{app_id}/batches/next
POST /internal/apps/{app_id}/batches/{batch_id}/commit
```

Responses carry generation, batch ID, operations, pinned input ranges, and
`has_more`. Actors never open backend databases.

DuckDB remains owned by the server process; mutations pass through one write
coordinator. PostgreSQL uses bounded snapshot reads and commits through its pool
with deterministic stream-head locking. Both provide identical publication,
version, tombstone, reconciliation, and idempotency semantics.

Defaults:

| Setting | Default |
| --- | ---: |
| Router coalescing delay | 50 ms |
| Target changed keys | 50,000 |
| Router safety scan | 1 second |
| Compaction interval | 60 seconds |
| Compaction chunk | 100,000 rows |

High-rate drivers may buffer on a small deadline or row threshold. Singleton
publication remains correct, and the server never acknowledges an uncommitted
in-memory buffer as durable.

## Failure behavior

- Lost wake-ups are recovered by the safety scan.
- Server/actor restart resumes durable versions or bootstrap staging.
- Failure before commit advances no input version.
- Lost commit responses retry the same batch ID.
- Reusing a publication ID with another hash fails.
- A failed shard prevents the owner commit.
- Webhook delivery retries independently from its durable intent.
- Concurrent input becomes a later pinned version.
- Stop waits for a transaction boundary.
- Compacted stopped cursors cause canonical reconciliation.
- Duplicate commits return one recorded result.
- Selector changes drain current work before topology replacement.

## Implementation sequence

1. Add canonical tombstones, row versions, stream heads, publication receipts,
   changed-key manifests, runtime state, subscriptions, bootstrap staging, and
   provenance to both stores. Implement atomic publication, snapshot difference,
   reconciliation, and compaction; remove old cursor/run tables.
2. Add private batch endpoints and clean `start`/`stop`/`reset` APIs. Add router,
   subscription index, safety scan, and compactor to server lifecycle.
3. Replace keep-alive loops with actor-local processing. Enforce pinned inputs,
   operations, pure transforms, and durable recovery. Retain workers only for
   declared heavy parallel work.
4. Route outputs through the same publication path. Remove completion outbox,
   public cursors, skipped ticks, obsolete stateless tasks, and interval config.
5. Add version lag, reconciliation, manifest, router, batch, and end-to-end
   latency metrics before tuning concurrency.

## Test and acceptance plan

Run one backend-contract suite against DuckDB and PostgreSQL covering:

- singleton/multirow publications and one version increment per affected stream;
- overlapping writers, deterministic locks, and duplicate-key normalization;
- matching/conflicting publication retries;
- atomic canonical rows, tombstones, heads, receipts, and manifests;
- whole-publication routing including an oversized publication;
- repeatable latest-state reads under concurrent correction;
- intermediate corrections coalesced to one latest value;
- superseded keys deferred safely and cursor-only batches;
- exact input-version provenance and duplicate app commits;
- rollback of output, versions, subscriptions, provenance, and webhook intent;
- full-history bootstrap added long after input ingestion;
- paged bootstrap with no partial output visibility;
- resume above and reconciliation below retained floors;
- compaction, generation reset, topology change, and output retraction.

Runtime tests cover lost wake-ups, restart at each phase, lost commit responses,
multi-hop chains without completion events, stop/start, reconciliation,
rejection of unversioned reads and mutable logical state, local execution for
ordinary apps, all-shards-success for parallel apps, and exclusive server
ownership of DuckDB.

Benchmarks measure input publication commit to output publication commit. On the
same compose stack, the 64-app one-row-per-second test must reach parity with no
failed batches, p50 below 500 ms, p95 below 2 seconds, and drain below 2 seconds.
DuckDB and PostgreSQL results are reported separately.

## Explicit consequences

- Existing runtime state and clients are unsupported; deployments initialize
  the new schema.
- Intermediate corrections may never be observed; newest canonical state wins.
- Publication receipts are durable; changed-key manifests are compactable.
- A stopped app may reconcile instead of replaying when manifests are gone.
- A newly added app can compute over all canonical history still retained.
- Reconciliation replaces all app-owned output and can be expensive.
- Continuous transforms are latest-state, pure, and page-separable. Windows,
  global aggregates, mutable state, and every-event delivery need other modes.
