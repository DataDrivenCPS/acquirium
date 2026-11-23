# Escapement build plan: the engine and the bindings

Written 2026-11-23 for whoever implements it. It covers step 6 of the
[implementation order](data-processing-model.md#implementation-order) in the
design: the log and admission, the epoch driver, the refresher, retention,
the query interface and the Python bindings. Steps 1 to 5 were built from
[the first build plan](escapement-build-plan.md),
[the plan build plan](escapement-plan-build-plan.md),
[the execution build plan](escapement-exec-build-plan.md) and
[the repair build plan](escapement-repair-build-plan.md), and are on the
Escapement branch `gtf-new-engine`.

The signatures below state intent. None of this code has been compiled.
Change a signature where the compiler requires it, and say what you changed
in your report.

## What you are building

Steps 1 to 5 built the parts. `repair` builds one epoch from source changes
and returns what to publish; `store` publishes it; `exec` runs the
functions. Nothing yet takes requests from writers, decides what goes into
each epoch, runs epochs as a service, keeps asynchronous instances
refreshed, or answers reads. This step adds that, and the Python package
that Acquirium will call in step 7.

| Crate | Path | Package | Contents |
| --- | --- | --- | --- |
| `engine` | `crates/engine` | `escapement-engine` | Admission, the epoch driver, the refresher, retention, status, snapshots and the query interface. The Rust API applications use. |
| `python` | `crates/python` | `escapement-bindings` | The Python extension module and its small Python package, passing record batches without copying. |

`engine` depends on every crate before it. `python` depends on `engine`
alone.

Three earlier crates change:

- `series` gains requests, receipts, the epoch record and module bytes, and
  a publication that also marks requests applied
  ([Milestone 1](#milestone-1-the-log-in-storage));
- `store` keeps them on DuckDB and PostgreSQL;
- `exec`'s pool takes module bytes from its caller, not only from files
  ([Module bytes](#module-bytes)).

Work in four milestones, each finished with its tests before the next:

1. **The log in storage**: requests, identities, epoch records, module bytes.
2. **The engine**: admission, the epoch driver, the refresher, retention.
3. **The query interface**, and the acceptance suite.
4. **The Python bindings**, and the measurements.

Not in this step: anything in Acquirium (step 7), including its adapter for
functions, its plan compiler and its read routes.

## Read first

The design is `docs/plans/data-processing-model.md` in the Acquirium
repository. Read these sections before writing code:

- One change, end to end
- The log and epochs
- Freshness and failure: Asynchronous refresh, What a reader sees
- Storage: Publication, Retention, Recovery
- Queries, all of it
- Threads
- The Acquirium boundary
- Invariants and tests
- Decisions

Then read the code of steps 1 to 5 in `crates/`:

- `series`: `Store`, `Publication`, `Changes`, `Op`, `check_plan`,
  `plan_streams`, `Records`, `Staging`, the contract suite.
- `store`: how `publish` runs its one transaction on each backend, and the
  reader connections in `DuckStore`.
- `exec`: `Pool`, its `Module` versions, `reload`, and `Settings`.
- `repair`: `build`, `Epoch`, `Refresh`, `publish`, `refresh` and
  `Refreshed`, `Report`, `Settings`, and `repair::testing`.

Where the design and this plan disagree, the design decides what the system
means and this plan decides crate layout, names and tests. If they conflict,
or the design is silent on something that changes behaviour, stop and ask
the user. Do not settle it yourself.

## Where things are

- The Escapement repository is `/home/gabe/src/acquirium/escapement`, on
  branch `gtf-new-engine`. The design is at `../docs/plans/` from there.
- The current engine is a reference for behaviour, not a starting point:
  - `src/engine.rs` holds its admission (group commit in `Admissions`, the
    limits in `Limits`);
  - `src/processor.rs` holds its background driver, its draining and its
    pause after a failed epoch;
  - `src/store/duckdb.rs` and `src/store/postgres.rs` hold its inbox tables
    and its two rotating tables of retry identities;
  - `bindings/python/src/lib.rs` is its Python API, which Acquirium calls
    today.
- `tests/` holds the 125 cases this step translates
  ([The acceptance suite](#the-acceptance-suite)).
- Acquirium's use of the current engine is `src/acquirium/core.py` (writes,
  reads, the change feed, stream statistics), `src/acquirium/apps.py`
  (activation and its preview) and `src/acquirium/ExperimentSamples.py`
  (receipts and the epochs that applied them). Read them to see what the
  query interface and the bindings must serve. Do not change them.

## Ground rules

These are the earlier build plans' rules.

- **Leave the current engine alone.** Do not edit `src/`, `bindings/`,
  `python/`, `tests/`, `examples/`, `benchmarks/` or `docs/`. Outside
  `crates/` you may edit only the root `Cargo.toml` (to add the workspace
  members), `Cargo.lock`, `.github/workflows/ci.yml` and the `Makefile`. The
  root `pyproject.toml` belongs to the current engine's package: the new
  package has its own, in `crates/python` (decision 12).
- **Toolchain.** Rust edition 2024, `rust-version` 1.89.
- **Dependencies.** Only what the earlier plans allowed, and:
  - `engine`: every engine crate, `arrow-array`, `arrow-schema`,
    `thiserror`, and `sha2` for request digests (in the lockfile, used by
    the current engine);
  - `python`: `engine`, `pyo3` at the version the lockfile has (`=0.29.2`),
    `pyo3-build-config`, and `arrow-array`'s `ffi` feature for the Arrow C
    stream interface (decision 13).

  The user confirmed these on 2026-11-23 (decision 13). Ask before adding
  anything else.
- **Safety.** `#![forbid(unsafe_code)]` in `engine`. `python` needs
  `unsafe` only where the Arrow C interface does; keep it in one module with
  a comment on each block.
- **Checks.** As in CI:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  - `cargo clippy --all-targets --no-default-features --locked -- -D warnings`
  - the tests of the engine crates, in release
  - for `python`, its Python tests through `uv run pytest`
- **Workspace.** Add both crates to `members`, `engine` to
  `default-members`, both with `publish = false`, and add them to the CI
  step and the Makefile target that run the engine crates' tests.
- **Commits.** Commit only when the user asks. No pushes and no pull
  requests unless asked.
- **Honesty.** Report what you ran and what it showed. If something was not
  measured or not tested, say so.

## Decisions this plan makes

The user confirmed the first fourteen on 2026-11-23. The rest follow from
the design.

1. **An accepted request is always applied.** Admission checks everything
   that could make a request fail later: its size, that its streams can be
   written, that its plan is valid against the streams as they will be when
   it applies. A request that passes is durable and will be applied; one
   that fails is refused and leaves no trace. So a receipt never turns into
   a failure, and nothing is dropped at epoch time. This is the current
   engine's rule.
2. **Stream kinds are settled at admission, in log order.** A write to a
   name no stream has creates a source stream, in the epoch that applies it.
   A write to a derived stream is refused. A plan that outputs a stream an
   earlier request made a source is refused. "Earlier" means accepted
   earlier, applied or not, so admission keeps the kinds that pending
   requests will create. A request refused for any reason reserves nothing.
3. **Request identities are kept for a bounded window.** Resending a request
   with the same `(source, request_id)` and the same payload returns its
   original receipt; a different payload is refused. Identities are kept
   for at least the `identities` most recent requests (default 100,000).
   After that an old identity is accepted again as a new request. This is
   the current engine's rule; the alternative is to keep every identity.
4. **A request's payload is deleted when it is applied**, in the epoch's own
   transaction. What stays is its sequence, identity, digest and the epoch
   that applied it. The design says publication stores a request's effects,
   so the payload is not needed again, and opening a store replays nothing.
5. **What goes in an epoch.** The driver takes a contiguous run of pending
   requests from the oldest, up to `epoch_requests` (default 128):
   - writes, in log order, so that a later write to a key wins;
   - at most one activation, which ends the run;
   - a refresh request is an epoch of its own, as `repair` requires;
   - retry requests join any epoch.

   Every derived reading is a function of the sources and the plan, so how
   requests are grouped into epochs changes when a result appears, never
   what it is. The tests check that.
6. **A fault keeps the requests pending.** Instance failures are recorded as
   `repair` records them, and the epoch is published. An `Err` from building
   or publishing (a storage fault, a bug) publishes nothing. The requests
   stay pending, `status` reports the error, and the driver tries the same
   epoch again after a backoff that grows to `fault_backoff` (default 30 s).
   A request accepted behind the fault, such as an activation that fixes
   it, is taken into the retried epoch. This is the current engine's
   behaviour.
7. **The refresher.** One thread, with its own Python pool, so refreshes
   never take workers from epochs. It refreshes one instance at a time:
   among asynchronous instances that are behind (a record with no error),
   the one whose record reflects the oldest epoch, ties in DAG order. It
   pins the published epoch, calls `repair::refresh`, and submits the
   resulting `Refresh` to the log as a request. It starts a refresh of an
   instance no sooner than `refresh_interval` (default 1 s) after the last
   one of that instance was applied, so that a stream of live epochs gives
   one refresh a second, not one per epoch.
8. **Retention is a call, not a schedule.** Nothing is pruned unless the
   caller calls `prune(before)`. The engine moves `before` back to the
   oldest epoch it still needs: a refresh's pinned epoch, a staged
   refresh's pinned epoch, and the reflects of every record (whose instance
   is otherwise rebuilt in full). It returns the retention point it set. An
   automatic schedule can come with Acquirium's settings in step 7.
9. **Module bytes are captured at activation.** An activation carries the
   bytes of every module its plan names that the store does not hold. The
   bindings read them from the module path when the plan is submitted, and
   admission refuses a plan whose bytes do not match its digests. The
   epoch that activates the plan stores them, and every pool loads modules
   from the store, never from files. So a file edited after a deploy
   changes nothing until a plan naming the new digest is activated, even
   across a restart. This is what the execution plan's notes for step 6
   ask for.
10. **Reads name streams, and return long form.** The query interface takes
    and returns stream names, not ids. `read` returns Arrow record batches
    with the columns `stream` (dictionary-encoded string), `time`
    (timestamp, nanoseconds, UTC), `number` (float64, null for text) and
    `text` (string, null for numbers), ordered by stream and then time, as
    an iterator of batches of at most `batch_rows` rows (default 65,536).
11. **Preview counts without evaluating.** `preview(plan)` reports, for
    each instance of the plan and each instance it removes:
    - how the activation changes it (new, redefined, rebound, unchanged,
      removed), from `plan::compare`;
    - the windows the activation itself makes dirty, and the rows they read,
      from the dirty rule and `extent`;
    - for an instance that reads a changed one, that it may change. Whether
      it does depends on what the functions return, which a preview does not
      run.

    Acquirium shows this before a deploy. It runs no function and writes
    nothing.
12. **The new Python package is separate until step 7.** It is built from
    `crates/python` with its own `pyproject.toml`, as the distribution
    `escapement-next`, imported as `escapement_next`. Step 7 renames it
    when Acquirium moves and the current package is retired. The current
    package keeps the name `escapement` meanwhile, so the two can be
    installed side by side and compared.
13. **Record batches cross into Python through the Arrow C stream
    interface.** Reads return an object with `__arrow_c_stream__`, which
    Polars and PyArrow consume without copying. Writes take any object with
    `__arrow_c_stream__` or `__arrow_c_array__`. This needs `arrow-array`'s
    `ffi` feature and no new crate; the alternative, `arrow`'s `pyarrow`
    feature, ties the arrow version to a pyo3 version.
14. **Writes cross as record batches.** A write request is a record batch
    with the columns `stream`, `time`, and `number` and `text` (exactly one
    set per row), with a row of neither meaning a delete of that key; and,
    separately, a list of range deletes `(stream, start, end)`. Range deletes
    apply first, then the rows in order. That is the order Acquirium's
    `ingest` builds today, with its `replace` argument.
15. **Group commit.** Concurrent submissions share admission transactions:
    one caller at a time commits everything waiting, up to
    `admission_requests` (default 128) or `admission_bytes` (default
    16 MiB), and each request gets the result it would have got alone. This
    is the current engine's `Admissions`.
16. **The epoch record.** Each published epoch stores its record in the same
    transaction: the requests it applied (first and last sequence), the
    plan it activated (its digest), the refresh it applied, its
    `repair::Report`, and the wall-clock time it committed. This is the
    repair plan's note for step 6.
17. **Threads.** As the design's table: callers admit; one driver thread
    builds and publishes epochs on `repair`'s pool and the epoch Python
    pool; one refresher thread with its own pool. No lock is held across a
    parallel call or a call to Python. A reader takes an epoch number and
    reads as of it, so readers never wait for an epoch.

## Milestone 1: the log in storage

### Requests

In `series`:

```rust
/// A request's identity: who sent it, and their name for it.
pub struct RequestId { pub source: String, pub id: String }

pub enum Request {
    /// Range deletes first, then the rows in order (decision 14).
    Write { deletes: Vec<(String, Interval)>, rows: RecordBatch },
    /// A plan, and the modules it names that the store does not hold.
    Activate { plan: Plan, modules: Vec<Module> },
    /// Retry these failed instances, or every one if empty.
    Retry(BTreeSet<InstanceId>),
    /// What the refresher asks for (moved here from `repair`).
    Refresh(Refresh),
}

pub struct Module { pub module: String, pub digest: String, pub bytes: Vec<u8> }

/// A request's place in the log. Sequences start at 1 and have no gaps.
pub struct Receipt { pub sequence: u64 }
```

`repair::Refresh` moves to `series`, which `repair` re-exports, since a
request carries it and `series` cannot depend on `repair`.

A request's digest is the SHA-256 of a canonical encoding of its payload:
the same request sent twice has the same digest however its batch was split
into arrays. Define the encoding once, in `series`, and test that it is
canonical.

### Storage

`Store` gains:

```rust
/// The receipts of the identities the store holds, with their digests.
fn identities(&self, ids: &[RequestId]) -> Result<Vec<Option<(Receipt, Digest)>>>;
/// Appends requests in one transaction, with consecutive sequences after
/// the last, and forgets identities older than `keep` requests. Returns
/// the first sequence.
fn append(&self, requests: &[(RequestId, Digest, &Request)], keep: u64) -> Result<u64>;
/// The highest sequence appended, and the highest applied.
fn accepted(&self) -> Result<u64>;
fn applied(&self) -> Result<u64>;
/// Pending requests after `after`, at most `limit`, oldest first.
fn pending(&self, after: u64, limit: usize) -> Result<Vec<(Receipt, Request)>>;
/// The epoch records after `after`, and the epoch that applied each
/// sequence, if one has.
fn epochs(&self, after: Epoch, limit: usize) -> Result<Vec<EpochRecord>>;
fn epochs_of(&self, sequences: &[u64]) -> Result<Vec<Option<Epoch>>>;
/// A module's bytes.
fn module(&self, module: &str, digest: &str) -> Result<Option<Vec<u8>>>;
```

`Publication` gains `.through(sequence)`, `.record(&EpochRecord)` and
`.storing(&[Module])`. Publishing checks that `through` is at or after the
last applied sequence and no later than the last accepted one, marks the
requests through it applied, deletes their payloads, stores the record and
the modules, and does all of it in the transaction that publishes the
readings. A publication that applies no request (an epoch the refresher
asked for is still a request, so this is rare) passes the last applied
sequence.

`EpochRecord` is decision 16's fields. The report is stored as JSON; it is
for people and for Acquirium's status pages, not for queries.

### Tables

On DuckDB and PostgreSQL, beside the existing tables:

| Table | Columns | Notes |
| --- | --- | --- |
| `requests` | `sequence`, `source`, `request_id`, `digest`, `kind`, `payload`, `epoch` | One row per accepted request. `payload` is null once applied; `epoch` is set then. The identity is unique among the rows kept. |
| `log` | `accepted`, `applied` | One row. |
| `epochs` | `epoch`, `first`, `last`, `plan`, `refresh`, `report`, `committed` | One row per published epoch. |
| `modules` | `module`, `digest`, `bytes` | Never deleted. |

Forgetting identities deletes the oldest applied rows of `requests` beyond
`keep`. A pending request is never forgotten, whatever `keep` is.

`payload` holds a write's batch as Arrow IPC and every other request as
canonical JSON. How it is laid out is up to you; say what you chose.

### Tests

- The contract suite gains the log: append and read back; identities and
  their digests; `keep`; payloads deleted and `epochs_of` filled by
  publication; a publication whose `through` is out of range refused;
  reopening keeps everything. It runs against the memory store and both
  backends.
- The canonical encoding: random batches split at random give one digest;
  any change to the payload changes it.
- A crash test on each backend, as the store's existing ones: a process
  killed around the publishing commit leaves either the epoch with its
  requests applied, or neither.

### Done when

The contract suite passes on all three stores, `repair` builds with
`Refresh` moved, and the checks pass.

## Milestone 2: the engine

### Settings

```rust
pub struct Settings {
    pub repair: escapement_repair::Settings,
    /// The epoch driver's Python pool, and the refresher's.
    pub epoch_pool: escapement_exec::Settings,
    pub refresh_pool: escapement_exec::Settings,
    pub epoch_requests: usize,       // 128 (decision 5)
    pub max_pending: usize,          // 10,000: a full inbox refuses (the design's capacity)
    pub max_request_bytes: usize,    // 4 MiB
    pub max_request_rows: usize,     // 10,000
    pub admission_requests: usize,   // 128 (decision 15)
    pub admission_bytes: usize,      // 16 MiB
    pub identities: u64,             // 100,000 (decision 3)
    pub refresh_interval: Duration,  // 1 s (decision 7)
    pub fault_backoff: Duration,     // 30 s (decision 6)
    pub batch_rows: usize,           // 65,536 (decision 10)
}
```

The defaults are the current engine's, where it has one.

### The interface

```rust
pub struct Engine { /* ... */ }

impl Engine {
    /// Opens the store and starts the driver and the refresher. Loads the
    /// modules the active plan names from the store into both pools.
    pub fn open(store: Arc<dyn Store>, settings: Settings) -> Result<Engine>;

    pub fn write(&self, id: RequestId, deletes: Vec<(String, Interval)>, rows: RecordBatch) -> Result<Receipt>;
    pub fn activate(&self, id: RequestId, plan: Plan, modules: Vec<Module>) -> Result<Receipt>;
    pub fn retry(&self, id: RequestId, instances: BTreeSet<InstanceId>) -> Result<Receipt>;

    pub fn snapshot(&self) -> Snapshot;
    pub fn snapshot_at(&self, epoch: Epoch) -> Result<Snapshot>;
    /// Waits, up to `timeout`, for the epoch that applies `receipt`.
    pub fn snapshot_after(&self, receipt: Receipt, timeout: Duration) -> Result<Snapshot>;

    pub fn preview(&self, plan: &Plan) -> Result<Preview>;
    pub fn status(&self) -> Status;
    pub fn epochs(&self, after: Epoch, limit: usize) -> Result<Vec<EpochRecord>>;
    pub fn epochs_of(&self, receipts: &[Receipt]) -> Result<Vec<Option<Epoch>>>;
    pub fn prune(&self, before: Epoch) -> Result<Epoch>;

    /// Stops the threads. With `drain`, publishes what is pending first.
    /// A refresh in progress stops at its next checkpoint, and resumes from
    /// its staged rows when the store is next opened.
    pub fn close(self, drain: bool) -> Result<()>;
}
```

`Status` holds the published epoch, accepted and applied sequences, the
pending count, the driver's last fault if it is paused, the refresher's
current instance, and both pools' statistics.

Behind the `testing` feature, `Settings::manual` opens the engine with no
threads, and `Engine::step` and `Engine::refresh_once` run one epoch and one
refresh on the caller's thread. The laws use them to choose every
interleaving; the other tests use the threads.

### Admission

Under the admission lock, for each request waiting, in order:

1. Look up its identity. A known identity with the same digest gets its
   receipt; with another digest, a refusal. Neither is checked further.
2. Check its size, and the inbox's capacity.
3. For a write: every stream is a source, or a name no stream or pending
   request has (decision 2). No delete names a derived stream. Times are
   neither unbounded end; numbers are finite.
4. For an activation: `Plan::validate`, `check_plan` against the stream
   kinds as pending requests leave them, and each module's bytes against
   its digest. A module the plan names whose bytes are neither given nor in
   the store is refused.
5. For a retry: every instance named exists in the active plan or in a
   pending activation.

Then append the accepted ones in one transaction and wake the driver. A
refused request reserves no stream kind and keeps no identity.

Admission keeps the kinds that pending requests will create in memory. On
open it rebuilds them from the published streams and the pending requests.

### The epoch driver

Its loop:

1. Wait until a request is pending, or `close`.
2. Take the epoch's requests (decision 5).
3. Build `Changes` on the published epoch from the writes: new streams for
   names first written, then range deletes and rows in log order, as
   `Op`s.
4. Call `repair::build` with the activation, the retries and the refresh.
5. Publish with `through`, the epoch record and the activation's modules.
6. Hand any new modules to both pools. Wake readers waiting in
   `snapshot_after`, and the refresher.

A fault at 3 to 5 is decision 6. An `Engine` holds the published epoch in
memory; it changes only at step 6.

A retry request with an empty set retries every instance with an error at
the base. An asynchronous instance's error is cleared by the epoch rule
(repair decision 17), so a retry of one starts a refresh.

### The refresher

Its loop:

1. Wait until an asynchronous instance is behind and not refreshed in the
   last `refresh_interval` (decision 7), or `close`.
2. Choose one, pin the published epoch, and call `repair::refresh` on its
   own pool. A staged refresh of the instance resumes from its checkpoint.
3. Submit the `Refresh` it returns as a request, under the identity
   `("refresher", "{instance}@{pinned}")`, and wait for the epoch that
   applies it.

The epoch rule decides the rest: a refresh redefined meanwhile is discarded
by `repair`, a rejected one is recorded, and a failed one waits for the
retry rule.

### Module bytes

`exec::Pool` gains `hold(&self, module, digest, bytes)`, which adds a version
from the caller's bytes, and a setting that stops it reading files at all.
The engine sets it, loads the active plan's modules on open, and holds each
new module after the epoch that stores it. A module that a plan names but no
pool holds is a fault of the engine, not of the instance: it means the store
lost bytes. The bindings read a module's file only when a plan is submitted
(decision 9). They run inside Python, so they find it as `import` would,
with `importlib.util.find_spec`, on the caller's `sys.path`.

### Retention

`prune(before)` is decision 8. It calls `Store::prune` with the point it
settles on, while holding off the refresher from pinning an older epoch.

### Laws to test

On the memory store with the reference executor, in manual mode, against
the oracle:

- **Batching is invisible.** Random requests, split into epochs at random
  and with activations at random places, give the same final snapshot as
  one request per epoch, and the same readings as the oracle.
- **Accepted means applied.** Every accepted request is applied by some
  epoch, in sequence order, and none is refused after its receipt.
- **Retries of the same request** get the same receipt, before and after
  it is applied, and across a reopen, while within `identities`.
- **Recovery is exact.** Close at random points, with and without
  draining, and with refreshes in progress; reopen and carry on; the end is
  the same as with no close.
- **Refreshes converge.** With asynchronous instances, a history that ends
  and is then left alone ends with every record clear and the oracle's
  readings.
- **Pruning never answers wrongly.** After `prune`, a read before the point
  is refused, and every other read is unchanged.

The same laws run on both backends with fewer cases, as `repair`'s
`backends.rs` does, with a fixed seed so counts do not vary.

### Done when

The laws pass on the three stores, the threads run them in a smoke test
with the Python pool, and the checks pass.

## Milestone 3: the query interface

### Snapshots and reads

`Snapshot` is an epoch number and a handle to the store. Its calls are the
design's [Queries](data-processing-model.md#queries):

```rust
impl Snapshot {
    pub fn epoch(&self) -> Epoch;
    pub fn read(&self, read: &Read) -> Result<Batches>;
    pub fn extent(&self, streams: Option<&[&str]>, range: Range) -> Result<Vec<Extent>>;
    pub fn describe(&self, streams: Option<&[&str]>) -> Result<Vec<Description>>;
    pub fn plan(&self) -> Result<Plan>;
    pub fn changed_since(&self, epoch: Epoch, streams: Option<&[&str]>) -> Result<BTreeMap<String, IntervalSet>>;
    pub fn versions(&self, streams: Option<&[&str]>, range: Range, after: Option<Epoch>) -> Result<Batches>;
    pub fn plan_versions(&self, after: Option<Epoch>) -> Result<PlanRows>;
}

pub struct Read<'a> {
    pub streams: &'a [&'a str],
    pub range: Range,          // start included, end excluded, either open
    pub limit: Option<usize>,  // per stream
    pub newest_first: bool,
}
```

`Batches` is an iterator of record batches (decision 10) that reads lazily,
so a large read does not hold every row at once. A stream named that does
not exist is an error naming it. A read as of an epoch before the retention
point is refused.

`describe` reports, for a derived stream, whether it is current and, if not,
the epoch it reflects and the reason, worked out as the design's
[maintenance record](data-processing-model.md#the-maintenance-record) says:
its own error; the instance upstream that holds it; or a refresh not
finished.

Most calls map onto `Store` calls that exist. `read` with a limit per stream
and newest first is the one that does not; add it to `Store` if `range`
cannot serve it well, and measure it against the design's
[Cost](data-processing-model.md#cost) table.

### The acceptance suite

The current engine's 125 cases in `tests/` are the specification of what
users rely on. Translate each one whose behaviour still exists into a test
of `engine`, in `crates/engine/tests/`, named as the original was. For each
one you drop, say why in a table in the report. Expect to drop, with reason:

- **Sealing, blocks and day files** (most of `epochs.rs` and some of
  `recovery.rs`): the new storage has no such layer.
- **Native functions** (parts of `contracts.rs`): removed by decision.
- **Sample keys and quality codes**: removed by decision.
- **Staged rebuilds** (`staged.rs`): synchronous work holds the epoch.
- **`explain`**: not in the query interface. Acquirium does not call it.

The rest map onto this step:

| File | Cases | Covers |
| --- | ---: | --- |
| `admission.rs` | 7 | Group commit, limits, identities (decisions 3 and 15) |
| `processor.rs` | 8 | The driver, draining, the pause after a fault (decision 6) |
| `recovery.rs` | 17 | Crashes around commit, reopening, identities across restarts |
| `isolation.rs` | 6 | Failures, holds and retries, through the engine |
| `cached.rs` | 6 | Asynchronous refresh, through the refresher |
| `contracts.rs` | 24 | Plan rules, range deletes, writes to derived streams refused, stream statistics as `extent`, Arrow reads, preview |
| `as_of.rs`, `rolling.rs`, `sequential.rs` | 17 | Window shapes; a sequential function becomes one that reads its own output |
| `python.rs` | 19 | Those about engine behaviour; worker-level ones are already in `exec` |
| `postgres.rs` | 6 | The PostgreSQL backend |

A case whose expected result differs under the new design (for example, a
removed instance's output now retracted, repair decision 2) is translated to
the new result, and listed in the report with the reason.

### Laws to test

- **Queries answer from the snapshot.** For random streams, ranges, limits,
  orders and epochs, each call equals the same answer computed from the
  oracle's snapshot.
- **The feed integrates to the snapshot.** Pull `versions(after)` into a
  second store, pull by pull at random intervals; its tables and its reads
  as of past epochs equal the first's.
- **Snapshots do not move.** Reads on one snapshot, interleaved with
  epochs, all see its epoch.
- **Backends agree** on every call.

### Done when

The query laws pass on the three stores, every case of the 125 is
translated or listed with a reason, and the checks pass.

## Milestone 4: the Python bindings

### The module

`escapement_next` (decision 12) is a pyo3 extension and a small Python
package beside it. It mirrors the Rust interface, with Python names:

```python
engine = escapement_next.Engine.open("plant.duckdb", python=sys.executable, workers=4)
receipt = engine.write("probe", "batch-17", rows, deletes=[("s1", None, None)])
snapshot = engine.snapshot(after=receipt, timeout=30)
frame = polars.from_arrow(snapshot.read(["s1", "s2"], start=t0, end=t1))
engine.activate("deploy-9", plan)          # reads module bytes from the path
engine.close(drain=True)
```

- Every call that can wait releases the GIL: admission, `snapshot(after=)`,
  reads, `close`.
- Times are integers of nanoseconds, or timezone-aware datetimes; a naive
  datetime is refused.
- A plan is passed as the plan crate's JSON, as a `dict` or a string. A
  small builder in Python is not in this step; Acquirium's compiler emits
  JSON in step 7.
- Errors are Python exceptions with one class per kind: refused, capacity,
  closed, timeout, not found, fault.
- The extension is built with maturin from `crates/python/pyproject.toml`,
  `abi3` for Python 3.11 and later, as the current package is.

### Tests

In `crates/python/tests/`, through `uv run pytest`:

- a round trip of writes and reads, with Polars and with PyArrow, checking
  that a read's buffers are not copied (the batch's buffer addresses are
  those the engine produced);
- a function DAG of two templates through the Python pool, with a late
  reading and a correction, as in the design's
  [example](data-processing-model.md#one-change-end-to-end);
- concurrent writers from Python threads sharing admission;
- `snapshot(after=)` across a reopen;
- each exception class;
- the `python.rs` cases of the acceptance suite that are about the
  bindings.

### Measurements

At Benicia scale (112 streams, a reading a minute, 90 days), on DuckDB and
PostgreSQL, through Python:

| Measurement | What it shows |
| --- | --- |
| Admission: requests a second, one writer and eight | Group commit |
| A live epoch: receipt to `snapshot(after=)` returning | Latency a writer sees |
| The design's [Cost](data-processing-model.md#cost) reads, through the bindings | The bindings' overhead over SQL |
| A correction of one stream's day, end to end | The common repair |
| A deploy of a new template over 90 days: preview, then activation to published | Deploy time |
| Refresh lag of an asynchronous template under live epochs | Decision 7's interval |

Add them to a benchmark binary in `crates/engine` like `repair`'s, sharing
`escapement_store::fixture`, and to the design's Measurements section when
the user asks.

### Done when

The Python tests pass, the measurements are in the report, and the checks
pass.

## Open points

Decide these by measurement or reading, and say what you found:

- **`epoch_requests`.** Measure live epochs with 1, 16 and 128 requests an
  epoch under steady writes.
- **The newest readings of every stream.** Whether `range` serves a read
  with a limit per stream, newest first, at the design's cost, or `Store`
  needs a call for it.
- **The refresher's interval.** Whether 1 s gives lag a reader can live
  with and work the pools can keep up with.

Stop and ask the user about anything else that changes what the system
means, including:

- a case of the acceptance suite whose old result you think should stand
  against this plan;
- anything admission would have to refuse that this plan does not list;
- adding a dependency.

## What to report

At the end of each milestone, write a short report:

- what was built, and which tests pass, with the command that ran them;
- for milestone 3, the table of acceptance cases dropped or changed, with
  reasons;
- for milestone 4, the measurements;
- every place you departed from this plan, and why;
- what you verified and what you only inferred;
- anything you could not test, and what it would take.

## For step 7

- **Acquirium's adapter.** `src/acquirium/core.py` calls
  `escapement_next` instead of `escapement`. Its paging, its
  sample-key checks and its quality handling go away.
- **The compiler** emits the plan crate's JSON: templates, instances and
  bindings with spans.
- **Read routes** call the query interface, and resolve names and the
  replacement chain through the graph.
- **The change feed**: Acquirium's `changes` pages `versions(after)`.
- **Retention**: a setting in Acquirium that calls `prune`.
- **Renaming the package** to `escapement`, and retiring the current engine.
- **Existing stores**: the design's open question, if any store must be
  carried over.

## Not in this plan

Anything in Acquirium; sampling and summarising in the engine; a cache of
readings; direct SQL to a running DuckDB store; S3; and the rest of the
design's [Deferred](data-processing-model.md#deferred) list. Do not start
them.
