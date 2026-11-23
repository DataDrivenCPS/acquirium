# Escapement build plan: repair

Written 2026-11-22 for whoever implements it. It covers step 5 of the
[implementation order](data-processing-model.md#implementation-order) in the
design: the dirty rule, runs, result comparison, maintenance records and
asynchronous refresh. Steps 1 to 4 were built from
[the first build plan](escapement-build-plan.md),
[the plan build plan](escapement-plan-build-plan.md) and
[the execution build plan](escapement-exec-build-plan.md), and are on the
Escapement branch `gtf-new-engine`.

The signatures below state intent. None of this code has been compiled.
Change a signature where the compiler requires it, and say what you changed
in your report.

## What you are building

`repair` turns one epoch's source changes into the whole epoch: every
derived reading that changes, and every maintenance record. It decides
which windows are dirty, builds runs and reads their rows from storage,
hands them to an executor, compares what comes back with what is stored,
and keeps the records that say how far behind each instance is. It also
computes asynchronous refreshes and applies them.

| Crate | Path | Package | Contents |
| --- | --- | --- | --- |
| `repair` | `crates/repair` | `escapement-repair` | The dirty rule, building runs, result comparison, failure and the hold rule, retries, limits, asynchronous refresh, and the full-recomputation oracle for tests. |

`repair` depends on `geometry`, `series`, `plan` and `exec`. It reads and
writes storage only through `series::Store` and `series::view`, so it is
tested on the in-memory store first and then on DuckDB and PostgreSQL. It
does not publish: it returns what an epoch would publish, and the caller
publishes it. Step 6's epoch driver is that caller.

Three earlier crates change, which the earlier plans did not foresee:

- `series` and `store` gain maintenance records (decision 1) and staged
  refreshes (decision 4);
- `exec` compares numbers as storage does (decision 3), and exposes which
  windows of a run exist (see [Building runs](#building-runs)).

Work in three milestones, each finished with its tests before the next:

1. **Maintenance records and staged refreshes in storage.**
2. **Synchronous epochs**, on the in-memory store, against the oracle.
3. **Asynchronous refresh, the backends, Python and the benchmark.**

Not in this step: the log, admission, receipts, the epoch driver's loop and
its thread, the epoch record, the refresher thread (all step 6);
the query interface and the bindings (step 6); anything in Acquirium
(step 7).

## Read first

The design is `docs/plans/data-processing-model.md` in the Acquirium
repository. Read these sections before writing code:

- One change, end to end
- Plans, especially the classification of instances on activation
- Windows, all of it
- Evaluation, all of it
- Freshness and failure, all of it
- Storage: Publication, and What processing needs from storage
- Threads
- Invariants and tests
- Decisions

Then read the code of steps 1 to 4 in `crates/`:

- `geometry`: `Window`, `reads`, `readers`, `owner`, `anchors_in`,
  `IntervalSet`.
- `series`: `Store`, `Changes`, `Op`, `view` (`View::building`, `range`,
  `latest`, `changed`, `apply`), `plan_streams`, and the contract suite.
- `plan`: `Plan::validate` and `Dag`, `compare` and `Comparison`
  (`rebound_spans`), `Plan::digest`, and `plan::testing`.
- `exec`: `Context`, `Run`, `WindowRef::direct`, `Run::stored` and what it
  must hold, `run`, `Outcome`, `FailureKind`, `reference::Reference` and
  `testing`.

Where the design and this plan disagree, the design decides what the system
means and this plan decides crate layout, names and tests. If they conflict,
or the design is silent on something that changes behaviour, stop and ask
the user. Do not settle it yourself.

## Where things are

- The Escapement repository is `/home/gabe/src/acquirium/escapement`, on
  branch `gtf-new-engine`. The design is at `../docs/plans/` from there.
- The current engine is a reference for behaviour, not a starting point.
  `src/engine.rs` and `src/scheduler.rs` hold its dirty tracking, failure
  records and retries; `tests/` holds the 125 cases that step 6 translates
  into the acceptance suite. Reading the failure and retry tests
  (`tests/recovery.rs`, `tests/cached.rs`, `tests/staged.rs`) shows what
  users already rely on.
- `crates/exec/tests/chain.rs` has a small driver that carries own-ports
  changes past the end of a run. The real rule here generalises it.

## Ground rules

These are the earlier build plans' rules.

- **Leave the current engine alone.** Do not edit `src/`, `bindings/`,
  `python/`, `tests/`, `examples/` or `benchmarks/`. Outside `crates/` you
  may edit only the root `Cargo.toml` (to add the workspace member),
  `Cargo.lock`, `.github/workflows/ci.yml` and the `Makefile`.
- **Toolchain.** Rust edition 2024, `rust-version` 1.89.
- **Dependencies.** Only what the earlier plans allowed. `repair` needs
  `escapement-geometry`, `-series`, `-plan`, `-exec`, `arrow-array`,
  `thiserror`, `rayon` 1 (decision 5), and `proptest` behind a `testing`
  feature, with `escapement-store` as a dev-dependency for the backend
  tests. Ask before adding anything else.
- **Safety.** `#![forbid(unsafe_code)]` in `repair`.
- **Checks.** As in CI:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  - `cargo clippy --all-targets --no-default-features --locked -- -D warnings`
  - the tests of the six engine crates, in release
- **Workspace.** Add `crates/repair` to `members` and `default-members`,
  with `publish = false`, and to the CI step and the Makefile target that
  run the engine crates' tests.
- **Commits.** Commit only when the user asks. No pushes and no pull
  requests unless asked.
- **Honesty.** Report what you ran and what it showed. If something was not
  measured or not tested, say so.

## Decisions this plan makes

The user confirmed the first five on 2026-11-22. The rest follow from the
design.

1. **Maintenance records are stored in this step.** The law is stated at
   every published epoch, retries resume from a record's cut, and the tests
   must reopen a store and carry on. So `series` gains the records, and
   `store` gains a `maintenance` table on DuckDB and PostgreSQL, versioned
   with `since` and `until` like the plan tables, so that the records as of
   any published epoch can be read. A publication carries the records the
   epoch sets and clears. The epoch record (what each instance evaluated)
   stays in step 6, with the log it belongs to; `repair` returns what goes
   in it.
2. **A removed instance's output is retracted** in the epoch that activates
   the plan without it, so no reading outlives the plan that explains it.
   Its history stays readable as of earlier epochs, and its stream stays in
   the `streams` table. A later instance that outputs the same stream is new,
   and computes it whole. The design does not say; the alternative is to
   keep the output frozen, with no owner and no record.
3. **`exec` compares numbers as storage does.** Storage treats `0.0` and
   `-0.0` as equal and keeps the stored one. `exec` compares by bits, and
   its documentation wrongly says that is what storage does. The mismatch is
   a real fault: within a run, a window that writes `-0.0` over a stored
   `0.0` hands `-0.0` to the later windows, while storage keeps `0.0` and a
   later run reads that. A function that can tell the two apart would then
   give different results depending on where runs split. The fix is in
   `exec`: equal by `==` for numbers, and in a run's own-ports state a new
   row equal to the stored one keeps the stored value, in the reference and
   the worker alike. The laws gain a case with signed zeros.
4. **Refreshes are staged in storage.** A refresh writes its output and a
   checkpoint (the anchors finished) to a staging table as its runs finish,
   so a restart resumes it from the checkpoint. An epoch applies a staged
   refresh by reading it from storage. Staged output is never visible to
   reads of the readings. The rules the design states (pinned epoch, one
   pending request, discard on redefinition, publish with the synchronous
   readers or not at all) are all here. The refresher thread and the log
   request that asks for a staged refresh to be applied are step 6.
5. **Parallelism uses rayon.** `repair` owns a `rayon::ThreadPool` of its
   own, sized by `Settings::threads`, rather than the global pool, since
   tasks block while a Python worker runs. Instances are spawned in a
   `rayon::scope` as they become ready: a finished instance spawns the
   readers it unblocked.
6. **The cut.** Each instance is brought from its cut to the candidate. The
   cut is its record's (`reflects`, `definition`) if it has one, otherwise
   the base epoch and the definition active there. The plan at the cut is
   `Store::plan(reflects)`. The dirty rule compares that plan with the plan
   being built, so activation, recovery and an ordinary epoch are one case.
7. **Failed windows are interval sets.** A record's `windows` is a set of
   anchor intervals: the anchors a per-window function's raise names, or for
   a failure that names no window (a crash, a timeout, a vectorized call's
   raise), the range of the windows the instance had to evaluate in the
   epoch. The user chose this over the failed run's range on 2026-11-23,
   since that range depends on the row budget; the design now says so. A
   limit exceeded over 90 days of minutes is one interval, not 129,600
   anchors. An instance that does not read its own output goes on through
   its runs after a raise, discarding their output, so that the record
   names every window that raises.
8. **When a failed instance is retried.** It is retried in an epoch when
   - this epoch's own changes (base to candidate, not cut to candidate)
     dirty one of its failed windows; for an instance that reads its own
     output, any dirty anchor at or before its last failed window;
   - the plan being activated redefines or rebinds it;
   - the caller asks, through the epoch's `retry` set.

   Comparing from the cut would find the failed windows dirty in every epoch,
   since they were dirty when they failed, and retry forever. When retried,
   the dirty rule runs from the cut, so everything that changed while it was
   behind is covered.
9. **A crash or a timeout is retried once**, in the same epoch, for that
   instance alone, before it is recorded.
10. **The hold rule is transitive.** Instances are visited in DAG order. An
    instance is held when a synchronous instance it reads has a record once
    that instance's turn in this epoch is over. A held instance keeps its
    output and gets a record with no error at its cut, or keeps the record it
    has. An asynchronous source never holds its readers.
11. **Limits.** `Limits { windows, input_rows, output_rows }`, per instance
    per epoch, as engine settings. Exceeding one fails the instance with a
    message naming the limit, recorded over the dirty anchors. None depends
    on the row budget (the user chose this on 2026-11-23): windows and input
    rows are checked before any call, against the windows that exist in the
    scope (for an instance that reads its own output, every one from its
    first dirty anchor) and the distinct rows they read. Output rows are
    checked once every run has succeeded, so a function's failure is
    recorded first, and that limit stops publication but saves no compute.
    Synchronous work within the limits is published whole, however large.
12. **Value comparison is storage's.** An output row is a change when the
    key has no stored value or `series::Value` says the values differ. That
    is the rule `Changes::apply` applies, so `repair` passes the results as
    `Op`s and lets `Changes` decide.
13. **A window sees a binding only as far as it reads.** A function gets
    each input's covers, the part of the window the binding feeds, and not
    the binding's whole span. Given the whole span, a function's output
    changed when an activation split a binding far from the window, which
    the dirty rule does not and should not see. The user chose this on
    2026-11-23; `exec` dropped `valid_from` and `valid_to` from its input.
14. **Own-ports runs read the base in their own bodies.** For the windows a
    run evaluates, `Run::stored` holds the output published at the base,
    not the epoch so far: the run replaces those rows before a later window
    reads them, and `exec` keeps a stored value that a new row equals, as
    storage does. Reading the epoch so far there made signed zeros depend on
    the runs.
15. **Refreshes have no limits.** A refresh does not hold up epochs, and
    background work is what asynchronous templates are for. The epoch that
    applies a refresh checks the limits on its synchronous readers as any
    epoch does. The user chose this on 2026-11-23.
16. **Any healthy instance downstream can reject a refresh.** An instance
    with no record at the base that fails in the epoch applying a refresh
    rejects it, whether it reads the refreshing instance or reads one that
    does: that epoch has no other changes, so the refresh caused the
    failure. The record names the first such instance by id. The user chose
    this on 2026-11-23.
17. **Epochs decide when a failed or rejected refresh is tried again.**
    Each epoch applies the retry rule (decision 8) to an asynchronous
    instance whose record has an error; for `Rejected`, a change to what it
    reads, an activation that changes it or the reader, or a request. When
    it is due, the epoch replaces the record with one at the same cut and no
    error, and the refresher refreshes it, since `refresh` does nothing for a
    record with an error. The error is not visible from then until the next
    refresh. The user chose this on 2026-11-23, over keeping the error and
    storing when it failed.
18. **What follows from the law for asynchronous instances.** An
    asynchronous instance with no record gets one at its cut, with no error,
    in an epoch that dirties it; a new one at the base, with the empty
    definition. A failed refresh is recorded by an epoch of its own, unless
    what it reads changed after its pinned epoch, which tries it again. An
    installed refresh clears the record if nothing the instance reads
    changed after the pinned epoch, and otherwise reflects that epoch. A
    rejected refresh's staged output is discarded.

## Milestone 1: maintenance records and staged refreshes in storage

### The record

In `series`, beside `Plan`:

```rust
pub struct Record {
    /// The definition digest (`Plan::digest`) the output reflects.
    pub definition: String,
    /// The epoch the output reflects the inputs of.
    pub reflects: Epoch,
    pub error: Option<RecordError>,
}

pub enum RecordError {
    /// The function failed on these windows.
    Failed { kind: String, message: String, windows: IntervalSet },
    /// A refresh was computed, and a synchronous reader failed on it.
    Rejected { reader: InstanceId, message: String },
}

pub type Records = BTreeMap<InstanceId, Record>;
```

`kind` is `exec::FailureKind` by name (`raised`, `invalid`, `load`,
`crashed`, `timed_out`, `output_limit`), and `limit` for a limit. `series`
does not depend on `exec`, so it stores the name.

### Storage

- `Publication` gains `records: &RecordChanges`: the records the epoch sets
  (`InstanceId -> Record`) and clears (`InstanceId`). Setting an instance's
  record to the one it has is no change.
- `Store::maintenance(epoch) -> Records`: the records as of a published
  epoch. Like `plan`, it can be read before the retention point.
- The `maintenance` table: `(template, instance, since, until, definition,
  reflects, error)`, with `error` as JSON (`json`, not `jsonb`, as the plan
  tables, so that what is written is what is read). An epoch that changes a
  record ends the old row and adds a new one. `LAYOUT` becomes 3.
- Publication refuses a record for an instance the plan after the epoch
  does not have, a record that reflects the epoch it is set in or a later
  one, and an epoch whose plan removes an instance without clearing its
  record (`series::check_records`).
- The memory store keeps the same history.

### Staged refreshes

A staged refresh is not versioned by epoch: at most one per instance, and
it is either applied by a publication or discarded.

```rust
pub struct Staging {
    pub pinned: Epoch,
    pub definition: String,
    /// The anchors whose output is staged: the checkpoint.
    pub finished: IntervalSet,
    /// The times whose stored output the staged rows replace.
    pub region: IntervalSet,
}
```

- `Store::stage(instance, &Staging, rows: &[Reading])`: adds one run's
  output to the instance's staged refresh, a later row for a key replacing
  an earlier one, and replaces its `Staging`, in one transaction
  (`series::check_stage`). Refused: an instance the active plan lacks; a
  pinned epoch not published; a staged refresh already there with another
  pinned epoch or definition (a conflict); a checkpoint or region smaller
  than the one there; a row outside the region, of a stream the instance
  does not output, or not finite.
- `Store::staging() -> BTreeMap<InstanceId, Staging>`, every staged
  refresh, so a refresher can find them after a restart, and
  `Store::staged(instance) -> RecordBatch`, a readings batch.
- `Store::discard(instance)`.
- `Publication` gains `applied: &[InstanceId]`, whose staged refreshes it
  removes in the publishing transaction; it refuses one that is not staged.
  The changes it publishes already hold their rows. A publication whose
  plan removes an instance also removes its staged refresh, rather than
  being refused: a refresh in flight would otherwise make the epoch fail.
- Two tables, `staging (template, instance, pinned, definition, finished,
  region)` and `staged (template, instance, stream, time, value, text)`.
  Neither is read by `range`, `edges`, `extent` or `changed`.

### Tests

- The contract suite gains records: random histories of record changes
  agree with the reference on all backends, as of every epoch; a
  publication that fails leaves the records as they were; reopening reads
  them back; JSON round trips exactly (messages with every kind of
  character, interval sets with unbounded ends).
- The SQL tests read the `maintenance` table directly and compare with
  `Store::maintenance`, as `tables.rs` does for the plan.
- Staging: random sequences of `stage`, `discard` and publications that
  apply agree with the reference, survive reopening, are never seen by
  reads of readings, and a publication that fails leaves them as they were.

### Done when

The contract suite passes on the memory store, DuckDB, PostgreSQL and, where
it is installed, TimescaleDB.

## Milestone 2: synchronous epochs

### The interface

```rust
pub struct Settings {
    pub limits: Limits,
    /// Input rows per run, at most, unless one window reads more.
    pub row_budget: usize,
    /// Threads building runs, reading and comparing. Default: the machine's
    /// available parallelism.
    pub threads: usize,
}

pub struct Limits { pub windows: u64, pub input_rows: u64, pub output_rows: u64 }

/// One epoch to build.
pub struct Epoch<'a> {
    /// The source changes, on top of the base epoch.
    pub changes: Changes,
    /// A plan to activate with this epoch, whole.
    pub activate: Option<&'a Plan>,
    /// Failed or rejected instances to retry on request.
    pub retry: BTreeSet<InstanceId>,
    /// An instance whose staged refresh to apply (milestone 3). An epoch
    /// that applies one has no source changes and no activation.
    pub refresh: Option<InstanceId>,
}

/// What the epoch publishes, and what it did.
pub struct Built {
    /// Streams the activated plan adds (`plan_streams`).
    pub streams: Vec<NewStream>,
    /// The source changes and every derived change.
    pub changes: Changes,
    pub records: RecordChanges,
    pub report: Report,
}

/// Per instance: evaluated, held, failed, skipped as current, or removed;
/// the anchors evaluated, the runs and the rows. Step 6 stores this as the
/// epoch record.
pub struct Report { /* ... */ }

pub fn build(
    store: &dyn Store,
    executor: &dyn Executor,
    settings: &Settings,
    epoch: Epoch,
) -> Result<Built, Error>;

/// Publishes `built` as the next epoch, with the plan if one was activated.
pub fn publish(store: &dyn Store, built: &Built, plan: Option<&Plan>) -> Result<Epoch, Error>;
```

`Error` is for an epoch that cannot be built: a storage error, an invalid
plan, an executor that could not attempt a run (`exec::Error`). A
function's failure is never an `Error`; it becomes a record.

### The order of an epoch

1. Read the plan and the records at the base. If a plan is activated,
   validate it, compare it with the base plan, and work out its new streams.
   The candidate's plan is the new one, or the base's.
2. Retract every removed instance's output, and clear its record
   (decision 2).
3. Visit the instances in DAG order, running ready ones in parallel.
   For each:
   - an asynchronous instance is skipped (milestone 3 applies refreshes);
   - a held instance gets its record (decision 10);
   - a failed instance not due for a retry keeps its record (decision 8);
   - otherwise its dirty anchors are worked out, its runs built and
     evaluated, its results compared, and its changes added to the
     candidate, or on failure discarded and recorded.
4. Merge every task's changes in key order, so the result does not depend
   on how the work interleaved.

An instance is ready when every instance it reads from has finished,
including those that changed nothing. Each task reads the published base
with the changes of finished instances laid over it (`View::building`),
never another task's unfinished work. Instances of one template that are
ready together share runs.

As built, instances are visited in groups: the instances of one template
at one depth of the DAG (its longest path from an instance that reads no
other), so that no instance of a group reads another. A group is ready
when every group it reads from has finished. The instances of a group
whose dirty windows fit in one run stop at that run, and their runs are
packed, in order, into runs of up to the row budget. `exec::run` pins a
failed run on the instances that caused it, a crash or a timeout is tried
again in a run alone, and an instance that reads its own output and
carries a change past the run's windows goes on alone. Each instance
comes to exactly what it would alone, its work counts included: an
instance counts each run it was in, and its own input rows.
`Settings::share_runs` turns sharing off, which makes every instance a
group of its own, as before.

### The dirty rule

For an instance with cut `(R, d)`, bound by the candidate plan:

```text
dirty = every anchor                                   if d is not its definition now
      + readers(role, changed(stream, R, candidate) ∩ span)   each binding
      + readers(role, span)                            each span rebound between
                                                       plan(R) and the candidate's
      + own-ports carry-forward                        as the windows run
```

- `changed` is `view::changed` over the candidate, so a source change of
  this epoch and an upstream instance's change made earlier in this epoch
  both count.
- `rebound_spans` of `compare(plan(R), candidate plan)` gives the third
  line. An instance missing from `plan(R)` is new: every anchor.
- "Every anchor" is bounded by what exists: the anchors whose reads meet a
  driving stream's readings (`extent`), together with the anchors that own
  stored output, which may need retracting.
- Dirty anchors are kept as an `IntervalSet` in anchor space and enumerated
  with `anchors_in`. For instant windows the anchors are the driving
  readings' times within the set.

### Building runs

- The windows to evaluate are the dirty anchors whose windows exist in the
  candidate. Existence is the driving-role rule `exec` checks. `exec`
  exposes it as a function, `Context::existing(windows, rows)`, so that
  the rule is written once.
- Runs hold windows of one template, divided by `row_budget`: windows go
  into a run until its rows would pass the budget. A run with one window
  may exceed it.
- A run's rows come from one `range` read over all its `Span` roles' streams
  and intervals (each window's reads clipped to the binding's span), and one
  `latest` read for its `Latest` roles: one request per window and binding,
  over the window's covers. `Run::rows` then holds each reading once.
- For an own-ports instance, `Run::stored` holds what `exec` requires: for a
  `Span` role the stored rows in the windows' reads and bodies; for a
  `Latest` role the rows from the first anchor to the last body's end and
  the latest row before the first anchor within `max_age`. All read through
  the candidate view, so an earlier run's changes in this epoch are seen.
- Windows of an own-ports instance are evaluated in anchor order, and its
  runs one after another. Every window from the first dirty one is sent,
  with `direct` set on the dirty ones, so that `exec`'s cutoff does the
  carrying within the run. After each run, the rows of its output that
  differ from storage dirty later anchors by `readers` of the own role.
  Those are evaluated next. This is `tests/chain.rs`'s driver, done with
  interval sets.

### Comparing results

For each instance that succeeds:

- The region its dirty anchors own is the union of their bodies.
- Every output row becomes `Op::Put`. Every stored key in that region that
  the results do not produce becomes a deletion: what a window no longer
  writes is retracted, and so is the output of a window that stopped
  existing.
- `Changes::apply` decides what is a change (decision 12).
- The instance's changes are what its readers' `changed` sees.

### Failure, holds and retries

- An instance that fails has its changes discarded, its output retained,
  and a record `{ definition: d, reflects: R, error: Failed { .. } }`.
  A record it had keeps its cut: a failed retry does not move `reflects`.
- A crash or a timeout is retried once first (decision 9).
- Limits are checked before the first run and after the last (decision 11).
- On success the record is cleared, and the instance's readers that were
  held evaluate from their own records later in the same epoch.
- Instances that are current and not dirty are not visited further, and
  have no record.

### The oracle

Behind `testing`, `recompute(store, executor, plan, epoch) -> BTreeMap<String,
BTreeMap<Time, Value>>`: every instance's output computed from nothing, from
the source readings as of `epoch`, in DAG order, every window that exists,
with no dirty rule, no storage of intermediate results and no cutoff. It
shares `exec`'s window semantics, which step 4 tested, and nothing of
`repair`'s.

For the maintenance law, `recompute_instance(store, executor, plan, id,
epoch)` computes one instance from its inputs as stored at `epoch`, which
is what a record with `reflects = epoch` claims.

### Generators

Behind `testing`:

- plans from `plan::testing::plans()`, with each template's function
  replaced by one of `exec::testing`'s functions for its shape (`summary`,
  `summary_vectorized` or `running`), chains of up to four templates deep
  included;
- histories: random epochs of puts, corrections, late readings and deletes
  of single keys and ranges; activations that redefine, rebind (a probe
  moving on a date), add and remove instances; retries on request;
- failures: `raise_at` parameters at random anchors, and functions that
  fail the whole run, toggled by activations.

### Laws to test

All on the memory store, with the reference executor.

| Law | Check |
| --- | --- |
| The function law | After every epoch of a history with no failures, every derived stream equals `recompute` at that epoch. |
| The dirty rule is sound | The same, and the rows outside the windows evaluated in an epoch were already equal to `recompute` before it. |
| The dirty rule is tight | On fixed cases, the windows evaluated are exactly the expected ones: the end-to-end example in the design (one window, then one window downstream that cuts off), a probe moving on a date (both instances, over the moved span only), a deletion that makes a window stop existing (retracted), a redefinition (every anchor of that instance and the dirty ones downstream). |
| Order does not matter | The same requests in different epochs and batches give the same final snapshot. |
| Runs are invisible | Random `row_budget`, `threads` and task completion order give the same `Built`, but for the work counts (runs and rows), with random limits and templates that tell `-0.0` from `0.0`. |
| The maintenance law | At every epoch of a history with failures, each instance with a record has stored output equal to `recompute_instance` at its `reflects` under its `definition`, and each instance without one equals it at the epoch. |
| Recovery is exact | When every failure is lifted and the instances retried, every stream equals `recompute`. |
| Failures stay put | A failing instance retains its output; independent branches publish; readers are held with records and no error; nothing reads a mixture. |
| Retries follow the rule | A failed instance is not evaluated on unrelated changes, and is evaluated when a failed window's input changes, on activation that changes it, and on request. An own-ports instance is retried on a change before its failed window. A crash is retried once. |
| Limits | Each limit, exceeded, fails only that instance, with nothing of its work published. |
| Removal | A removed instance's output is gone at the activation epoch and readable as of the epoch before. |
| Reopening | Building on a store reopened between any two epochs gives the same history. |

### Done when

The laws pass on the memory store, and on DuckDB for the function law and
the maintenance law.

## Milestone 3: refresh, backends, Python, benchmark

### Asynchronous refresh

```rust
/// Computes a refresh of an asynchronous instance as of published epoch
/// `pinned`, from its record's `reflects`, staging each run's output in the
/// store as it finishes. Resumes a staged refresh of the same instance,
/// pinned epoch and definition from its checkpoint; discards any other.
/// Returns when every dirty anchor is staged.
pub fn refresh(
    store: &dyn Store,
    executor: &dyn Executor,
    settings: &Settings,
    instance: &InstanceId,
    pinned: Epoch,
) -> Result<Report, Error>;
```

- A refresh reads as of `pinned`, published, with no candidate. It applies
  the dirty rule from the record's cut, or from scratch if the instance has
  never been computed.
- A refresh is not started while a synchronous instance it reads is behind
  at `pinned`.
- A refresh that fails discards what it staged and records `Failed`.
- `build` with `refresh: Some(instance)` applies that instance's complete
  staged refresh in an epoch of its own: discarded if the instance was
  redefined or removed since `pinned`; otherwise the stored output in the
  staged region is replaced by the staged rows, the record set to
  `reflects = pinned` (cleared if `pinned` is the base), and the
  synchronous instances that read it evaluated as in any epoch. The
  publication removes the staged refresh in the same transaction.
- If a synchronous reader that was healthy fails, the epoch publishes none
  of it: no staged output and no reader results. The refreshing instance
  records `Rejected { reader, message }`, and the readers keep their
  output and their records.
- A rejected refresh is tried again when its inputs change, when the reader
  is changed by an activation, or on request. A failed refresh records
  `Failed` as a synchronous instance would.

Tests: the maintenance law holds through histories with asynchronous
instances refreshed at random points; a refresh that a reader rejects
publishes nothing and records `Rejected`; a redefinition between refresh
and application discards it; readers of an asynchronous instance are never
held by it; a refresh interrupted after any run and resumed, on a reopened
store, stages the same output as one that ran straight through.

### Backends

The function law, the maintenance law and reopening, on DuckDB and
PostgreSQL, and on TimescaleDB where it is installed. The backend tests read
`readings`, `history` and `maintenance` with SQL after each epoch and
compare with the memory store given the same history.

### Python

A handful of histories through `exec::Pool` with `exec::testing`'s Python
twins agree with the reference executor, including a crash retried once and
a timeout recorded.

### Measurements

Add `crates/repair/src/bin/bench.rs` (`escapement-repair-bench`), on
DuckDB, with the store bench's Benicia fixture: 112 source streams, a
reading a minute, 90 days, and a plan of 300 instances of 10 templates. With
the reference executor, so the figures are `repair`'s own costs, and once
with the Python pool. Median and slowest after a warm-up.

| Operation | Report |
| --- | --- |
| A live epoch: 224 new readings | Time, and its parts: dirty rule, reads, executor, comparison |
| A live epoch with half of them revisions | Time |
| A correction of one stream's day | Time, windows evaluated |
| A probe moving to another group on a date 30 days back | Time, windows evaluated |
| The first evaluation of a new template over 90 days | Time, windows, rows |
| Redefining one template | Time |
| An epoch with one instance failing, and its retry | Time |
| A refresh of an asynchronous template over a day, and its application | Time |
| The same live epoch through the Python pool, 4 workers | Time |

## Open points

Decide these by measurement or reading, and say what you found:

- **The row budget's default.** Measure live epochs and the first
  evaluation with a few budgets.
- **Reading a run's rows.** One `range` query per run with many intervals:
  check that DuckDB and PostgreSQL plan it well at Benicia scale, against
  one query per stream.
- **Instances of one template sharing runs.** Measure the live epoch with
  and without it. Built (see the schedule above). At Benicia scale on
  DuckDB, the live epoch's 270 windows go in 9 runs, not 270. Through the
  Python pool, the epoch takes 210 ms shared and 246 ms not, and the
  executor's time, summed over threads, falls from 1.02 s to 84 ms. On the
  reference executor it makes no difference (204 ms and 193 ms, within
  noise). What is left of a live epoch is reading each instance's rows,
  one `range` call each, and comparing.

Stop and ask the user about anything else that changes what the system
means, including:

- anything about which windows an epoch evaluates that the design and this
  plan do not settle;
- a case where the maintenance law cannot hold as stated;
- adding a dependency.

## What to report

At the end of each milestone, write a short report:

- what was built, and which tests pass, with the command that ran them;
- the benchmark table, for milestone 3;
- every place you departed from this plan, and why;
- what you verified and what you only inferred;
- anything you could not test, and what it would take.

## For step 6

- **The epoch record**: store `Report` with each epoch.
- **The refresher thread**, which calls `repair::refresh` and then submits
  the request to apply it.
- **Retention and refreshes**: a pinned epoch must not be pruned while a
  refresh reads it or a staged refresh waits on it.
- **Requests**: retry and refresh requests in the log, which fill
  `Epoch::retry` and `Epoch::refresh`.
- **Module bytes in the store**, from the execution plan.

## Not in this plan

The log and admission, receipts, the epoch driver's loop and its threads,
the refresher thread, the query interface, the Python bindings, translating
the current engine's acceptance tests, and anything in Acquirium. Do not
start them.
