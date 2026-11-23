# Escapement build plan: execution

Written 2026-11-22 for whoever implements it. It covers step 4 of the
[implementation order](data-processing-model.md#implementation-order) in the
design: the run protocol and generic Python workers. Steps 1 to 3 were built
from [the first build plan](escapement-build-plan.md) and
[the plan build plan](escapement-plan-build-plan.md), and are on the
Escapement branch `gtf-new-engine`.

The signatures below state intent. None of this code has been compiled.
Change a signature where the compiler or Python requires it, and say what you
changed in your report.

## What you are building

A *run* is the engine's one call into user code: many windows, across many
instances of one template. You build the crate that turns a run into calls
of a Python function and turns what the function returns into checked output
rows, together with the pool of Python processes that does the calling.

| Crate | Path | Package | Contents |
| --- | --- | --- | --- |
| `exec` | `crates/exec` | `escapement-exec` | The run and its result, the per-window semantics, output checks, failure isolation, a reference executor for tests, the Python worker and its pool. |

`exec` depends on `geometry` and `plan`, not on `series` or `store`: it never
reads storage. Whoever builds a run (step 5) reads the rows and hands them
over. Nothing else in the workspace changes, apart from the workspace files.

Work in two milestones. Finish the first, with its tests passing, before
starting Python.

1. **The run, in Rust.** The types, the semantics of a run, output checks,
   failure isolation, and a reference executor that runs Rust closures. This
   is the specification the Python worker must meet, and what step 5 will
   test the dirty rule against.
2. **Python.** The wire protocol, the worker, the pool, and its limits. The
   Python executor must agree with the reference on random runs.

Not in this step: deciding which windows to run, reading rows from storage,
comparing results with storage, maintenance records, retries and refresh
(all step 5); the epoch driver and the bindings (step 6); the Acquirium
adapter and anything in Acquirium (step 7).

## Read first

The design is `docs/plans/data-processing-model.md` in the Acquirium
repository. Read these sections before writing code:

- Plans
- Windows, all of it, especially Driving roles, Reading its own output and
  Binding spans clip a window
- Functions, all of it
- Evaluation: Early cutoff, and Order and parallelism within an epoch
- Freshness and failure: Failure and Recovery
- Python execution, all of it
- Threads
- Decisions

Then read the code of steps 1 to 3 in `crates/`: `escapement-geometry`'s
`Window`, `Role`, `reads` and `owner`, and `escapement-plan`'s `Plan`,
`Template`, `Function`, `RoleSpec`, `Reads` and `Canonical`.

Where the design and this plan disagree, the design decides what the system
means and this plan decides crate layout, names and tests. If they conflict,
or the design is silent on something that changes behaviour, stop and ask
the user. Do not settle it yourself.

## Where things are

- The Escapement repository is `/home/gabe/src/acquirium/escapement`, on
  branch `gtf-new-engine`. The design is at `../docs/plans/` from there.
- The current engine's Python execution is your reference, not your
  starting point:
  - `src/python_pool.rs`: a bounded pool of persistent workers, recycled by
    job count and age, with a queue deadline, process groups, a Linux
    address-space limit and one-thread numeric libraries. Each worker is
    tied to one module. That is the design's main complaint: a six-stage
    chain over six files took 752 ms an epoch with four workers, against
    7.4 ms with the functions in one file.
  - `src/executor.rs`: builds jobs, reads the module file and checks its
    digest, and decodes results. It passes them through files in a
    temporary directory.
  - `python/worker.py`: the worker. It keeps the protocol on a private
    descriptor so that prints cannot corrupt it, sends user output to a log,
    and checks the returned frames. Read it closely. The descriptor handling
    carries over.
  - `tests/python.rs`: the Python executor's tests. The worker-level ones
    (address-space limit, timeouts, output limits, malformed results, warm
    reuse across entrypoints, recycling, failures discarding processes,
    window coverage, silent bindings) have counterparts below.
- Acquirium today names a function by file path, keeps its bytes under
  their content hash in
  `app-functions/{digest}.py` and generates a wrapper module per function
  (`src/acquirium/apps.py`, `_group_function`). Its adapter is
  `src/acquirium/functions.py`. Neither is changed here. They show what the
  adapter layer above the engine expects: Polars frames, nanosecond times
  and the context keys the current worker sends.

## Ground rules

These are the earlier build plans' rules, with the dependency list extended.

- **Leave the current engine alone.** Do not edit `src/`, `bindings/`,
  `python/`, `tests/`, `examples/` or `benchmarks/`. Outside `crates/` you
  may edit only the root `Cargo.toml` (to add the workspace member),
  `Cargo.lock`, `.github/workflows/ci.yml` and the `Makefile`.
- **Toolchain.** Rust edition 2024, `rust-version` 1.89, as the workspace
  sets them. The worker must run on Python 3.11 and later, as the root
  `pyproject.toml` requires.
- **Dependencies.** Everything the earlier plans allowed, and also, at the
  versions the workspace already locks:
  - `arrow-array`, `arrow-schema` and `arrow-ipc`, `=60.1.0`, the version
    DuckDB uses, so that batches pass to and from the store unchanged;
  - `serde_json = "1"`, for the context;
  - `sha2 = "0.10"`, for the context digest;
  - `tempfile = "3"`, for the directory that holds the worker script;
  - `libc = "0.2"` on Unix only, to kill a worker's process group;
  - `thiserror = "2"`, as `series` uses it;
  - `proptest = "1"`, optional behind a `testing` feature, as in `plan`.

  On the Python side the worker may import only the standard library,
  `pyarrow` and `polars`, which the root `pyproject.toml` already lists.
  Ask before adding anything else.
- **Safety.** `#![deny(unsafe_code)]` in the crate, with one
  `#[allow(unsafe_code)]` on the function that sends `SIGKILL` to a
  process group. Nothing else is unsafe. `std` has no safe way to signal a
  process group, and a function that started a subprocess must not outlive
  a timeout.
- **Platforms.** CI runs on Linux, macOS and Windows, and the pool must
  work on all three. On Windows there are no process groups, and killing
  the child must be enough (a virtual environment's `python.exe` is a
  launcher whose job object takes the interpreter with it). A subprocess
  that a function starts can outlive a timeout on Windows; say so in the
  report, and do not add a dependency to fix it. The memory ceiling is
  Linux only. Setting it elsewhere is refused, as today. The dependencies
  and `libc` were approved by the user, who wants Windows supported now
  and macOS eventually: keep every Unix-only call behind `cfg(unix)` and
  every Linux-only one behind `cfg(target_os = "linux")`.
- **Checks.** These must pass, as in CI:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  - `cargo clippy --all-targets --no-default-features --locked -- -D warnings`
  - the tests of the five engine crates, in release
- **Python in tests.** The tests find Python as the root package's tests do:
  `ESCAPEMENT_PYTHON`, or the `.venv` that `uv sync` builds (`bin/python`,
  or `Scripts/python.exe` on Windows). If neither exists, a test that needs
  Python fails with a message saying to run `uv sync`. It does not skip:
  CI runs `uv sync` before the engine crates' tests already.
- **Workspace.** Add `crates/exec` to `members` and `default-members`, with
  `publish = false`, and to the CI step and the Makefile target that run
  the engine crates' tests. Make the Makefile target run `uv sync --locked`
  first.
- **Commits.** Commit only when the user asks. No pushes and no pull
  requests unless asked.
- **Honesty.** Report what you ran and what it showed. If something was not
  measured or not tested, say so.

## Decisions this plan makes

These settle points the design leaves open. The first five need the user's
confirmation before you start; the rest follow from the design.

1. **A function is named like an entry point: `package.module:function`.**
   This is the form `pyproject.toml` uses for `[project.scripts]`. A plan's
   `Function` keeps its fields from step 3: `module` is the dotted module
   path and `name` the attribute. The module is found on the worker's
   `sys.path`, as `import` would find it, so it lives in the environment
   the engine's Python runs in. Its digest is the SHA-256 of the module's
   source file, in hex.

   The pool reads a module's file once, the first time it is needed, and
   keeps the bytes under (module, digest). Every worker loads the module
   from the pool's bytes, never from disk, so editing a file changes
   nothing that runs. `reload`, like `systemctl daemon-reload`, reads every
   module the pool knows again, keeps each new version beside the old one,
   and returns what changed. Nothing switches until a plan naming the new
   digests is activated: that redefines the changed templates together,
   which recomputes their history. Until then the active plan keeps
   running the bytes it was compiled against. So several files can be
   edited and taken up at once, and no run sees a half-changed set.

   `resolve` finds a module and returns its digest, for a caller compiling
   a plan. A run whose (module, digest) the pool does not hold, and cannot
   find on disk, fails with `Load`. That happens only when the file changed
   before the pool first read it, as after a restart. Only the module's own
   file is hashed, not the modules it imports. (Acquirium's generated
   wrapper modules, which import their real source by path, do not fit
   this. Replacing them is step 7's.)
2. **Functions receive Polars frames, with datetimes.** Chosen by the
   user (option A). Time columns are `Datetime("ns", "UTC")`: the same
   64-bit integers, exact, shown as dates. A function returns its time
   column the same way. Times in the context are Python `datetime`s in UTC,
   each with its exact nanoseconds beside it under a `_ns` name, because a
   `datetime` holds only microseconds: a function that writes its output at
   the anchor of a point window must be able to name that nanosecond. A
   `datetime` is the time rounded down to the microsecond. The other choice,
   making the microsecond the engine's finest time, was declined.
3. **Early cutoff for a template that reads its own output happens in the
   worker.** Confirmed by the user. The design says evaluation stops at the
   first window whose result is unchanged and moves on to the next dirty
   one. Only the worker sees each result before the next window runs, so a
   run of such a template carries which windows are dirty from outside, and
   the stored rows to compare with (see
   [Own ports](#own-ports-and-early-cutoff)). The user asked that it be
   checked against chains with history windows: the laws and the chain
   test there do that.
4. **A `Latest` role reads each bound stream's latest reading** (confirmed
   by the user), not one
   reading across all the role's streams. The design says "the role's
   latest reading", and with one stream per role the two agree. A role
   bound to two probes gets the latest of each.
5. **The context leaves out `epoch`, `applied_through` and `plan_id`.** The
   current worker passes them. No function uses them: only
   `examples/profiling/worker.py` reads `epoch`, to label its timing
   records. A function that did use them would compute different results
   depending on when it ran, which the function law forbids.
6. **No sample key and no quality code.** A reading's value is a number or
   text (design decisions).
7. **The static context is cached by digest.** It is the template and every
   instance's parameters, outputs and bindings, in canonical JSON, and its
   digest is the SHA-256 of that text. A worker keeps every context and
   module it has loaded until it is recycled. The pool knows what each
   worker holds and sends a context only when the worker lacks it.
8. **Messages go over pipes, not files.** Each message is a small JSON
   header with its length, followed by Arrow IPC streams whose lengths the
   header gives. No temporary files per run.

## Milestone 1: the run

### Types

```rust
pub use escapement_geometry::{Interval, Time};
pub use escapement_plan::{InstanceId, Mode, Plan, Template};
use arrow_array::RecordBatch;

/// What every run of one template under one plan shares: the template, and
/// each instance's parameters, outputs and bindings.
pub struct Context { /* template id, Template, instances, canonical JSON, digest */ }

impl Context {
    /// The context of `template` in `plan`. `plan` must be valid.
    pub fn new(plan: &Plan, template: &str) -> Result<Context, Error>;
    pub fn digest(&self) -> &str;
    /// The canonical JSON a worker receives.
    pub fn json(&self) -> &str;
}

/// One window to evaluate.
pub struct WindowRef {
    pub instance: String,          // the instance key
    pub anchor: Time,
    /// Dirty from outside the run. Only a template with an own-ports role
    /// may say false: see Own ports.
    pub direct: bool,
}

/// Many windows, across instances of one template, in one call.
pub struct Run<'a> {
    pub context: &'a Context,
    /// In any order. No window twice.
    pub windows: Vec<WindowRef>,
    /// Every reading the windows read through bound roles, once each:
    /// instance, role, stream, time, value, text.
    pub rows: RecordBatch,
    /// For a template with own-ports roles, the stored output those roles
    /// read and the run compares with: instance, port, time, value, text.
    /// Empty otherwise.
    pub stored: RecordBatch,
}

/// What a run returns.
pub struct RunResult {
    /// instance, port, time, value, text; sorted by instance, port, time.
    /// Only instances that succeeded have rows.
    pub outputs: RecordBatch,
    pub instances: BTreeMap<String, Outcome>,
}

pub enum Outcome {
    /// The windows evaluated, in anchor order. Their bodies are what the
    /// output replaces. Every window of the run, unless early cutoff
    /// skipped some.
    Done { evaluated: Vec<Time> },
    Failed { failure: Failure, windows: Vec<Time> },
}

pub struct Failure { pub kind: FailureKind, pub message: String }

pub enum FailureKind {
    /// The function raised.
    Raised,
    /// The function returned something the checks refuse.
    Invalid,
    /// The module could not be found or imported, its function is
    /// missing, or its file no longer has the plan's digest.
    Load,
    /// The worker died, hit its memory ceiling or broke the protocol.
    Crashed,
    TimedOut,
    OutputLimit,
}

/// Anything that evaluates runs: the Python pool, or the reference.
pub trait Executor: Send + Sync {
    /// Evaluates `run` once. A failure of the whole run (a crash, a
    /// timeout, a vectorized call that raised) is reported as every
    /// instance failing with the same failure; see Isolating failures.
    fn evaluate(&self, run: &Run) -> Result<RunResult, Error>;
}

/// Evaluates `run`, isolating the instances that fail it whole.
pub fn run(executor: &dyn Executor, run: &Run) -> Result<RunResult, Error>;
```

`Error` is for a run that could not be attempted: a context that does not
match the plan, a malformed batch, a worker that could not start, a queue
wait that ran out. A function's failure is never an `Error`. It is an
`Outcome`.

Validate a run before evaluating it, and refuse with `Error::Invalid`:

- each window names an instance of the context, at an anchor its window
  shape allows, at most once;
- `direct` is false only for a template with an own-ports role;
- each row names an instance of the run, one of its bound roles and a
  stream bound to it in that role, and its time lies in the read interval
  of some window of that instance (rows the run cannot use are a caller's
  bug, and the current engine's test
  `batch_rows_outside_affected_windows_fail_without_publishing_outputs`
  catches the same thing);
- each row has exactly one of value and text, and a number is finite;
- `stored` names only own ports of instances in the run.

### What one window receives

The per-window semantics are written once in Rust here, and once more in
the Python worker in Milestone 2. The two must agree on every run.

For window `t` of instance `i`, and each role `r` of the template:

- `reads = window.reads(role, t)` from `geometry`.
- The *inputs* of `r` are the bindings of `i` in role `r` whose span meets
  `reads`, including those with no rows in it. For each,
  `covers = reads ∩ span`. The function sees every input, so it can tell a
  silent sensor from a missing one and decline when too little of the
  window is covered.
- For a `Span` role, the window's rows are every row of the input's stream
  in role `r` with its time in `covers`.
- For a `Latest` role, the window's rows are, for each stream, the latest
  row of that stream in role `r` whose time lies in the `covers` of one of
  the stream's inputs. At most one row per stream.
- For an own-ports role, see [Own ports](#own-ports-and-early-cutoff). Its
  inputs are one per port it reads, with `stream` the instance's output
  stream for that port and `covers = reads`.

The per-window call is:

```python
outputs = function(rows, context)
# rows: pl.DataFrame with columns role, stream, time (Datetime ns UTC),
#       value (Float64), text (String), sorted by role, stream, time.
# context: {
#   "template": str, "instance": str, "parameters": dict,
#   "anchor": datetime, "anchor_ns": int,
#   "body": [start, end], "body_ns": [start, end],
#   "inputs": [{"role", "stream", "context",
#               "valid_from", "valid_from_ns", "valid_to", "valid_to_ns",
#               "reads", "reads_ns", "covers", "covers_ns",
#               "port": str or absent}, ...],
# }
# outputs: {port: pl.DataFrame with time (Datetime, UTC), value and text,
#           or time and value only}
```

Unbounded ends are `None`, in both forms. `context` is a fresh dictionary for every call,
so a function that changes it changes nothing else.

The vectorized call receives the whole run. Each row is repeated once per
window that reads it, tagged with that window. Each output row names its
instance, and belongs to the window whose body holds its time.

```python
outputs = function(rows, context)
# rows: pl.DataFrame with instance, anchor, role, stream, time, value, text
# context: {
#   "template": str,
#   "instances": {key: {"parameters": dict}},
#   "windows": pl.DataFrame with instance, anchor, body_start, body_end,
#   "inputs": pl.DataFrame with instance, anchor, role, stream, context,
#             valid_from, valid_to, reads_start, reads_end, covers_start,
#             covers_end,
# }
# Every time column is Datetime("ns", "UTC"), null where unbounded.
# outputs: {port: pl.DataFrame with instance, time, value, text}
```

A template with an own-ports role is never vectorized; `plan` already
refuses one.

### Output checks

The checks are the design's, applied per instance. An instance whose output
fails one is `Failed` with `Invalid` and a message naming the rule, the port
and the time, and none of its rows are returned. Other instances are
unaffected.

- The result has exactly the declared ports. A port with no rows is
  allowed: it retracts what the window owned there.
- Each frame has the expected columns and types. A numeric function may
  leave out `text`.
- Each row has exactly one of value and text, and a number is finite.
- Each row's time lies in the body of one of the instance's evaluated
  windows (`owner` from `geometry`, and it must be a window of the run).
- A port has at most one row at each time.

### Own ports and early cutoff

For a template with an own-ports role, the windows of each instance are
evaluated in anchor order, one after another.

- An own-ports role reads `stored`. A window reads only output from before
  its anchor: a `Span` reads `[t - before, t)`, a `Latest` the latest row
  before `t` no older than `max_age`.
- After a window is evaluated, its output replaces the stored rows in its
  body, on every port. Later windows in the run read the replacement. An
  empty output on a port leaves that part of the body empty, so a later
  `Latest` may find an older row.
- A window is evaluated when it is `direct`, or when the rows its
  own-ports roles read now differ from the rows they read in `stored` as
  sent. For a `Span` role that is the rows in `[t - before, t)`; for a
  `Latest` role it is the one latest row before `t`. Other windows are
  skipped: they are not in `evaluated`, and have no output. A skipped
  window's inputs are exactly what they were, so its output would equal
  the stored rows, and skipping it changes nothing.
- Compare what is read, not whether an interval meets a changed body.
  With a `Latest` role of unbounded age, every later window's interval
  meets every earlier body, and that test would re-evaluate the rest of
  the run after any change: no cutoff at all.
- Rows are equal when their times are equal and their values are equal,
  numbers by their bits, as `series` compares a write with the stored
  value. `-0.0` differs from `0.0`.
- If a window fails, the instance is `Failed` with that window, and its
  later windows are not evaluated.

`direct` means dirty from anything outside this run: a changed input
from another instance, a binding change, a definition change, or a change
to the instance's own output made by an earlier run. A run looks no
further than its own windows. When an evaluated window near the end of a
run changes, later windows of the instance may read the change. The
caller finds them by applying the dirty rule to the instance's changed
output, and runs them next. Write both on `WindowRef::direct` and
`Run::windows`.

What changed, for the instance's readers downstream, is every row the run
returns that differs from the stored row at its time, in every evaluated
window, not only the last. A reader with a history window, a lookback
of an hour say, is dirtied from all of them by the dirty rule. Step 5
does that comparison; `exec` returns every evaluated window's output so
that it can.

The caller must put in `stored`, for every port an own-ports role reads:
for a `Span` role, every stored row in the windows' read intervals and
bodies; for a `Latest` role, every stored row from the first window's
anchor to the last window's body end, and the latest stored row before the
first anchor within `max_age`. Write this on `Run::stored`. `exec` cannot
check it, and step 5 must meet it.

### Isolating failures

A failure the per-window adapter catches belongs to one instance, and the
others carry on. A failure of the whole run (`Crashed`, `TimedOut`,
`OutputLimit`, a vectorized call that raised, or a protocol error) cannot
be attributed. `exec::run` then splits the run's instances into two halves
and runs each, recursively, until each failure is down to one instance.
The windows of the instances that pass keep their results. A single-instance
run that fails whole reports that instance `Failed` with every window it
had in the run.

`run` does not retry. Whether a timeout or a crash is retried once is the
recovery rule, which is step 5's.

### The reference executor

Behind the `testing` feature, `testing::Reference` implements `Executor`
with Rust closures registered under a module, a name and a digest. It
applies the semantics above directly: the selection, the own-ports
replacement and cutoff, the output checks, and whole-run failures, which a
closure signals by returning `Fail::Run(kind)` in place of a result. It is
written for clarity, not speed, and it is the oracle the Python worker is
held to.

Also behind `testing`, strategies for random runs: a random valid template
and instances (from `plan`'s generator, restricted to the window shapes
that make anchors cheap to list), random bindings with spans and contexts,
random windows at valid anchors, and the rows those windows read, drawn so
that bindings that end inside a window, silent bindings and duplicate
streams across roles all occur.

### Laws to test

All against the reference, as properties.

| Law | Check |
| --- | --- |
| Run invariance | Divide a run's windows into runs at random. Per-window: the union of their results equals the whole run's. Vectorized, with a function that computes each window from its own rows: the same. |
| A window sees what it reads | A function that returns, per window, a digest of the rows and inputs it was given matches an independent computation from `geometry` and the bindings. |
| Own ports | A run of an own-ports template equals the same windows run one at a time, each against the stored rows the earlier ones left. |
| Cutoff skips only what cannot change | With random `direct` flags, and own-ports roles of random `Span` and `Latest` reach (including unbounded `max_age`), the rows that differ from `stored` after applying the run's output are exactly those that differ after evaluating every window. |
| Cutoff cuts | A running total reading the latest row before each bucket, with one `direct` window early in a run of 50, evaluates only up to the first window whose total comes out unchanged, and any later `direct` windows. |
| Chains see every change | A three-stage chain: a running total (own ports, `Latest`), a rolling mean over it with a lookback of three buckets on a point grid, and a second running total (own ports, `Span` of two buckets) over that. Random corrections to the source. A small test-only driver evaluates each stage with cutoff, finds the next stage's dirty windows from the rows that changed by `geometry`'s `readers`, and runs them, continuing past the end of a run as above. The final outputs of every stage equal a full recomputation. Step 5 builds the real driver; this one exists to check that cutoff leaves it nothing to miss. |
| Failures stay put | Instances that raise on random windows fail with exactly those windows; the other instances' output equals a run without them. |
| Isolation finds the culprits | Random instances fail the whole run. `run` reports exactly those as failed, the others' output equals a run without them, and the number of evaluations is at most `1 + 2 f ⌈log2 n⌉` for `f` failing instances of `n`. |
| Checks refuse | Each rule of the output checks, broken, fails only that instance with `Invalid` and a message naming the rule. |
| Runs are validated | Each validation rule, broken, refuses the run with `Error::Invalid`. |

### Done when

- The laws pass.
- `Context`'s canonical JSON is stable: the same plan gives the same text
  and digest, and rebinding one instance changes the digest.

## Milestone 2: Python

### The wire protocol

One worker serves one run at a time over its stdin and stdout pipes. Each
message is:

```text
u32 little-endian length of the header
header: UTF-8 JSON
then each payload the header lists, as raw bytes of the lengths it gives
```

| Message | From | Header | Payloads |
| --- | --- | --- | --- |
| `ready` | worker | protocol version, pid | |
| `resolve` | engine | module, name | |
| `resolved` | worker | file and digest, or an error | the file's bytes |
| `module` | engine | module, digest | the module's bytes |
| `context` | engine | digest | the context's JSON |
| `run` | engine | module, digest, name, mode, context digest, window count | Arrow IPC streams: windows, rows, stored |
| `result` | worker | per instance: outcome, failing windows, message | Arrow IPC stream: outputs |

- The engine sends `module` and `context` only when the worker lacks them.
  The worker replies to `resolve` with `resolved` and to `run` with
  `result`, and to nothing else.
- The engine reads a payload only after checking its length against the
  output limit. A longer one fails the run with `OutputLimit`, and the
  worker is killed.
- The windows batch carries what the Rust side worked out per window:
  instance, anchor, body, `direct`, and per role the read interval. The
  worker recomputes nothing about window shapes.

Bump the protocol version whenever a message changes. A worker that
announces another version is refused.

### The worker

`crates/exec/python/worker.py`, embedded with `include_str!` and written
once per pool into the pool's temporary directory.

- **Descriptors.** As the current worker does: the protocol moves to a
  private descriptor at start, before any import, and descriptors 1 and 2
  go to a pipe the pool reads. A print, a native library writing to
  descriptor 1, or a subprocess cannot write into the protocol.
- **Resolving.** For `resolve`, the worker finds the module with
  `importlib.util.find_spec`, reads its source file, and returns the bytes
  with their SHA-256. It does not import the module. A module whose spec
  has no source file (a compiled extension, a namespace package) is
  refused.
- **Loading.** For `module`, the worker executes the bytes it was sent as
  the module, under its real name in `sys.modules`, with the spec's
  package and file set so that relative imports and tracebacks work. The
  bytes executed are the bytes that were hashed, so a file edited in
  between cannot slip through. A module that fails to import, or lacks the
  attribute, fails every instance of the run with `Load`, and the worker
  is discarded.
- **One version per worker.** A worker that has loaded a module at one
  digest cannot load it at another under the same name. The pool does not
  give it a run that needs the other: it retires it when it returns and
  starts a fresh one if needed. Only a recompiled plan does this.
- **Calling modes.** The per-window adapter builds each window's frame and
  context as [specified](#what-one-window-receives), calls the function,
  checks the output, and on an exception records the instance as failed
  and carries on with the next instance. The vectorized adapter makes one
  call.
- **Checks.** The worker applies the output checks, so that it can name a
  failing instance precisely. The engine applies them again to what comes
  back: the worker is ours, but a function can corrupt the interpreter.
- **Limits.** The memory ceiling is `RLIMIT_AS`, set by the worker before
  importing anything, from an environment variable, on Linux. Numeric
  libraries get one thread each, as today (`POLARS_MAX_THREADS`,
  `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`).

### The pool

```rust
pub struct Settings {
    pub python: PathBuf,
    /// Workers, at most. Default: the machine's available parallelism.
    pub workers: usize,
    /// Recycle a worker after this many runs, or this age.
    pub max_runs: u64,
    pub max_age: Duration,
    /// Address-space ceiling per worker, Linux only.
    pub memory: Option<u64>,
    /// A run's time allowance: `run_allowance + window_allowance × windows`.
    pub run_allowance: Duration,
    pub window_allowance: Duration,
    pub max_output_bytes: u64,
    /// The most of a run's printed output kept for its failure message.
    pub max_log_bytes: usize,
    /// How long a run waits for a free worker.
    pub queue_wait: Duration,
}

pub struct Pool { /* ... */ }

pub struct Resolved { pub file: PathBuf, pub digest: String }

pub struct Reloaded {
    pub module: String,
    pub file: PathBuf,
    pub old: String,
    /// `None` when the module can no longer be found.
    pub new: Option<String>,
}

impl Pool {
    pub fn new(settings: Settings) -> Result<Pool, Error>;
    /// The digest of `module` as the pool holds it, reading its file only
    /// if the pool holds no version yet. For a caller filling in a plan's
    /// digests. Fails if the module or its attribute `name` cannot be
    /// found.
    pub fn resolve(&self, module: &str, name: &str) -> Result<Resolved, Error>;
    /// Reads every module the pool holds from disk again, keeps the new
    /// versions beside the old, and returns the modules whose digest
    /// changed. Afterwards `resolve` returns the new digests. Runs are
    /// unaffected: each names the digest it runs.
    pub fn reload(&self) -> Result<Vec<Reloaded>, Error>;
    pub fn stats(&self) -> Stats;
}

impl Executor for Pool { /* ... */ }
```

- **Generic workers.** Any worker can run any function. Given a run, take
  an idle worker that already holds its module and context; then any idle
  worker that does not hold another digest of its module; then start one if under `workers`; then wait, up to
  `queue_wait`. Starting a worker happens outside the lock.
- **One run at a time.** No lock is held while a worker runs.
- **Recycling.** A worker past `max_runs` or `max_age` is retired when it
  returns, not mid-run.
- **Discarded on failure.** A worker that ran a run in which anything
  raised, crashed, timed out or hit a limit is killed, with its process
  group on Unix, and never reused.
- **Logs.** The pool keeps the first `max_log_bytes` of what a run printed
  and puts them in its failure messages. The rest is dropped, not stored.
- **Stats.** Counters: workers started, reused and retired (by reason),
  runs, failures by kind, queue waits and how long they took, and modules
  and contexts sent. Per-process memory sampling is not in this step.
- **Dropping the pool** kills every worker and reaps it.

### Tests

- **The worker agrees with the reference.** Random runs from the
  Milestone 1 strategies, with a fixed set of test functions written in
  both Python and Rust (one returns a digest of what each window sees, one
  keeps a running total through an own-ports role, one is vectorized, one
  raises at chosen anchors), give the same `RunResult` from the pool as
  from `Reference`. Use fewer cases than the in-memory laws (say 200).
- **Run invariance through Python**, for both modes, as in Milestone 1.
- **Isolation through Python.** A function that calls `os._exit` for chosen
  instances, one that sleeps past the allowance, and one that returns more
  than the output limit: `run` finds exactly those instances, the others'
  output is correct, and no process is left behind (check the reaped pids,
  as `assert_reaped` does today).
- **Generic workers.** With one worker, runs of functions from six
  modules, then the first again, start one process. Two modules defining
  the same names give their own results. A module defining a dataclass,
  and a module in a package that uses a relative import, load.
- **Pinning and reload.** `resolve` gives the SHA-256 of the module's
  file. After editing two modules' files, runs on warm workers and on
  fresh ones still give the old results. `reload` reports exactly those
  two, with their old and new digests, and runs naming the old digests
  still give the old results. Runs naming the new digests give the new
  results, on workers that did not hold the old version. A second `reload`
  with nothing edited reports nothing. A module deleted before `reload` is
  reported with no new digest, and its old version keeps running. A pool
  started after an edit fails runs naming the old digest with `Load`, and
  a message saying the file changed. A module, or an attribute, that does
  not exist fails with `Load`. Test modules live in a temporary directory
  put on the workers' `PYTHONPATH`.
- **Caching.** The second run of a context sends no `context` message, and
  a rebinding sends one. A module's bytes go to each worker once.
- **Prints do not corrupt the protocol**: at import, in the function, from
  a native write to descriptor 1, and from a subprocess. The printed text
  appears in a failure message, truncated at `max_log_bytes`.
- **Limits.** The memory ceiling on Linux (a function allocating past it
  fails `Crashed` or `Raised`, and the pool carries on); `max_runs` and
  `max_age` recycle; `queue_wait` runs out with `Error`, and leaves no
  waiter behind.
- **Malformed output.** Each output check, broken from Python, fails only
  that instance.
- **Concurrency.** Eight threads submitting random runs to a pool of three
  workers all get the reference's results.
- **Settings refused**: zero workers, zero allowances, a memory ceiling off
  Linux, a Python path that does not exist.

### Measurements

Add `crates/exec/src/bin/bench.rs` (`escapement-exec-bench`). Report each
with its median and slowest, after a warm-up.

| Operation | Report |
| --- | --- |
| Start a worker, to `ready` | Time |
| `reload` of 60 modules, none changed | Time |
| A one-window run on a warm worker | Time: the protocol's fixed cost |
| A per-window run of 100 and of 10,000 windows, one row each | Time, and per window |
| The same windows vectorized | Time |
| A Benicia-shaped epoch: 300 instances of 10 templates, one window each, on 4 workers | Time |
| A six-stage chain, one template per module, on 4 workers | Time per epoch, against the design's 752 ms (one module per worker) and 7.4 ms (one file) |
| Isolating one crashing instance of 300 | Time, and runs taken |

There are no other figures to compare with. Report what you find.

### Done when

- The Milestone 1 laws still pass, and the Python tests pass on Linux.
- CI runs the five crates' tests on Linux, macOS and Windows.
- The benchmark reports the table above.

## Open points

Decide these by measurement or by reading, and say what you found:

- **Repeating rows for vectorized runs.** A rolling window with a long
  lookback repeats each row once per window that reads it. Measure the
  vectorized row count against the per-window one on the benchmark's
  windows, and say whether it matters.
- **Dictionary encoding.** Instance, role, stream and port repeat on every
  row. Try dictionary-encoded columns, and keep them only if the benchmark
  shows a gain.
- **The memory ceiling on macOS.** `RLIMIT_AS` is not enforced there. Say
  whether another limit is worth having, without adding a dependency.

Stop and ask the user about anything else that changes what the system
means, including:

- any part of what a window receives that this plan and the design do not
  settle;
- adding a dependency not listed above;
- anything that would let a function see state other than its inputs,
  bindings and parameters.

## What to report

At the end of each milestone, write a short report:

- what was built, and which tests pass, with the command that ran them;
- the benchmark table, for Milestone 2;
- every place you departed from this plan, and why;
- what you verified and what you only inferred;
- anything you could not test, and what it would take.

## For step 6

- **Store module bytes.** Decided with the user on 2026-11-22. The pool's
  pinned copies live only as long as the process, so a file edited before
  a restart leaves runs of the active plan failing with `Load`. Step 6
  keeps every module's bytes in a store table by (module, digest), written
  in the same transaction as the plan that first names them, and gives the
  pool those bytes in place of reading the file. Design `exec`'s pool so
  that where it gets a version's bytes can be swapped without changing
  its callers: one internal source that reads files now, and a store
  later.

## Not in this plan

The dirty rule, building runs from storage, comparing results with storage,
maintenance records, retries, refresh, the request log, the epoch driver,
the query interface, the Python bindings, the Acquirium adapter, translating
the current engine's acceptance tests, and anything in Acquirium. Do not
start them.
