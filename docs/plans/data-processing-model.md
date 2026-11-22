# Data processing model

Proposed design, 2026-11-22. Not implemented. It covers the Escapement engine
and the boundary between the engine and Acquirium. The [decisions](#decisions)
it assumes are listed near the end, followed by the questions it leaves open.

## Summary

Acquirium keeps derived time series true. An app declares a relationship, such
as "this stream is the one-minute mean of those readings". The system
maintains it when readings arrive late, are corrected or are deleted, when the
plant model changes which streams feed a calculation, and when the
calculation's code changes.

The design follows from one model:

> Everything is a versioned series. Each derived series is owned by one
> operator instance. An instance is a pure function from the intervals its
> windows read to the intervals they own. A change to data, bindings or code
> is a set of dirty intervals. The engine evaluates the windows that read
> them, publishes the difference in one atomic epoch, and records the epoch
> each instance's output reflects.

Four ideas carry it. Each replaces a family of special cases with one concept.

| Idea | Statement |
| --- | --- |
| One window shape | A window is an anchor time, an interval it reads per input, and an interval it owns. |
| One function contract | The engine hands a function a run of windows and replaces exactly what those windows own. |
| One dirty rule | Changed intervals go in; the windows that read them come out. |
| One maintenance record | Each instance records the epoch its output reflects, and why if it is behind. |

The primary workload is a DAG of Python functions. The engine's job is to
store series, decide what to recompute, hand Python its inputs as columns, and
publish the results atomically.

## Goals and non-goals

Goals:

- A model that can be stated on one page, where every feature is an instance
  of it.
- DAGs of Python functions over time series, kept correct under late data,
  corrections, deletions, plant-model changes and code changes.
- Results that do not depend on arrival order, on how work was batched, or on
  thread timing.
- Atomic publication: a source change and everything computed from it
  synchronously become visible together.
- Work proportional to what changed, not to the length of history.
- A multi-threaded Rust core in small crates, with columnar data from storage
  to the function and back.
- DuckDB and PostgreSQL as backends with the same capabilities.

Non-goals:

- SQL operators or a growing library of native functions. The existing native
  functions stay as they are.
- Dropping late data behind a watermark.
- Running on more than one node.
- Units in the engine. Functions convert units, as they do today.
- Moving oversize synchronous work to the background, or committing parts of
  the DAG independently. Both are [deferred](#deferred).

## Vocabulary

| Term | Meaning |
| --- | --- |
| Series (stream) | A named sequence of readings. Source series are written by requests; derived series by one instance. |
| Reading | A number or text at a key: stream, event time, sample. |
| Epoch | One batch of requests applied and published together. Epoch numbers order publications. |
| Template | One calculation, declared once: function, window, roles, ports, freshness. |
| Role | A named input of a template. A driving role's readings decide which windows exist. |
| Instance | A template applied to one group, with its own parameters and output series. |
| Binding | One row saying a stream feeds an instance in a role over a span of event time. |
| Window | One evaluation of an instance: an anchor, what it reads, what it owns. |
| Run | A batch of windows of one template, handed to a function in one call. |
| Maintenance record | The epoch an instance's output reflects, and the cause if it is behind. |

An instance is what the current engine calls an operator. A window is what it
calls a group.

## Series and time

### Readings

A reading's key is its stream, its event time in UTC nanoseconds, and a sample
key that distinguishes readings at the same time. Its value is a finite number
or text, with a quality code. Writing an existing key replaces the reading.
A request can also delete a key or a range of event time.

### Two clocks

| Clock | Answers | Used for |
| --- | --- | --- |
| Event time | Which readings a computation reads | Windows, binding spans |
| Epochs | When the engine applied a change | Receipts, snapshots, history, the change feed |

These are the valid time and transaction time of temporal databases
[[Jensen and Snodgrass 1999]](#references). A reading from last year can arrive
in today's epoch. It is never dropped for being late; it takes its place at
its own event time and the results that read it are revised
[[Akidau et al. 2015]](#references).

### Versions, snapshots and the change feed

A version is one value a key has held, stamped with the epoch that wrote it:

```text
(stream, time, sample, since) -> value or text, quality | deleted
```

Versions are never changed. A key's value as of epoch E is its version with
the greatest `since` at or before E. A snapshot is an epoch number: every read
is as of an epoch, so a reader holding epoch 7 keeps seeing epoch 7.

The change feed lists each epoch's signed changes: `-1` retracts a row and
`+1` inserts one. The feed is the derivative of the versions and the versions
are its integral [[Budiu et al. 2023]](#references). Integrating the feed
through epoch E must give exactly the snapshot at E, which is a property the
tests check.

## The log and epochs

Every change enters through one ordered, durable log.

- **Requests.** A request is a batch of mutations to source series or a plan
  activation. Its identity is `(source, request_id)`. Resending the same
  payload returns the original receipt; a different payload under that
  identity is rejected.
- **Receipts.** A receipt means the request is durable. It does not mean the
  request is visible. *Accepted* is the highest durable request; *applied* is
  the highest request included in a published epoch.
- **Epochs.** An epoch takes a bounded contiguous prefix of the pending
  requests. It starts from the last published snapshot and builds a private
  candidate. One transaction commits the derived versions, the maintenance
  records and an epoch record: the requests applied, the plan activated if
  any, and what each instance evaluated.
- **Readers.** Readers see published snapshots only. `read_after(receipt)`
  waits until the receipt's request is applied.
- **Capacity.** The inbox is bounded. A full inbox refuses new requests so
  that writers back off [[Welsh et al. 2001]](#references).

Source writes are logged as commands and derived results as values. Replaying
a command is cheap and exact. Recomputing a result would run every function
again, so recovery reads results back and never calls a function
[[Malviya et al. 2014]](#references). If the process stops before an epoch
commits, the candidate is discarded and its requests stay pending.

## Plans

A plan is the complete description of what the engine computes. It has three
relations.

```text
Template { id, function, window, roles, ports, freshness, annotations }
Instance { template, key, parameters, outputs, annotations }
Binding  { template, instance, role, stream, valid_from, valid_to, context }
```

- **Template.** One calculation. `roles` names its inputs and how each is
  read. `ports` names its outputs. `freshness` is synchronous or asynchronous.
- **Instance.** A template applied to one group, such as one tank. `outputs`
  maps each port to the series this instance owns. `parameters` are values
  the function receives.
- **Binding.** A stream feeds an instance in a role, for event times in
  `[valid_from, valid_to)`. `context` is free-form text the function receives
  with that input, such as unit conversion factors.

For example, a site wants the mean temperature of each of its two tanks every
minute. Tank A has two probes and tank B has one. A dash means unbounded.

```text
Template
  id          tank_mean
  function    mean_temperature in tanks.py, pinned by digest
  window      one-minute buckets
  roles       temperature
  ports       mean
  freshness   synchronous

Instance
  template    key      parameters   outputs
  tank_mean   tank_a   {}           mean -> urn:tank_mean/tank_a/mean
  tank_mean   tank_b   {}           mean -> urn:tank_mean/tank_b/mean

Binding
  template    instance   role          stream        valid_from   valid_to
  tank_mean   tank_a     temperature   urn:probe_1   -            -
  tank_mean   tank_a     temperature   urn:probe_2   -            -
  tank_mean   tank_b     temperature   urn:probe_3   -            -
```

The template is written once. Each tank is one instance row and each probe is
one binding row. A third tank adds an instance and its bindings; the template
does not change.

If probe 2 moves to tank B on 1 March, only bindings change. Its row for
tank A gets an end, and a row for tank B is added:

```text
  tank_mean   tank_a     temperature   urn:probe_2   -            2026-03-01
  tank_mean   tank_b     temperature   urn:probe_2   2026-03-01   -
```

Tank A's mean uses probe 2 before 1 March and tank B's uses it from then on.
The engine recomputes both tanks from 1 March and leaves earlier results
alone.

Rules:

- Each derived series has exactly one owning instance.
- The instances form a DAG: an instance may read series owned by others, and
  there are no cycles.
- An instance's *definition* is its template's function, window, roles and
  ports, plus its own parameters and outputs. The engine computes a digest of
  it. Bindings are not part of the definition.
- `annotations` are opaque to the engine. They are versioned with the plan,
  are not part of the definition, and are not passed to functions. The
  integrator uses them to keep its own facts beside the plan.

A plan is activated by a request in the log, so it takes effect at an epoch
like any other change. The engine compares the new plan with the active one
and classifies each instance as new, redefined, rebound (same definition,
different bindings), unchanged or removed. Every past plan stays readable by
epoch.

Declaring a calculation once and binding it to many groups lets the engine
evaluate every instance of a template in one call to the function. It also
makes a change in group membership a change to a few binding rows.

## Windows

### The shape

A window has an anchor time `t`, an interval it reads for each role, and an
interval it owns. All intervals are half-open and in nanoseconds.

```text
Window { anchors: Instant | Grid { origin, step }, own }
Role   { name, reach, driving }

Reach  = Span   { before, after }   reads [t - before, t + after)
       | Latest { max_age }         reads the role's latest reading at or
                                    before t, no older than max_age
```

- **Anchors.** `Grid` anchors lie at `origin + k * step`. `Instant` anchors
  are the timestamps at which a driving role has a reading.
- **Reach.** Each role has a reach, which says what a window reads from that
  role.
- **Driving.** Each role is driving or not. A *driving* role's readings
  decide which windows exist. By default a `Span` role drives and a `Latest`
  role does not. [Driving roles](#driving-roles) explains the choice.
- **Ownership.** The window owns the output interval `[t, t + own)`, with
  `own` at most `step`. By default it owns only `t`. Owned intervals of one
  instance never overlap.
- **Existence.** A window exists when at least one driving reading lies in
  its read interval. A window that does not exist has no output, and the
  function is not called for it. Output therefore never depends on whether a
  window once held readings.

The familiar windows are instances of this shape:

| Window | Anchors | Reads | Owns |
| --- | --- | --- | --- |
| Pointwise | each driving timestamp | readings at `t` | `t` |
| Fixed bucket of width `w` | grid of `w` | `[t, t + w)` | `t` |
| Rolling, lookback `L`, lookahead `A` | grid of `step` | `[t - L, t + A]` | `t` |
| Range of width `w`, margins `L` and `A` | grid of `w` | `[t - L, t + w + A)` | `[t, t + w)` |
| As-of role, in any of the above | (not driving by default) | latest in `[t - max_age, t]` | (nothing) |

The range window is the one cleaning, filling and resampling need. Its
function reads a bucket with some margin on each side and returns a series at
any timestamps inside the bucket. A fixed bucket is a rolling window with no
lookback.

Treating window assignment as a function from readings to windows follows the
Dataflow model [[Akidau et al. 2015]](#references).

### Driving roles

A driving role's readings decide which windows exist. A role that is not
driving supplies a value to those windows and never creates one. By default
a `Span` role drives and a `Latest` role does not. Every template has at
least one driving role.

For example, a second template subtracts the latest setpoint from each tank
temperature reading. Its window is pointwise. `temperature` is a `Span` role
and `setpoint` is a `Latest` role, both with the default.

```text
time          09:00   09:01   09:01:30   09:02   09:03
temperature   20.0    20.5               21.0
setpoint      19.0            20.0
output        1.0     1.5                1.0
```

- Three temperature readings give three windows and three outputs. Each
  window reads the latest setpoint at or before its own time.
- The setpoint written at 09:01:30 creates no window and gives no output at
  09:01:30. It changes what the 09:02 window reads.
- At 09:03 there is no temperature reading, so there is no window and no
  output, although a setpoint is in effect.

The same holds on a grid: a bucket with no driving readings does not exist,
whatever its `Latest` roles hold.

A role that is not driving still takes part in the dirty rule. If the
09:01:30 setpoint arrives late or is corrected, the 09:02 window is
recomputed.

Declaring a role not driving adds no capability. It gives the same result as
letting the role drive and having the function return nothing for the
windows that role alone created, because the engine treats an empty result
and a window that does not exist alike. The declaration saves the calls. A
measurement taken hourly that looks up a series with a reading every second
would otherwise call the function 3,600 times to write one row. The default
also means a plain function such as `temperature - setpoint` writes rows
only where there is a measurement.

A `Latest` role can be declared driving. When both inputs hold their value
until they change, the output should change when either does. An alarm for
"temperature above setpoint", where the temperature is reported only on
change, declares both roles `Latest` and driving. A window then exists at
every timestamp where either has a reading, and reads the latest of both.
Declared that way, the example above gains one window, at 09:01:30, which
reads 20.5 and 20.0.

A driving `Latest` role on a grid needs a bounded `max_age`. Its windows
exist while a reading is in effect, and would otherwise continue without
end.

### Geometry

The scheduler needs three functions and nothing else about a window:

```text
reads(role, t)        the interval window t reads for the role
readers(role, [a,b))  the anchors whose read interval meets [a,b)
owner(s)              the anchor whose owned interval holds time s
```

For a `Span` role, `readers(role, [a,b))` is every anchor `t` with
`a - after < t < b + before`. For a `Latest` role, a reading at `s` can be
read by anchors in `[s, s + max_age]`. The engine may shorten that range to
end at the stream's next reading, taking the later of the old and new states.

Mapping each reading to the windows it belongs to, with no assumption that
readings arrive in order, is the window-ID approach
[[Li et al. 2005a]](#references).

The soundness law: if a window's output can depend on a reading at `s`, then
its anchor is in `readers(role, [s, s+1))`. Everything the engine skips rests
on this law. Reach adds along a path through the DAG: a window with reach `L`
feeding one with reach `M` spreads a correction across `L + M`.

### Binding spans clip a window

A binding's validity can end inside a window. If a sensor leaves a tank at
14:32, the 14:30 bucket reads its readings only up to 14:32. The function is
told the window's full extent and the part each input covers, so it can
decline to answer when too little of the window is covered.

## Functions

### The law

For every window `t` of every instance:

```text
output in own(t) = f(readings in reads(t), bindings in effect, parameters)
```

A function is pure and deterministic. It receives every reading its window
reads, not the change that triggered it. It returns the complete result for
what the window owns. It keeps no state between calls, reads no clock, and
its code is pinned by content digest.

Three guarantees follow:

- **Order does not matter.** The same readings and plan give the same
  outputs, in any arrival order.
- **Corrections apply.** A late or corrected reading recomputes every window
  that reads it.
- **A new plan recomputes the past.** A changed definition recomputes the
  instance. A changed binding recomputes the windows in its span.

### What a function returns

A function returns rows for each declared port. Each row's time must lie in
the window's owned interval, numbers must be finite, and keys must be unique.
Returning no rows for a port retracts what the window owned on that port.

### Runs: the one wire contract

The engine never calls a function for a single window. It sends a *run*:
many windows, across many instances of one template, in one call.

| Sent | Contents |
| --- | --- |
| Static context | The template, and each instance's parameters and bindings. Sent once per plan and template; the worker caches it. |
| Rows | Every reading the run's windows read, once each: instance, role, stream, time, sample, value or text, quality. |
| Windows | Each window's instance and anchor, and the interval it reads per role. |
| Carried state | For sequential templates: each instance's starting state and its stored outputs. |

| Returned | Contents |
| --- | --- |
| Output rows | Instance, port, time, sample, value or text, quality. |
| Status | Success or an error message, per instance. |

The engine checks every returned row against the windows it asked for, and
replaces exactly what those windows own. Both directions are Arrow record
batches. Handing a long-lived Python process a columnar batch and calling the
function once per group is the pattern of Spark's pandas function API
[[Spark]](#references).

### Calling modes are adapters

How the user's function is called is decided in the worker, on top of the
run. The engine sees one contract.

| Mode | The function receives | Use |
| --- | --- | --- |
| Per window (default) | One window's readings and its context | Most calculations |
| Vectorized | The whole run, each row tagged with its window | Large recomputations in Polars or pandas |
| Sequential | One window at a time in anchor order, with the previous result | Running totals, filters, recursive estimators |

A sequential window receives `previous`: the latest earlier window of the
same instance that wrote output, with what it read and what it wrote. The
state is the output itself, so it is stored, versioned and recovered like any
series. A reset period, such as a day, starts the state afresh at each
boundary.

### Run invariance

Results must not depend on how the engine divides windows into runs. The
per-window and sequential modes guarantee this by construction. A vectorized
function must compute each window from that window's rows alone. The engine
can check it: a test mode evaluates the same windows under different
divisions and compares the results.

## Evaluation

### Changed intervals

Storage answers one question for the scheduler:

```text
changed(stream, after, through) -> a set of event-time intervals
```

It returns where the stream differs between two epochs. A few changed keys
come back as exact points. Many come back as ranges. Returning too much is
always safe; it costs work, never correctness. BTrDB answers the same
question as a storage operation: the time ranges that differ between two
versions of a stream. DISTIL, the processing framework built on BTrDB, keeps
derived streams correct when data changes or arrives out of order
[[Andersen and Culler 2016; Andersen et al. 2015]](#references).

### The dirty rule

To bring an instance from one cut to another, where a cut is a definition and
an epoch:

```text
dirty anchors =
    every anchor                                      if the definition differs
  + readers(role, changed(stream, e0, e1) in span)    for each binding
  + readers(role, span)                               for each span over which a
                                                      binding was added, removed
                                                      or altered
  + the next window after t                           for a sequential template,
                                                      when the output at t changed

evaluate = the dirty anchors whose windows exist at e1
replace  = stored output owned by a dirty anchor is replaced by the results;
           whatever is not produced again is retracted
```

The same rule serves every situation. Only the starting cut differs.

| Situation | Starting cut | What is dirty |
| --- | --- | --- |
| A write, a correction, a late reading | The previous epoch | The windows that read that key |
| A deletion | The previous epoch | The same windows; one left without readings stops existing and its output is retracted |
| The plant model moves a stream to another group | The previous epoch | Both instances, over the binding's span |
| New code or parameters | None | Everything the instance owns |
| A failed instance is retried | The epoch its output reflects | The windows that read what changed in the meantime |
| An asynchronous instance refreshes | The epoch of its last refresh | The same |

### Early cutoff

After evaluating, the engine compares each result with the stored rows. Only
rows that differ become changes, and only changes are passed downstream. An
instance whose results are unchanged causes no further work. This is early
cutoff in a build system, with windows as the build keys and the maintenance
record as the trace of what an output was built from
[[Mokhov et al. 2018]](#references).

A sequential instance uses the same mechanism along event time. Repair starts
at the earliest dirty window and continues forward while each result differs
from the stored one. It stops at the first window that hands on unchanged
state, or at a reset boundary.

### Order and parallelism within an epoch

- An instance is ready when every instance it reads from has finished, even
  one that changed nothing.
- The scheduler collects the instances that are ready, groups them by
  template, and divides their windows into runs by a row budget. Under load,
  more instances are ready at once, so batches grow.
- Runs execute in parallel on a thread pool. Building a run's input and
  comparing its output are also parallel. A run is a small unit of work that
  any idle thread can take [[Leis et al. 2014]](#references).
- Each task reads the published snapshot plus the candidate's changes from
  instances that have finished. No task reads another's unfinished work.
- Results are merged in stream and key order, so the epoch record and the
  change feed are identical however the work was interleaved.

Two tasks can run at the same time when neither writes what the other reads
or writes. Single ownership of series and the readiness rule ensure that.

### Limits

Each instance has budgets for windows, input rows and output rows per epoch.
Exceeding one is a failure of that instance, handled like any other failure.
Because synchronous work holds the epoch, a single very large change is
published late, not partially.

## Freshness and failure

### The maintenance record

```text
Maintenance { definition, reflects, cause }

cause = Error { message }          the function failed
      | Upstream { instances }     an instance it reads from is behind
      | Refreshing { pinned }      an asynchronous refresh is in progress
```

The law: at every published epoch, an instance's stored output equals its
function applied to its inputs as of `reflects`, under `definition`. An
instance with no record is current: it reflects the published epoch under the
active plan. In steady state there are no records.

Recording how far each part of a computation has progressed, and letting
parts lag behind a shared clock, is the frontier of Naiad and the `since` and
`upper` of Materialize's storage layer [[Murray et al. 2013;
Materialize]](#references).

### Synchronous and asynchronous templates

- **Synchronous** (the default). The instance is evaluated in every epoch
  that dirties it. The epoch does not publish until every healthy synchronous
  instance reflects it.
- **Asynchronous.** The instance is not evaluated in epochs. It refreshes in
  the background, and its published output lags by design. Instances that
  read it treat its output as they would a source series.

### Failure

A function error fails that instance for the epoch. Its attempt is discarded,
its stored output is retained, and a record with `Error` is committed with
the epoch. Source data and independent branches publish as usual.

The hold rule: an instance is not evaluated while a synchronous instance
upstream of it is behind. It retains its output and gets a record with
`Upstream`. An instance never computes on a mixture of fresh inputs and a
stale synchronous input.

A commit failure is different: no part of an epoch becomes visible without a
successful transaction.

### Recovery

A failed instance is retried when its inputs change, when an activation
changes it, or on explicit request. The retry applies the dirty rule from the
record's cut. On success the record is cleared, and the instances it was
holding evaluate from their own records in the same epoch.

Recovery costs what changed while the instance was behind, because the record
says where to resume.

### Asynchronous refresh

1. A refresh pins an epoch `P` and applies the dirty rule from `reflects` to
   `P`.
2. It evaluates on its own workers and stages its output privately, with a
   checkpoint so that a restart resumes.
3. When complete, it submits a publication request to the log. One such
   request may be pending at a time.
4. The epoch that applies the request installs the staged output, sets
   `reflects` to `P`, and evaluates the synchronous instances that read it.

While a refresh runs, readers and consumers see the last published output.
If the plan redefined the instance in the meantime, the staged output is
discarded.

### What a reader sees

A read returns data as of an epoch. For each derived series it can also
report whether the series is current and, if not, the epoch it reflects and
the cause, including causes upstream. A reader that needs a current answer
can wait for one.

## Python execution

### Workers

Functions run in a pool of long-lived Python processes.

- **Workers are generic.** A worker imports the libraries once and loads
  function modules on demand, each from bytes pinned by digest into its own
  namespace. Any worker can run any function, so the pool is sized by CPU
  count, not by the number of functions in the DAG.
- **One job at a time.** A worker runs one run, then returns to the pool.
- **Bounded.** Each worker has a memory ceiling, each run a time allowance
  and an output size limit. Workers are recycled by age and job count.
- **Discarded on failure.** A worker that ran a failing or timed-out call is
  not reused.

Sharing an interpreter between functions relies on the purity the function
law already requires.

A worker that could hold only one module made a six-stage chain spread over
six files take 752 ms an epoch with four workers, against 7.4 ms with the
functions in one file, because every stage started a new process.

### Isolating a failure within a run

Failure is recorded per instance. The per-window and sequential adapters
catch an exception, mark that instance failed, and continue with the others.
When a vectorized call, a crash or a timeout fails a whole run, the engine
splits the run by instance and tries the halves until the failing instances
are found.

### Layers

| Layer | Responsibility |
| --- | --- |
| Engine worker | The run protocol, module loading, calling modes, output checks |
| Acquirium adapter | Datetimes in place of nanoseconds, pandas or Polars frames, `emit` and related helpers |
| The user's function | `(frame, context)` returning a frame per port |

The user's function signature does not change.

## Storage

### What processing needs from storage

Every read is as of an epoch.

| Operation | Used by |
| --- | --- |
| `range(stream, interval, epoch)` as a record batch | Building runs, comparing results, queries |
| `latest(stream, at or before t, max_age, epoch)` | `Latest` roles, sequential state |
| `changed(stream, after, through)` | The dirty rule |
| `stats(stream, epoch)`: count, first and last time | Planning, previews |
| `publish(epoch, segments, records)` in one transaction | Every writer |

### Layout

The layout is a log in front of blocks.

- **The log** holds accepted requests and epoch records until a seal covers
  them.
- **Blocks.** A series is a sequence of blocks, each covering an aligned
  interval of event time. A block keeps every version of every key in its
  interval. An open block lives in memory as columns. A sealed block is an
  immutable Parquet file. Time-aligned blocks per series, with the recent
  ones in memory, follow Gorilla [[Pelkonen et al. 2015]](#references).
- **Segments.** A segment is a sorted, columnar batch of versions for one
  series. Every writer produces segments: an epoch's results, a refresh, a
  bulk import, a seal. Publication references them in one transaction. Small
  segments are stored in the log; large ones are files.
- **Seals.** A background seal merges a block's segments into a new file,
  then swaps the directory entries and trims the log in one transaction. A
  write buffer flushed to immutable sorted files that are merged later is the
  log-structured merge-tree [[O'Neil et al. 1996]](#references). Immutable
  files made current by a transactional directory follow Delta Lake
  [[Armbrust et al. 2020]](#references).
- **One index for every reader.** Epochs, queries, history and the change
  feed all read the same versioned blocks, as in shared arrangements
  [[McSherry et al. 2020]](#references).
- **Memory** is bounded by a byte budget shared by open blocks and a cache of
  decoded parts of files.

Each segment records its event-time range and epoch range, which is what
`changed` reads. Recent epochs also keep their exact changed keys in memory.

### Backends

Two narrow interfaces separate the engine from where bytes live.

| Interface | Holds | Implementations |
| --- | --- | --- |
| Catalog | The log, retry identities, epoch records, plans, maintenance records, the block directory | DuckDB, PostgreSQL |
| Block store | Immutable block files: put, get, delete | A local directory; an object store later |

Seals, staging, refreshes and recovery are written once, above both
interfaces. DuckDB and PostgreSQL therefore have the same capabilities, and
one contract test suite runs against each.

### Recovery

Opening a store loads the block directory and replays the log after the last
seal: source requests are re-applied and derived versions are read back.
Requests beyond the published epoch are the next epoch's input.

## Threads

| Stage | Threads | Owns |
| --- | --- | --- |
| Admission | Callers, sharing commits | Appends to the inbox |
| Epoch driver | One | The candidate, and the only writes to published state |
| Compute pool | A work-stealing pool | Building runs, reading storage, comparing results |
| Python pool | Worker processes | Function calls |
| Refresher | One, with its own workers | Asynchronous refreshes |
| Sealer | One | Seals |

Rules:

- A published snapshot is immutable and is replaced by swapping a pointer.
  Readers never wait for an epoch.
- Only the epoch driver changes published state. Tasks read snapshots and
  return results.
- No lock is held across a parallel call or a call to Python.
- Each stage has a bounded queue. Admission is refused at the inbox, where
  work enters, and a refresh waits for capacity before it starts
  [[Welsh et al. 2001; Welsh and Culler 2003]](#references).

## Crates

| Crate | Contents | Depends on |
| --- | --- | --- |
| `geometry` | Intervals, interval sets, the window shape, `reads`, `readers`, `owner`. No I/O. | |
| `series` | Segments, blocks, snapshots, the reads and `changed`, seals, the cache | `geometry` |
| `log` | The inbox, retry identities, epoch records, recovery, the catalog and block-store interfaces | `series` |
| `plan` | Templates, instances, bindings, validation, the DAG, plan comparison | `geometry` |
| `repair` | The dirty rule, runs, result comparison, maintenance records, refresh | `series`, `plan`, `exec` |
| `exec` | The run protocol, the Python worker pool, the existing native functions | `plan` |
| `engine` | Stages, limits, status and the public API | all of the above |
| `python` | Bindings that pass record batches without copying | `engine` |

`geometry` is pure and small. Its laws are property-tested on their own, and
every other crate relies on them.

## The Acquirium boundary

Acquirium owns everything semantic: the graph, selection queries, units,
value kinds and the HTTP and Python interfaces. The engine owns series, plans
and computation.

| Acquirium | Engine |
| --- | --- |
| App | Template |
| Group | Instance key |
| Selection, resolved against the graph | Binding rows |
| Output port of a group | A series named from the app, group and port |
| App declaration, replacement chain, units | Annotations |

- **Compiling.** Compiling is a function from the graph and the app
  declarations to templates, instances, bindings and the metadata of the
  output series. It repeats until the selections stop changing, because
  registering an output can make another selection match it.
- **Deploying.** A deploy activates the compiled plan. The declarations ride
  on the plan as annotations, so the engine's log is the one record of what
  is deployed. On restart Acquirium reads the active plan and rebuilds the
  output metadata in the graph from it. The graph and the engine remain
  separate transactions; the graph's copy is a projection that can always be
  rebuilt.
- **Membership over time.** A selection resolved against one graph gives
  bindings with no start or end. Resolving it against successive versions of
  the graph gives bindings with spans, which is how a probe that really moved
  on a date is recorded.
- **Replacement chains.** Which series is the current, raw or stored stage of
  a measurement is kept in annotations. A read resolves it from the plan at
  the read's epoch.
- **Reads and writes.** Both cross the boundary as Arrow record batches.

## Invariants and tests

| Invariant | Check |
| --- | --- |
| The function law holds for every window | Compare with an independent full recomputation |
| The dirty rule is sound | Random changes; results outside the evaluated windows must already match the recomputation |
| Order does not matter | Shuffle request order and batching; compare final snapshots |
| Runs are invisible | Random run boundaries and task completion order; compare with serial evaluation |
| The feed integrates to the snapshot | Integrate the feed through each epoch and compare |
| Recovery is exact | Stop at random points around commit and seal; reopen and compare |
| Backends agree | Run the same histories on DuckDB and PostgreSQL |
| The maintenance law holds | Fail and recover instances at random; compare with recomputation at the recorded epoch |

Full recomputation is the oracle throughout. It is slow and obviously right.

## Implementation order

Each step leaves the system working and is checked against the oracle.

1. **Generic Python workers.** A worker loads any function module.
2. **Runs.** One call per template across instances, columnar in both
   directions, with the static context cached. Until templates exist in the
   plan, instances are grouped by identical function and window.
3. **The window shape.** The `geometry` crate, with the existing windows
   expressed in it and the range window added.
4. **The dirty rule.** `changed` in storage; one discovery path.
5. **Plans as templates, instances and bindings.** Acquirium's compiler emits
   them; declarations move into annotations.
6. **The maintenance record.** Failure and refresh state become one record;
   recovery resumes from it.
7. **Storage.** One write path through segments; the catalog and block-store
   interfaces; PostgreSQL at parity.

## Decisions

Made on 2026-11-22.

- **PostgreSQL stays** as a backend alongside DuckDB, for deployment.
- **Synchronous work holds the applied frontier**, including a single very
  large change. Consistency comes first; moving such work to the background
  can be considered later. This document assumes the same for the first
  evaluation of a new or redefined instance.
- **DAGs of Python functions are the priority.** Native functions are kept as
  they are and are not extended.
- **Unit conversion stays in Python functions.** This follows from the
  priority above and was not decided separately. The engine does not
  transform values.

## Open questions

- **Block files under PostgreSQL.** The first choice is the server's data
  directory. Storing blocks in the database, or in an object store, would
  keep the store in one place. A consistent backup needs the catalog and the
  blocks as of the same seal.
- **Retry cadence.** Retrying a failed instance on every input change repeats
  work up to the failing window. A limit on attempts between explicit retries
  may be needed.
- **Refresh and failing consumers.** When a synchronous consumer fails while
  a refresh is applied, this design publishes the refresh and holds the
  consumer, as it would for a source write. Requiring both to publish
  together is the alternative.
- **Compiling against a scratch graph.** Compilation currently publishes
  output metadata to the graph between rounds. Whether it can run against an
  in-memory overlay depends on how inference is applied.
- **Sequential templates over overlapping reads.** The model orders windows
  by anchor, so it permits a sequential rolling window. Whether to allow it
  is undecided.
- **Instant anchors with a longer reach.** The shape permits a window at each
  reading that looks back over a span. It is not needed first.
- **Pruning history.** If old versions are ever pruned, an instance whose
  record is older than the pruning point must be rebuilt in full.
- **Plan size.** Bindings with spans grow with every change in membership.
  Activating a difference in binding rows, in place of a whole plan, may be
  needed.

## Deferred

- **Chaining stages in one worker**, so that a chain of functions over one
  group does not return to the engine between stages.
- **Shared partial results for overlapping windows**, as in panes and stream
  slicing [[Li et al. 2005b; Traub et al. 2019]](#references). Here it would
  be an upstream template, not a new mechanism.
- **Computing old windows only when read**, as in Noria's partial state
  [[Gjengset et al. 2018]](#references).
- **Background evaluation of oversize synchronous work**, and independent
  commits for unrelated parts of the DAG.

## References

What each source contributed is given with it. Two attributions were checked
against summaries and not the papers' full text: BTrDB's operation for the
ranges that differ between versions, and the details of the window-ID
approach.

- **Akidau et al. 2015.** T. Akidau and others. "The Dataflow Model: A
  Practical Approach to Balancing Correctness, Latency, and Cost in
  Massive-Scale, Unbounded, Out-of-Order Data Processing." PVLDB 8(12).
  Event time apart from processing time; windows as an assignment of readings;
  results revised by retraction when late data arrives.
- **Andersen and Culler 2016.** M. P. Andersen and D. E. Culler. "BTrDB:
  Optimizing Storage System Design for Timeseries Processing." FAST 2016.
  <https://www.usenix.org/conference/fast16/technical-sessions/presentation/andersen>
  Every version of a series kept; the ranges that changed between two
  versions as a storage operation.
- **Andersen et al. 2015.** M. P. Andersen, S. Kumar, C. Brooks, A. von Meier
  and D. E. Culler. "DISTIL: Design and Implementation of a Scalable
  Synchrophasor Data Processing System." IEEE SmartGridComm 2015.
  <https://www.escholarship.org/uc/item/61w8z66w>
  Derived streams whose results hold when data changes or arrives out of
  order.
- **Armbrust et al. 2020.** M. Armbrust and others. "Delta Lake:
  High-Performance ACID Table Storage over Cloud Object Stores." PVLDB 13(12).
  Immutable Parquet files made current by a transactional directory.
- **Budiu et al. 2023.** M. Budiu, T. Chajed, F. McSherry, L. Ryzhyk and
  V. Tannen. "DBSP: Automatic Incremental View Maintenance for Rich Query
  Languages." PVLDB 16(7). <https://arxiv.org/abs/2203.16684>
  The specification: snapshots and changes as integral and derivative, and
  the feed property that follows.
- **Gjengset et al. 2018.** J. Gjengset and others. "Noria: Dynamic,
  Partially-Stateful Data-Flow for High-Performance Web Applications."
  OSDI 2018. Partial state, deferred.
- **Jensen and Snodgrass 1999.** C. S. Jensen and R. T. Snodgrass. "Temporal
  Data Management." IEEE TKDE 11(1), 36-44. Valid time and transaction time
  as separate dimensions.
- **Leis et al. 2014.** V. Leis, P. Boncz, A. Kemper and T. Neumann.
  "Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the
  Many-Core Age." SIGMOD 2014. Small units of work taken by any idle thread.
- **Li et al. 2005a.** J. Li, D. Maier, K. Tufte, V. Papadimos and
  P. A. Tucker. "Semantics and Evaluation Techniques for Window Aggregates in
  Data Streams." SIGMOD 2005. Windows identified by ID; each reading mapped
  to the windows it belongs to, with no ordering assumption.
- **Li et al. 2005b.** J. Li, D. Maier, K. Tufte, V. Papadimos and
  P. A. Tucker. "No Pane, No Gain: Efficient Evaluation of Sliding-Window
  Aggregates over Data Streams." SIGMOD Record 34(1). Shared partial results,
  deferred.
- **Malviya et al. 2014.** N. Malviya, A. Weisberg, S. Madden and
  M. Stonebraker. "Rethinking Main Memory OLTP Recovery." ICDE 2014.
  Logging commands in place of their effects, and recovering by replay.
- **Materialize.** The `persist` storage layer, in which each collection has
  a `since` and an `upper` frontier.
  <https://dev.materialize.com/api/rust/src/mz_persist_client/lib.rs.html>
  A recorded position per collection that readers can inspect and wait on.
- **McSherry et al. 2020.** F. McSherry, A. Lattuada, M. Schwarzkopf and
  T. Roscoe. "Shared Arrangements: Practical Inter-Query Sharing for
  Streaming Dataflows." PVLDB 13(10). One indexed, versioned structure shared
  by every reader.
- **Mokhov et al. 2018.** A. Mokhov, N. Mitchell and S. Peyton Jones. "Build
  Systems à la Carte." Proc. ACM Program. Lang. 2(ICFP).
  <https://www.microsoft.com/en-us/research/publication/build-systems-la-carte>
  Early cutoff, and a recorded trace of what each output was built from.
- **Murray et al. 2013.** D. G. Murray, F. McSherry, R. Isaacs, M. Isard,
  P. Barham and M. Abadi. "Naiad: A Timely Dataflow System." SOSP 2013.
  Frontiers as the record of how far a computation has progressed.
- **O'Neil et al. 1996.** P. O'Neil, E. Cheng, D. Gawlick and E. O'Neil. "The
  Log-Structured Merge-Tree (LSM-Tree)." Acta Informatica 33. A write buffer
  flushed to immutable sorted files, merged in the background.
- **Pelkonen et al. 2015.** T. Pelkonen and others. "Gorilla: A Fast,
  Scalable, In-Memory Time Series Database." PVLDB 8(12). Time-aligned blocks
  per series with the recent ones in memory.
- **Spark.** Apache Spark's pandas function API, `applyInPandas`.
  <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html>
  Arrow batches between the engine and Python processes, with the function
  called once per group.
- **Traub et al. 2019.** J. Traub and others. "Efficient Window Aggregation
  with General Stream Slicing." EDBT 2019. Slices shared across overlapping
  windows under out-of-order data, deferred.
- **Welsh et al. 2001.** M. Welsh, D. Culler and E. Brewer. "SEDA: An
  Architecture for Well-Conditioned, Scalable Internet Services." SOSP 2001.
  Stages joined by bounded queues.
- **Welsh and Culler 2003.** M. Welsh and D. Culler. "Adaptive Overload
  Control for Busy Internet Servers." USITS 2003. Admission control at the
  stage where requests compete.
