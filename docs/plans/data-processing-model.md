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
decide what to recompute, hand Python its inputs as columns, and publish the
results atomically into tables that the database stores and SQL can query.
Reads go through one small [query interface](#queries).

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
- DuckDB and PostgreSQL as backends with the same capabilities, and the
  readings in ordinary tables that SQL can query.
- One small query interface for reading series, the same on both backends.

Non-goals:

- A storage engine. The database stores the readings and decides how to
  split, compress and index them.
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

A version is one value a key has held. It carries the epoch that wrote it
and, once it has been replaced or deleted, the epoch that ended it:

```text
(stream, time, sample, since, until) -> value or text, quality
```

A version's value never changes. A key's value as of epoch E is its version
with `since` at or before E and `until`, if it has one, after E. A snapshot
is an epoch number: every read is as of an epoch, so a reader holding epoch 7
keeps seeing epoch 7.

The change feed lists each epoch's signed changes: a version adds `+1` at its
`since` and `-1` at its `until`. The feed is the derivative of the versions
and the versions are its integral [[Budiu et al. 2023]](#references).
Integrating the feed through epoch E must give exactly the snapshot at E,
which is a property the tests check.

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
  candidate. One transaction writes the epoch's changes to the
  [tables](#tables), with the maintenance records and an epoch record: the
  requests applied, the plan activated if any, and what each instance
  evaluated.
- **Readers.** Readers see published snapshots only. `read_after(receipt)`
  waits until the receipt's request is applied.
- **Capacity.** The inbox is bounded. A full inbox refuses new requests so
  that writers back off [[Welsh et al. 2001]](#references).

A request waits in the log as a command until an epoch applies it.
Publication stores its effects: the new source readings and the results the
functions computed. Opening a store therefore calls no function and replays
nothing. If the process stops before an epoch commits, the candidate is
discarded and its requests stay pending.

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

A caller submits the whole plan each time, which makes an activation
idempotent. The engine [stores](#tables) only the rows that differ from the
active plan, so an activation costs storage in proportion to what it changed.
Binding rows with spans accumulate only as fast as group membership really
changes.

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
  `own` at most `step`. By default it owns only `t`, and a window at an
  `Instant` anchor always owns only `t`. Owned intervals of one instance
  never overlap.
- **Existence.** A window at an `Instant` anchor exists because a driving
  reading has that timestamp. A window at a `Grid` anchor exists when a
  driving reading lies in its read interval. A window that does not exist
  has no output, and the function is not called for it. Output therefore
  never depends on whether a window once held readings.

Anchors, reach and driving are independent choices. Any role may have any
reach at either kind of anchor.

The familiar windows are instances of this shape:

| Window | Anchors | Reads | Owns |
| --- | --- | --- | --- |
| Pointwise | each driving timestamp | readings at `t` | `t` |
| At each reading, lookback `L` | each driving timestamp | `[t - L, t]` | `t` |
| Fixed bucket of width `w` | grid of `w` | `[t, t + w)` | `t` |
| Rolling, lookback `L`, lookahead `A` | grid of `step` | `[t - L, t + A]` | `t` |
| Range of width `w`, margins `L` and `A` | grid of `w` | `[t - L, t + w + A)` | `[t, t + w)` |
| As-of role, in any of the above | (not driving by default) | latest in `[t - max_age, t]` | (nothing) |

The range window is the one cleaning, filling and resampling need. Its
function reads a bucket with some margin on each side and returns a series at
any timestamps inside the bucket. A fixed bucket is a rolling window with no
lookback.

A lookback at each reading serves a calculation that a sparse measurement
triggers. At each laboratory sample, a function can read the hour of sensor
readings before it. The laboratory role drives. The sensor role is a `Span`
with a one-hour lookback, declared not driving.

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

On a grid, the anchors in a range follow from arithmetic. `Instant` anchors
are found by reading the driving roles' timestamps in the range.

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

A sequential template may use any window. Windows are ordered by anchor and
what they own never overlaps, so read intervals that overlap do not disturb
the order. A cleaning function can therefore carry state from bucket to
bucket while reading a margin on each side.

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

A sequential instance uses the same mechanism along event time. Repair
evaluates the dirty windows in anchor order, and continues past each one
while its result differs from the stored one. Where a window hands on
unchanged state, repair skips to the next dirty window. A change does not
carry across a reset boundary.

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

cause = Error { message, windows }     the function failed on these windows
      | Upstream { instances }         an instance it reads from is behind
      | Refreshing { pinned }          an asynchronous refresh is in progress
      | Rejected { reader, message }   a refresh was computed, and a reader
                                       failed on it
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

A failed instance is retried when a window it failed on is dirtied, which
means an input that window reads has changed. It is also retried when an
activation changes its definition, bindings or limits, and on explicit
request. New data elsewhere does not retry it: functions are deterministic,
so the same inputs would fail the same way.

- A failure that names no window, such as a failed vectorized call, records
  the range of windows in the failed run.
- A sequential instance counts any change from its last reset up to the
  failing window, because state flows forward into that window.
- A timeout or a crash can be transient, so it is retried once automatically
  before the rule above applies.

The retry applies the dirty rule from the record's cut. On success the record
is cleared, and the instances it was holding evaluate from their own records
in the same epoch.

Recovery costs what changed while the instance was behind, because the record
says where to resume.

### Asynchronous refresh

1. A refresh pins an epoch `P` and applies the dirty rule from `reflects` to
   `P`.
2. It evaluates on its own workers and stages its output privately, with a
   checkpoint so that a restart resumes.
3. When complete, it submits a publication request to the log. One such
   request may be pending at a time.
4. An epoch of its own applies the request. It installs the staged output,
   sets `reflects` to `P`, and evaluates the synchronous instances that read
   it.

A refresh publishes together with its synchronous readers or not at all. If
a healthy reader fails on the new output, the epoch publishes none of it. The
old output keeps serving, the readers keep their results, and the refreshing
instance records `Rejected` with the reader's error. A source write cannot be
refused, because it is a fact about the plant. A refresh is the engine's own
result, so it can be. A rejected refresh is attempted again when its inputs
change, when the reader is changed, or on request.

While a refresh runs, readers and consumers see the last published output.
If the plan redefined the instance in the meantime, the staged output is
discarded.

### What a reader sees

A read returns data as of an epoch. For each derived series it can also
report whether the series is current and, if not, the epoch it reflects and
the cause, including causes upstream. A reader that needs a current answer
can wait for one. The [query interface](#queries) reports this through
`describe`.

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

The database is the store. Published readings live in ordinary tables in
DuckDB or PostgreSQL, and publishing an epoch is one transaction that updates
them. SQL can query them. How the tables are split, compressed and indexed is
the database's job, not the engine's.

### Tables

```text
readings (stream, event_time, sample, value, text, quality, since)
history  (stream, event_time, sample, value, text, quality, since, until)
streams  (id, name, kind)
```

- **`readings`** holds the current value of every key, one row per key.
  `since` is the epoch that wrote it.
- **`history`** holds every version that has been replaced or deleted.
  `until` is the epoch that ended it.
- **`streams`** maps the integer in `stream` to the stream's name, and says
  whether the stream is a source or derived.

A version is in exactly one of the two tables. The three questions the engine
asks of versions are plain queries:

| Question | Rows |
| --- | --- |
| The current value | `readings` |
| The value as of epoch E | `readings` with `since <= E`, and `history` with `since <= E < until` |
| The change feed of epoch E | `+1` for each version with `since = E`; `-1` for each `history` row with `until = E` |

Keeping current rows in one table and ended versions in another, each with
the period it held, is the layout of system-versioned tables in SQL:2011
[[Kulkarni and Michels 2012]](#references). Epochs, queries, past snapshots
and the change feed all read these same tables, as every reader shares one
arrangement in [[McSherry et al. 2020]](#references).

The alternative was one append-only table of versions with a view that picks
the latest. The view made an average over a day of all streams take 878 ms in
place of 38 ms, so current values have their own table.

The plan is stored the same way, in three more tables:

```text
templates (id, definition, annotations, since, until)
instances (template, key, parameters, outputs, annotations, since, until)
bindings  (template, instance, role, stream, valid_from, valid_to, context,
           since, until)
```

Each row carries the epochs it was active for, and `until` is empty while it
is active. The plan as of epoch E is the rows with `since <= E` whose `until`
is empty or after E. These tables are small, so active and ended rows share a
table. Which streams feed which outputs is then a SQL join of `bindings`,
`instances` and `streams`.

### Publication

An epoch publishes in one transaction:

1. Each key the epoch rewrote or deleted has its row moved from `readings` to
   `history`, with `until` set to the epoch.
2. The epoch's new values are inserted into `readings`.
3. The epoch record and the maintenance records are written, and the applied
   requests leave the inbox.

A SQL reader therefore sees whole epochs. An asynchronous refresh or a bulk
import stages its rows in a table of its own, and the epoch that applies it
moves them with the same three steps.

### The working set

The engine keeps the recent readings of each stream in memory as columns,
within a byte budget. Live windows read this working set and do not query the
database. Serving recent data from memory follows Gorilla
[[Pelkonen et al. 2015]](#references). The working set is a cache: it can be
dropped at any time and refilled from the tables.

### What processing needs from storage

Every read is as of an epoch. A read that the working set cannot answer is
one SQL query that returns a record batch.

| Operation | Used by |
| --- | --- |
| `range(streams, intervals, epoch)` as a record batch | Building runs, comparing results, queries |
| `latest(streams, times, max_age, epoch)`: the last reading at or before each time | `Latest` roles, sequential state |
| `changed(stream, after, through)` | The dirty rule, queries |
| `stats(streams, interval, epoch)`: count, first and last time within an interval | Planning, previews, queries |
| `versions(streams, interval)`: every version, with `since` and `until` | Queries |
| `publish(epoch, changes, records)` in one transaction | Every writer |

- A run's input is one `range` query over all its streams and intervals, not
  one query per window.
- `changed` is answered from memory for recent epochs, and from the `since`
  and `until` columns for older ones.

### Backends

One storage interface has two implementations, each in its database's own
SQL. A single contract test suite runs against both.

| | DuckDB | PostgreSQL |
| --- | --- | --- |
| Tables | Native tables | Tables, or TimescaleDB hypertables where the extension is installed |
| Splitting and compression | DuckDB's columnar storage | TimescaleDB chunks and compression |
| Finding a key | Row-group statistics; no index | A primary key on stream, time and sample |
| Direct SQL from outside | Not while the engine runs | Any client |

The capabilities are the same on both. Size and speed differ; see
[Measurements](#measurements).

Where TimescaleDB is installed, the PostgreSQL backend sets the tables up for
it [[TimescaleDB]](#references):

- `readings` and `history` are hypertables split on `event_time`. `readings`
  keeps its primary key. The index on time alone that TimescaleDB adds by
  default is left out.
- TimescaleDB compresses the chunks that have closed, segmented by stream and
  ordered by time. Its own policy does this, and recompresses a chunk after
  corrections have touched it.
- A reading in a compressed chunk can still be replaced, and the primary key
  is still enforced there.
- The statement that moves replaced rows to `history` names one stream and
  one time range as constants. TimescaleDB then decompresses only the batches
  that hold those readings. Given a join, or a list of times with no range,
  it decompresses every batch of the chunk or of the stream.
- Publication lifts TimescaleDB's limit on rows decompressed in one
  transaction. The default is 100,000, and recomputing one derived stream
  over ninety days replaces 129,600 readings.

### SQL access

- The [query interface](#queries) is the supported way to read. The engine
  writes the SQL, so epochs and history are handled the same way for every
  caller and on both backends.
- `readings` and `history` stay ordinary, documented tables. `readings` has
  one row per stream, time and sample.
- On PostgreSQL any client can also query them directly while the engine
  runs.
- While the engine has a DuckDB file open, no other process can open it, even
  to read. Reads of a DuckDB store go through the query interface.
- Writes go through requests. A row written to the tables directly is
  invisible to the engine, and nothing is recomputed from it.

### Retention

Nothing is pruned by default. Acquirium's experiments read an observation as
of the epoch that recorded it, which needs `history`.

`history` gains a row each time a stored reading is replaced. That includes
every revision of a result whose window is still filling: an hourly mean fed
once a minute adds about sixty rows an hour. One such result per stream makes
`history` grow about as fast as the sources fill `readings`.

A store can set a retention period. Rows of `history` that ended before the
retention point H are then deleted. H is the compaction frontier of shared
arrangements [[McSherry et al. 2020]](#references). The consequences:

- Current values are untouched.
- A read as of an epoch before H is refused. It is never answered wrongly.
- The change feed starts at H.
- An instance whose maintenance record reflects an epoch before H is rebuilt
  in full when it next runs, because `changed` can no longer be answered for
  it.

### Recovery

Opening a store reads the published epoch from the database. Nothing is
replayed, and the working set fills as it is read. Requests still in the
inbox are the next epoch's input.

### Measurements

Measured on 2026-11-22 with synthetic data. Caches were warm. The scripts are
not in the repository. The first set used 1,000 streams at 1 Hz, on DuckDB
2.0.0 and a local PostgreSQL 14.22 without TimescaleDB.

| Layout | Bytes per reading | One stream, one hour | One stream, one day |
| --- | ---: | ---: | ---: |
| DuckDB table, rows in the order epochs wrote them | 8.5 | 2.2 ms | not measured |
| DuckDB table, rows sorted by stream and time | 6.2 | 1.0 ms | 1.9 ms |
| PostgreSQL table with a primary key | about 120 | 1.6 ms | 35 ms |
| One Parquet file per stream and day (not used) | 7.0 | 1.5 ms | 2.1 ms |

- **Publication.** An epoch of 2,000 rows, half of them revisions of existing
  keys, took a median of 11 ms on DuckDB and 12 ms on PostgreSQL.
- **Batched reads.** 100 streams for one day in one DuckDB query took 31 ms.
- **Growth.** A DuckDB read of one stream for a day took 1.9 ms from a table
  holding one day of all streams, and 10 to 12 ms from one holding thirty
  days (2.6 billion readings).
- **Expected size.** A deployment like Benicia, with 112 streams at one
  reading a minute, writes about 59 million readings a year: about 0.5 GB on
  DuckDB, 7 to 9 GB on PostgreSQL without compression, and about 0.9 GB on
  TimescaleDB with closed chunks compressed.

TimescaleDB 2.28.3 on PostgreSQL 18.4 was measured in a container, on data
shaped like Benicia: 112 streams at one reading a minute for 90 days, which
is 14.5 million readings, in seven-day chunks. All chunks but the newest were
compressed.

| | Uncompressed | Compressed |
| --- | ---: | ---: |
| Bytes per reading in `readings` | 149 | 15 |
| Bytes per version in `history`, with no index | 85 | 8.9 |
| One stream, one day | 1.4 ms | 1.3 ms |
| One stream, 90 days | 87 ms | 52 ms |
| Daily mean of one stream over 90 days | 90 ms | 7.5 ms |
| Mean of each stream over one day | 16 ms | 2.5 ms |
| Latest reading at or before a time | 0.3 ms | 1.0 ms |
| Replace one day of one stream (1,440 readings) | 14 ms | 19 ms |
| Replace 90 days of one stream (129,600 readings) | 1.2 s | 1.6 s |

- **Publication.** An epoch of 224 rows into the newest chunk, half of them
  revisions, took a median of 4 ms.
- **The primary key** held in compressed chunks: a duplicate key was
  rejected, and no key had two rows after the corrections.
- **Corrections need a range.** Replacing one day of one stream decompressed
  2 batches (2,000 readings) when the statement gave the stream and a time
  range. Given only the list of times, it decompressed all 132 batches of
  the stream.
- **The limit.** A statement that exceeds the decompression limit fails
  after it has decompressed, and the space stays allocated until a vacuum.
  Three such failures grew a 329 MiB table to 1.8 GiB.
- **Recompression.** Recompressing twelve chunks after corrections took 11
  to 13 s.
- **Not measured.** TimescaleDB's compression policy running beside
  publication, and real plant data. The read times were taken with
  TimescaleDB's default index on time also in place.

## Queries

Reading goes through one small interface that the engine provides. It serves
Acquirium and any other application built on the engine, so it speaks only of
streams, times and epochs. The engine turns each call into SQL over the
[tables](#tables), or answers it from the working set. A caller never chooses
between `readings` and `history`, and never handles epochs by hand.

Today the engine reads one stream at a time and returns raw readings only.
Acquirium's client makes one request per stream, pages through each, and
lines the streams up afterwards.

### Snapshots

Every read is made on a snapshot, and a snapshot is one epoch.

| Call | Snapshot |
| --- | --- |
| `snapshot()` | The latest published epoch |
| `snapshot(epoch)` | An earlier epoch, back to the [retention](#retention) point |
| `snapshot(after=receipt)` | The first epoch that includes that request. The call waits for it |

Every read on one snapshot sees the same epoch, across streams and across
calls.

### Reading

One call returns readings:

```text
read(streams, start, end, limit, newest_first)
```

It returns each reading of each stream in the range: stream, time, sample,
number or text, and quality.

- **Range.** Times are UTC nanoseconds. A range includes `start` and excludes
  `end`. Either bound may be left open.
- **Limit and order.** A limit counts per stream, and the order can be newest
  first. The newest reading of every stream is one call. So is the reading
  of every stream at or before a past time.
- **Results.** Results are Arrow record batches in long form, with a stream
  column, ordered by stream and then time. They arrive as a stream of
  batches.

The engine returns readings as they are stored. It does not sample,
interpolate or aggregate them. It does not transform values on the way in
([Decisions](#decisions)), and it does not on the way out.

### Shaping readings

Lining streams up, resampling and summarising are computation. Computation
has two homes: the caller's dataframe library for a one-off, and a template
for a result worth keeping.

| Need | How | Measured |
| --- | --- | --- |
| The newest value of every stream | `read` with a limit of one, newest first | 1.8 ms |
| The value of every stream at a past time | The same, with `end` at that time | 2.0 ms |
| Ten streams lined up at each minute of a day | `read`, then an as-of join by the caller | 2.1 ms to read, 2.8 ms to join |
| Hourly statistics of one stream over 90 days | `read`, then a grouped aggregate by the caller | 31 ms to read 129,600 readings, 5 ms to aggregate |
| Daily statistics of every stream over 90 days | The same | 370 ms to read 14.5 million readings (509 MiB), 300 ms to aggregate |

The caller's part was run in Polars, in the engine's process. The first two
rows looked back one day.

The last row is where reading everything is wasteful. A summary that is read
often, or over years, is a derived series: a template with an hourly bucket
whose ports are the count, minimum, mean and maximum. It is maintained like
any other result and read with `read`. Sampling and summarising inside the
engine are [deferred](#deferred).

### Reads about the store

| Call | Returns |
| --- | --- |
| `extent(start, end, streams)` | The streams that have a reading in the range, each with its count and its first and last time within the range. Either bound may be left open. With no streams named, every stream is considered |
| `describe(streams)` | For each stream: source or derived. For a derived series, whether it is current and, if not, the epoch it reflects and the cause ([What a reader sees](#what-a-reader-sees)). With no streams named, every stream |
| `changed_since(epoch, streams)` | The intervals of event time whose readings differ between that epoch and the snapshot's |
| `history(streams, start, end)` | Every version of the readings in the range, with `since` and `until` |

`extent` answers which streams have data between two times, and how much. A
stream with no reading in the range has no row. `describe` says what a stream
is and whether it is up to date, whether or not it holds readings.

`changed_since` returns the same [changed intervals](#changed-intervals) that
drive the dirty rule. A consumer that keeps its own copy re-reads only those
intervals. The [change feed](#versions-snapshots-and-the-change-feed) is
separate: it lists each epoch's changes row by row, for a consumer that
follows the store.

Reading as of a version, and asking which ranges changed between two
versions, follow BTrDB [[Andersen and Culler 2016]](#references). Here one
epoch covers every stream, where BTrDB versions each stream on its own
[[BTrDB API]](#references).

### Cost

The calls were run as SQL on DuckDB 2.0.0 over a table shaped like Benicia:
112 streams at one reading a minute for 90 days, in the order epochs wrote
them. They were not run on PostgreSQL.

| Call | Time |
| --- | ---: |
| `read`: one stream, one day | 2.5 ms |
| `read`: ten streams, one day | 2.5 ms |
| `read`: one stream, 90 days (129,600 readings) | 31 ms |
| `read`: the newest reading of every stream, looking back a day | 1.8 ms |
| `read`: the newest three readings of every stream, with no bound | 24 ms |
| `read` on an earlier epoch: one stream, one day | 1.6 ms |
| `extent`: every stream, one day | 2.8 ms |
| `extent`: every stream, with no bounds | 13 ms |
| `changed_since`: one stream | 9 ms |

A read on an earlier epoch returned the readings as they were before 1,440
corrections.

### Around the interface

Acquirium adds names from the graph, the replacement chain and units. On
PostgreSQL the tables can also be queried directly, with the database's own
aggregates. A SQL endpoint for a running DuckDB store is
[deferred](#deferred).

## Threads

| Stage | Threads | Owns |
| --- | --- | --- |
| Admission | Callers, sharing commits | Appends to the inbox |
| Epoch driver | One | The candidate, and the only writes to published state |
| Compute pool | A work-stealing pool | Building runs, reading storage, comparing results |
| Python pool | Worker processes | Function calls |
| Refresher | One, with its own workers | Asynchronous refreshes |

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
| `series` | The working set, snapshots, and the storage interface: the reads, `changed` and publication | `geometry` |
| `log` | The inbox, retry identities, epoch records, and the DuckDB and PostgreSQL implementations of storage | `series` |
| `plan` | Templates, instances, bindings, validation, the DAG, plan comparison | `geometry` |
| `repair` | The dirty rule, runs, result comparison, maintenance records, refresh | `series`, `plan`, `exec` |
| `exec` | The run protocol, the Python worker pool, the existing native functions | `plan` |
| `engine` | Stages, limits, status, the query interface and the rest of the public API | all of the above |
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
  registering an output can make another selection match it. It runs on a
  private copy of the inferred graph and publishes nothing until the plan is
  activated. Inference is a pure function of a snapshot of the graph, so the
  candidate output metadata can be substituted into that snapshot.
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
- **Reads and writes.** Both cross the boundary as Arrow record batches. A
  read route in Acquirium is one of the engine's [query calls](#queries).
  Acquirium resolves names and the replacement chain through the graph, calls
  the engine, and converts units on the way out.

## Invariants and tests

| Invariant | Check |
| --- | --- |
| The function law holds for every window | Compare with an independent full recomputation |
| The dirty rule is sound | Random changes; results outside the evaluated windows must already match the recomputation |
| Order does not matter | Shuffle request order and batching; compare final snapshots |
| Runs are invisible | Random run boundaries and task completion order; compare with serial evaluation |
| The feed integrates to the snapshot | Integrate the feed through each epoch and compare |
| Recovery is exact | Stop at random points around a commit; reopen and compare |
| The tables are the snapshot | After each epoch, read `readings` and `history` with SQL and compare with the engine's snapshot and feed |
| Queries answer from the snapshot | For random streams, ranges, limits and epochs, compare each query call with the same answer computed from the oracle's snapshot |
| Backends agree | Run the same histories and queries on DuckDB and PostgreSQL |
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
7. **Storage.** `readings` and `history` on both backends, written at
   publication; the working set; the query interface; PostgreSQL at parity.

## Decisions

Made on 2026-11-22.

- **The backends are DuckDB and PostgreSQL**, with TimescaleDB where it is
  installed. S3 may follow.
- **Readings are queryable with SQL** in the backend database. This is part
  of the reason for building on DuckDB and PostgreSQL.
- **Reads go through a query interface that the engine provides.** It is
  basic, and it is designed for Acquirium and other applications, not copied
  from another system. The engine, not the caller, then handles how the data
  model is queried.
- **Scale is the database's job.** How tables are split, compressed and
  indexed is left to the database. Deployments are expected to resemble
  Benicia: about a hundred streams at one reading a minute.
- **Synchronous work holds the applied frontier**, including a single very
  large change. Consistency comes first; moving such work to the background
  can be considered later. This document assumes the same for the first
  evaluation of a new or redefined instance.
- **DAGs of Python functions are the priority.** Native functions are kept as
  they are and are not extended.
- **Unit conversion stays in Python functions.** This follows from the
  priority above and was not decided separately. The engine does not
  transform values.
- **A refresh publishes with its synchronous readers or not at all.**
- **A failed instance is retried when its failing window's inputs change**,
  not on every new input.
- **Sequential templates may use any window**, including windows whose read
  intervals overlap.

## Open questions

None at present. [Measurements](#measurements) lists what was not measured.

## Deferred

- **Sampling and summarising in the engine.** Two shaped reads were designed
  and measured, and left out.
  - **`sample`**, the value of each stream at given times, is an as-of join
    [[DuckDB AsOf; pandas merge_asof]](#references). It saves nothing over
    `read` with a limit and a join by the caller.
  - **`summarize`**, statistics for each period of a fixed width, is what
    `time_bucket` gives [[TimescaleDB]](#references) and what BTrDB's
    statistical windows give [[BTrDB API]](#references). It pays off only
    for a coarse view of many streams over a long span. Daily statistics of
    every stream over 90 days took 60 ms in DuckDB, against 670 ms and
    509 MiB when the caller reads everything. It would also put a choice of
    statistics in the engine, with questions of time weighting and quality
    codes that the engine otherwise leaves to functions.
- **Direct SQL to a running DuckDB store.** Reads go through the
  [query interface](#queries), so this is not needed first. Two ways were
  examined.
  - **A route on the Acquirium server**, beside `/sparql`. It takes SQL
    text, refuses anything that is not a `SELECT`, and returns a record
    batch. Only Acquirium's client and plain HTTP can use it.
  - **DuckDB's Quack server** [[DuckDB Quack]](#references), started inside
    the engine's process. Any DuckDB client attaches to it and queries the
    live tables as if they were local. It is in beta, and only DuckDB
    clients speak it. By default a client can also write: a test client
    dropped `readings`. An authorization rule that admits only a parsed
    `SELECT` and transaction boundaries refused every write tried.

  Tested with DuckDB 2.0.0's Python package, which is the version Escapement
  bundles: a Quack client saw only whole epochs. Publishing 100 rows took
  6.0 ms while a client queried, against 5.7 ms with no server. Neither way
  serves tools that connect over the PostgreSQL protocol, such as Grafana.
  Those need the PostgreSQL backend.

- **Chaining stages in one worker**, so that a chain of functions over one
  group does not return to the engine between stages.
- **Shared partial results for overlapping windows**, as in panes and stream
  slicing [[Li et al. 2005b; Traub et al. 2019]](#references). Here it would
  be an upstream template, not a new mechanism.
- **Computing old windows only when read**, as in Noria's partial state
  [[Gjengset et al. 2018]](#references).
- **Background evaluation of oversize synchronous work**, and independent
  commits for unrelated parts of the DAG.
- **Batching table writes.** Several epochs' changes written in one
  transaction, if per-epoch writes come to limit throughput. SQL readers
  would then trail the engine by the batch.
- **Ordering and splitting large DuckDB tables.** Past billions of readings,
  re-sorting closed time ranges by stream and splitting tables by time keep
  reads flat.
- **S3.** Old readings as Parquet objects, attached to the database so that
  SQL still reaches them.

## References

What each source contributed is given with it. Two attributions were checked
against summaries and not the full text: the details of the window-ID
approach, and SQL:2011's system-versioned tables. BTrDB's calls were checked
against the documentation of its Python and Go bindings and its gRPC
definition, not against the paper.

- **Akidau et al. 2015.** T. Akidau and others. "The Dataflow Model: A
  Practical Approach to Balancing Correctness, Latency, and Cost in
  Massive-Scale, Unbounded, Out-of-Order Data Processing." PVLDB 8(12).
  Event time apart from processing time; windows as an assignment of readings;
  results revised by retraction when late data arrives.
- **Andersen and Culler 2016.** M. P. Andersen and D. E. Culler. "BTrDB:
  Optimizing Storage System Design for Timeseries Processing." FAST 2016.
  <https://www.usenix.org/conference/fast16/technical-sessions/presentation/andersen>
  Every version of a series kept; the ranges that changed between two
  versions as a storage operation; statistics over windows as a read.
- **Andersen et al. 2015.** M. P. Andersen, S. Kumar, C. Brooks, A. von Meier
  and D. E. Culler. "DISTIL: Design and Implementation of a Scalable
  Synchrophasor Data Processing System." IEEE SmartGridComm 2015.
  <https://www.escholarship.org/uc/item/61w8z66w>
  Derived streams whose results hold when data changes or arrives out of
  order.
- **BTrDB API.** The documentation of BTrDB's Python bindings.
  <https://btrdb.readthedocs.io/en/latest/working/stream-view-data.html>
  Reads at a version of a stream, and statistics over windows of a stream.
- **Budiu et al. 2023.** M. Budiu, T. Chajed, F. McSherry, L. Ryzhyk and
  V. Tannen. "DBSP: Automatic Incremental View Maintenance for Rich Query
  Languages." PVLDB 16(7). <https://arxiv.org/abs/2203.16684>
  The specification: snapshots and changes as integral and derivative, and
  the feed property that follows.
- **DuckDB AsOf.** "AsOf Join." DuckDB documentation.
  <https://duckdb.org/docs/stable/guides/sql_features/asof_join>
  The value of a series as of a time, written as a join.
- **DuckDB Quack.** "Quack: The DuckDB Client-Server Protocol." DuckDB, 2026.
  <https://duckdb.org/2026/05/12/quack-remote-protocol>
  A server inside a DuckDB process that other DuckDB processes attach to. A
  candidate for direct SQL to a DuckDB store, deferred.
- **Gjengset et al. 2018.** J. Gjengset and others. "Noria: Dynamic,
  Partially-Stateful Data-Flow for High-Performance Web Applications."
  OSDI 2018. Partial state, deferred.
- **Jensen and Snodgrass 1999.** C. S. Jensen and R. T. Snodgrass. "Temporal
  Data Management." IEEE TKDE 11(1), 36-44. Valid time and transaction time
  as separate dimensions.
- **Kulkarni and Michels 2012.** K. Kulkarni and J.-E. Michels. "Temporal
  Features in SQL:2011." SIGMOD Record 41(3), 34-43.
  <https://sigmodrecord.org/2012/09/30/temporal-features-in-sql2011/>
  A current table beside a history table, each version carrying the period
  it held.
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
- **pandas merge_asof.** `pandas.merge_asof`.
  <https://pandas.pydata.org/docs/reference/api/pandas.merge_asof.html>
  A join on the nearest earlier key in place of an equal key, within a
  tolerance.
- **Pelkonen et al. 2015.** T. Pelkonen and others. "Gorilla: A Fast,
  Scalable, In-Memory Time Series Database." PVLDB 8(12). Recent readings
  served from memory.
- **Spark.** Apache Spark's pandas function API, `applyInPandas`.
  <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html>
  Arrow batches between the engine and Python processes, with the function
  called once per group.
- **TimescaleDB.** Hypertables and compression.
  <https://docs.timescale.com/use-timescale/latest/compression/about-compression>
  A table split by time and compressed in place, inside PostgreSQL.
  `time_bucket`: <https://www.tigerdata.com/docs/api/latest/hyperfunctions/time_bucket>
  Readings grouped into periods of a fixed width from an origin.
- **Traub et al. 2019.** J. Traub and others. "Efficient Window Aggregation
  with General Stream Slicing." EDBT 2019. Slices shared across overlapping
  windows under out-of-order data, deferred.
- **Welsh et al. 2001.** M. Welsh, D. Culler and E. Brewer. "SEDA: An
  Architecture for Well-Conditioned, Scalable Internet Services." SOSP 2001.
  Stages joined by bounded queues.
- **Welsh and Culler 2003.** M. Welsh and D. Culler. "Adaptive Overload
  Control for Busy Internet Servers." USITS 2003. Admission control at the
  stage where requests compete.
