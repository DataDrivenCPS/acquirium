# Escapement build plan: geometry and storage

Written 2026-11-22 for whoever implements it. It covers steps 1 and 2 of the
[implementation order](data-processing-model.md#implementation-order) in the
design: the window geometry and the storage layer. Later steps get their own
plans.

The signatures below state intent. None of this code has been compiled.
Change a signature where the compiler or a database requires it, and say what
you changed in your report.

## What you are building

A new engine is being built beside the current one. You build its first
three crates. Nothing in the current engine changes.

| Crate | Path | Package | Contents |
| --- | --- | --- | --- |
| `geometry` | `crates/geometry` | `escapement-geometry` | Intervals, the window shape, `reads`, `readers`, `owner`. Pure. |
| `series` | `crates/series` | `escapement-series` | The storage interface, the rules for versions, an in-memory reference store, and the contract tests. No I/O. |
| `store` | `crates/store` | `escapement-store` | The storage interface on DuckDB and on PostgreSQL, with TimescaleDB set up where it is installed. |

Work in that order. Finish each crate, with its tests passing, before
starting the next.

## Read first

The design is `docs/plans/data-processing-model.md` in the Acquirium
repository. Read these sections before writing code:

- One change, end to end
- Series and time
- Windows, all of it
- Evaluation: Changed intervals
- Storage, all of it
- Queries, to see which reads the storage layer must make possible
- Invariants and tests
- Decisions

Where the design and this plan disagree, the design decides what the system
means and this plan decides crate layout, names and tests. If they conflict,
or the design is silent on something that changes behaviour, stop and ask
the user. Do not settle it yourself.

## Where things are

- The Escapement repository is `/home/gabe/src/acquirium/escapement`. It is
  its own git repository inside the Acquirium checkout. The design is at
  `../docs/plans/` from there.
- The current engine is the root package `escapement-core`, in `src/`, with
  Python bindings in `bindings/python`. It is your reference, not your
  starting point.

Useful places in the current engine:

| File | What to learn from it |
| --- | --- |
| `src/store/duckdb.rs` | Transactions and bulk appends with the `duckdb` crate |
| `src/store/postgres.rs` | Binary `COPY` in and out, and the advisory lock that admits one owner |
| `tests/postgres.rs` | Starting a throwaway PostgreSQL server for tests, one schema per test |
| `tests/recovery.rs` | Stopping a process around a commit and checking what reopening sees |
| `tests/epochs.rs` | Comparing results with a full recomputation |

## Ground rules

- **Uncommitted work.** The repository is on `gtf-storage-0.2` with 66
  changed files not committed, including `Cargo.toml` and `Cargo.lock`. Do
  not modify, stash or discard them. Before your first change, ask the user
  how they want that work handled. The expected answer is that they commit
  it and you start a new branch from it.
- **Leave the current engine alone.** Do not edit `src/`, `bindings/`,
  `python/`, `tests/`, `examples/` or `benchmarks/`. Outside `crates/` you
  may edit only the root `Cargo.toml` (to add workspace members),
  `Cargo.lock`, `.github/workflows/ci.yml` and the `Makefile`.
- **Toolchain.** Rust edition 2024, `rust-version` 1.89, as the workspace
  sets them.
- **Dependencies.** Use the versions the root package pins, so the builds
  are shared: `duckdb = "=1.20000.2"` with `bundled` (this is DuckDB 2.0.0),
  `postgres = "0.19"` behind a `postgres` feature that is on by default,
  `thiserror = "2"`. Arrow crates must be `=60.1.0`, the version DuckDB
  uses, so record batches pass between them. You may add `proptest` and
  `tempfile` as dev-dependencies, and `postgresql_embedded` as the root
  package does. Ask before adding anything else.
- **Checks.** These must pass, as in CI:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  - the tests of the three crates
- **Safety.** `#![forbid(unsafe_code)]` in all three crates.
- **Workspace.** Add the three crates to `members` and `default-members`,
  with `publish = false`. Add a CI step that runs their tests.
- **Commits.** Commit only when the user asks. No pushes and no pull
  requests unless asked.
- **Honesty.** Report what you ran and what it showed. If something was not
  measured or not tested, say so.

## Milestone 1: `geometry`

The crate holds the window shape and nothing that needs data. Every later
crate relies on its laws.

### Types

```rust
/// UTC nanoseconds. i64::MIN and i64::MAX mean "unbounded" and are not
/// valid reading times.
pub type Time = i64;

/// [start, end). Empty when start >= end.
pub struct Interval { pub start: Time, pub end: Time }

/// Sorted, disjoint intervals that do not touch.
pub struct IntervalSet { /* ... */ }

pub enum Anchors { Instant, Grid { origin: Time, step: i64 } }
pub enum Body { Point, Bucket }
pub struct Window { pub anchors: Anchors, pub body: Body }

pub enum Reach {
    Span { before: i64, after: i64 },
    Latest { max_age: Option<i64> },
}
/// Whether a role reads bound streams or the template's own ports.
pub enum Source { Bound, OwnPorts }
pub struct Role { pub reach: Reach, pub driving: bool, pub source: Source }
```

`Interval` needs `point(t)`, which is `[t, t + 1)`, and `contains`,
`intersect` and `is_empty`. `IntervalSet` needs `insert`, `union`,
`intersect`, `subtract`, `contains`, iteration, and `coalesce(max)`, which
reduces the set to at most `max` intervals by merging across the smallest
gaps.

### Functions

| Function | Meaning |
| --- | --- |
| `Window::validate(&roles)` | Refuses a shape the design does not allow. Rules below. |
| `Window::body(t)` | `Point`: `[t, t + 1)`. `Bucket`: `[t, t + step)`. |
| `Window::reads(&role, t)` | The interval window `t` reads for the role. Table below. |
| `Window::readers(&role, changed)` | The interval of anchor times whose `reads` meets `changed`. The exact inverse of `reads`. |
| `Window::anchors_in(interval)` | On a grid, the anchors in the interval, by arithmetic. `None` for `Instant`, whose anchors are found from data by a later crate. |
| `Window::owner(s)` | The anchor whose body holds time `s`, if any. |
| `clip(reads, valid)` | A read interval cut to a binding's span. An intersection. |

What `reads` returns:

| Reach | Source | Reads |
| --- | --- | --- |
| `Span` | `Bound` | The body, with `before` in front and `after` behind: `[body.start - before, body.end + after)` |
| `Span` | `OwnPorts` | `[t - before, t)` |
| `Latest` | `Bound` | The interval searched for the latest reading: `[t - max_age, t + 1)`, from the far past when there is no `max_age` |
| `Latest` | `OwnPorts` | `[t - max_age, t)` |

For a `Latest` role the window uses at most one reading per stream from that
interval: the last. Which one that is depends on data, so it is not decided
here.

`validate` refuses:

- a grid whose `step` is not positive;
- a `Bucket` without a grid;
- a negative `before` or `after`, or a `max_age` that is not positive;
- a set of roles with no driving role;
- a driving role on the template's own ports;
- a `Span` on the template's own ports with an `after`, or with no `before`;
- a driving `Latest` role on a grid with no `max_age`.

All arithmetic saturates at the unbounded values. No input may panic or
overflow. `owner` on a grid uses floor division, so times before the origin
work.

### Laws to test

Use `proptest`. These are cheap, so run thousands of cases.

| Law | Statement |
| --- | --- |
| Sound | If `s` is in `reads(role, t)`, then `t` is in `readers(role, [s, s + 1))`. |
| Exact | If `t` is in `readers(role, [s, s + 1))`, then `s` is in `reads(role, t)`. |
| Additive | `readers` of an interval equals the union of `readers` of its points. Check by sampling. |
| Ownership | On a bucket grid every time has exactly one owner, the owner is an anchor, and the time lies in the owner's body. Bodies of different anchors do not overlap. On a point grid `owner(s)` exists exactly when `s` is an anchor. |
| Own output is earlier | For a role on the template's own ports, `reads(role, t)` ends at or before `t`. |
| Grid arithmetic | `anchors_in` agrees with listing every anchor by brute force. |
| Interval sets | Every operation keeps the set sorted, disjoint and not touching, and agrees with a plain set of integers on a small domain. |
| Bounds | No panic for times and widths near `i64::MIN` and `i64::MAX`. |

Also write one unit test for each row of the design's table of familiar
windows (Windows: The shape), checking the exact `Reads` and `Owns` it lists,
and one for the laboratory-sample example beneath it.

### Done when

- Every law and table test passes.
- The public items are documented.
- The crate has no dependencies outside the standard library, apart from
  dev-dependencies.

## Milestone 2: `series`

This crate says what storage means. It holds the rules for versions once, in
plain Rust, so that the database code in `store` stays simple.

### Data

```rust
pub use escapement_geometry::{Interval, IntervalSet, Time};

/// 0 is the empty store. The first publication is epoch 1.
pub type Epoch = u64;
pub struct StreamId(pub u32);
pub enum StreamKind { Source, Derived }

/// A finite number or text. The constructor refuses NaN and infinities.
pub enum Value { Number(f64), Text(String) }
```

Two values are equal when both are numbers and `==`, or both are text with
the same bytes.

Reads return Arrow record batches. Define each schema once, with a function
that checks a batch against it:

| Batch | Columns | Order |
| --- | --- | --- |
| Readings | `stream` UInt32, `time` Int64, `value` Float64 or null, `text` Utf8 or null | stream, time |
| Versions | The same, then `since` UInt64, `until` UInt64 or null | stream, time, since |
| Latest | `request` UInt32, then the readings columns | request |
| Extent | `stream` UInt32, `count` UInt64, `first` Int64, `last` Int64 | stream |

In every batch a key appears once, and exactly one of `value` and `text` is
set.

### The rules for versions

These are the design's rules (Series and time; Storage: Tables), stated so
they can be tested:

- A key is a stream and a time. A stream holds at most one reading at an
  instant.
- A version is a value a key has held, with the epoch that wrote it
  (`since`) and, once it is replaced or deleted, the epoch that ended it
  (`until`).
- The value of a key as of epoch `E` is its version with `since <= E` and
  no `until` at or before `E`.
- Writing the value a key already holds changes nothing and makes no new
  version.
- A delete covers an interval of time in one stream. A single key is the
  interval `[t, t + 1)`.

### Changes: the epoch being built

An epoch is built in memory before it is published. `Changes` is that
epoch's difference from the published one.

```rust
pub enum Op {
    Put { stream: StreamId, time: Time, value: Value },
    Delete { stream: StreamId, interval: Interval },
}

/// For each key the epoch alters: the value published before it, and the
/// value it will have. Holds only keys where the two differ.
pub struct Changes {
    /* key -> (before: Option<Value>, after: Option<Value>) */
}
```

- **Applying operations.** Operations apply in order, and a later one wins.
  To apply a batch of operations, `Changes` needs the published readings at
  the keys and intervals the batch touches. So the flow is: ask the batch
  which keys and intervals it touches, read those from the store at the
  published epoch, then apply.
- **Net effect only.** If a key ends the epoch with the value it had when
  the epoch began, it leaves `Changes`. A key written twice in one epoch
  yields one new version.
- **Ended and begun.** `Changes` gives two lists for publication: the keys
  whose published version ends, and the new versions that begin.
- **Changed intervals.** `Changes::changed(stream)` gives the interval set
  of keys it alters. This is how `changed` is answered for the epoch being
  built.

A plain ordered map is enough for now. Columnar storage for `Changes` is a
later optimisation, if profiling asks for it.

### The storage interface

```rust
pub trait Store: Send + Sync {
    fn published(&self) -> Result<Epoch>;
    /// Reads before this epoch are refused.
    fn retention(&self) -> Result<Epoch>;

    fn stream_ids(&self, names: &[&str]) -> Result<Vec<Option<StreamId>>>;
    fn stream_info(&self, ids: &[StreamId]) -> Result<Vec<StreamInfo>>;

    fn range(&self, epoch: Epoch, requests: &[(StreamId, Interval)])
        -> Result<RecordBatch>;
    fn latest(&self, epoch: Epoch, requests: &[LatestRequest])
        -> Result<RecordBatch>;
    fn extent(&self, epoch: Epoch, streams: Option<&[StreamId]>,
        interval: Interval) -> Result<RecordBatch>;
    fn changed(&self, streams: &[StreamId], after: Epoch, through: Epoch)
        -> Result<Vec<IntervalSet>>;
    fn versions(&self, through: Epoch, streams: Option<&[StreamId]>,
        interval: Interval, after: Option<Epoch>) -> Result<RecordBatch>;

    fn publish(&self, epoch: Epoch, streams: &[NewStream],
        changes: &Changes) -> Result<()>;
    fn prune(&self, before: Epoch) -> Result<()>;
}

pub struct LatestRequest {
    pub stream: StreamId,
    pub at: Time,
    pub max_age: Option<i64>,
    /// Strictly before `at`. Used for a role on the template's own ports.
    pub strict: bool,
}
```

What each call means:

| Call | Meaning |
| --- | --- |
| `range` | The readings of each stream in its intervals, as of the epoch. A reading that several requests cover appears once. |
| `latest` | For each request, the last reading at or before `at` (before it, when `strict`), no older than `max_age`. A request with no answer has no row. |
| `extent` | For each stream with a reading in the interval as of the epoch: the count, and the first and last time. With no streams named, every stream. |
| `changed` | For each stream, a set of intervals that contains every key whose value differs between the two epochs. See below. |
| `versions` | Every version the snapshot at `through` can see, in the interval. A version that ends after `through` is reported with no `until`. With `after`, only versions that began or ended after that epoch. |
| `publish` | Publishes `epoch`, which must be `published() + 1`. It ends the published versions `changes` lists and begins its new ones, in one transaction. An epoch with no changes still publishes. |
| `prune` | Deletes versions that ended at or before the epoch, and makes it the retention point. Current values are untouched. |

`changed` may return more than what differs, never less. Two requirements
bound it:

- It contains every key whose value differs between the epochs.
- When at most 1,024 keys of a stream had a version begin or end between the
  epochs, it returns exactly those keys, as points. Beyond that it returns
  at most 1,024 intervals, formed by `IntervalSet::coalesce`.

Errors are one `thiserror` enum. It needs at least: the epoch is out of
order, the read is before the retention point, the epoch is not yet
published, a value is not finite, and a backend error.

### Reading the epoch being built

A window evaluated inside an epoch must see that epoch's changes laid over
the published snapshot. Write this once, here, over any `Store`:

```rust
pub struct View<'a> {
    pub epoch: Epoch,
    pub changes: Option<&'a Changes>,
}
pub fn range(store: &dyn Store, view: &View, ...) -> Result<RecordBatch>;
pub fn latest(store: &dyn Store, view: &View, ...) -> Result<RecordBatch>;
pub fn extent(store: &dyn Store, view: &View, ...) -> Result<RecordBatch>;
```

The backends never see `Changes` on a read. `latest` is the hard one: when
the stored answer is a key the epoch has ended, the next older reading is
needed. Choose your own method. The law below is what it must satisfy.

### The reference store

`MemoryStore` implements `Store` over an ordered map of keys to their
versions. Write it to be obviously right, not fast. It is the oracle for the
contract tests, and the dirty rule will later be tested against it.

### The contract tests

Every implementation of `Store` must pass one suite. Put it in this crate
behind a `contract` feature, as a macro or function that takes a way to open
a fresh store and, where the store is durable, to reopen it. Each property
should be its own test, so a failure names the property.

Generate random histories with `proptest`: several streams; puts of numbers
and text; rewrites of the same value; late writes far in the past; single
deletes and range deletes; keys written more than once in an epoch; empty
epochs.

| Property | Check |
| --- | --- |
| Agrees with the reference | Apply the same history to the store and to `MemoryStore`. At every epoch, and at random earlier epochs, every read with random arguments returns the same batch. |
| Equal writes make no version | Rewriting a held value leaves `versions` unchanged. |
| `changed` is sound and tight | It contains every key that differs. Up to 1,024 touched keys it is exactly the touched keys. |
| The feed rebuilds the store | A copy that applies `versions(after = cursor)` at random intervals ends with the same versions as the store, and gives the same readings as of random epochs. |
| A view equals the next epoch | For any changes, every read through `View { epoch, changes }` equals the same read at `epoch + 1` after those changes are published. |
| Publication is atomic | A publication made to fail partway leaves `published()` and every read unchanged. |
| Epochs are in order | Publishing any epoch but `published() + 1` is refused and changes nothing. |
| Retention | After `prune(H)`, reads at or after `H` are unchanged, reads before `H` are refused, and current values are untouched. |
| Reopening | After closing and reopening a durable store, every read is unchanged. |
| Readers see whole epochs | Threads reading at fixed epochs while epochs publish always get the reference's answer for their epoch. |
| Batches are well formed | Every batch matches its schema and order, with no repeated key. |

`Changes` also needs its own tests, with no store: applying operations in
batches or one by one gives the same result; a key returned to its published
value leaves `Changes`; and `Changes` never holds a key whose value before
and after are equal.

### Done when

- `MemoryStore` passes the contract suite.
- The `Changes` tests pass.
- The crate depends only on `geometry`, the Arrow crates and `thiserror`,
  apart from dev-dependencies.

## Milestone 3: `store`

Two implementations of `Store`: DuckDB and PostgreSQL. All SQL in the new
engine lives in this crate. Later steps add tables here for the plan, the
inbox and the maintenance records, and they join the same publication
transaction. Leave room for that: keep publication as one function that owns
the transaction.

```rust
/// A `postgres://` URL opens PostgreSQL. Anything else is a DuckDB file.
pub fn open(target: &str, options: &Options) -> Result<Box<dyn Store>>;
```

### Tables

The design fixes `readings`, `history` and `streams` (Storage: Tables). On
both backends:

```sql
CREATE TABLE streams  (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL UNIQUE,
                       kind VARCHAR NOT NULL);
CREATE TABLE readings (stream INTEGER NOT NULL, event_time BIGINT NOT NULL,
                       value DOUBLE, text VARCHAR, since BIGINT NOT NULL);
CREATE TABLE history  (stream INTEGER NOT NULL, event_time BIGINT NOT NULL,
                       value DOUBLE, text VARCHAR, since BIGINT NOT NULL,
                       until BIGINT NOT NULL);
```

Add a small table for the store's own facts: its layout version, the
published epoch and the retention point. Opening a store with another layout
version is refused.

| | DuckDB | PostgreSQL |
| --- | --- | --- |
| Key on `readings` | None. No index of any kind. | `PRIMARY KEY (stream, event_time)` |
| Key on `history` | None | An index on `(stream, event_time)`. Not in the design; see Open points. |
| One owner at a time | The file lock DuckDB already takes | An advisory lock, as the current engine does |
| Namespace | The file | A schema, named in the options, so tests can each use their own |

Where TimescaleDB is installed (`pg_extension` has `timescaledb`), set the
tables up as the design says (Storage: Backends). These statements were run
on TimescaleDB 2.28.3 with PostgreSQL 18.4:

```sql
SELECT create_hypertable('readings', 'event_time',
       chunk_time_interval => <seven days in ns>::bigint,
       create_default_indexes => false);
ALTER TABLE readings SET (timescaledb.compress,
       timescaledb.compress_segmentby = 'stream',
       timescaledb.compress_orderby = 'event_time');
-- history the same, ordered by 'event_time, since'
```

The chunk width and the age at which chunks are compressed are options, with
seven days as the default for both.

### Publication

One transaction, on both backends:

1. Check that the epoch is `published + 1`, and take the published epoch's
   row so that two publishers cannot interleave.
2. Add the new streams.
3. Load the ended keys and the begun versions into temporary tables, in
   bulk.
4. Move each ended key's row from `readings` to `history`, with `until` set
   to the epoch.
5. Check that the number of rows moved equals the number of ended keys. If
   not, the changes were worked out against another snapshot: abort.
6. Insert the begun versions into `readings`, with `since` set to the epoch.
7. Record the epoch and commit.

On PostgreSQL steps 4 to 6 can be:

```sql
WITH old AS (
  DELETE FROM readings r USING ended e
  WHERE r.stream = e.stream AND r.event_time = e.event_time
  RETURNING r.*)
INSERT INTO history
  SELECT stream, event_time, value, text, since, $epoch FROM old;
INSERT INTO readings
  SELECT stream, event_time, value, text, $epoch FROM began;
```

TimescaleDB needs step 4 written differently. See Known traps.

### Reads

Every read is as of an epoch. This form was measured, and is always correct
even while a later epoch is publishing:

```sql
SELECT stream, event_time, value, text FROM readings
 WHERE <streams and times> AND since <= $epoch
UNION ALL
SELECT stream, event_time, value, text FROM history
 WHERE <streams and times> AND since <= $epoch AND until > $epoch
```

Do not drop the `history` half for "the latest epoch". Another epoch can
publish between choosing the epoch and running the query, and its replaced
rows are then only in `history`.

`versions` with `after`, which is the change feed:

```sql
SELECT stream, event_time, value, text, since, NULL AS until FROM readings
 WHERE since > $after AND since <= $through
UNION ALL
SELECT stream, event_time, value, text, since,
       CASE WHEN until <= $through THEN until END
  FROM history
 WHERE since <= $through
   AND (since > $after OR (until > $after AND until <= $through))
```

For `latest`, the prototypes used `max_by` and window functions on DuckDB,
and `ORDER BY event_time DESC LIMIT 1` per stream on PostgreSQL. A `range`
over many streams and intervals should be one query, not one per request.

### Concurrency

One thread publishes. Any number of threads read, each on its own
connection, and none of them waits for a publication. A reader sees an epoch
entirely or not at all.

### Tests

- **The contract suite**, three times: a DuckDB file, PostgreSQL, and
  TimescaleDB.
- **The tables are the snapshot.** After each epoch of a random history,
  read `readings` and `history` with plain SQL on a separate connection and
  compare with the reference. This is the promise that the tables can be
  queried directly.
- **Stopping around a commit.** Kill the process before and after the
  commit, reopen, and check that the store shows the previous epoch entirely
  or the new one entirely. `tests/recovery.rs` shows the technique.
- **TimescaleDB with compressed chunks.** Compress every chunk but the
  newest, then run the contract suite and these cases: a correction of one
  key, of a scattered handful, of a whole day of one stream, and of ninety
  days of one stream; a range delete; a duplicate key refused.

Test servers:

| Backend | How |
| --- | --- |
| DuckDB | A file in a temporary directory |
| PostgreSQL | `ESCAPEMENT_TEST_POSTGRES` names a database; otherwise start one with `postgresql_embedded`, as `tests/postgres.rs` does |
| TimescaleDB | `ESCAPEMENT_TEST_TIMESCALE` names a database. Without it these tests print that they were skipped. Add a CI job with a TimescaleDB service so they run there. |

On this machine `docker` is podman, and this started a usable TimescaleDB:

```sh
docker run -d --name ts-test -e POSTGRES_PASSWORD=bench \
  -p 127.0.0.1:55432:5432 docker.io/timescale/timescaledb-ha:pg18.4-ts2.28.3
# postgresql://postgres:bench@127.0.0.1:55432/postgres
```

### Measurements

Write a benchmark program in the crate that builds the fixture the design
used and prints the figures below for each backend. The fixture is shaped
like the Benicia plant: 112 streams, one reading a minute, 90 days, which is
14.5 million readings.

The design's figures came from SQL run from Python scripts, not from an
engine. Treat them as what to expect, not as limits. Report yours beside
them, and look into any that is more than three times worse.

| Operation | DuckDB | TimescaleDB, compressed | PostgreSQL |
| --- | ---: | ---: | ---: |
| `range`: one stream, one day | 2.5 ms | 1.3 ms | 0.9 ms |
| `range`: ten streams, one day | 2.5 ms | | |
| `range`: one stream, 90 days | 31 ms | 52 ms | |
| `range` at an earlier epoch: one stream, one day | 1.6 ms | | |
| `latest`: one stream | | 1.0 ms | |
| `latest`: every stream, looking back a day | 1.8 ms | | |
| `extent`: every stream, one day | 2.8 ms | | |
| `changed`: one stream | 9 ms | | |
| `versions` after an epoch: the last 60 epochs | 2.2 ms | 60 ms | 92 ms |
| Publish 224 rows, half of them revisions | | 4 ms | |
| Replace one day of one stream | | 19 ms | |
| Replace 90 days of one stream | | 1.6 s | |
| Bytes per reading | about 8.5 | 14 | 92 |

Blank cells were not measured. Fill them in.

Also measure the one thing the design lists as not measured: TimescaleDB's
compression policy running while epochs publish. Report whether publication
slows or fails while a chunk is being compressed.

### Known traps

These were found while prototyping. Each cost time.

- **TimescaleDB decompresses far too much unless the statement is shaped
  right.** The statement that removes replaced rows from a compressed chunk
  must name one stream and one time range as constants. Given a join, or a
  list of times with no range, it decompresses every batch of the chunk or
  of the stream. Replacing one day of one stream decompressed 2 batches
  with a constant stream and range, and all 132 batches of the stream with
  only a list of times.
- **So step 4 of publication on TimescaleDB is one statement per stream and
  time range**, with the stream and the bounds written as constants. Group
  each stream's ended keys into runs that are close in time. A range that
  spans months to reach a few scattered keys is still slow: 200 scattered
  keys took 9.6 s in one wide range, against about 3.6 ms per key as
  separate statements.
- **TimescaleDB limits rows decompressed per transaction.** The default is
  100,000, and replacing ninety days of one stream needs 129,600. Run
  `SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0` in
  the publication transaction.
- **A statement that exceeds that limit fails after decompressing**, and the
  space stays allocated until a vacuum. Three such failures grew a 329 MiB
  table to 1.8 GiB. Do not rely on catching the error.
- **Leave out TimescaleDB's default index on time.** It adds about 30 bytes
  a reading and nothing here uses it. Hence `create_default_indexes =>
  false`.
- **A compression policy on a hypertable with an integer time column needs
  an "integer now" function.** The prototypes compressed chunks by hand and
  never ran a policy. Check the TimescaleDB documentation for the installed
  version before writing this.
- **A running DuckDB file cannot be opened by another process**, not even
  to read. Tests that inspect a DuckDB store with plain SQL must do it from
  the same process.
- **Do not give `readings` a key or index on DuckDB.** The design relies on
  DuckDB's row-group statistics. DuckDB has also been known to reject a
  delete and insert of the same key inside one transaction when a unique
  index exists, which is exactly what publication does. Check whether that
  still holds in 2.0 before relying on either behaviour.
- **In PostgreSQL, the feed with no index scans the whole table.** That was
  92 ms on the fixture and grows with the store. A BRIN index on
  `readings (since)` took 40 KiB and brought it to 5 ms, measured on a table
  whose rows lay in the order epochs wrote them. The design treats it as
  optional.

### Done when

- The contract suite passes on DuckDB, PostgreSQL and TimescaleDB.
- The other tests in this section pass.
- The benchmark runs on all three and its table is in your report.
- CI runs the three crates' tests, with PostgreSQL and TimescaleDB.

## Open points

Decide these by measurement, and say what you found:

- **An index on `history` in plain PostgreSQL.** A read as of an earlier
  epoch looks up `history` by stream and time. The design's table of
  backends lists no index there. This plan adds one on
  `(stream, event_time)`. Measure a read at an earlier epoch with and
  without it. On TimescaleDB, compression orders `history` by stream and
  time already, so measure before adding one.
- **A BRIN index on `readings (since)` in PostgreSQL**, for the feed.
  Optional in the design. Add it if the feed is slow without it and the
  index stays useful after corrections have reused space in old pages.

Stop and ask the user about anything else that changes what the system
means, including:

- any rule about versions that this plan and the design do not settle;
- adding a dependency not listed above;
- changing a table's columns.

## What to report

At the end of each milestone, write a short report:

- what was built, and which tests pass, with the command that ran them;
- the benchmark table, for Milestone 3;
- every place you departed from this plan, and why;
- what you verified and what you only inferred;
- anything you could not test, and what it would take.

## Not in this plan

Plans, the run protocol, Python workers, the dirty rule, maintenance
records, the inbox and admission, the epoch driver, the query interface, the
Python bindings, and anything in Acquirium. Do not start them.
