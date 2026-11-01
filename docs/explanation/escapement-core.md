---
title: Escapement core
---

Escapement is Acquirium's only time-series store and execution engine. The
server owns one Escapement engine and its background processor. The store lives
at `[server] engine_path`: `escapement.duckdb` under the data directory by
default, or a `postgres://` URL, with tables in the URL's `search_path` schema.

The engine keeps a durable inbox of accepted writes and plan activations. Each
epoch applies the next run of accepted requests, evaluates the affected app
groups in dependency order, and commits source values, derived values, version
history and progress together. While an epoch runs, readers see the last
published one.

Acquirium owns everything semantic: the graph and its ontology inference,
stream registrations, app compilation, unit resolution, and the HTTP and Python
interfaces. Stream registrations live only in RDF. The server rebuilds its
reference lookup from the graph whenever the graph changes; there is no SQL
stream registry. Experiment records are kept in a separate SQL ledger; see
[Experiments](experiments.md).

## Writes

A write request carries upserts or deletes for one source under a
`request_id`, at most 10,000 rows. Larger imports are split into separate
requests, each accepted on its own. Resending the same request ID with the same
payload returns the original receipt within Escapement's retry window; a
different payload under that ID is a conflict.

A receipt means the write is durable, not yet visible. The server waits up to
`wait` seconds for it to publish; `wait=0` returns at acceptance, and a timeout
leaves the write pending, not rejected. A full inbox or an unavailable engine
answers 503 with `Retry-After` before anything commits. The client resends for
up to `retry_for` seconds (60 by default; drivers allow 5 minutes).
Drivers insert with `wait=0`, so they move at the pace of acceptance, not of
publication; see [Drivers and the engine](drivers.md#drivers-and-the-engine).

Each reading is a finite number or text. A stream's `value_kind`, in the graph,
decides how a written value is stored; see [Values](values.md).

## Reads

Times are UTC nanoseconds and ranges are half-open. A read uses one snapshot or
an explicit epoch throughout its pages, and a `DataObject` reads all its streams
at one epoch. `after=<sequence>` waits for that receipt to publish first, so a
writer can read its own data. Stream counts and time bounds come from the
engine's statistics without reading rows.

## Apps

A deploy compiles every app declaration into one complete plan. Output metadata
is registered and selections are re-resolved until they stop changing; a
declaration that never stabilizes fails. Escapement validates the plan and
rejects cycles and outputs with two owners. `dry_run=True` asks Escapement what
the plan would re-evaluate, without activating it.

The deploy intent is saved in `apps.json` under the data directory, replaced
atomically. The engine records the accepted and active plans, and a restart
reconciles the two. A plan ID names its content; each activation has its own
retry identity, so returning to an earlier plan activates it again.

A function that fails publishes nothing from its epoch and pauses the processor,
for every app. Fix the cause, then call `retry_engine()`.

Shutdown waits up to `drain_timeout` seconds for pending writes to publish.
Escapement cannot interrupt a running function, so one still running at the
deadline is left to the process's exit. Writes that were accepted but not
published stay in the inbox and are processed at the next start, as they are
after a failed function or a crash.

Graph writes and epochs are separate transactions. App inspection reports the
graph revision a plan was compiled against; nothing promises an atomic graph
and data change.

## Change feed

`GET /engine/changes` pages Escapement's signed change history with an
exclusive cursor, the published epoch it was cut at, and the last complete
epoch. A consumer commits its sink updates together with its cursor, buffering
any epoch split across pages. The
[streaming workload](../../examples/streaming/README.md) shows durable delivery,
asynchronous publication, complete-epoch consumption and restart.

## Limits

Escapement keeps current values in memory and never prunes history; there is no
retention policy yet. Retention, larger histories and stateful operators belong
in Escapement, not in a second Acquirium store or scheduler.
