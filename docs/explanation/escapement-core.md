---
title: Escapement core
---

Escapement Next is Acquirium's time-series store and execution engine. The
server owns one engine with background epoch and refresh workers. The default
store is `escapement-epoch.duckdb` under the data directory. Set `engine_path`
to use another file or a PostgreSQL URL.

To move an existing deployment, start with a separate data directory and
import the source readings and app declarations. Activating the apps
recomputes their derived readings. Keep the legacy store for any experiments
or consumers that still need its epoch history.

The engine keeps a durable inbox of accepted writes and plan activations. Each
epoch applies the next run of accepted requests, evaluates the affected app
groups in dependency order, and commits source values, derived values, version
history and progress together. While an epoch runs, readers see the last
published one.

Acquirium owns everything semantic: the graph and its ontology inference,
stream registrations, app compilation, unit resolution, and the HTTP and Python
interfaces. Stream registrations live in RDF. The server rebuilds its reference lookup
whenever the graph changes. Experiment records are kept in a separate SQL ledger; see
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

A deploy compiles every app declaration into one complete plan. Selections
are re-resolved against candidate output metadata in a private graph until
they stop changing. A declaration that never stabilizes fails.
Escapement validates the plan and
rejects cycles and outputs with two owners. `dry_run=True` asks Escapement what
the plan would re-evaluate, without activating it.

The active plan records the declarations and the output graph. Once the plan
publishes, Acquirium projects its output metadata into the graph. A restart
rebuilds that projection from the active plan. Each activation has its own
retry identity, so returning to an earlier plan activates it again.

A failed function retains its last successful outputs. Its downstream operators
are blocked, while source data and independent branches continue publishing.
Failure status is committed with the values and survives restart.
[How derived streams stay current](apps.md#published-snapshots-and-failure-recovery)
describes freshness and recovery; [app deployment and recovery](../how-to/check-deploy-apps.md#recovery-from-a-failed-calculation)
shows how to retry.

Shutdown waits up to `drain_timeout` seconds for pending writes to publish.
Escapement cannot interrupt a running function, so one still running at the
deadline is left to the process's exit. Writes that were accepted but not
published stay in the inbox and are processed at the next start after a crash
or a database failure.

Graph writes and epochs are separate transactions. App inspection reports the
graph revision a plan was compiled against; nothing promises an atomic graph
and data change.

## Change feed

`GET /engine/changes` derives signed changes from Escapement's stored versions, so a consumer
can follow every committed source and derived change and resume after a
restart. [Consume streaming changes](../how-to/consume-streaming-changes.md)
describes how to use it.

## Limits

Escapement stores current values and reading history in DuckDB or PostgreSQL.
Acquirium keeps history by default.
