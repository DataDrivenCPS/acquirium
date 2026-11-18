---
title: Materialization internals
---

In this article we'll describe how the server runs apps internally: the tables it keeps, how one scheduling round works, and what differs between the storage backends.
The code calls this part of the server materialization, and it is in `src/acquirium/Materialization/`.
This page is for readers who work on the server.
[How apps work](apps.md) has the same topic without the internals.

## Terms

- **Deployment**: the stored definition of one app: name, entrypoint (`module:Class`), digest of the module file, outputs, parameters and settings.
- **Binding**: one deployment together with the input streams and output streams of one call. A `per_match` app has one binding per match, an `all_matches` app has one.
- **Revision**: a counter in the timeseries store. Every write takes the next revision, and every row keeps the revision that last wrote it.
- **Consumed revision**: the revision up to which a binding has processed its inputs.

A binding has two identifiers.
The `signature` is a hash of the app name, the digest, the parameters, the inputs with their metadata, the outputs and the window settings.
The `progress_key` is a hash of the app name and the URIs of the input and output streams only.
The progress is stored under the `progress_key`, so a change of the code or of the parameters keeps the progress.

## Tables

All state is in the timeseries store, next to the data.

| Table | Content |
|---|---|
| `timeseries` | The values of all streams, the outputs of apps included. Each row has `last_revision` and a `deleted` flag |
| `system_state` | The current revision |
| `binding_progress` | `progress_key` and consumed revision |
| `stream_resets` | For a stream that was written with `replace=True`, the revision of the last replacement |
| `materialization_deployments` | One row per deployed app, as JSON |
| `materialization_lineage` | For each binding: the input streams, the output streams and a hash of the query match |
| `materialization_work` | One row per binding that has a long time range in progress: a cursor, the end of the range and the revisions |

The output streams are also written to the graph: the stream with the `source_id` `derived:<app name>`, its property with label, unit and the name of the app.
These triples are replaced as a whole each time the set of bindings changes.

The plan (the bindings, their order and the loaded app instances) is kept in memory.
It is built again from the deployments table when the server starts and when the graph changes.

## One scheduling round

The server runs one coordinator loop.
When a round did no work, the loop waits `materialization_poll_seconds` before the next one.

A round does the following:

1. It compares the version of the graph with the version of the plan. When they differ, each deployment is compiled again: the module is imported, the digest is checked and `build_query()` is run. A deployment that fails keeps its previous bindings and its error is shown by `list_apps()`.
2. It orders the bindings in layers. A binding that reads the output of another binding is in a later layer.
3. For each layer it reads once which streams were written or reset after the lowest consumed revision of the layer. A binding without a changed input and without a row in `materialization_work` only has its consumed revision moved forward.
4. The other bindings are run on a thread pool of `materialization_workers` threads. `batch_delay` and `min_interval` of the app can hold a binding back in this step.
5. The results of the layer are written in one transaction, and the next layer is read after that.

A binding that failed keeps its consumed revision and is tried again in the next round.
The bindings that read its outputs are skipped in the meantime.

## Reading and writing one batch

The worker thread opens its own connection and one read transaction.
In this transaction it reads the consumed revision, the current revision, the time range of the input rows with a revision between the two, and the input values.
The transaction is closed before `transform()` is called.

The time range of the changed rows is the changed window.
The output window and the read window are derived from it with `every`, `lookback` and `lookahead`, see [Time windows of a call](../reference/apps/app-class.md#time-windows-of-a-call).
The inputs hold all current values of the read window, and not only the changed rows.

The write transaction checks three things before it accepts a result:

- The binding is still part of the active plan. A new deployment or a removal makes the results of running calls invalid.
- The consumed revision is still the one the batch was read with.
- No input stream was reset after the batch was read.

For an accepted result the rows of each assigned output in the output window are marked as deleted, the new rows are written with a new revision, and the consumed revision is set to the revision the batch was read at.
Rows of the result outside the output window are dropped.
The deleted rows are kept as markers, so that an app that reads this output sees the removal as a change.

## Long time ranges

When the changed window is longer than one day, the binding gets a row in `materialization_work`.
The range is then processed in pieces of about one day, widened to whole buckets when `every` is set.
The cursor moves with each piece in the same transaction as the output.
The consumed revision moves when the last piece is done.
This is how a backfill continues after a restart.

`reprocess_app()` uses the same table.
Its row has the same start and end revision, so the consumed revision is not changed.
A binding can have one row only, and a second request is rejected until the first is done.

Apps with `lookback = "all"` are not split.
Each call reads everything that is stored for the inputs.

## Replaced streams

`insert_timeseries(..., replace=True)` deletes the rows of the stream and writes its revision to `stream_resets`.
A binding with such an input does one call over everything that is stored for its inputs.
Its outputs are deleted and written again, and they get a reset entry too, so the apps that read them do the same.

## Changes of the plant model

After a new compilation the bindings are compared with `materialization_lineage`.
A binding is calculated again over its whole stored time range in two cases:

- An output stream that exists already now has other inputs. This happens to a named output when the query matches other streams.
- The match of the binding changed while its streams stayed the same, for instance a label or a unit. For an `all_matches` binding any change of any match counts.

## Storage backends

The scheduling is the same for both backends.
`RevisionStore` in `revision_store.py` uses a few methods of the store for the connections and the writes.

| | DuckDB | PostgreSQL / TimescaleDB |
|---|---|---|
| Timestamps | `TIMESTAMP` in UTC | `TIMESTAMPTZ` |
| Read transaction | `conn.begin()` on a new connection | `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` on a new connection |
| Write of rows | delete the rows with the same stream and time, then insert | `COPY` into a temporary table, then insert with `ON CONFLICT` |
| SQL parameters | `?` | `%s` |

With TimescaleDB the outputs are in the same hypertable as all other values.
Its chunks are compressed by stream after seven days.

## Server settings

These keys of the `[server]` table of `acquirium.toml` control the loop:

| Key | Default | Meaning |
|---|---|---|
| `materialization_poll_seconds` | `0.25` | Wait between two rounds when there was no work |
| `materialization_workers` | `2` | Largest number of `transform()` calls at the same time |
| `materialization_error_log_seconds` | `30` | Shortest time between two log lines about failing apps |

## Modules

| Module | Content |
|---|---|
| `models.py` | `App`, `OutputSpec`, `StreamSet`, `InputBatch`, `Binding`, the ordering of bindings, `align` |
| `planner.py` | `Deployment`, and the compilation of deployments into bindings |
| `revision_store.py` | Reading of batches, the work table, the write transaction |
| `scheduler.py` | The thread pool and the errors per binding |
| `runtime.py` | `Materializer`: deploy, remove, reprocess, check, the scheduling round and the status |
| `worker.py` | Import of the app module and the digest check |
| `local.py`, `checks.py` | `acquirium app check --local` and the shape of a check result |
