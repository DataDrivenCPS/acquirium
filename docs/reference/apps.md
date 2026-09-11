# App reference

Apps maintain derived streams from the latest available inputs. When a reading
is corrected, the app recalculates the affected results, which may change or
be removed. A calculation must produce the same final results whether its
inputs arrive in one batch or several.

## App

Subclass `acquirium.App` and implement:

```python
def build_query(self, plant):
    ...  # return an Acquirium Query

def transform(self, inputs, output, context):
    ...  # assign declared output ports
```

Constructors accept the `parameters` supplied at deployment. Keep instances
stateless because bindings may execute concurrently, and keep external side
effects out of `transform` because the runtime may retry a calculation.

| Attribute | Default | Meaning |
|---|---|---|
| `name` | class name | Deployment and derived source identity |
| `grouping` | required | `"per_match"` for one call per match, or `"all_matches"` for one aggregate |
| `outputs` | required | Mapping of port names to stream declarations |
| `every` | none | Complete fixed UTC buckets, e.g. `"1m"` |
| `lookback` | `"0s"` | Preceding input dependency, or `"all"` |
| `lookahead` | `"0s"` | Following input dependency |
| `backfill` | `False` | Process retained history on first activation |
| `batch_delay` | `"0s"` | Advanced: wait after the first pending revision to batch rapid changes into one invocation |
| `min_interval` | none | Advanced: wait this long after a successful invocation before running that binding again |

Durations accept nonnegative `timedelta` values or strings ending in `ms`,
`s`, `m`, `h`, or `d`. `every` must be positive.

Most apps should keep `batch_delay` and `min_interval` at their defaults. They
control wall-clock execution behavior and do not change the event-time window
defined by `every`, `lookback`, and `lookahead`. Their timing state resets on
server restart, and failed transforms can retry at the materialization polling
cadence rather than waiting for `min_interval`.

### Query matches

A per-match invocation receives the streams resolved by one distinct query
match. Repeated identical results are deduplicated. A query that associates
the same stream references with different entity bindings is rejected; make
the entity selection unambiguous.

An all-matches invocation receives the union of selected streams under each
alias. It also receives the result table. A one-row result retains
`context.row`; multi-row aggregation has no single row.

Query dependencies must exclude unintended derived streams. Use
`measurement(app="upstream-name")` to select a producer explicitly.

## Output declarations

`aq.output.stream(value_kind=..., **metadata)` derives a stable stream name
from the app, port, and bound input references.

`aq.output.named(stream_name, value_kind=..., **metadata)` uses the exact
reference name under source `derived:<app>`.

Set `App.grouping` explicitly to control which query matches each call receives.
The output declaration only controls naming. A named stream must have a single
owning binding, so multiple per-match bindings cannot publish to it.

| Metadata | Meaning |
|---|---|
| `value_kind` | Required: `"numeric"` or `"text"` |
| `point_uri` | Existing point to attach to; otherwise a point is generated |
| `label` | Human-readable label |
| `unit` | Unit URI |
| `quantity_kind` | Quantity-kind URI |
| `medium` | Medium URI |
| `substance` | Substance URI |
| `data_source` | Queryable source tag |
| `properties` | Predicate URI to tuple of object URIs |

Without an explicit label, a generated stream is named from its input label
and app/port, or its named stream identity. Code edits do not change stream IDs.
Changing the bound inputs changes a generated identity; named outputs remain
stable across input membership changes.

### Output tables

Assign each port at most once per invocation. Acceptable values are Arrow
tables/record batches, Polars dataframes, and pandas dataframes.

Tables must have exactly `time` and `value` columns. Timestamps must be unique,
timezone-aware, and non-null. They are normalized to UTC microseconds.
Values must be non-null and match the declared kind. Numeric values are stored
as float64; text as strings. Empty tables must still have these typed columns.

Assigning a table replaces the port's stored results within
`context.output_window`; rows returned outside that interval are discarded.
An empty table therefore removes the previous results in the window. Leaving
a port unassigned preserves its data. The runtime commits these output changes
and the corresponding progress updates in one transaction.

## StreamSet

`inputs[alias]` is a StreamSet with:

- `stream`: the sole descriptor, raising if there is not exactly one.
- `streams`: all descriptors (reference URI, point URI, label, unit).
- `window`: the complete input read interval.
- `every`: the app's declared bucket duration, if any.
- `collect()`: loaded Arrow table with `ref_uri`, `time`, `value`.
- `df("polars")` or `df("pandas")`: loaded data as a dataframe.
- `batches()`: chunks of the loaded table, not streaming database reads.
- `changes`: live rows with revisions in the invocation's revision range.
- `in_unit(unit_uri)`: a converted StreamSet; missing/incompatible units raise.

Deleted readings are absent from the loaded data, but their tombstones record
the revision of the deletion so that the runtime can schedule affected apps.
Because `changes` contains only live rows, it can be empty when a deletion
causes the invocation.

## InputBatch

The `context` argument exposes:

| Field | Meaning |
|---|---|
| `row` | Individual match mapping, when one exists |
| `result` | Full deduplicated query result as a Polars dataframe |
| `changed_window` | Changed timestamp extent, or current durable work interval |
| `output_window` | Output interval owned by this invocation |
| `read_window` | Input interval including context |
| `binding_signature` | Compiled binding diagnostic identity |
| `graph_revision` | Graph version used in planning |
| `from_revision`, `to_revision` | Input revision range |

Match columns use query aliases. A stream alias holds its point URI, with
`<alias>_ref`, `<alias>.label`, and `<alias>.unit` alongside it. Non-stream
entities appear under their own aliases. Internal work cursor fields are
reserved for runtime bookkeeping.

When the query's match table changes, the runtime schedules a repair of the
retained output. This includes changes to the sensors in a named aggregate
and to the full query result available to per-match apps. The runtime persists
a fingerprint of that context so it can detect changes across restarts.
Changing the app's code alone still requires explicit reprocessing.

## TimeWindow

`TimeWindow(start, end)` normalizes datetimes to UTC; naive values mean UTC.
Bounds are inclusive at microsecond precision. Reversed windows are rejected.

For a changed extent `[a,b]`, trailing dependency `L`, and leading dependency
`A`, affected outputs span `[a-A,b+L]`. Their input context extends another
`L` before and `A` after. When `every` is declared, the output interval
expands to whole UTC buckets before the read interval is constructed.

`lookback="all"` reads the retained extent, including prior output timestamps
needed to remove obsolete results. Its memory use grows with retained history.

### Custom output timestamps

Override `output_window(self, changed)` to return a TimeWindow when output
timestamps map differently from ordinary pointwise, rolling, or bucketed data.
The default returns None and uses the declarations above.

The override must be deterministic and include every output timestamp affected
by the change. Input context still follows `lookback` and `lookahead`.
The runtime enforces replacement inside the returned interval.
Use the default for ordinary resampling and rolling calculations.

## aq.align

`aq.align(inputs, every=None, aggregate="mean")` returns a wide Polars frame
with `time` and one column per stream. With one stream per alias, columns use
the alias. Multiple streams use `alias[label-or-reference]`.

Omitting `every` uses `App.every`. An explicit value must agree with that
declaration. Declare buckets on scheduled apps so the runtime can read complete
buckets. Direct use outside apps can supply `every` explicitly.

Aggregates: mean, min, max, sum, first, last, median, count.
Streams missing a bucket contribute nulls. Combine available values or drop
incomplete rows according to the calculation's requirements.

## Check and deploy

- `client.check_app(AppClass, parameters=None, limit=None, search_path=None)`
  runs on the server and returns per-binding inputs, windows, outputs, and errors.
- `acquirium app check module:Class --local` executes in the caller's process.
  Local failures raise with their traceback; server failures appear in results.
- `client.deploy_app(AppClass, parameters=None)` validates before activating.
- `client.remove_app(name)` forgets deployment, progress, and pending work;
  retained output history is not deleted.
- `client.reprocess_app(name, start, end)` schedules retained output repair.
  Bucketed apps expand the interval to complete buckets. Pending work survives
  restart, and a conflicting reprocessing request is rejected.

Check outputs include `assigned`, `rows`, `truncated`, `value_kind`,
`stream`, `ref_name`, and `values`. Use `assigned` to distinguish an empty
replacement from an output the transform left unchanged. Checks load retained
input history; the result limit only bounds the output included in the response.

### Configuration deployment

The server can deploy apps after its configured drivers start:

```toml
[[apps]]
spec = "./plant_apps.py:Celsius"
threshold = 40.0
```

`spec` is `module:Class` or `path/to/file.py:Class`; relative file paths are
resolved against the configuration file. Other keys except the optional
display `name` are passed to the app constructor as `parameters`. The class
must remain importable from the same module whenever the server recompiles or
restarts, and its source must match the deployment digest. A deployment error
is logged without stopping the server or other configured apps.

For programmatic registration, `spec` may instead name a callable with the
signature `registrar(client, parameters)`. It may deploy apps itself and return
`None`, or return one `App` class or an iterable of them. Configuration keys
are passed to the registrar; classes it returns are deployed without separate
constructor parameters.

## HTTP API

| Method | Endpoint | Purpose |
|---|---|---|
| PUT | `/apps/{name}` | Deploy a definition |
| POST | `/apps/check` | Dry run |
| DELETE | `/apps/{name}` | Remove an app |
| POST | `/apps/{name}/reprocess?start=...&end=...` | Schedule output repair |
| GET | `/materialization/dag` | Bindings, edges, progress, and errors |

Deployment JSON carries name, entrypoint, executable_digest, outputs,
parameters, grouping, and window/scheduling attributes. Durations are integer
microseconds; lookback may be `"all"`. Grouping must be explicitly set to
`"per_match"` or `"all_matches"`, including for named outputs. Missing, null,
and invalid grouping values are rejected when the deployment is constructed.
Unknown deployment fields are also rejected.

DAG statuses are idle, pending, running, waiting, failed, or reprocessing.
Errors and last-success timestamps are process diagnostics; consumed progress
and pending work are durable. Global revision lag can include unrelated writes.

For the scheduling and recovery algorithm, see
[Materialization internals](../explanation/materialization-internals.md#scheduling-and-recovery).
