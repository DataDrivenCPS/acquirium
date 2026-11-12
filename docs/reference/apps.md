---
title: App interface
---

An `aq.App` declares inputs, named output ports, a function, and a window. App
names use letters, digits, underscores, dots, and hyphens. See
[Soft sensor app](../tutorials/first-app.md) for a complete example.

## Input selections

`Selection.from_query(query, alias=..., group=None)` turns a query into an app
input. `alias` identifies the measurement and becomes its input role. Without a
`group`, each matched stream gets its own calculation. With an entity alias as
`group`, all streams for that entity feed one calculation.

The selection stores the query, rather than a fixed list of matches. Acquirium
resolves it again when the graph changes. Scope queries to the intended points
or sources; `exclude_app` can exclude an app's own outputs from a broad query.

`Selection(query, stream="stream", group="group", role="input")` accepts SPARQL
returning the named columns. Here `group=None` combines all selected streams into
one app-wide group. Returned columns are available as strings in each input
row's JSON `context`.

`target_unit` adds resolved conversion factors to that row context. The function
must apply the factors; declaring a target unit does not convert the frame.

### Outputs of another app

`Selection.outputs_of(app, port, role=..., combine=False)` selects an upstream
output. By default it preserves the upstream groups. `combine=True` feeds every
upstream group into one calculation. The upstream app and port must exist before
deployment, and the upstream app cannot be removed while a deployed app selects it.

Output points also carry `acq:producedBy`, `acq:outputPort`, and `acq:outputGroup`
for selection through SPARQL.

### Group parameters

A selection's `parameters` maps group IDs to JSON values:

```python
aq.Selection.outputs_of(fill, "turbidity", role="filled", parameters={
    "urn:bulk/loc-2": {"limit": 0.5},
    "urn:bulk/loc-3": {"limit": 0.3},
})
```

Only the listed groups are selected; skipped groups appear in deployment
warnings. The function reads the values from `context["parameters"]`. A changed
parameter recalculates that group's history. Two selections giving the same
group conflicting values for a parameter fail deployment.

Acquirium always includes the selected group key as
`context["parameters"]["group"]`, including when no settings are configured.
In Python this key is always a tuple. Grouping by tank gives `(tank_uri,)`;
grouping by stream gives `(stream_reference_uri,)`;
`Selection(..., group=None)` gives `(app_name,)`. These keys have one item
because a selection currently groups by one column. JSON deployment responses
represent the tuple as an array. The keys in the selection's `parameters`
mapping remain group ID strings. The name `group` is reserved and cannot be
configured to a different value.

## Outputs

`Output(unit=None, quantity_kind=None, label=None, on_group=False)` describes a
numeric output. The port name is the key in the app's `outputs` mapping and in
the function's return value.

`on_group=True` attaches the output point to its group using `s223:hasProperty`.
The group must be a URI. Queries can then find the derived measurement through
the entity it describes, alongside its source measurements.

Output references depend on app name, group, and port. Changing the group's
membership keeps that identity. Derived source IDs are reserved as
`derived:<app-name>`. Membership applies to the whole stored history; see
[Derived stream updates](../explanation/apps.md).

## Windows

Window declarations use integer nanoseconds for durations and origin timestamps.
In a Python function's context, durations are `datetime.timedelta` values and
timestamps are timezone-aware UTC `datetime` values.

`Pointwise()` supplies readings at one timestamp and emits there. It is the
default. Asynchronous streams are not automatically forward-filled.

`FixedBucket(width_ns, origin_ns=0)` supplies a half-open bucket and emits at its
start. Results can change before or after the bucket ends; there is no wait for
wall-clock completion.

`Rolling(lookback_ns, step_ns, lookahead_ns=0, origin_ns=0)` emits at grid points
`origin + k * step`. At time `t` it reads `[t - lookback, t + lookahead]`, including
both endpoints. A correction repairs every window that reads the changed time.
Empty spans produce no results. Overlapping windows reread their shared history.

Apps do not choose arbitrary output timestamps or run on a wall-clock schedule.

## Built-in functions

`aq.Native.mean(port, per_stream=False)`, `sum`, `min`, `max`, and `count` reduce
all input rows in each window to one numeric result. An empty window produces
no row. The default mean weights observations equally.

`aq.Native.threshold(port, above)` passes through a window's single input value
when it is greater than `above`; otherwise it produces no row. It does not emit
zeros and ones. These functions execute without a Python calculation file.

Use `aq.Parameter("limit")` in place of a function argument to resolve it from
each group's parameters, for example:

```python
function=aq.Native.threshold("alarm", above=aq.Parameter("limit"))
```

Deployment fails when a group lacks the required parameter. For a complete
average-and-threshold example, see [Soft sensor calculations](../how-to/write-soft-sensors.md).

### Totals and integrals

`aq.Native.cumulative_sum(port, reset=None)` adds each window's sum to the
previous total. `aq.Native.integral(port, per_ns, reset=None, max_gap_ns=None)`
integrates one reading per pointwise timestamp. For a flow measured per hour,
use an hour in nanoseconds as `per_ns` to obtain a volume.

`reset=aq.FixedBucket(width_ns)` restarts a total at each bucket boundary.
Corrections repair later totals until the results stop changing; a reset bounds
the repair to the rest of its period. Every repaired window counts toward the
engine's per-operator group limit, so use resets for long-running totals.

An integral assigns a step across a boundary to the period in which it ends.
A step longer than `max_gap_ns` adds nothing. Integrals require the default
`Pointwise()` window and one reading per timestamp. Rolling windows cannot carry
a total because their windows overlap.

## Python functions

`PythonFunction.from_file(path, entrypoint, batch=False, sequential=False)` pins
a function's file content. The entrypoint has signature `function(frame, context)`.
The [calculation guide](../how-to/write-soft-sensors.md#python-functions) walks through one.

For diagrams connecting stream groups, windows, timestamps, and function calls,
see [Soft sensor inputs](../explanation/soft-sensor-inputs.md).

### Dataframe type

The first parameter's annotation selects the input dataframe:

- `frame: pandas.DataFrame`: pandas 2.x, with Arrow-backed columns.
- `frame: polars.DataFrame`: Polars.
- No annotation: Polars.

Imports such as `import pandas as pd`, aliases, string annotations, and
`from __future__ import annotations` work. Acquirium resolves the annotation
once when a worker loads the function. Other frame annotations are rejected.
Deployment syntax stays the same.

pandas inputs use `table.to_pandas(types_mapper=pd.ArrowDtype)`. Numeric, text,
and timestamp columns retain Arrow storage and nulls. Numeric nulls appear as
`pd.NA`; timestamps have dtype `timestamp[us, tz=UTC][pyarrow]`. Context
timestamps remain Python datetimes. pandas operations may allocate new buffers
or convert individual columns; Arrow-backed input does not guarantee that
every pandas operation is zero-copy.

During a calculation, `emit`, `empty`, `previous_rows`, and `previous_outputs`
use the selected library. pandas missing scalar values passed to `emit` produce
an empty result. `emit_windows` accepts either library and returns that same
kind of dataframe. Outside a calculation, helpers default to Polars.

An app may return pandas or Polars dataframes, including different types on
different output ports. Return timestamps in a `time` column; use
`reset_index()` when a pandas result has a time index. Apps using different
libraries can feed each other. The [turbidity tutorial](../tutorials/turbidity.md#cleaning-steps)
uses a pandas cleaner followed by a Polars alarm.

### Inputs and outputs

The input frame has `role`, `stream`, `time`,
`value`, `text`, and JSON `context` columns. Each reading has either a
numeric `value` or `text`. Selected graph columns are strings in the row context.

`time` is the input sample's timestamp, in UTC microseconds. In Polars its
type is `Datetime("us", "UTC")`; in pandas it is an Arrow timestamp column.
It is the timestamp supplied when the sample was inserted. `stream` identifies
the input stream.

The function's context describes the calculation. `output_time` is its output
timestamp; `window_start` and `window_end` bound the window. `parameters`
holds the group key and configured parameter values, and `operator` holds its declaration,
including `operator["window"]`.

`context["operator"]["outputs"]` maps each declared output name to its full
stream URI for the current group. The function returns dataframes under those
output names; Acquirium routes them to the corresponding URIs.

Context timestamps are timezone-aware Python datetimes. Function timestamps
and window declarations must align to microseconds; finer values are rejected
without rounding. All context window bounds are half-open: start included,
end excluded. For a pointwise window, the end is one microsecond after its
timestamp. For a rolling window, it is one microsecond after the last included
timestamp.

Within `operator["window"]`, `width`, `lookback`, `lookahead`, and `step` are
timedeltas; `origin` is a UTC datetime. Input validity and span bounds are UTC
datetimes, with `None` for an unbounded validity interval. As-of ages in
`operator["as_of"]` are timedeltas. The app declaration and deployment reports
retain the engine's integer representation.

For `Pointwise()`, `output_time` is the input timestamp. For `Rolling()`, it is
the output's grid point, and the frame can contain samples before or after
that point, as allowed by the declared window. `emit(context, value)` uses
`output_time` as the result's timestamp.

Context also lists the declared inputs, including those with no readings. Each
input's `span_start` and `span_end` describe the part of the window it
covers, as a half-open span.

Return a mapping with every declared port. Each port's frame has these columns
in order. The types below are Polars types; pandas frames use the equivalent
Arrow types or compatible numeric and timezone-aware datetime types.

| Column | Type |
| --- | --- |
| `time` | `Datetime("us", "UTC")` |
| `value` | `Float64` |
| `text` | `String` |

Numeric results set `value` and leave `text` null; numeric functions may also
omit the `text` column. Use finite numbers.
`acquirium.functions.emit(context, value)`
returns a one-row dataframe at `context["output_time"]`; a `None` value produces
an empty dataframe. The helper constructs the function's return value.
Acquirium stores the results after the function returns.
`empty()` also builds an empty frame. An empty result removes the previous
result for that window. Return the complete result, including unchanged rows.

Functions use only their declared inputs and frozen context. Do not query the
live graph, fetch undeclared history, or depend on wall-clock time. Results must
be independent of process lifetime and invocation order; mutable globals are
not persistent calculation state.

### Batch functions

With `batch=True`, one call receives every affected window. The frame gains a
`output_time` UTC datetime column, and `context["windows"]` lists their contexts,
including empty windows. A reading shared by overlapping windows appears once
per window.

```python
import polars as pl
from acquirium.functions import emit_windows

def bucket_mean(frame, context):
    means = frame.group_by("output_time").agg(pl.col("value").mean())
    return {"mean": emit_windows(means)}
```

`emit_windows` accepts `output_time` and `value` or `text` columns.
Omitting a window removes its old result. A row at an undeclared window timestamp
fails the computation. Switching batch mode recalculates the app's history. The
[bulk-ingest example](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/bulk_ingest) compares both modes.

### Sequential functions

With `sequential=True`, the function runs in time order, once per window.
`context["previous"]` holds the latest earlier window that wrote outputs: its
`output_time`, input `rows`, and `outputs` by port. It is absent for the first
window. Outputs carry the state, so return every value needed by the next window.

`acquirium.functions.previous_value(context, port, default)` reads an earlier output.
`acquirium.functions.same_period(context, width)` takes a positive
`datetime.timedelta` and helps restart state at period boundaries. A repair
continues until state and outputs stop changing.
`batch=True` does not apply to sequential functions. The
[membrane-monitoring example](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/membrane_monitoring)
uses this interface for drift detection and parameter estimation.

### As-of inputs

`Selection(..., as_of=max_age_ns)` reads each selected stream's latest reading
at or before the window timestamp, no older than the maximum age. It retains
that reading's own `time`. `outputs_of` accepts the same argument.

An as-of role creates no windows; other inputs determine when to calculate.
A correction repairs the windows that read the value, up to its next reading or
maximum age. All selections of one role must use the same age. Changing it
recalculates history. As-of inputs require a Python function.

## Deployment

`client.deploy_app(app)` creates or updates an app. `list_apps()` lists them;
`inspect_app(name)` reports its state and compiled operators. Each deployment
keeps the other apps and validates the full set, rejecting cycles or outputs
with conflicting owners.

Deployment and inspection report input `matches` and `warnings`. Zero matches
are valid, for example before a driver registers its streams.

`deploy_app(app, dry_run=True)` previews work without activating the change.
Its `apps` entry lists operators as `new`, `redefined`, `rebound`, or `unchanged`,
along with groups to re-evaluate, input rows read, and output rows replaced.
`downstream` identifies dependent apps; `removed` lists retractions;
`within_limits` checks the counted work against engine limits. Apps affected by
`metadata_changes` use current outputs in the preview, so further membership
changes can become apparent only after the real deployment.

`client.remove_app(name)` removes an app after its dependants are removed. Its
current outputs are retracted; historical versions remain. Output ownership
stays reserved. For operating steps and failure recovery, see
[deploy and inspect apps](../how-to/check-deploy-apps.md).

### Deployment files

Paths name files on the server at deployment. `PythonFunction.from_file` hashes
the file; deployment checks the hash and retains a copy under it. The running
app uses that copy, so the source file can be edited or removed afterwards.
To deploy an edit, rebuild the declaration and redeploy; the path can stay the
same. Identical file content keeps the same calculation version.

The worker copies the calculation file into a temporary directory. Helpers
must be in that file or installed packages; adjacent files and paths relative
to `__file__` are unavailable. Dependencies use the server's installed Python
environment. Pin that environment and record interpreter and platform versions
with the release.

The file digest does not cover installed packages or model assets. Version
those separately and record their identities in the calculation when they affect
results. Changing a dependency alone does not trigger history recalculation.
Drain accepted work before changing a shared environment, preserve earlier
releases for recovery, and deploy a newly versioned calculation.

Complete any work spawned by a function before returning.

### Failure status

Function failures are isolated to an operator and its downstream consumers.
Their last successful values remain queryable. Other groups and apps continue
updating. `inspect_app(name)["app"]` includes:

- `status`: `active`, `failed` for an evaluation error, or `blocked` for a failed
  upstream operator. `pending` means the desired plan has not activated.
- `stale`: whether any operator in the app is failed or blocked.
- `failures`: a map keyed by operator ID. Each entry contains `error` (on the
  failed calculation), `blocked_by` (failed operator IDs), and `since_epoch`
  (the first epoch whose inputs were not successfully processed).

`engine_status()["engine"]["failures"]` and `engine_snapshot()["failures"]`
expose the full operator failure map.
Publication of a receipt does not mean all derived streams are current.
Failure status survives restart. A corrected deployment, changed input, or
`retry_engine()` requests another attempt. Recovery recomputes the affected
branch from current inputs, including missed deletions, before clearing its
failure status. A database commit failure still stops publication globally.

Branches share an epoch commit. A slow function can delay publication until it
finishes or times out.

Worker-pool settings and limits are in [server configuration](server-config.md).
