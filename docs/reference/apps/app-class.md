---
title: App declarations, windows, and functions
---

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
zeros and ones. Acquirium supplies these calculations to the Python workers.

Use `aq.Parameter("limit")` in place of a function argument to resolve it from
each group's parameters, for example:

```python
function=aq.Native.threshold("alarm", above=aq.Parameter("limit"))
```

Deployment fails when a group lacks the required parameter. For a complete
average-and-threshold example, see [Soft sensor calculations](../../how-to/write-soft-sensors.md).

### Totals and integrals

`aq.Native.cumulative_sum(port, reset=None)` adds each window's sum to the
previous total. `aq.Native.integral(port, per_ns, reset=None, max_gap_ns=None)`
integrates one reading per pointwise timestamp. For a flow measured per hour,
use an hour in nanoseconds as `per_ns` to obtain a volume.

`reset=aq.FixedBucket(width_ns)` restarts a total at each bucket boundary.
Corrections repair later totals until the results stop changing. Every repaired
window counts toward the engine's limits per instance.

An integral assigns a step across a boundary to the period in which it ends.
A step longer than `max_gap_ns` adds nothing. Integrals require the default
`Pointwise()` window and one reading per timestamp.

## Python functions

`PythonFunction.from_file(path, entrypoint, batch=False, sequential=False)` pins
a function's file content. The entrypoint has signature `function(frame, context)`.
The [calculation guide](../../how-to/write-soft-sensors.md#python-functions) walks through one.

For diagrams connecting stream groups, windows, timestamps, and function calls,
see [Soft sensor inputs](../../explanation/soft-sensor-inputs.md).

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

During a calculation, `emit`, `empty`, and `previous_outputs`
use the selected library. pandas missing scalar values passed to `emit` produce
an empty result. `emit_windows` accepts either library and returns that same
kind of dataframe. Outside a calculation, helpers default to Polars.

An app may return pandas or Polars dataframes, including different types on
different output ports. Return timestamps in a `time` column; use
`reset_index()` when a pandas result has a time index. Apps using different
libraries can feed each other. The [turbidity tutorial](../../tutorials/turbidity.md#cleaning-steps)
uses a pandas cleaner followed by a Polars alarm.

### Inputs and outputs

The input frame has `input`, `stream`, `time`,
`value`, `text`, and JSON `context` columns. Each reading has either a
numeric `value` or `text`. Selected graph columns are strings in the row context.

`time` is the input sample's timestamp, in UTC microseconds. In Polars its
type is `Datetime("us", "UTC")`; in pandas it is an Arrow timestamp column.
It is the timestamp supplied when the sample was inserted. `input` is the
row's [input name](#inputs), and `stream` identifies the input stream.

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

With `batch=True`, one call receives a batch of affected windows. The frame gains a
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
`context["previous"]` holds the latest earlier output window's `output_time`
and the latest reading on each port from that window. Store any input values
needed by a later window on an output port.

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

An as-of input creates no windows; other inputs determine when to calculate.
A correction repairs the windows that read the value, up to its next reading or
maximum age. All selections of one input must use the same age. Changing it
recalculates history.
