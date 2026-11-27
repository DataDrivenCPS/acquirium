---
title: Soft sensor calculations
---

An app keeps one stream computed from others: an average, an alarm, an estimate
of something no sensor measures. This guide starts with the calculations
Acquirium has built in, then moves to functions of your own, and covers the
choices you make along the way: the window, state carried between results,
speed, and settings that differ between groups.

It uses the sine-wave server from [Soft sensor app](../tutorials/advanced/first-app.md).
Start it if it is not running, and connect:

```python
from pathlib import Path
import acquirium as aq
import pandas as pd

client = aq.Acquirium(server_url="127.0.0.1", server_port=8000)
wave = client.query().measurement(alias="wave", uri="urn:example:sine-wave")
```

## Built-in mean function

Averages, sums, minimums, maximums, counts and thresholds need no Python. The
engine computes them itself, which is faster and leaves no calculation file to
keep track of:

```python
mean = aq.App(
    name="wave-mean",
    inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
    outputs={"mean": aq.Output()},
    function=aq.Native.mean("mean"),
    window=aq.FixedBucket(pd.Timedelta("4s")),
)
client.deploy_app(mean)
client.query().measurement(alias="mean", app="wave-mean").dataframe().head(6)
```
```text
shape: (6, 2)
┌─────────────────────────┬───────────┐
│ time                    ┆ mean      │
│ ---                     ┆ ---       │
│ datetime[ns, UTC]       ┆ f64       │
╞═════════════════════════╪═══════════╡
│ 2026-11-10 17:10:08 UTC ┆ 0.0       │
│ 2026-11-10 17:10:12 UTC ┆ 0.603553  │
│ 2026-11-10 17:10:16 UTC ┆ -0.603553 │
│ 2026-11-10 17:10:20 UTC ┆ 0.603553  │
│ 2026-11-10 17:10:24 UTC ┆ -0.603553 │
│ 2026-11-10 17:10:28 UTC ┆ 0.603553  │
└─────────────────────────┴───────────┘
```

Four seconds is half a cycle of the wave, so the averages alternate between the
top half and the bottom half. Each result is stamped at the start of its bucket.
It appears as soon as the bucket has a reading, and updates as more arrive,
without waiting for the bucket to end. The first bucket here caught only the
wave's first reading, which was zero.

Each stream the query matches gets its own mean. In a plant, the same app over a
query for every turbidity sensor would give one averaged stream per sensor.

## App output selection

A second app can read the first one's output:

```python
high = aq.App(
    name="high-mean",
    inputs={"mean": aq.Selection.outputs_of(mean, "mean")},
    outputs={"alarm": aq.Output()},
    function=aq.Native.threshold("alarm", above=0.5),
)
client.deploy_app(high)
client.query().measurement(alias="alarm", app="high-mean").dataframe().head(3)
```
```text
shape: (3, 2)
┌─────────────────────────┬──────────┐
│ time                    ┆ alarm    │
│ ---                     ┆ ---      │
│ datetime[ns, UTC]       ┆ f64      │
╞═════════════════════════╪══════════╡
│ 2026-11-10 17:10:12 UTC ┆ 0.603553 │
│ 2026-11-10 17:10:20 UTC ┆ 0.603553 │
│ 2026-11-10 17:10:28 UTC ┆ 0.603553 │
└─────────────────────────┴──────────┘
```

The threshold passes an average through when it is above 0.5, and writes
nothing otherwise, so only the top halves of the wave remain. When an average
changes, because a late reading arrived in its bucket, the threshold follows.

`aq.Native.cumulative_sum` and `aq.Native.integral` keep running totals, such
as the volume pumped from a flow rate. The
[app reference](../reference/apps/app-class.md#built-in-functions) lists every built-in
function.

## Python functions

When no built-in fits, an app can call a Python function. We will compute the
wave's *spread*: the gap between its highest and lowest reading over a stretch
of time. Save this as `spread.py` in the directory you are working in:

```python
from acquirium.functions import emit


def spread(frame, context):
    # Ignore missing values before finding the range within this window.
    values = frame["value"].drop_nulls()
    if values.is_empty():
        value = None
    else:
        value = values.max() - values.min()
    # emit() uses this window's output timestamp; None clears any old result.
    return {"spread": emit(context, value)}
```

As in [your first app](../tutorials/advanced/first-app.md#declare-the-calculation), the
function receives readings in `frame` and returns one result for each output.
`emit()` stamps the value at the timestamp in `context`. Given `None`, it writes
nothing, and clears anything written at that timestamp before.

The input column `time` holds each sample's timestamp as a UTC datetime.
The context's
`output_time` is the output timestamp chosen by the window. For a fixed bucket,
that is its start; for a rolling window, it is the grid point being calculated.
Neither field describes when the server received the data. This spread
function only needs the values, so it leaves timestamp handling to `emit`.

An unannotated function receives Polars. Annotate its first parameter with
`pd.DataFrame` to receive Arrow-backed pandas 2.x, or `pl.DataFrame` to select
Polars explicitly. The function still takes `(frame, context)` and returns a
dataframe for each output. See the [dataframe type reference](../reference/apps/app-class.md#dataframe-type)
and the [pandas cleaning example](../tutorials/turbidity.md#cleaning-steps).

Because it is an ordinary function, you can try it before deploying anything:

```python
import polars as pl
from spread import spread

context = {"output_time": pd.Timestamp("2026-01-01T00:00:00Z")}
spread(pl.DataFrame({"value": [0.0, 1.0, -1.0]}), context)["spread"]["value"]
```
```text
shape: (1,)
Series: 'value' [f64]
[
	2.0
]
```

The server copies the file and runs it on its own. Anything the function uses
must be in this file or in a package installed on the server; a helper module
next to it is not available.

Deploy it over ten-second buckets:

```python
app = aq.App(
    name="wave-spread",
    inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
    outputs={"spread": aq.Output()},
    function=aq.PythonFunction.from_file(Path("spread.py"), "spread"),
    window=aq.FixedBucket(pd.Timedelta("10s")),
)
client.deploy_app(app)

spreads = client.query().measurement(alias="spread", app="wave-spread")
spreads.dataframe()
```
```text
shape: (5, 2)
┌─────────────────────────┬────────┐
│ time                    ┆ spread │
│ ---                     ┆ ---    │
│ datetime[ns, UTC]       ┆ f64    │
╞═════════════════════════╪════════╡
│ 2026-11-10 17:10:10 UTC ┆ 2.0    │
│ 2026-11-10 17:10:20 UTC ┆ 2.0    │
│ 2026-11-10 17:10:30 UTC ┆ 2.0    │
│ 2026-11-10 17:10:40 UTC ┆ 2.0    │
│ 2026-11-10 17:10:50 UTC ┆ 0.0    │
└─────────────────────────┴────────┘
```

The window cuts the wave into ten-second buckets and calls the function once
for each. A bucket holds a whole cycle, from −1 up to 1 and back, so its spread
is 2. The last bucket has only just started: it holds one reading so far, so its
spread is 0, and it will grow as the bucket fills.

## Window types

The window decides which readings each call sees. Replace the fixed bucket with
a rolling window that looks back four seconds, every two seconds:

```python
app = aq.App(
    name="wave-spread",
    inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
    outputs={"spread": aq.Output()},
    function=aq.PythonFunction.from_file(Path("spread.py"), "spread"),
    window=aq.Rolling(
        lookback=pd.Timedelta("4s"),
        step=pd.Timedelta("2s"),
    ),
)
client.deploy_app(app)
spreads.dataframe().tail(4)
```
```text
shape: (4, 2)
┌─────────────────────────┬──────────┐
│ time                    ┆ spread   │
│ ---                     ┆ ---      │
│ datetime[ns, UTC]       ┆ f64      │
╞═════════════════════════╪══════════╡
│ 2026-11-10 17:10:46 UTC ┆ 1.707107 │
│ 2026-11-10 17:10:48 UTC ┆ 1.0      │
│ 2026-11-10 17:10:50 UTC ┆ 1.707107 │
│ 2026-11-10 17:10:52 UTC ┆ 1.0      │
└─────────────────────────┴──────────┘
```

Four seconds is half a cycle, so the spread now rises and falls with the wave.
The function is the same; only the window changed, and Acquirium recomputed the
stored history to match.

There are three windows to choose from. `Pointwise()` calls the function once
for each timestamp, which suits conversions and checks on single readings.
`FixedBucket` gives regular summaries, a value per minute or per hour.
`Rolling` gives a trailing view at every step; add `lookahead` when a result
needs readings from both sides, as gap filling does. A wider window costs more:
each result reads more history, and a late or corrected reading revisits every
window that covers it.

## Sequential functions

Some calculations need their own previous result, such as a filter or a running
estimate. Save this smoothing filter as `smooth.py`:

```python
from acquirium.functions import emit, previous_value


def smooth(frame, context):
    reading = frame["value"][0]
    # Start at the first reading; later calls use the preceding output.
    earlier = previous_value(context, "smooth", reading)
    return {"smooth": emit(context, 0.8 * earlier + 0.2 * reading)}
```

and deploy it with `sequential=True`:

```python
smoother = aq.App(
    name="wave-smooth",
    inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
    outputs={"smooth": aq.Output()},
    function=aq.PythonFunction.from_file(Path("smooth.py"), "smooth", sequential=True),
    window=aq.Pointwise(),
)
client.deploy_app(smoother)
```

Put the two side by side:

```python
smooth = client.query().measurement(alias="smooth", app="wave-smooth")
wave.dataframe().join(smooth.dataframe(), on="time").head(7)
```
```text
shape: (7, 3)
┌────────────────────────────────┬────────────┬───────────┐
│ time                           ┆ wave       ┆ smooth    │
│ ---                            ┆ ---        ┆ ---       │
│ datetime[ns, UTC]              ┆ f64        ┆ f64       │
╞════════════════════════════════╪════════════╪═══════════╡
│ 2026-11-10 17:10:10.909771 UTC ┆ 0.0        ┆ 0.0       │
│ 2026-11-10 17:10:12.082770 UTC ┆ 0.707107   ┆ 0.141421  │
│ 2026-11-10 17:10:13.114759 UTC ┆ 1.0        ┆ 0.313137  │
│ 2026-11-10 17:10:14.139910 UTC ┆ 0.707107   ┆ 0.391931  │
│ 2026-11-10 17:10:15.161927 UTC ┆ 1.2246e-16 ┆ 0.313545  │
│ 2026-11-10 17:10:16.190043 UTC ┆ -0.707107  ┆ 0.109414  │
│ 2026-11-10 17:10:17.242986 UTC ┆ -1.0       ┆ -0.112468 │
└────────────────────────────────┴────────────┴───────────┘
```

The smoothed wave follows the original, lagging behind it and never reaching
its peaks, because each result is four parts the previous result to one part
the new reading.

`sequential=True` runs the windows in time order and hands each call the
previous window's results. `previous_value` reads one of them, or returns the
default for the very first reading. The outputs are the only state that
carries over. A global variable would not work: when an old reading is
corrected, Acquirium reruns from that point forward, and a variable would hold
whatever the last call happened to leave in it.

## Batch functions

By default the function is called once per window. That is fine for a live
stream, but a backfill or a correction can touch thousands of windows at once,
and then the cost of each call dominates. With `batch=True`, one call receives
every affected window together, with an `output_time` column saying which window
each reading belongs to:

```python
import polars as pl
from acquirium.functions import emit_windows


def spread(frame, context):
    # A batch contains several windows; calculate each window's range separately.
    spreads = frame.group_by("output_time").agg(
        (pl.col("value").max() - pl.col("value").min()).alias("value")
    )
    return {"spread": emit_windows(spreads)}
```

Save this version as `spread_batch.py` and pass
`aq.PythonFunction.from_file(Path("spread_batch.py"), "spread", batch=True)`.
The results are the same. The
[bulk-ingest example](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/bulk_ingest)
runs a chain of apps both ways on a month of plant data: 7.6 seconds in batch
against 47 seconds one window at a time.

## Group parameters

When one app serves several groups, such as one per filter, each group can have
its own settings without a copy of the calculation. The selection's
`parameters` gives each group its values. A built-in calculation refers to one
with `aq.Parameter`, as in `aq.Native.threshold("alarm", above=aq.Parameter("limit"))`,
and a Python function reads it from `context["parameters"]`. The
[turbidity tutorial](../tutorials/turbidity.md#sustained-turbidity-threshold)
uses this to give each filter its own limit and alarm duration, and changing one
filter's value recomputes only that filter's history.

## Related examples and references

For complete examples with small input and output tables, see
[Data cleaning with multiple apps](chain-cleaning-apps.md),
[Data cleaning with multiple outputs](clean-data-with-multiple-outputs.md), and
[Soft sensor parameters](parameterize-soft-sensors.md).

To see what a change will recompute before you make it, to update a
function's code, or to recover from a function that fails, see
[App deployment and recovery](check-deploy-apps.md). The
[app reference](../reference/apps/app-class.md#python-functions) describes every column
of the frame and every field of the context.
