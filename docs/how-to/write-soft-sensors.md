---
title: Write a soft sensor
---

Use an Acquirium app to keep a derived stream up to date as source data
arrives or is corrected. The example below calculates a four-second mean
and publishes an alarm when the mean exceeds a limit. Adapt its input
selection, window, and outputs to your own streams.

If you haven't deployed an app before, start with [Build your first soft
sensor](../tutorials/advanced/first-app.md).

## Python functions

Save this as `wave_calculations.py`. Each function returns a mapping from
output port to result.

```python
def bucket_mean(frame, context):
    return {"mean": frame["value"].mean()}


def high_mean(frame, context):
    value = frame["value"].drop_nulls().first()
    return {"alarm": value if value is not None and value > 0.5 else None}
```

A scalar is stamped at `context["output_time"]`, the timestamp chosen by the
window. The input frame's `time` column holds the original UTC sample
timestamps. Returning `None` clears the result for that port and timestamp.

Functions receive Polars by default. Annotate the first parameter with
`pd.DataFrame` for Arrow-backed pandas. Test a calculation on a small frame
before deploying it.

```python
import polars as pl
from wave_calculations import bucket_mean

assert bucket_mean(pl.DataFrame({"value": [0.0, 1.0]}), {}) == {"mean": 0.5}
```

The server copies the calculation file when you deploy. Put helpers in that
file or install them as packages on the server. See the
[function reference](../reference/apps/app-class.md#python-functions) for
frame columns, context fields, and dataframe returns.

## Choose inputs and windows

To run the examples, [start the sine-wave server](../tutorials/advanced/first-app.md#start-the-server)
from the repository root.

In another terminal, start a Python session in the directory containing
`wave_calculations.py`. Connect to the server, select an input, and deploy.

```python
from pathlib import Path

import acquirium as aq
import pandas as pd

client = aq.Acquirium(server_url="127.0.0.1", server_port=8000)
wave = client.query().measurement(alias="wave", uri="urn:example:sine-wave")
calculations = Path("wave_calculations.py").resolve()
mean = aq.App(
    name="wave-mean",
    inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
    outputs={"mean": aq.Output()},
    function=aq.PythonFunction.from_file(calculations, "bucket_mean"),
    window=aq.FixedBucket(pd.Timedelta("4s")),
)
client.deploy_app(mean)
print(client.query().measurement(alias="mean", app=mean.name).dataframe().tail())
```

The default grouping gives each matched stream its own calculation. Use
`group` with an entity alias to calculate one result per entity, such as one
average per tank. For several named inputs or a calibration value that should
be carried forward, see [app inputs](../reference/apps/inputs.md).

| Use case | Window | Result timestamp |
| --- | --- | --- |
| Convert or check each reading | `aq.Pointwise()` | The sample timestamp |
| Summarize regular intervals | `aq.FixedBucket(pd.Timedelta("4s"))` | The start of each bucket |
| Calculate a moving statistic | `aq.Rolling(lookback=pd.Timedelta("4s"), step=pd.Timedelta("2s"))` | Each point on the configured time grid |

Fixed buckets update as readings arrive. A rolling window can include
`lookahead` when a result needs later samples, as it does for interpolation.
A correction revisits each window that reads the affected sample.

## Chain apps

Use `Selection.outputs_of(app, port)` to reuse one calculation's result in
another. Deploy the upstream app first. Its output groups carry through to the
downstream app.

```python
high = aq.App(
    name="high-mean",
    inputs={"mean": aq.Selection.outputs_of(mean, "mean")},
    outputs={"alarm": aq.Output()},
    function=aq.PythonFunction.from_file(calculations, "high_mean"),
    window=aq.Pointwise(),
)
client.deploy_app(high)
print(client.query().measurement(alias="alarm", app=high.name).dataframe().tail())
```

The alarm stores the mean only where it exceeds 0.5. When a correction takes
the mean below the threshold, Acquirium retracts that alarm row. To act on
those changes, use the [streaming change feed](consume-streaming-changes.md).

For a cleaning stage that becomes the default value for an existing
measurement, use [`ReplacementOutput`](../reference/apps/outputs.md#replacement-outputs).
Read with `view="raw"` to see the originals. The [turbidity pipeline
tutorial](../tutorials/turbidity.md) walks through cleaning, interpolation,
and a sustained alarm.

## Publish multiple outputs

When several results share the same inputs and window, declare a port for each
one. For example, declare `outputs={"mean": aq.Output(label="Mean signal"),
"peak": aq.Output(label="Peak signal")}` and return both keys from the function.

```python
def summarize(frame, context):
    values = frame["value"].drop_nulls()
    return {"mean": values.mean(), "peak": values.max()}
```

Use the app name and output label to select one port in a measurement query,
or `Selection.outputs_of(summary_app, "peak")` to feed the named port to
another app. The [bulk-ingest example](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/bulk_ingest)
chains a gap filler and publishes mean, maximum, and reporting count together.

## Set parameters by group

Pass a mapping of group IDs to settings as `parameters=` on a selection.
The function reads those settings from `context["parameters"]`. In the
[turbidity alarm](../tutorials/turbidity.md#sustained-turbidity-threshold), the
combined outlet uses a limit of 0.3 NTU for five minutes, while Filter 3 uses
0.5 NTU for fifteen minutes. Both use the same Python function.

Use the exact group IDs from the selection. Groups omitted from the mapping
are skipped with a deployment warning. Updating a group's parameters and
redeploying recalculates its stored history. See the
[group parameter reference](../reference/apps/outputs.md#group-parameters)
for the mapping syntax and grouping rules.

## Use previous results or batch windows

- For a smoother or running total, set `sequential=True` on
  `PythonFunction.from_file`. Use `previous_value(context, port, default)` to
  read a prior output. See [sequential functions](../reference/apps/app-class.md#sequential-functions).
- For a large backfill, set `batch=True` and group the input by `output_time`.
  Use `emit_windows` to return results for those times. See
  [batch functions](../reference/apps/app-class.md#batch-functions) for a mean
  that processes multiple windows in one call.

To preview a change, inspect deployment, or recover a failed app, see
[App deployment and recovery](check-deploy-apps.md). For diagrams of grouping
and timestamps, see [Soft sensor inputs](../explanation/soft-sensor-inputs.md).
