---
title: App deployment and recovery
---

Deploying an app can mean recomputing months of history. This guide shows how to see what a
deployment will do before you make it, how to check on an app afterwards, how
to change its code, and how to recover when a calculation fails. The examples
continue from [Write a soft sensor](write-soft-sensors.md#choose-inputs-and-windows), on the
sine-wave server.

## Deployment preview

Before switching `wave-mean` from fixed buckets to a rolling window, preview
how much history the change would recompute.

```python
from dataclasses import replace

rolling_mean = replace(
    mean,
    window=aq.Rolling(lookback=pd.Timedelta("4s"), step=pd.Timedelta("2s")),
)
preview = client.deploy_app(rolling_mean, dry_run=True)["app"]
print(preview["instances"])
```

The preview lists the affected app groups and estimates the input rows
and stored results involved. If you deployed `high-mean`, the preview flags
that its inputs may change. The counts depend on how much data the driver has
produced. See the [deployment reference](../reference/apps/deploying.md)
for the preview fields.

The preview does not run your function, so it cannot tell you whether the
function works. Try the function on a small frame first, as the
[calculation guide](write-soft-sensors.md#python-functions) does.

Check `preview["warnings"]` as well. A selection that matches no streams
is allowed, because the driver may not have registered them yet, but more often
it means the query is narrower than you meant.

## Deployment and status

```python
client.deploy_app(rolling_mean)
state = client.inspect_app("wave-mean")["app"]
state["status"], state["error"], state["warnings"]
```
```text
('active', None, [])
```

`state["operators"]` lists what is running: for each group, its input streams
and the stream each output is written to. Deploying one app leaves the others
in place.

The same steps work from the command line, given a file with a function that
takes `(client, parameters)` and returns the `App`:

```sh
acquirium app deploy examples/transformation/threshold_app.py:high_wave --dry-run
acquirium app deploy examples/transformation/threshold_app.py:high_wave
acquirium app inspect high-wave --json
```

## Calculation updates

Edit the function file, rebuild the app with `aq.PythonFunction.from_file(...)`,
and deploy it again. The path can stay the same. Acquirium identifies the code
by its content hash and saves a copy when you deploy. Editing or removing the
original file does not change a running app.

Changed code creates a new calculation version and recomputes the app's history.
A preview shows how much work that involves. Redeploying identical code keeps
the same version, even if its source path changes.

## Recovery from a failed calculation

Here is a function with a bug, saved as `ratio.py`. It divides by each
reading, and the sine wave passes through zero:

```python
def ratio(frame, context):
    return {"ratio": 1 / frame["value"][0]}
```

Deploy it:

```python
ratio_app = aq.App(
    name="wave-ratio",
    inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
    outputs={"ratio": aq.Output()},
    function=aq.PythonFunction.from_file(Path("ratio.py"), "ratio"),
    window=aq.Pointwise(),
)
client.deploy_app(ratio_app)["app"]["status"]
```
```text
'failed'
```

The very first reading is zero, so the calculation fails. App inspection
reports why:

```python
print(client.inspect_app("wave-ratio")["app"]["error"])
```
```text
execution failed: Python function failed: Traceback (most recent call last):
  ...
  File "/tmp/.tmpVdTlNB/function.py", line 2, in ratio
    return {"ratio": 1 / frame["value"][0]}
                     ~~^~~~~~~~~~~~~~~~~~~
ZeroDivisionError: float division by zero
```

Acquirium keeps this app's last successful results and marks it `failed`, with
`stale=True`. Apps that consume its outputs keep their results and become
`blocked`. Their `failures` entries identify the failed upstream operators in
`blocked_by`. A first execution that fails has no earlier results to retain.

Raw readings and unrelated apps continue updating. A published receipt confirms
that the write was committed; check app status to see whether a calculation is
current.

To recover, edit `ratio.py`:

```python
def ratio(frame, context):
    value = frame["value"][0]
    if abs(value) < 1e-9:
        return {"ratio": None}
    return {"ratio": 1 / value}
```

Rebuild the declaration and deploy it:

```python
fixed_app = aq.App(
    name="wave-ratio",
    inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
    outputs={"ratio": aq.Output()},
    function=aq.PythonFunction.from_file(Path("ratio.py"), "ratio"),
    window=aq.Pointwise(),
)
client.deploy_app(fixed_app)
```

A moment later the app is running:

```python
client.inspect_app("wave-ratio")["app"]["status"]
```
```text
'active'
```

Acquirium recomputes the failed branch from current inputs, including corrections
and deletions received while it was blocked. Recovered results and downstream
updates become visible together. Their failure status clears.

When the cause is outside the code, such as a missing package, fix that and call
`client.retry_engine()`. This requests another attempt without requiring a new
reading. A change to a failed operator's inputs also triggers another attempt.
Failure status and retained results survive a server restart.

## App removal

`client.remove_app(name)` removes an app and its current results. Earlier
snapshots keep their history. Remove any apps that read its outputs first; the
server refuses to remove an app something else depends on.
