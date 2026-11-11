---
title: Deploy, change and repair apps
---

Deploying an app can mean recomputing months of history, and a function that
fails stops every app until it is fixed. This guide shows how to see what a
deployment will do before you make it, how to check on an app afterwards, how
to change its code, and how to recover when a calculation fails. The examples
continue from [calculating a derived stream](write-soft-sensors.md), on the
sine-wave server.

## Preview a deployment

Before switching `wave-spread` from ten-second buckets to a rolling window, ask
what the change would do:

```python
preview = client.deploy_app(app, dry_run=True)
preview["app"]["apps"]
```
```text
{'wave-spread': {'operators': 1, 'groups': 23, 'input_rows': 78,
                 'cleared_rows': 5, 'changes': {'redefined': 1},
                 'downstream': False}}
```

Nothing has changed yet. The preview says the app would be *redefined*, which
means its results are thrown away and recomputed: 23 windows to calculate, from
78 stored readings, replacing the 5 bucket results it has now. No other app
reads its output, so nothing `downstream` is affected. On a real plant, the
same numbers tell a change to one filter's settings apart from a rebuild of a
year of history.

The preview does not run your function, so it cannot tell you whether the
function works. Try the function on a small frame first, as the
[calculation guide](write-soft-sensors.md#write-your-own-function) does.

Check `preview["app"]["warnings"]` as well. A selection that matches no streams
is allowed, because the driver may not have registered them yet, but more often
it means the query is narrower than you meant.

## Deploy and check the state

```python
client.deploy_app(app)
state = client.inspect_app("wave-spread")["app"]
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

## Change the code

Save the new version of a function at a new path, build the app with that path,
and deploy it. `spread_batch.py` in the calculation guide was deployed this way.
Acquirium recomputes the app's history with the new code, and a preview first
shows how much that is.

Editing a deployed file in place does not work. Acquirium records the file's
digest when the app is deployed, and refuses to run a file that no longer
matches; [how derived streams stay current](../explanation/apps.md#functions-must-be-repeatable)
explains why. Keep the old file where it is until the change has gone through,
because work accepted under the old deployment may still need it.

## Recover from a failed calculation

Here is a function with a bug, saved as `ratio.py`. It divides by each
reading, and the sine wave passes through zero:

```python
from acquirium.functions import emit


def ratio(frame, context):
    return {"ratio": emit(context, 1 / frame["value"][0])}
```

Deploy it:

```python
ratio_app = aq.App(
    name="wave-ratio",
    inputs=[aq.Selection.from_query(wave, alias="wave")],
    outputs={"ratio": aq.Output()},
    function=aq.PythonFunction.from_file(Path("ratio.py"), "ratio"),
    window=aq.Pointwise(),
)
client.deploy_app(ratio_app)["app"]["status"]
```
```text
'pending'
```

The very first reading is zero, so the app never gets going. The engine
reports why:

```python
print(client.engine_status()["error"])
```
```text
execution failed: Python function failed: Traceback (most recent call last):
  ...
  File "/tmp/.tmpVdTlNB/function.py", line 5, in ratio
    return {"ratio": emit(context, 1 / frame["value"][0])}
                                   ~~^~~~~~~~~~~~~~~~~~~
ZeroDivisionError: float division by zero
```

The failure stops publication for every app, not only this one. The driver's
writes are still accepted and stored, but `wave-spread` and `wave-smooth` stop
updating, and readers keep seeing the last good snapshot.
[How derived streams stay current](../explanation/apps.md#readers-see-a-consistent-snapshot)
explains why one failure holds back the rest.

To recover, save a corrected version at a new path, `ratio_v2.py`, as for any
change of code:

```python
from acquirium.functions import emit


def ratio(frame, context):
    value = frame["value"][0]
    if abs(value) < 1e-9:
        return {"ratio": emit(context, None)}
    return {"ratio": emit(context, 1 / value)}
```

Deploy it, and tell the engine to try again:

```python
fixed_app = aq.App(
    name="wave-ratio",
    inputs=[aq.Selection.from_query(wave, alias="wave")],
    outputs={"ratio": aq.Output()},
    function=aq.PythonFunction.from_file(Path("ratio_v2.py"), "ratio"),
    window=aq.Pointwise(),
)
client.deploy_app(fixed_app)
client.retry_engine()
```

A moment later the app is running:

```python
client.inspect_app("wave-ratio")["app"]["status"]
```
```text
'active'
```

The retry works through the waiting writes in order, under the newest
deployment, and the other apps catch up. It reaches the fix however many writes
arrived while the engine was paused.

When the cause is outside the code, such as a missing package or a deleted
calculation file, fix that and call `client.retry_engine()` alone. Keep each
calculation file until the work that needs it has gone through.

## Remove an app

`client.remove_app(name)` removes an app and its current results. Earlier
snapshots keep their history. Remove any apps that read its outputs first; the
server refuses to remove an app something else depends on.
