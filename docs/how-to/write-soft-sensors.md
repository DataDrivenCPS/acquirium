---
title: Write a Python calculation
---

An app can call your Python function for each input window. The function receives
a Polars dataframe and returns its result. Start with
[your first app](../tutorials/first-app.md) if you have not deployed one yet.

Suppose you want the spread between a minute's highest and lowest turbidity
readings. Save this in `calculations.py`:

```python
from escapement import emit

def spread(frame, context):
    values = frame["value"].drop_nulls()
    value = None if values.is_empty() else values.max() - values.min()
    return {"spread": emit(context, value)}
```

`emit` stamps the result at the window's timestamp. A `None` value produces no
row and clears any earlier result for that window. Always return every declared
output port, including when it has no value.

Select a registered point and connect the function to a one-minute window:

```python
from pathlib import Path
import acquirium as aq

query = client.query().measurement(
    alias="turbidity", uri="urn:bulk/loc-2-turbidity"
)
app = aq.App(
    name="turbidity-spread",
    inputs=[aq.Selection.from_query(query, alias="turbidity")],
    outputs={"spread": aq.Output(unit="http://qudt.org/vocab/unit/NTU")},
    function=aq.PythonFunction.from_file(Path("calculations.py"), "spread"),
    window=aq.FixedBucket(60_000_000_000),
)
client.deploy_app(app)
```

Use your own point URI, or the Filter 3 point from the
[turbidity demo](../tutorials/turbidity.md). The calculation file must be available
on the server, with its dependencies installed in the server's Python environment.
Keep helpers in the same file: neighbouring files are not copied to the worker.

## Choose what the function sees

`Pointwise()` supplies the readings at one timestamp. `FixedBucket(width_ns)`
supplies one bucket, such as the minute above. For a trailing ten-minute spread
every minute, change the window to:

```python
window=aq.Rolling(lookback_ns=600_000_000_000, step_ns=60_000_000_000)
```

The function stays the same. A correction now revises every window that reads
that timestamp. A longer lookback means more history to read for each result.

Keep the calculation dependent on the supplied frame and context. Read parameters
from `context["parameters"]`; avoid reading live files, calling services, or using
the current time, since the function may run again to repair old results.

## Update the calculation

`PythonFunction.from_file` records the file's content digest. Put a changed
calculation at a new path, construct the app with that path, and deploy it again.
The new function recalculates the app's stored history.

Keep old files available while accepted work may still need them. The digest
covers the calculation file; packages and model assets need their own versioning.
See the [deployment contract](../reference/apps.md#deployment-files) for that case.

Use [deploy and inspect apps](check-deploy-apps.md) to preview a change or recover
from a failed function. The [function reference](../reference/apps.md#python-functions)
has the frame schema and optional batch and sequential modes.
