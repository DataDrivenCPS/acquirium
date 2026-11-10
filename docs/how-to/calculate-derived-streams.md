---
title: Calculate derived streams
---

For an average, sum, minimum, maximum, count, or threshold, use a built-in
function. This example assumes you have started the sine-wave driver; see
[your first app](../tutorials/first-app.md) for a complete run.

Select the registered point and average its readings in one-minute buckets:

```python
import acquirium as aq

MINUTE = 60_000_000_000
query = client.query().measurement(
    alias="wave", uri="urn:example:sine-wave"
)
mean = aq.App(
    name="minute-signal",
    inputs=[aq.Selection.from_query(query, alias="wave")],
    outputs={"mean": aq.Output()},
    function=aq.Native.mean("mean"),
    window=aq.FixedBucket(MINUTE),
)
client.deploy_app(mean)
```

Each selected stream gets its own result. The mean weights readings equally and
is stamped at the bucket's start. It updates as the driver adds readings, without
waiting for the minute to end. For a sine wave, a full cycle averages close to
zero.

To keep only high values, add a downstream app:

```python
high = aq.App(
    name="high-mean",
    inputs=[aq.Selection.outputs_of(mean, "mean", role="mean")],
    outputs={"alarm": aq.Output()},
    function=aq.Native.threshold("alarm", above=0.1),
)
client.deploy_app(high)
```

The threshold passes through minute averages above 0.1. At other timestamps it
produces no row, removing any previous alarm there. Query the result with
`client.query().measurement(app="high-mean").dataframe()`.

For a custom calculation, [write a Python function](write-soft-sensors.md).
The [app reference](../reference/apps.md#built-in-functions) also covers running
totals and integrals.
