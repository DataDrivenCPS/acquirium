---
title: Calculate derived streams
---

For an average, sum, minimum, maximum, count, or threshold, use a built-in
function. This example assumes you have registered a temperature stream; see
[your first app](../tutorials/first-app.md) for a complete run.

Select the registered point and average its readings in one-minute buckets:

```python
import acquirium as aq

MINUTE = 60_000_000_000
query = client.query().measurement(
    alias="temperature", uri="urn:example:temperature:fahrenheit"
)
mean = aq.App(
    name="minute-temperature",
    inputs=[aq.Selection.from_query(query, alias="temperature")],
    outputs={"mean": aq.Output(unit="http://qudt.org/vocab/unit/DEG_F")},
    function=aq.Native.mean("mean"),
    window=aq.FixedBucket(MINUTE),
)
client.deploy_app(mean)
```

Each selected stream gets its own result. The mean weights readings equally and
is stamped at the bucket's start. New or corrected readings revise that bucket.

To keep only high values, add a downstream app:

```python
high = aq.App(
    name="high-temperature",
    inputs=[aq.Selection.outputs_of(mean, "mean", role="temperature")],
    outputs={"alarm": aq.Output(unit="http://qudt.org/vocab/unit/DEG_F")},
    function=aq.Native.threshold("alarm", above=86),
)
client.deploy_app(high)
```

The threshold passes through temperatures above 86 °F. At other timestamps it
produces no row, removing any previous alarm there. Query the result with
`client.query().measurement(app="high-temperature").dataframe()`.

For a custom calculation, [write a Python function](write-soft-sensors.md).
The [app reference](../reference/apps.md#built-in-functions) also covers running
totals and integrals.
