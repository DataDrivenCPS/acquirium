---
title: Soft sensor parameters
---

Keep settings in the app declaration so one Python function can serve sensors
with different limits. This example gives two normalized signals thresholds
of 0.5 and 0.8, then changes the second threshold without editing Python code.
It starts a local server and inserts three readings per sensor; no driver or
external dataset is needed.

## Parameterized threshold function

Save this as `parameterized_threshold.py` in your working directory:

```python
from acquirium.functions import emit


def threshold(frame, context):
    # Acquirium supplies the settings for this sensor's group on every call.
    limit = context["parameters"]["limit"]
    # Pointwise has at most one input sample here; first() returns None if empty.
    value = frame["value"].drop_nulls().first()
    if value is not None and value <= limit:
        value = None  # Keep only readings strictly above the configured limit.

    # Pointwise output keeps the sample's timestamp, including for late data.
    # None produces an empty result, clearing an earlier alarm after a correction.
    return {"high": emit(context, value)}
```

The function passes the original value through when it is strictly above its
limit. It emits no row otherwise. Each group will contain one input stream,
so a pointwise call has at most one reading.

Here the input's `time` (the sample timestamp) and the output's
`context["output_time"]` refer to the same instant. `emit` handles the output
timestamp, so the function needs no time arithmetic. The `time` column in the
query results below shows that same instant as a datetime.

## Sensor registration and sample readings

Run these blocks in order in a Python session beside the calculation file.
With the repository's development environment, use `uv run python`.

```python
from datetime import timedelta
from pathlib import Path
import acquirium as aq

client = aq.init(data_dir="./parameterized-sensor-data")
start = aq.to_timestamp("2026-01-01T00:00:00 UTC")
client.register_streams([
    {
        "source_id": "parameter-demo",
        "ref_name": name,
        "point_uri": f"urn:parameter-demo:{name}",
        "value_kind": "numeric",
    }
    for name in ("sensor-a", "sensor-b")
])
rows = [
    # These are sample timestamps, independent of when the batch is uploaded.
    (start + timedelta(minutes=minute), value)
    for minute, value in enumerate([0.4, 0.7, 0.9])
]
client.insert_timeseries_batch("parameter-demo", {
    "sensor-a": rows,
    "sensor-b": rows,
})
```

Each input stream has three rows:

| Time (UTC, January 1) | sensor-a | sensor-b |
| --- | ---: | ---: |
| 00:00 | 0.4 | 0.4 |
| 00:01 | 0.7 | 0.7 |
| 00:02 | 0.9 | 0.9 |

## Sensor grouping and threshold parameters

The selection below uses the sensor point's URI as its group ID and follows
its external reference to the stored stream. The parameter keys must match
these group IDs exactly.

```python
sensor_query = """
PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
SELECT ?stream ?group WHERE {
    VALUES ?group { <urn:parameter-demo:sensor-a> <urn:parameter-demo:sensor-b> }
    ?group ref:hasExternalReference ?stream .
}
"""


def threshold_app(limit_a, limit_b):
    return aq.App(
        name="parameterized-high",
        inputs={"signal": aq.Selection(
            sensor_query,
            # Map the selected sensor groups to their own limits.
            parameters={
                "urn:parameter-demo:sensor-a": {"limit": limit_a},
                "urn:parameter-demo:sensor-b": {"limit": limit_b},
            },
        )},
        outputs={"high": aq.Output()},
        function=aq.PythonFunction.from_file(
            Path("parameterized_threshold.py"), "threshold"
        ),
        # Check each sensor at each of its sample timestamps.
        window=aq.Pointwise(),
    )


app = threshold_app(limit_a=0.5, limit_b=0.8)
client.deploy_app(app)
```

This deploys one app with two groups and one output stream per group, using
the same calculation file. The group parameters arrive in
`context["parameters"]`. Acquirium also supplies the group key there as
`context["parameters"]["group"]`, such as `("urn:parameter-demo:sensor-a",)`.
The group key is always a tuple, including when it has one item.
A row's `context` column contains metadata from the input selection.

Only groups listed in the `parameters` mapping are selected. Leaving a group
out skips it and produces a deployment warning; it does not assign a default
limit. A selected group must supply the keys that the function reads.

## Results by sensor

```python
high = client.query().measurement(alias="high", app=app.name)
# compact=False keeps the full ref_uri identifying each output stream.
print(high.dataframe(shape="narrow", compact=False).select(
    "time", "ref_uri", "value_numeric"
))
```

There are three result rows. The table below abbreviates the reference URIs
as their sensor groups:

| Time (UTC, January 1) | Group | value_numeric |
| --- | --- | ---: |
| 00:01 | sensor-a | 0.7 |
| 00:02 | sensor-a | 0.9 |
| 00:02 | sensor-b | 0.9 |

To see which reference belongs to each group, inspect the output metadata:

```python
print(client.client.sparql_query("""
PREFIX acq: <urn:acquirium#>
PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
SELECT ?group ?stream WHERE {
    ?point acq:producedBy "parameterized-high" ;
           acq:outputGroup ?group ;
           ref:hasExternalReference ?stream .
}
"""))
```

## Parameter updates and recalculation

```python
client.deploy_app(threshold_app(limit_a=0.5, limit_b=0.6))
print(high.dataframe(shape="narrow", compact=False).select(
    "time", "ref_uri", "value_numeric"
))
```

Sensor B now also emits 0.7 at 00:01, so there are four result rows. Sensor A's
results stay the same. Redeploying with a changed group parameter recomputes
that group's stored history, not just future readings. Output identities stay
the same because the app name, groups, and port have not changed.

For this simple operation, the built-in threshold can replace the Python
function. In an app declaration using the same selection and parameters, use:

```python
native_threshold = aq.Native.threshold("high", above=aq.Parameter("limit"))
```

Use that value as the app's `function`. Keeping the Python version here shows
how to read settings in more involved algorithms, such as the
[multi-output cleaner](clean-data-with-multiple-outputs.md).

```python
aq.shutdown()
```
