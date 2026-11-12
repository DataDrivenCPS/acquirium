---
title: Data cleaning with multiple apps
---

Use separate apps when you want to reuse a cleaning stage or attach another
calculation to its output. This example rejects invalid readings, interpolates
short gaps, and branches off an alarm:

```text
raw → valid → interpolated
                  └→ high
```

Each stage publishes an ordinary stream that you can query. The example uses
a normalized signal whose valid range is 0 through 1, a local server, and four
readings. It needs no driver or additional packages beyond Acquirium.

`frame["time"]` contains the samples' timestamps as UTC datetimes.
`context["output_time"]` is the minute for which we
are calculating an output. For example, the result at 00:01 can use readings
at 00:00 and 00:02. Arrival time does not enter the calculation.

## Invalid reading rejection and interpolation

Save this as `pipeline_functions.py` in your working directory:

```python
from datetime import timedelta

import polars as pl
from acquirium.functions import emit


def reject_invalid(frame, context):
    # Keep only numeric readings in the sensor's valid range, including 0 and 1.
    values = frame.filter(pl.col("value").is_between(0.0, 1.0))["value"]
    # Pointwise has at most one reading here; first() returns None if empty.
    return {"valid": emit(context, values.first())}


def interpolate(frame, context):
    output_time = context["output_time"]
    readings = frame.select("time", "value").drop_nulls("value").sort("time")
    sample_time = pl.col("time")

    # Preserve a valid observation at this minute before trying to fill a gap.
    exact = readings.filter(sample_time == output_time)
    if not exact.is_empty():
        return {"filled": emit(context, exact["value"].first())}

    # Find the nearest valid observation on each side of the missing minute.
    before = readings.filter(sample_time < output_time).tail(1)
    after = readings.filter(sample_time > output_time).head(1)
    value = None
    if not before.is_empty() and not after.is_empty():
        earlier_time, earlier_value = before.row(0)
        later_time, later_value = after.row(0)
        gap = later_time - earlier_time
        if gap <= timedelta(minutes=2):
            # Linear interpolation: move this fraction of the way to the later value.
            fraction = (output_time - earlier_time) / gap
            value = earlier_value + fraction * (later_value - earlier_value)
    return {"filled": emit(context, value)}
```

`emit(context, None)` produces no row and retracts any previous result at that
timestamp. The interpolation function fills only gaps bounded by valid readings
at most two minutes apart. Its lookahead lets a later reading complete a gap;
the result can therefore change when new data arrives.

## Server setup and sample readings

Run these blocks in order in a Python session in the same directory as the
calculation file. With the repository's development environment, use
`uv run python` to start the session.

```python
from datetime import timedelta
from pathlib import Path
import acquirium as aq

MINUTE = 60 * 1_000_000_000
start = aq.to_timestamp("2026-01-01T00:00:00 UTC")
client = aq.init(data_dir="./cleaning-pipeline-data")

client.register_datasource("cleaning-pipeline")
client.register_streams([{
    "source_id": "cleaning-pipeline",
    "ref_name": "signal",
    "point_uri": "urn:cleaning-pipeline:signal",
    "value_kind": "numeric",
}])
# These timestamps describe the samples, even though we insert them together.
client.insert_timeseries("cleaning-pipeline", "signal", [
    (start + timedelta(minutes=minute), value)
    for minute, value in enumerate([0.2, 9.0, 0.8, 0.4])
])
raw = client.query().measurement(
    alias="raw", uri="urn:cleaning-pipeline:signal"
)
print(raw.dataframe())
```

The input has four rows and two columns: `time` and `raw`.

| Time (UTC, January 1) | raw |
| --- | ---: |
| 00:00 | 0.2 |
| 00:01 | 9.0 |
| 00:02 | 0.8 |
| 00:03 | 0.4 |

The value 9.0 represents a bad reading, outside the signal's valid range.

## App connections and deployment

```python
calculations = Path("pipeline_functions.py")
valid = aq.App(
    name="pipeline-valid",
    inputs=[aq.Selection.from_query(raw, alias="raw")],
    outputs={"valid": aq.Output()},
    function=aq.PythonFunction.from_file(calculations, "reject_invalid"),
    window=aq.Pointwise(),
)
filled = aq.App(
    name="pipeline-filled",
    inputs=[aq.Selection.outputs_of(valid, "valid", role="valid")],
    outputs={"filled": aq.Output()},
    function=aq.PythonFunction.from_file(calculations, "interpolate"),
    # Calculate every minute using observations from both sides of that minute.
    window=aq.Rolling(
        lookback_ns=2 * MINUTE,
        step_ns=MINUTE,
        lookahead_ns=2 * MINUTE,
    ),
)
high = aq.App(
    name="pipeline-high",
    inputs=[aq.Selection.outputs_of(filled, "filled", role="filled")],
    outputs={"high": aq.Output()},
    # Keep the filled value only when it is strictly above the alarm limit.
    function=aq.Native.threshold("high", above=0.5),
)

# Deploy upstream apps first, so each downstream port exists when selected.
for app in (valid, filled, high):
    client.deploy_app(app)
```

`Selection.outputs_of` preserves each upstream group. Adding more raw sensors
to the selection would create an independent pipeline for each sensor. The
alarm passes through values strictly above 0.5; a missing alarm row means clear.

## Raw and intermediate stream queries

```python
valid_data = client.query().measurement(alias="valid", app=valid.name)
filled_data = client.query().measurement(alias="filled", app=filled.name)
high_data = client.query().measurement(alias="high", app=high.name)

# Join explicitly selected streams for one view of the example's pipeline.
table = raw.dataframe()
for query in (valid_data, filled_data, high_data):
    table = table.join(query.dataframe(), on="time", how="left")
print(table.sort("time"))
```

The result has four rows and five columns. A dash below means null after the
join: that stream has no row at that timestamp.

| Time (UTC, January 1) | raw | valid | filled | high |
| --- | ---: | ---: | ---: | ---: |
| 00:00 | 0.2 | 0.2 | 0.2 | — |
| 00:01 | 9.0 | — | 0.5 | — |
| 00:02 | 0.8 | 0.8 | 0.8 | 0.8 |
| 00:03 | 0.4 | 0.4 | 0.4 | — |

An application that wants the cleaned sensor should use `filled_data`.
Querying the original point still selects the raw stream. Designating a derived
stream as the authoritative result for that point, and automatically traversing
all its descendants, are future query features; this example selects its
stages explicitly.

## Corrections and downstream recalculation

```python
# Replace the reading at 00:02; this is a correction, not an extra sample.
client.insert_timeseries("cleaning-pipeline", "signal", [
    (start + timedelta(minutes=2), 0.4),
])
print(filled_data.dataframe())
print(high_data.dataframe())
```

The filled stream now has values 0.2, 0.3, 0.4, 0.4 at the same four minutes.
The alarm is empty. Acquirium recalculates the affected windows through the
chain, including the interpolation at 00:01.

```python
aq.shutdown()
```

The local data directory is retained. For a calculation that publishes several
cleaning stages from one function, see [clean data with several outputs](clean-data-with-multiple-outputs.md).
