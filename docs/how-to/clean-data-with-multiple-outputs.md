---
title: Data cleaning with multiple outputs
---

Put several cleaning steps in one Python function when you want to keep the
algorithm together and publish intermediate results for inspection. This
example produces four streams from one normalized sensor:

- `thresholded`: the original reading, if it is between 0 and 1.
- `imputed`: the reading at this minute, filling short gaps by interpolation.
- `downsampled`: a two-sample mean, emitted every second minute.
- `moving_average`: a trailing three-sample mean of the imputed signal.

This example expects samples on UTC minute boundaries, with at most one
reading per minute. A missing minute and an out-of-range reading are both gaps;
only a single missing minute between two valid readings is filled.

All four outputs belong to one app and share its window. The function emits
one value per port at the current window timestamp, or no row. It does not
return an entire historical series from a single call.

The input column `time` contains sample timestamps as UTC datetimes.
`context["output_time"]` is the minute whose result is being calculated. These are
data timestamps; the calculation does not use the time the server received a
sample. See [timestamps in the examples](soft-sensor-examples.md#timestamp-conventions).

## Cleaning function

Save this as `cleaning_function.py` in your working directory:

```python
from datetime import timedelta

import polars as pl
from acquirium.functions import emit


def clean(frame, context):
    output_time = context["output_time"]
    ports = ("thresholded", "imputed", "downsampled", "moving_average")
    if frame.is_empty():
        return {port: emit(context, None) for port in ports}

    samples = frame.select("time", "value")
    times = samples["time"]
    if not times.min() <= output_time <= times.max():
        return {port: emit(context, None) for port in ports}

    # Invalid readings become null, just like a missing minute on the grid.
    samples = samples.with_columns(
        pl.when(pl.col("value").is_between(0.0, 1.0))
        .then(pl.col("value")).otherwise(None).alias("value")
    )
    # Build the five-minute grid, including one future minute for interpolation.
    grid = pl.DataFrame({"time": pl.datetime_range(
        output_time - timedelta(minutes=3), output_time + timedelta(minutes=1),
        interval="1m", time_unit="us", time_zone="UTC", eager=True,
    )})
    values = grid.join(samples, on="time", how="left").sort("time")["value"]

    # Fill one missing minute with the mean of its two valid neighbors.
    # shift(1) is the preceding minute; shift(-1) is the following minute.
    # Two consecutive gaps stay null because neither has two valid neighbors.
    imputed = values.fill_null((values.shift(1) + values.shift(-1)) / 2)

    # The five grid rows are minutes -3, -2, -1, 0, +1 relative to the output.
    # Summaries use only minutes -2, -1, 0; +1 is for interpolation alone.
    recent = imputed.slice(1, 3)
    downsampled = None
    if output_time.minute % 2 == 0:
        downsampled = recent.tail(2).mean()  # Emit only on even UTC minutes.

    # emit() stamps every nonempty result at output_time, through context.
    return {
        "thresholded": emit(context, values[-2]),
        "imputed": emit(context, recent[-1]),
        "downsampled": emit(context, downsampled),
        "moving_average": emit(context, recent.mean()),
    }
```

The means use available values, so the first few windows can produce results
before a full history exists. Longer unfilled gaps are also skipped by the
means. For a stricter policy, require all two or three samples before emitting
the corresponding mean.

## Sample readings

Run the remaining blocks in order in a Python session beside the calculation
file. With the repository's development environment, start it with
`uv run python`.

```python
from datetime import timedelta
from pathlib import Path
import acquirium as aq

MINUTE = 60 * 1_000_000_000
start = aq.to_timestamp("2026-01-01T00:00:00 UTC")
client = aq.init(data_dir="./multi-output-cleaning-data")
client.register_datasource("multi-output-cleaning")
client.register_streams([{
    "source_id": "multi-output-cleaning",
    "ref_name": "signal",
    "point_uri": "urn:multi-output-cleaning:signal",
    "value_kind": "numeric",
}])
# Insert actual sample times, spaced exactly one minute apart.
client.insert_timeseries("multi-output-cleaning", "signal", [
    (start + timedelta(minutes=minute), value)
    for minute, value in enumerate([0.2, 9.0, 0.8, 0.4, 0.6])
])
raw = client.query().measurement(
    alias="raw", uri="urn:multi-output-cleaning:signal"
)
print(raw.dataframe())
```

The input has five rows and two columns, `time` and `raw`. The values are
0.2, 9.0, 0.8, 0.4, and 0.6 at successive minutes starting at midnight.
The reading 9.0 will be rejected and replaced with 0.5 by interpolation.

## App declaration and output ports

```python
cleaner = aq.App(
    name="signal-cleaner",
    inputs=[aq.Selection.from_query(raw, alias="raw")],
    outputs={
        "thresholded": aq.Output(label="Valid signal"),
        "imputed": aq.Output(label="Gap-filled signal"),
        "downsampled": aq.Output(label="Two-minute signal mean"),
        "moving_average": aq.Output(label="Three-sample moving average"),
    },
    function=aq.PythonFunction.from_file(Path("cleaning_function.py"), "clean"),
    window=aq.Rolling(
        lookback_ns=3 * MINUTE,
        step_ns=MINUTE,
        lookahead_ns=MINUTE,
    ),
)
client.deploy_app(cleaner)
```

The three-minute lookback is deliberate: the moving average needs the sample
two minutes ago, whose interpolation may need the sample three minutes ago.
The one-minute lookahead lets us fill the current minute using its next
neighbor. The function reads only this declared window and uses no external
state. Lookahead results can change as later samples arrive.

The output keys must match the function's returned keys. Labels make the
streams easy to find; on physical measurements, also declare the appropriate
`unit` and `quantity_kind` on each output.

## Output stream queries

```python
table = raw.dataframe()
for port, output in cleaner.outputs.items():
    # app selects this calculation; label selects one of its four ports.
    query = client.query().measurement(
        alias=port, app=cleaner.name, label=output.label
    )
    table = table.join(query.dataframe(), on="time", how="left")
print(table.sort("time"))
```

The joined result has five rows and six columns. Values below are rounded to
four decimal places; a dash means the output has no row at that minute.

| Time (UTC, January 1) | raw | thresholded | imputed | downsampled | moving_average |
| --- | ---: | ---: | ---: | ---: | ---: |
| 00:00 | 0.2 | 0.2 | 0.2 | 0.2 | 0.2 |
| 00:01 | 9.0 | — | 0.5 | — | 0.35 |
| 00:02 | 0.8 | 0.8 | 0.8 | 0.65 | 0.5 |
| 00:03 | 0.4 | 0.4 | 0.4 | — | 0.5667 |
| 00:04 | 0.6 | 0.6 | 0.6 | 0.5 | 0.6 |

The downsampled stream emits on even UTC minutes. At 00:02 it averages the
imputed values at 00:01 and 00:02: `(0.5 + 0.8) / 2 = 0.65`.
The moving average at that time also includes 00:00, giving 0.5.

## Downstream app inputs

The intermediate streams are usable even though their calculations live in
one function. A downstream app can select the `imputed` port directly:

```python
high = aq.App(
    name="cleaned-signal-high",
    inputs=[aq.Selection.outputs_of(cleaner, "imputed", role="signal")],
    outputs={"high": aq.Output()},
    function=aq.Native.threshold("high", above=0.5),
)
client.deploy_app(high)
print(client.query().measurement(alias="high", app=high.name).dataframe())
```

This produces two rows: 0.8 at 00:02 and 0.6 at 00:04. Late readings and
corrections update all affected cleaner outputs and then their consumers.
Acquirium tracks the cleaner as one calculation; document the internal steps
in Python, as above, so readers can understand how each output was obtained.

```python
aq.shutdown()
```

See [build a cleaning pipeline](chain-cleaning-apps.md) for separate apps per
stage, or [reuse a function with different parameters](parameterize-soft-sensors.md)
to make the limits configurable.
