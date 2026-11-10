---
title: Your first app
---

A driver produces readings; an app derives another stream from them. We will
generate a sine wave, write a Python function that keeps values above 0.5, and
deploy it through the client API.

## Start the input stream

From the repository root, with the [development environment](getting-started.md)
installed:

```sh
uv run --locked acquirium server --config examples/transformation/acquirium.toml
```

This starts the sine-wave driver. Leave the server running while you work in a
second terminal or notebook. Stop it with Ctrl-C when you finish.

The [driver](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/transformation/publish.py) uses the same mechanisms
as [your first driver](first-driver.md):

```python
import math
from acquirium import PollingIngestDriver

class SineWaveDriver(PollingIngestDriver):
    def setup(self):
        self.phase = 0.0
        self.declare(
            "signal", point_uri="urn:example:sine-wave", value_kind="numeric"
        )

    def read(self):
        self.add("signal", math.sin(self.phase))
        self.phase += math.pi / 4
```

The config supplies `source_id="sine-wave"` and a one-second interval. The
server calls `read()` on that schedule; the polling base handles timestamps,
buffering, and insertion. The wave completes a cycle every eight readings.

## Read what the driver produces

Connect to the server from Python:

```python
import acquirium as aq

client = aq.Acquirium(server_url="127.0.0.1", server_port=8000)
readings = client.query().measurement(uri="urn:example:sine-wave").dataframe(shape="narrow")
readings.select("time", "value_numeric").tail(4)
```

Here are the first few readings from a run, showing just time and value. Your
timestamps will differ:

```text
shape: (4, 2)
┌────────────────────────────────┬───────────────┐
│ time                           ┆ value_numeric │
│ ---                            ┆ ---           │
│ datetime[ns, UTC]              ┆ f64           │
╞════════════════════════════════╪═══════════════╡
│ 2026-11-08 17:46:10.768455 UTC ┆ 0.0           │
│ 2026-11-08 17:46:11.875366 UTC ┆ 0.707107      │
│ 2026-11-08 17:46:12.897379 UTC ┆ 1.0           │
│ 2026-11-08 17:46:13.913289 UTC ┆ 0.707107      │
└────────────────────────────────┴───────────────┘
```

The `value_numeric` column follows the wave: approximately 0, 0.707, 1, 0.707,
0, −0.707, −1, −0.707, then repeats. Rerun the query to fetch newer readings.
We will keep the three readings above 0.5 in each cycle, with their original
values and timestamps.

## What the function receives

Acquirium calls your function with two arguments: `frame` and `context`.

`frame` is a Polars dataframe of input readings. We will process one reading at
a time, so this example receives **one row with eight columns**. Here is that
row laid out vertically; long metadata strings are shortened:

| Column | Value in this row |
| --- | --- |
| `role` | `"wave"` |
| `stream` | `"urn:acquirium#7cf58971-2f06-503c-83b5-6da39e9177b9"` |
| `event_time_ns` | `1794159971875366000` |
| `sample_key` | `0` |
| `value` | `0.7071067811865475` |
| `text` | `null` |
| `quality` | `0` |
| `context` | `'{"ext0":"urn:acquirium#…","extname0":"signal",…}'` |

The number we want is in `value`. `frame["value"][0]` reads that number from the
first row. The other columns identify the reading and describe its source.

The separate `context` argument is a dictionary describing the calculation.
Its `window_ns` field tells us which timestamp the result belongs to:

```python
# The relevant part of context:
{"window_ns": 1794159971875366000}
```

Here it is the same timestamp as the input reading. The driver supplied the
reading's time; the calculation keeps it.

## What the function returns

The function returns a dictionary of output dataframes. Our app has one output,
called `"high"`.

If the reading is above 0.5, we want a one-row dataframe containing that value.
`emit(context, value)` creates this dataframe: it takes the timestamp from
`context` and puts the number in the `value` column. For the reading above:

```text
shape: (1, 5)
┌─────────────────────┬────────────┬──────────┬──────┬─────────┐
│ event_time_ns       ┆ sample_key ┆ value    ┆ text ┆ quality │
│ ---                 ┆ ---        ┆ ---      ┆ ---  ┆ ---     │
│ i64                 ┆ i64        ┆ f64      ┆ str  ┆ u32     │
╞═════════════════════╪════════════╪══════════╪══════╪═════════╡
│ 1794159971875366000 ┆ 0          ┆ 0.707107 ┆ null ┆ 0       │
└─────────────────────┴────────────┴──────────┴──────┴─────────┘
```

`empty()` creates a dataframe with the same columns and no rows. We use that
when the reading is below the limit, or when an input has been deleted.
Returning it clears any earlier result at that timestamp.

## Write the calculation

With those input and output shapes in mind, the function is:

```python
from escapement import emit, empty

def threshold(frame, context):
    if frame.is_empty():
        return {"high": empty()}

    value = frame["value"][0]
    if value > 0.5:
        result = emit(context, value)
    else:
        result = empty()

    return {"high": result}
```

Save it in `examples/transformation/sine_threshold.py`. The last line returns
our result dataframe under the output name `"high"`.

## Connect the function to the stream

Back in your Python session, select the input and describe the app:

```python
from pathlib import Path

query = client.query().measurement(alias="wave", uri="urn:example:sine-wave")
app = aq.App(
    name="high-wave",
    inputs=[aq.Selection.from_query(query, alias="wave")],
    outputs={"high": aq.Output()},
    function=aq.PythonFunction.from_file(
        Path("examples/transformation/sine_threshold.py"), "threshold"
    ),
    window=aq.Pointwise(),
)
```

The selection supplies the stream's readings. `Pointwise()` asks for a separate
calculation at each timestamp. The output declaration's `"high"` must match the
port returned by the function.

`PythonFunction.from_file` names the calculation file and entrypoint. The file
must be available on the server; this local example shares its filesystem with
your Python session.

## Deploy and read the result

```python
client.deploy_app(app)
high = client.query().measurement(app="high-wave").dataframe(shape="narrow")
high.select("time", "value_numeric").tail(3)
```

For the readings above, the result is:

```text
shape: (3, 2)
┌────────────────────────────────┬───────────────┐
│ time                           ┆ value_numeric │
│ ---                            ┆ ---           │
│ datetime[ns, UTC]              ┆ f64           │
╞════════════════════════════════╪═══════════════╡
│ 2026-11-08 17:46:11.875366 UTC ┆ 0.707107      │
│ 2026-11-08 17:46:12.897379 UTC ┆ 1.0           │
│ 2026-11-08 17:46:13.913289 UTC ┆ 0.707107      │
└────────────────────────────────┴───────────────┘
```

Deployment calculates results for readings already stored, then keeps them
current as the driver adds more. The derived stream contains values above 0.5,
with no rows for the lower parts of the wave. Read it through the same query
interface as the source stream.

To change the calculation, save the revised function at a new path and deploy
an app pointing to it. Acquirium recalculates its stored history. The
[Python calculation guide](../how-to/write-soft-sensors.md) covers updating code
and choosing larger windows.

## Load the declaration at startup

You can also keep the declaration in a file. The companion
[threshold_app.py](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/transformation/threshold_app.py) packages the
same declaration as `high_wave(client, parameters)`, a factory that returns an
`App`.

Add it to `examples/transformation/acquirium.toml`:

```toml
[[apps]]
spec = "threshold_app.py:high_wave"
```

Restart Acquirium. The server calls the factory and deploys the returned app
alongside the driver.

Next, [follow a turbidity alarm](turbidity.md) to combine several apps, work with
windows, and see how late readings change a downstream alarm.
