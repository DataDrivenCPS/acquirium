---
title: Soft sensor app
---

Raw readings are rarely the end of the story. Operators want hourly averages,
alarms that ignore a single noisy reading, and estimates of things no sensor
measures directly. In Acquirium, an **app** computes one stream from others and
keeps the result current: when a reading arrives, the app's output follows,
and when a reading is corrected, the output is corrected too.

We start as small as possible: a driver that generates a sine wave, and an app
that keeps the values above 0.5. No plant model is needed.

## Sine-wave input stream

From the repository root, with the development environment installed:

```sh
uv run --locked acquirium server --config examples/transformation/acquirium.toml
```

This starts a server with one driver. Leave it running, and work in a second
terminal or a notebook. The driver is short:

```python
import math
from acquirium import PollingIngestDriver

class SineWaveDriver(PollingIngestDriver):
    def setup(self):
        self.phase = 0.0
        self.declare("signal", point_uri="urn:example:sine-wave", value_kind="numeric")

    def read(self):
        self.add("signal", math.sin(self.phase))
        self.phase += math.pi / 4
```

The server calls `read()` once a second, as the config asks, and takes care of
timestamps and inserts. The wave completes a cycle every eight readings.

Connect and look at what it has produced:

```python
import acquirium as aq

client = aq.Acquirium(server_url="127.0.0.1", server_port=8000)
wave = client.query().measurement(uri="urn:example:sine-wave")
wave.dataframe(shape="narrow").select("time", "value_numeric").tail(4)
```
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

Your timestamps will differ. The values follow the wave: 0, 0.707, 1, 0.707,
0, −0.707, −1, −0.707, and round again. Of each eight, three are above 0.5, and
those are the ones we want to keep, with their original timestamps.

## Function inputs

An app's calculation is an ordinary Python function. Acquirium calls it with
two arguments, `frame` and `context`.

### Input dataframe

`frame` is a Polars dataframe of input readings. Our app looks at one reading
at a time, so each call gets one row. Here is such a row, turned on its side,
with long strings shortened:

| Column | Value |
| --- | --- |
| `role` | `"wave"` |
| `stream` | `"urn:acquirium#7cf58971-2f06-503c-83b5-6da39e9177b9"` |
| `time` | `2026-11-08 17:46:11.875366 UTC` |
| `sample_key` | `0` |
| `value` | `0.7071067811865475` |
| `text` | `null` |
| `quality` | `0` |
| `context` | `'{"ext0":"urn:acquirium#…","extname0":"signal",…}'` |

`value` holds the reading. `time` holds its sample timestamp as a UTC datetime
with microsecond precision. The dataframe's `context`
column contains metadata for the input stream as a JSON string.

### Function context

The second argument, `context`, is a dictionary describing the function call.
Acquirium provides the result's timestamp in `context["output_time"]`. For the
reading above:

| Field in `context` | Value | Meaning |
| --- | --- | --- |
| `output_time` | `2026-11-08 17:46:11.875366 UTC` | Output timestamp, as a timezone-aware Python datetime. |

This app processes each sample timestamp separately, so `output_time` equals
the input reading's `time`.

## Function outputs

The function returns a dictionary with one dataframe per output. We will call
our single output `"high"`.

Two helpers build those dataframes. `emit(context, value)` makes a one-row
frame holding `value` at the timestamp in `context`. `empty()` makes a frame
with no rows. Returning `empty()` matters more than it looks: if the input
reading is later deleted or corrected below 0.5, an empty result clears
whatever the app wrote at that timestamp before.

So the function is:

```python
from acquirium.functions import emit, empty

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

The example keeps it in `examples/transformation/sine_threshold.py`. It needs
no access to the server and no knowledge of where the data came from, which
makes it easy to test on its own.

## App declaration

The app declaration says which readings go in, which outputs come out, which
function to run, and how to slice the input into calls:

```python
from pathlib import Path

wave = client.query().measurement(alias="wave", uri="urn:example:sine-wave")
app = aq.App(
    name="high-wave",
    inputs=[aq.Selection.from_query(wave, alias="wave")],
    outputs={"high": aq.Output()},
    function=aq.PythonFunction.from_file(
        Path("examples/transformation/sine_threshold.py"), "threshold"
    ),
    window=aq.Pointwise(),
)
```

The input is the query you already ran, and the output's name, `"high"`,
matches the key the function returns. `Pointwise()` asks for one call per
timestamp. Larger windows, such as a minute of readings at a time, are the
subject of the [turbidity tutorial](turbidity.md).

The server runs the function, so the path must exist on the server. Here the
server and your session share a filesystem.

## Deployment and output queries

```python
client.deploy_app(app)
high = client.query().measurement(app="high-wave")
high.dataframe(shape="narrow").select("time", "value_numeric").tail(3)
```
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

Deploying did two things. It computed results for every reading already
stored, and it will keep computing them as the driver adds more. Run the query
again in a few seconds and there are more rows. The derived stream is an
ordinary stream: you query it like any other.

To change the calculation, save the new version of the function at a new path
and deploy the app again with that path. Acquirium recomputes the stored
history with the new code; [changing the code](../how-to/check-deploy-apps.md#calculation-updates)
explains why the path must change. To deploy the app every time the server
starts, list it under `[[apps]]` in the [server configuration](../reference/server-config.md#drivers-and-apps).

The [Turbidity alarm pipeline](turbidity.md) tutorial follows a chain of apps
and shows how a late reading changes downstream results.
