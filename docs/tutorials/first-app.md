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

This function leaves `frame` unannotated, so it receives a Polars dataframe.
Annotating it with `pd.DataFrame` selects Arrow-backed pandas instead; see the
[dataframe type reference](../reference/apps.md#dataframe-type).

Our app looks at one reading at a time, so each call gets one row.
Here is such a row, turned on its side,
with long strings shortened:

| Column | Value |
| --- | --- |
| `role` | `"wave"` |
| `stream` | `"urn:acquirium#7cf58971-2f06-503c-83b5-6da39e9177b9"` |
| `time` | `2026-11-08 17:46:11.875366 UTC` |
| `value` | `0.7071067811865475` |
| `text` | `null` |
| `context` | `'{"ext0":"urn:acquirium#…","extname0":"signal",…}'` |

`value` holds the reading. `time` holds its sample timestamp as a UTC datetime
with microsecond precision. The dataframe's `context`
column contains metadata for the input stream as a JSON string.

### Function context

The second argument, `context`, is a dictionary describing the function call.
`output_time` is the result's timestamp. `operator` contains the app declaration,
including an `outputs` mapping from output names to stream URIs. This app will
declare an output named `"high"`. For the reading above:

- **Output timestamp:** `context["output_time"]`

  - A timezone-aware Python datetime for the result.
  - Value: `2026-11-08 17:46:11.875366 UTC`.

- **Output streams:** `context["operator"]["outputs"]`

  - Maps each declared output name to its stream URI.
  - Here, `"high"` maps to `urn:acquirium#ad846115-4174-570b-af13-e01751c17bd6`.

- **Group key:** `context["parameters"]["group"]`

  - A tuple identifying the group. Here the group is one input stream.
  - Value: `("urn:acquirium#7cf58971-2f06-503c-83b5-6da39e9177b9",)`.

This app processes each sample timestamp separately, so `output_time` equals
the input reading's `time`.

The name `"high"` identifies an output within the app. Each selected input
stream gets its own `"high"` stream with a distinct URI.

Context also contains the window boundaries, `window_start` and `window_end`,
and any configured settings in `parameters`. The
[function context explanation](../explanation/soft-sensor-inputs.md#function-context)
and [app reference](../reference/apps.md#python-functions) describe the other fields.

## Function outputs

An app declares named outputs. This example will declare one output, `"high"`.
The function must return a dictionary mapping each declared name to a dataframe
of results. This example uses Polars.

Each row contains a result's timestamp and value. For the reading above, the
`"high"` dataframe should contain:

| time | value | text |
| --- | ---: | --- |
| 2026-11-08 17:46:11.875366 UTC | 0.7071067811865475 | null |

`time` is the output timestamp from `context["output_time"]`. A numeric result
goes in `value`; `text` is null. The complete
[output schema](../reference/apps.md#python-functions) lists the column types.

### Single-reading helper

When a function computes one reading, `emit(context, value)` is a convenience
function for constructing this one-row dataframe. It fills `time` from
`context["output_time"]` and supplies the other columns shown above.

The function returns that dataframe under the `"high"` key. Acquirium uses
that key to find the destination URI in `context["operator"]["outputs"]` and
stores the returned rows there.

### Empty output

`empty()` constructs a dataframe with the same columns and types, but no rows.
Return it for `"high"` when there should be no reading at this output timestamp.
If an earlier call produced a reading there, Acquirium removes it. This handles
deletions and corrections to 0.5 or below.

### Threshold function

The threshold function chooses between a one-row dataframe and an empty one:

```python
from acquirium.functions import emit, empty


def threshold(frame, context):
    if frame.is_empty():
        return {"high": empty()}

    value = frame["value"][0]
    if value > 0.5:
        # Build one result row at context["output_time"].
        result = emit(context, value)
    else:
        # No result should remain at this timestamp.
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

The app declares `"high"` in `outputs`. The function returns its dataframe
under that same key. `Pointwise()` asks for one call per
timestamp. Windows that span several minutes of readings are the subject
of the [turbidity tutorial](turbidity.md).

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

To change the calculation, edit the file, rebuild the `PythonFunction.from_file(...)`
declaration, and deploy the app again. Acquirium recomputes the stored
history with the new code; [changing the code](../how-to/check-deploy-apps.md#calculation-updates)
explains how the content hash identifies each version. To deploy the app every time the server
starts, list it under `[[apps]]` in the [server configuration](../reference/server-config.md#drivers-and-apps).

The [Turbidity alarm pipeline](turbidity.md) tutorial cleans readings in one
app and alarms on the result in another, and shows how a late reading changes
both.
