---
title: Build your first soft sensor
---

A soft sensor uses existing readings to calculate a new stream. Here you'll
build a threshold detector that records a sine-wave reading whenever it
exceeds 0.5. The driver generates the input, a Python function checks each
reading, and an app declaration connects the two.

You'll need a checkout of the [Acquirium repository](https://github.com/DataDrivenCPS/acquirium)
and uv. Run the commands from the repository root.

## Start the server

In the first terminal, start Acquirium with the example configuration.

```sh
uv run --locked acquirium server --config examples/transformation/acquirium.toml
```

The configuration starts the sine-wave driver in `examples/transformation/publish.py`.
Wait for `Started config driver 'SineWaveDriver'` in the server log. The
driver writes one reading per second. Leave the server running while you work.

## Read the input signal

In a second terminal, run `uv run --locked python` and connect to the server.

```python
import acquirium as aq

client = aq.Acquirium(server_url="127.0.0.1", server_port=8000)
wave = client.query().measurement(alias="wave", uri="urn:example:sine-wave")
print(wave.dataframe().tail(8))
```

Once the driver has collected eight readings, you should see the signal rise
above 0.5 and fall again. If the table has fewer rows, wait a few seconds
and run the last two lines again. The timestamps below are from one run.

```text
shape: (8, 2)
┌────────────────────────────────┬─────────────────────┐
│ time                           ┆ wave__ns1:sine-wave │
│ ---                            ┆ ---                 │
│ datetime[ns, UTC]              ┆ f64                 │
╞════════════════════════════════╪═════════════════════╡
│ 2026-11-27 22:37:46.337206 UTC ┆ 0.0                 │
│ 2026-11-27 22:37:47.365859 UTC ┆ 0.707107            │
│ 2026-11-27 22:37:48.385850 UTC ┆ 1.0                 │
│ 2026-11-27 22:37:49.409411 UTC ┆ 0.707107            │
│ 2026-11-27 22:37:50.432213 UTC ┆ 1.2246e-16          │
│ 2026-11-27 22:37:51.453819 UTC ┆ -0.707107           │
│ 2026-11-27 22:37:52.477383 UTC ┆ -1.0                │
│ 2026-11-27 22:37:53.497894 UTC ┆ -0.707107           │
└────────────────────────────────┴─────────────────────┘
```

## Define the threshold

Open `examples/transformation/sine_threshold.py`. This file contains the
calculation the server will run.

```{literalinclude} ../../../examples/transformation/sine_threshold.py
:language: python
:pyobject: threshold
```

The `value` column contains the numeric reading. The function returns that
reading on the `high` output port when it exceeds 0.5. Returning `None`
leaves the output empty at that timestamp.

## Connect the calculation to the stream

In the same Python session, declare the app using the `wave` query you
just ran.

```python
app = aq.App(
    name="high-wave",
    inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
    outputs={"high": aq.Output()},
    function=aq.PythonFunction.from_file(
        "examples/transformation/sine_threshold.py", "threshold"
    ),
    window=aq.Pointwise(),
)
```

`Selection.from_query` selects the same sine-wave measurement you read earlier.
`PythonFunction.from_file` points to the calculation in the separate file.
The `high` port matches the function's return key. `Pointwise()` runs the
function at each input timestamp and publishes any result at that same time.

## Deploy and read the output

Deploy the app and query the new stream by app name.

```python
client.deploy_app(app)

high = client.query().measurement(alias="high", app=app.name)
print(high.dataframe().tail(4))
```

```text
shape: (3, 2)
┌────────────────────────────────┬──────────────────────┐
│ time                           ┆ high__high-wave high │
│ ---                            ┆ ---                  │
│ datetime[ns, UTC]              ┆ f64                  │
╞════════════════════════════════╪══════════════════════╡
│ 2026-11-27 22:37:47.365859 UTC ┆ 0.707107             │
│ 2026-11-27 22:37:48.385850 UTC ┆ 1.0                  │
│ 2026-11-27 22:37:49.409411 UTC ┆ 0.707107             │
└────────────────────────────────┴──────────────────────┘
```

Compare the timestamps with the input table. The three readings above 0.5
appear at their original times. Deployment checks the readings already stored,
and the server keeps evaluating new readings after you exit Python. Run the
last Python line again a few seconds later to see new results. When you're
finished, stop the server with Ctrl-C in the first terminal.

## Next steps

- [Build a turbidity alarm pipeline](../turbidity.md) to clean readings and detect when they stay above a threshold.
- [Write a soft sensor](../../how-to/write-soft-sensors.md) to use this server to calculate a windowed mean and chain another detector.
