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
It writes one reading per second. Leave the server running while you work.

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
and run the last line again.

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

Open `examples/transformation/threshold_app.py`. It tells Acquirium which
stream to read, which function to run, and which output to publish.

```{literalinclude} ../../../examples/transformation/threshold_app.py
:language: python
```

`Selection.from_query` selects the same sine-wave measurement you read earlier.
`PythonFunction.from_file` points to the calculation in the separate file.
The `high` port matches the function's return key. `Pointwise()` runs the
function at each input timestamp and publishes any result at that same time.

## Deploy and read the output

Back in the Python session, import the declaration, deploy it, and query the
new stream by app name.

```python
from examples.transformation.threshold_app import high_wave

app = high_wave(client, {})
client.deploy_app(app)

high = client.query().measurement(alias="high", app=app.name)
print(high.dataframe().tail(4))
```

The derived stream contains readings such as 0.707 and 1.0. Each one has
the timestamp of its source reading. Deployment checks the readings already
stored, and the server keeps evaluating new readings after you exit Python.
Run the last line again a few seconds later to see new results. When you're
finished, stop the server with Ctrl-C in the first terminal.

## Next steps

- [Build a turbidity alarm pipeline](../turbidity.md) to clean readings and detect when they stay above a threshold.
- [Write a soft sensor](../../how-to/write-soft-sensors.md) to use this server to calculate a windowed mean and chain another detector.
