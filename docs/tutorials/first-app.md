---
title: Your first app
---

A probe reports water temperature in Fahrenheit. We want a Celsius stream that
stays current, including when somebody corrects an old reading. An app connects
the input stream to that calculation.

## Run the example

From the repository root, with the [development environment](getting-started.md)
installed:

```sh
uv run --locked python examples/transformation/publish.py
```

The script starts a temporary server, converts three readings, corrects one,
and shuts down. It checks these results:

```text
68, 77, 86 °F become [20.0, 25.0, 30.0] °C
Correct 77 °F to 68 °F: [20.0, 20.0, 30.0] °C
```

Let's walk through the calls that produce them. The complete code is in
[publish.py](../../examples/transformation/publish.py) and
[temperature_conversion.py](../../examples/transformation/temperature_conversion.py).

## Register the input

For the walkthrough, start a local client and describe the probe:

```python
import acquirium as aq

client = aq.init(data_dir="/tmp/acquirium-first-app", exact_only=True)
client.register_streams([{
    "source_id": "temperature-example",
    "ref_name": "fahrenheit",
    "point_uri": "urn:example:temperature:fahrenheit",
    "unit": "http://qudt.org/vocab/unit/DEG_F",
    "value_kind": "numeric",
}])
```

The source and reference name identify the stream when writing readings. The
point URI and unit let us find it through a query. We do not need a plant model
for this example.

## Write the calculation

The calculation in `temperature_conversion.py` is:

```python
from escapement import emit, empty

def convert(frame, context):
    if frame.is_empty():
        return {"celsius": empty()}
    fahrenheit = frame["value"][0]
    return {"celsius": emit(context, (fahrenheit - 32) * 5 / 9)}
```

The frame contains the input readings for one calculation. Here we use one
reading at a time. `emit` creates an output at the same timestamp, and
`"celsius"` names the output port. If the input is deleted, `empty()` removes
the corresponding output.

## Connect it to the input

Back in the calling script, select Fahrenheit streams and give their readings
to `convert`:

```python
from pathlib import Path
import acquirium as aq

query = client.query().measurement(
    alias="temperature", unit="http://qudt.org/vocab/unit/DEG_F"
)
app = aq.App(
    name="fahrenheit-to-celsius",
    inputs=[aq.Selection.from_query(query, alias="temperature")],
    outputs={"celsius": aq.Output(unit="http://qudt.org/vocab/unit/DEG_C")},
    function=aq.PythonFunction.from_file(
        Path("examples/transformation/temperature_conversion.py"), "convert"
    ),
    window=aq.Pointwise(),
)
client.deploy_app(app)
```

`Pointwise` runs the calculation separately for each timestamp in each selected
stream. The output's unit describes the result; the arithmetic in `convert`
performs the conversion. Deploying the app also calculates results for any
readings already stored.

The function file must be available on the server. In this local example the
script and server share a filesystem.

## Insert and read

Write a reading, then wait for its result to be published:

```python
from datetime import datetime, timezone

when = datetime(2026, 1, 1, tzinfo=timezone.utc)
response = client.insert_timeseries("temperature-example", "fahrenheit", [(when, 68)])
client.wait(response["receipts"][0])
```

The receipt identifies the write. Waiting ensures that both the input and its
derived result are ready to read.

Find the output by the app that produced it:

```python
result = client.query().measurement(app="fahrenheit-to-celsius")
result.dataframe(shape="narrow")
```

The `value_numeric` column contains 20.0. Derived streams use the same query
interface as source streams.

## Correct a reading

Write a different value at the same source timestamp:

```python
response = client.insert_timeseries("temperature-example", "fahrenheit", [(when, 86)])
client.wait(response["receipts"][0])
result.dataframe(shape="narrow")
```

The Celsius value is now 30.0. We changed the source reading; the app updated
the derived reading without another deploy or a separate backfill. Call
`aq.shutdown()` when you finish the walkthrough.

Next, [follow a turbidity alarm](turbidity.md) to combine several apps, work with
windows, and see how a correction changes a downstream alarm.
