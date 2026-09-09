---
title: Your first app
---

Drivers load sensor readings into Acquirium. Apps use those readings to
calculate derived streams, such as converted temperatures, averages, or anomaly
flags. In this tutorial, you will run an app that converts Celsius readings to
Fahrenheit, then adapt the approach to smooth readings from multiple sensors.

The full working files are in `examples/transformation/` in the repository.

## 1. Declare the app

An app is a Python class with three parts: a query that finds its inputs, a
declaration of its outputs, and the calculation itself.

```python
# temperature_conversion.py
import polars as pl
import acquirium as aq

INPUT_SOURCE = "temperature-example-input"
OUTPUT_POINT = "urn:example:temperature:fahrenheit"


class CelsiusToFahrenheit(aq.App):
    name = "celsius-to-fahrenheit"
    grouping = "all_matches"
    backfill = True
    outputs = {
        "fahrenheit": aq.output.named(
            "fahrenheit",
            value_kind="numeric",
            point_uri=OUTPUT_POINT,
            unit="http://qudt.org/vocab/unit/DEG_F",
        ),
    }

    def build_query(self, plant):
        return plant.query().measurement(alias="temperature", data_source=INPUT_SOURCE)

    def transform(self, inputs, output, context):
        celsius = inputs["temperature"].df()
        output["fahrenheit"] = celsius.select(
            "time", (pl.col("value") * 9.0 / 5.0 + 32.0).alias("value")
        )
```

The query selects measurement streams from `INPUT_SOURCE` and exposes their
readings under the alias `temperature`. This example publishes one input
stream, so `grouping = "all_matches"` gives the transform that stream's readings
in a single call. Every app must explicitly choose its grouping mode.

The `outputs` declaration gives the Fahrenheit stream the name `fahrenheit`
within this app and attaches it to `OUTPUT_POINT`. In `transform`, the app reads
a dataframe, applies the conversion, and assigns the `time` and `value` columns
to that output. Setting `backfill = True` also processes readings already in
storage when the app first becomes active.

Acquirium selects the interval to recompute and saves the output together with
its processing progress in one transaction. The transform only needs to
calculate results for the input window it receives.

## 2. Check it without saving anything

Before deploying, run it as a dry run against the data already stored:

```bash
uv run acquirium app check ./temperature_conversion.py:CelsiusToFahrenheit
```

The check reports the matched streams, computed values, and any transform
errors without saving a deployment or output data. It is useful for checking
an app against an existing dataset before activating it.

## 3. Deploy it with the server

The example config deploys the class at startup:

```toml
[[apps]]
spec = "./temperature_conversion.py:CelsiusToFahrenheit"
```

Start the server in one terminal:

```bash
uv run acquirium server --config examples/transformation/acquirium.toml
```

## 4. Feed it and watch the derived stream appear

In another terminal:

```bash
uv run python examples/transformation/publish.py
```

The script registers a Celsius input stream, writes six samples, and polls the
output point until the derived Fahrenheit values appear. Further writes to the
Celsius stream cause the app to recompute the affected interval. Correcting an
earlier Celsius reading also updates its Fahrenheit result.

## 5. Find what it produced

You can query the derived stream through the same API used for sensor data.
Acquirium records the producing app on each derived point, so the `app` filter
selects this calculation's output:

```python
from acquirium import Acquirium

acq = Acquirium(server_url="127.0.0.1", server_port=8000)
acq.query().measurement(alias="f", app="celsius-to-fahrenheit").data()
```

The producing-app metadata is added automatically. Other metadata, such as a
unit, label, or quantity kind, comes from the output declaration. For example,
declaring `quantity_kind` makes the output discoverable by queries for that
quantity kind alongside measured streams. See the
[apps guide](../apps.md#convert-each-sensor) for an example.

## 6. Make it react to every sensor

To calculate a separate result for every temperature sensor, use
`grouping = "per_match"`. The following app smooths each sensor's readings with
a ten-minute rolling average:

```python
class TemperatureSmoother(aq.App):
    name = "temperature-smoother"
    grouping = "per_match"
    lookback = "10m"
    outputs = {"smooth": aq.output.stream(value_kind="numeric", unit="http://qudt.org/vocab/unit/DEG_C")}

    def build_query(self, plant):
        return plant.query().measurement(alias="temperature", quantity_kind="temperature")

    def transform(self, inputs, output, context):
        temperature = inputs["temperature"].in_unit("DEG_C").df()
        if temperature.is_empty():
            return
        output["smooth"] = temperature.sort("time").select(
            "time", pl.col("value").rolling_mean_by("time", window_size="10m").alias("value")
        )
```

With `grouping = "per_match"`, each query match gets a separate call to
`transform`. The `output.stream` declaration derives a stable output name from
that match's inputs, so adding another sensor produces another smoothed stream
without requiring you to name it. An app that combines several sensors would
instead declare `grouping = "all_matches"`.

The rolling mean needs earlier readings to calculate values near the beginning
of the output interval. `lookback = "10m"` tells the runtime to load that context
and to revisit later outputs when an earlier reading is corrected. The runtime
publishes only rows within the selected output interval, even if the transform
returns additional rows from its input context.

Finally, `.in_unit("DEG_C")` converts the readings before calculating the mean.
This allows sensors reporting compatible units to use the same transform. A
missing or incompatible unit raises an error.

## Where to go next

- [Apps](../apps.md) — the walkthrough of lookback, scheduling, output identities,
  and deployment.
- [App reference](../reference/apps.md) — the complete class contract.
- [How it works](../reference/apps.md#how-it-works) — what the server does
  after you deploy: windows, transactions, recovery.
