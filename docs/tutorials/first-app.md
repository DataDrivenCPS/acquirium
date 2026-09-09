---
title: Your first app
---

Drivers load sensor readings into Acquirium. Apps use those readings to
calculate derived streams, such as converted temperatures, averages, or anomaly
flags. In this tutorial, you will run an app that converts Fahrenheit readings to
Celsius, then adapt the approach to smooth readings from multiple sensors.

The full working files are in `examples/transformation/` in the repository.

## 1. Declare the app

An app is a Python class with three parts: a query that finds its inputs, a
declaration of its outputs, and the calculation itself.

```python
# temperature_conversion.py
import polars as pl

import acquirium as aq


class FahrenheitToCelsius(aq.App):
    name = "fahrenheit-to-celsius"
    grouping = "per_match"
    backfill = True
    outputs = {
        "celsius": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
        ),
    }

    def build_query(self, plant):
        return plant.query().measurement(alias="temperature", unit="DEG_F")

    def transform(self, inputs, output, context):
        fahrenheit = inputs["temperature"].df()
        output["celsius"] = fahrenheit.select(
            "time", ((pl.col("value") - 32.0) * 5.0 / 9.0).alias("value")
        )
```

The query selects every measurement stream whose unit is `DEG_F`, regardless
of its data source, and exposes the readings under the alias `temperature`.
With `grouping = "per_match"`, each matched stream gets a separate call to
`transform`. Every app must explicitly choose its grouping mode.

The `output.stream` declaration gives each input its own derived Celsius
stream. In `transform`, the app reads a dataframe, applies the Fahrenheit-to-
Celsius conversion, and assigns the `time` and `value` columns to that output.
The outputs declare `DEG_C`, so they do not match the app's `DEG_F` input query.
Setting `backfill = True` also processes readings already in storage when the
app first becomes active.

Acquirium selects the interval to recompute and saves the output together with
its processing progress in one transaction. The transform only needs to
calculate results for the input window it receives.

## 2. Check it without saving anything

Before deploying, run it as a dry run against the data already stored:

```bash
uv run acquirium app check ./temperature_conversion.py:FahrenheitToCelsius
```

The check reports the matched streams, computed values, and any transform
errors without saving a deployment or output data. It is useful for checking
an app against an existing dataset before activating it.

## 3. Deploy it with the server

The example config deploys the class at startup:

```toml
[[apps]]
spec = "./temperature_conversion.py:FahrenheitToCelsius"
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

The script registers a Fahrenheit input stream, writes six samples, and queries
the app's outputs until the derived Celsius values appear. Further writes to
any matched Fahrenheit stream cause the app to recompute its affected interval.
Correcting an earlier Fahrenheit reading also updates its Celsius result.

## 5. Find what it produced

You can query the derived stream through the same API used for sensor data.
Acquirium records the producing app on each derived point, so the `app` filter
selects this calculation's output:

```python
from acquirium import Acquirium

acq = Acquirium(server_url="127.0.0.1", server_port=8000)
acq.query().measurement(alias="c", app="fahrenheit-to-celsius").data()
```

The producing-app metadata is added automatically. Other metadata, such as a
unit, label, or quantity kind, comes from the output declaration. For example,
declaring `quantity_kind` makes the output discoverable by queries for that
quantity kind alongside measured streams. See the
[apps guide](../apps.md#convert-each-sensor) for an example.

## 6. Smooth each sensor's readings

The same `grouping = "per_match"` setting also works for calculations that need
several readings from each sensor. The following app uses a ten-minute rolling
average and selects inputs by quantity kind:

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
