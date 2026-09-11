---
title: Your first app
---

Drivers load sensor readings into Acquirium. Apps use those readings to
calculate derived streams, such as converted temperatures, averages, or anomaly
flags. In this tutorial, you will run an app that converts water-temperature
readings from Fahrenheit to Celsius and query the derived values.

The full working files are in `examples/transformation/` in the repository.
Run the commands below from the repository root. The example publisher supplies
water-temperature readings, so you do not need an existing plant dataset.

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

A *query match* is one result of the app's input query. Here, each match
identifies one temperature stream. A more detailed query could match a pump
together with its flow and pressure streams; those related inputs would belong
to one match.

The query selects every measurement stream whose unit is `DEG_F`, regardless
of its data source, and exposes the readings under the alias `temperature`.
*Grouping* determines which matches go into one call to `transform`.
With `grouping = "per_match"`, each matched water-temperature stream gets a
separate call and its own conversion. The other option, `"all_matches"`,
passes all selected streams together—for example, to calculate an average
across several tank sensors. Every app must explicitly choose its grouping mode.

An *output port* is a named place where the app assigns a result table. Here,
`"celsius"` is the port name: it appears both in the `outputs` declaration and
in `output["celsius"]`. A port name belongs to the app's calculation; it is not
a sensor identifier. The `output.stream` declaration gives each matched input
its own derived Celsius stream for that port.

For an aggregate across sensors, `output.named` can give the result a name that
stays the same as sensors join or leave. Grouping chooses the inputs for a call;
the output declaration chooses how its result is identified.

In `transform`, the app reads a dataframe, applies the Fahrenheit-to-Celsius
conversion, and assigns the `time` and `value` columns to that output.
The outputs declare `DEG_C`, so they do not match the app's `DEG_F` input query.
Setting `backfill = True` also processes readings already in storage when the
app first becomes active.

Acquirium selects the interval to recompute and saves the output together with
its processing progress in one transaction. The transform only needs to
calculate results for the input window it receives.

## 2. Check it without saving anything

If your server already has Fahrenheit water-temperature readings, you can
check the app before deploying it:

```bash
uv run acquirium app check ./examples/transformation/temperature_conversion.py:FahrenheitToCelsius
```

The check reports the matched streams, computed values, and any transform
errors without saving a deployment or output data. It is useful for checking
an app against an existing dataset before activating it. If you are starting
with an empty server, continue below to start the example server and publish
its sample data; you can run this check afterward.

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

The script registers a Fahrenheit water-temperature stream, writes six samples,
and queries the app's outputs until the derived Celsius values appear. Further
writes to any matched Fahrenheit stream cause the app to recompute its affected interval.
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
[calculation recipes](../how-to/calculate-derived-streams.md#convert-each-water-temperature-sensor) for an example.

## Where to go next

- [Calculate derived streams](../how-to/calculate-derived-streams.md) — minute
  averages, rolling calculations, combining pump sensors, and temperature flags.
- [Check, deploy, and repair an app](../how-to/check-deploy-apps.md) — dry runs,
  code updates, reprocessing, and execution controls.
- [Apps explained](../explanation/apps.md) — query matches, output identities,
  and why corrections recompute windows.
- [App reference](../reference/apps.md) — the complete class contract.
