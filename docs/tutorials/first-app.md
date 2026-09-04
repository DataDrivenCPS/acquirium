---
title: Your first app
---

A driver gets plant data *into* Acquirium. An app derives new streams *from*
that data and keeps them up to date: soft sensors, unit conversions, KPIs,
anomaly flags. In this tutorial you will run a complete app — Celsius samples
in, a derived Fahrenheit stream out — and then modify it.

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
        if celsius.is_empty():
            return
        output["fahrenheit"] = celsius.select(
            "time", (pl.col("value") * 9.0 / 5.0 + 32.0).alias("value")
        )
```

Read it top to bottom:

- `build_query` says *what to read*: every measurement stream tagged with the
  example's data source, under the alias `temperature`.
- `outputs` says *what to write*: one derived numeric stream. It is a
  **named** output — the stream's identity is exactly `fahrenheit` under this
  app, so anything else can find it directly. (The other flavor,
  `aq.output.per_row(...)`, generates one stream beside each matched input;
  you'll use it below.)
- `transform` is *the calculation*: a dataframe of `time` and `value` rows in,
  a dataframe of `time` and `value` rows out.
- `backfill = True` says the first run should process data that was already
  stored, not just data arriving later.

Notice what is absent: no scheduling code, no checkpoint handling, no retry
logic. Your job is to recompute the window of data you are handed; Acquirium
decides when to call you and commits your output together with its saved
position in one transaction.

## 2. Check it without saving anything

Before deploying, run it as a dry run against the data already stored:

```bash
uv run acquirium app check ./temperature_conversion.py:CelsiusToFahrenheit
```

It prints which streams the query matched and the values `transform`
computed, and saves none of it — no deployment, no derived stream, no
progress. If the query matched nothing, or the transform raised, you find
out here rather than after deploying.

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
output point until the derived Fahrenheit values arrive — typically well under
a second later. Every new Celsius write from now on triggers a fresh
Fahrenheit computation for exactly the changed range.

## 5. Make it react to every sensor

The app above binds all matches into one call. The more common plant pattern —
“do this beside every sensor” — uses a `per_row` output:

```python
class TemperatureSmoother(aq.App):
    name = "temperature-smoother"
    lookback = "10m"
    outputs = {"smooth": aq.output.per_row(value_kind="numeric", unit="http://qudt.org/vocab/unit/DEG_C")}

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

Three new ideas:

- `output.per_row` runs `transform` once per matched stream and creates
  one derived stream beside each — a thousand sensors become a thousand
  smoothed streams with no naming on your part. (A `named` output does the
  opposite: one call over every match, one stream.)
- `lookback = "10m"` hands each call ten minutes of context
  before the new data, so the rolling mean is correct at the edge. Re-emitting
  the whole window is safe: outputs are keyed by (stream, time), so recomputed
  values overwrite themselves.
- `.in_unit("DEG_C")` delivers every stream in Celsius no matter what unit
  each sensor reports; a stream that cannot convert raises loudly rather than
  feeding mis-scaled values into the calculation.

## Where to go next

- [Apps](../apps.md) — the walkthrough of lookback, scheduling, output flavors,
  and deployment.
- [App reference](../reference/apps.md) — the complete class contract.
- [Incremental materialization implementation](../materialization-implementation.md)
  — what the server does after you deploy.
