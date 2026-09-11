---
title: Calculate derived streams
---

An app uses a semantic query to select input streams, calculates new values
from their readings, and publishes the results as derived streams. Acquirium
keeps those results up to date as readings arrive or are corrected. This guide
starts with water-temperature conversion, then extends it to averages and alarms.
It assumes you have worked through [Your first app](../tutorials/first-app.md).
The input source tags used below should match those registered by your drivers.

## Convert each water-temperature sensor

Put the class in an importable module, such as `plant_apps.py`:

```python
import acquirium as aq
import polars as pl

class Celsius(aq.App):
    name = "celsius"
    grouping = "per_match"
    backfill = True
    outputs = {
        "temperature": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
            quantity_kind="http://qudt.org/vocab/quantitykind/Temperature",
        )
    }

    def build_query(self, plant):
        return plant.query().measurement(
            alias="temperature", data_source="raw-water-temperature"
        )

    def transform(self, inputs, output, context):
        frame = inputs["temperature"].in_unit(
            "http://qudt.org/vocab/unit/DEG_C"
        ).df()
        output["temperature"] = frame.select("time", "value")
```

Here, `grouping = "per_match"` gives each matched sensor a separate call to
`transform`. The call receives that sensor's readings through
`inputs["temperature"]`; its `.stream` attribute provides the sensor's metadata.
`output.stream` gives each result a stable identity based on the app, output
port, and input stream.

Every app must explicitly declare `grouping` as either `"per_match"` or
`"all_matches"`. A missing or invalid value is rejected on instantiation.
A query match can also contain related sensors, such as flow and pressure
measurements on the same pump; per-match grouping keeps those inputs together.

The query selects only `raw-water-temperature` inputs so that the app does not read
its own output. A downstream app can select the converted streams with
`measurement(app="celsius")`.

The `.in_unit` call converts compatible input units before the calculation.
A missing or incompatible unit raises an error.

## Read inputs and assign results

The transform receives three arguments: `inputs` holds readings selected by the
query, `output` collects the tables to publish, and `context` describes the
match and calculation windows. Each table assigned to an output port contains
`time` and `value` columns. See the reference for the complete
[StreamSet accessors](../reference/apps.md#streamset),
[context fields](../reference/apps.md#inputbatch), and
[output table requirements](../reference/apps.md#output-tables).

Assigning a table to an output port replaces the stored results within
`context.output_window`. Assign an empty table when previous results in that
interval should be removed, or leave the port unassigned to keep them unchanged.
The runtime discards returned rows outside the output window, allowing the
transform to read extra input context without overwriting adjacent results.

## Average into complete minutes

To calculate an average water temperature for each minute, set `every = "1m"`.
The runtime uses this setting to read complete minute buckets, and `aq.align`
uses it to resample the readings:

```python
class MinuteTemperature(Celsius):
    name = "minute-temperature"
    grouping = "per_match"
    every = "1m"

    def transform(self, inputs, output, context):
        converted = {
            "temperature": inputs["temperature"].in_unit(
                "http://qudt.org/vocab/unit/DEG_C"
            )
        }
        output["temperature"] = aq.align(converted).rename(
            {"temperature": "value"}
        )
```

If a new reading falls within a minute that already has a result, the runtime
reads all available readings in that minute and calculates it again. The result
reflects the data currently stored; later arrivals can revise it. The runtime
does not wait for every sensor to report or for the minute to become final.

## Calculate a rolling result

Declare how much preceding input each output depends on:

```python
class RollingTemperature(Celsius):
    name = "rolling-temperature"
    grouping = "per_match"
    lookback = "10m"

    def transform(self, inputs, output, context):
        frame = inputs["temperature"].in_unit(
            "http://qudt.org/vocab/unit/DEG_C"
        ).df().sort("time")
        output["temperature"] = frame.select(
            "time",
            pl.col("value").rolling_mean_by("time", window_size="10m"),
        )
```

A correction at noon can affect outputs through 12:10. Those outputs also need
the readings preceding noon. The runtime computes the affected output interval
and reads additional context, then publishes only that output interval.

Use `lookahead` for calculations depending on later inputs. Use
`lookback = "all"` only when the calculation needs the complete retained
history on every invocation; that history must fit in memory.

## Combine several sensors

An average flow across several pumps needs readings from their sensors in the
same call.
Set `grouping = "all_matches"` to receive them together, and use `output.named`
to give the aggregate a name that remains stable as the sensor set changes:

```python
class PlantAverageFlow(aq.App):
    name = "plant-average-flow"
    grouping = "all_matches"
    every = "1m"
    backfill = True
    outputs = {
        "average": aq.output.named("plant-average-flow", value_kind="numeric")
    }

    def build_query(self, plant):
        return plant.query().measurement(
            alias="flow", data_source="raw-pump-flow"
        )

    def transform(self, inputs, output, context):
        frame = aq.align(inputs)
        output["average"] = frame.select(
            "time", pl.mean_horizontal(pl.exclude("time")).alias("value")
        ).drop_nulls()
```

This produces a minute average from the sensors that have readings in that
minute. Later readings revise it. Normalize mixed units before combining them.
A named output has one owner, so it cannot be shared by multiple per-match calls.

## Flag high water temperatures

This example flags water temperatures above an illustrative 40 C threshold.
Choose a threshold appropriate to the process being monitored.

```python
class HighTemperature(Celsius):
    name = "high-temperature"
    grouping = "per_match"
    outputs = {"alarm": aq.output.stream(value_kind="text")}

    def transform(self, inputs, output, context):
        frame = inputs["temperature"].in_unit(
            "http://qudt.org/vocab/unit/DEG_C"
        ).df()
        output["alarm"] = frame.filter(pl.col("value") > 40).select(
            "time", pl.lit("water temperature above 40 C").alias("value")
        )
```

If a reading is corrected below 40 C, its alarm disappears. Downstream apps
observe the removal. This stream describes alarms justified by the current
readings; it is not an immutable record of notifications previously sent.

See [Check, deploy, and repair an app](check-deploy-apps.md) to run these
calculations against your data and keep their outputs current.
