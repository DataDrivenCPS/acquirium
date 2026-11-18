---
title: Calculate derived streams
---

In this article we'll describe four apps that go one step further than the app of [Declaring and deploying Apps](../tutorials/advanced/first-app.md):
an app that combines two streams, an app that calculates one result from all matches, an app with parameters, and an app that reads the output of another app.

The examples use the plant of [5 - A Full Example](../tutorials/a-full-example.md).
Run that page first, and run the examples of this page in the same folder.
Each app is saved in its own file, see the note in [Check, deploy, and repair an app](check-deploy-apps.md#apply-a-code-change-to-old-data).

## Combine two streams

The recovery of the reverse osmosis stage is the permeate flow divided by the feed flow.
The app below calculates it for every hour.
Save it as `ro_recovery.py`:

```python
import polars as pl

import acquirium as aq


class RORecovery(aq.App):
    name = "ro-recovery"
    grouping = "all_matches"    # one call that receives both inputs
    every = "1h"                # calculate in whole hours
    backfill = True             # also calculate for the data already stored
    outputs = {
        "recovery": aq.output.named(
            "ro-recovery",
            value_kind="numeric",
            label="RO recovery",
            unit="http://qudt.org/vocab/unit/PERCENT",
        )
    }

    def build_query(self, plant):
        return (
            plant.query()
            .entity(uri="urn:swro/RO", alias="ro")
            .measurement(alias="feed", uri="urn:swro/RO-in-flow-mass-water")
            .measurement(frm="ro", alias="permeate", uri="urn:swro/RO-out-flow-mass-water")
        )

    def transform(self, inputs, output, context):
        df = aq.align(inputs)
        if "feed" not in df.columns or "permeate" not in df.columns:
            return      # one of the inputs has no value in this time range
        df = df.drop_nulls()
        output["recovery"] = df.select(
            "time", (100 * pl.col("permeate") / pl.col("feed")).alias("value")
        )
```

- `grouping = "all_matches"` gives all matched streams to one call of `transform()`. With `"per_match"` each call receives one stream only, so two streams cannot be combined there.
- The query has two measurements, `feed` and `permeate`. These aliases are the keys of `inputs`.
- `every = "1h"` makes the app calculate in whole hours. It is required by `aq.align()`.
- `aq.align(inputs)` returns one dataframe with a `time` column and one column per alias. Each row is one hour, and each value is the mean of the stream in that hour.
- `aq.output.named("ro-recovery", ...)` gives the output a fixed `stream_name`.

**Note:** `aq.align()` only has a column for an input that has values in the time range of the call. This is why `transform()` returns early when a column is missing. An output that is not assigned is left as it is.

**Note:** In `build_query()` a URI has to be written in full, such as `urn:swro/RO`. A prefix such as `wbs:RO` matches nothing here.

`check_app()` runs the app on the stored data without saving anything:

```python
import acquirium as aq
from ro_recovery import RORecovery

acq = aq.init()

result = acq.check_app(RORecovery, limit=3)
binding = result["bindings"][0]

print(binding["input_rows"])
for row in binding["outputs"]["recovery"]["values"]:
    print(row)
```
```text
{'feed': 8760, 'permeate': 8760}
{'time': '2025-01-01T00:00:00+00:00', 'value': 47.63232968002666}
{'time': '2025-01-01T01:00:00+00:00', 'value': 48.358042017433824}
{'time': '2025-01-01T02:00:00+00:00', 'value': 48.64284764032491}
```

## Calculate one result from all matches

The app below adds up the power of all pumps.
Save it as `pump_total.py`:

```python
import polars as pl

import acquirium as aq


class TotalPumpPower(aq.App):
    name = "total-pump-power"
    grouping = "all_matches"    # one call that receives the streams of all pumps
    backfill = True
    outputs = {
        "total": aq.output.named(
            "total-pump-power",
            value_kind="numeric",
            label="Total pump power",
            unit="http://qudt.org/vocab/unit/KiloW",
        )
    }

    def __init__(self, min_power=0.0):
        self.min_power = min_power      # in kW

    def build_query(self, plant):
        return plant.query().entity("pump").measurement(alias="power", quantity_kind="power")

    def transform(self, inputs, output, context):
        df = inputs["power"].in_unit("http://qudt.org/vocab/unit/KiloW").df()
        total = df.group_by("time").agg(pl.col("value").sum())
        output["total"] = total.filter(pl.col("value") >= self.min_power)
```

- The query is the one of the tutorial. With `"all_matches"` the power streams of all pumps arrive together in `inputs["power"]`.
- `in_unit()` converts the values of every stream to one unit. Here the streams are in watts and the result is in kilowatts.
- The dataframe has the columns `stream_uri`, `time` and `value`, so the sum per timestamp is a `group_by`.
- The output is a named output, so it keeps its name when a pump is added to the plant model.

```python
from pump_total import TotalPumpPower

result = acq.check_app(TotalPumpPower, limit=3)
binding = result["bindings"][0]

print([stream["label"] for stream in binding["inputs"]["power"]])
for row in binding["outputs"]["total"]["values"]:
    print(row)
```
```text
['P1 mechanical power', 'P2 mechanical power']
{'time': '2025-01-01T00:00:00+00:00', 'value': 1293.5014267041884}
{'time': '2025-01-01T01:00:00+00:00', 'value': 1283.0819545391284}
{'time': '2025-01-01T02:00:00+00:00', 'value': 1263.095505389945}
```

**Note:** A named output is one stream. An app with `grouping = "per_match"` and a named output cannot be deployed when its query has more than one match.

## Give an app parameters

`TotalPumpPower` has an `__init__()` with the parameter `min_power`.
Totals below this value are not written.
The value is given with `parameters` when the app is checked or deployed:

```python
result = acq.check_app(TotalPumpPower)
print(result["bindings"][0]["outputs"]["total"]["rows"])

result = acq.check_app(TotalPumpPower, parameters={"min_power": 1250.0})
print(result["bindings"][0]["outputs"]["total"]["rows"])
```
```text
8760
5771
```

The parameters are stored with the deployment, so they have to be plain values such as numbers, strings and lists.

**Note:** The server creates the app once and uses it for all calls. Keep the parameters on `self`, and do not store results of a calculation there.

## Read the output of another app

The app below calculates the mean of the recovery over the last 24 hours.
Its input is the output of `RORecovery`.
Save it as `ro_recovery_24h.py`:

```python
import polars as pl

import acquirium as aq


class RORecovery24h(aq.App):
    name = "ro-recovery-24h"
    grouping = "per_match"
    lookback = "24h"            # every value needs the 24 hours before it
    backfill = True
    outputs = {
        "mean": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/PERCENT",
        )
    }

    def build_query(self, plant):
        return plant.query().measurement(alias="recovery", app="ro-recovery")

    def transform(self, inputs, output, context):
        df = inputs["recovery"].df().sort("time")
        output["mean"] = df.select(
            "time", pl.col("value").rolling_mean_by("time", window_size="24h")
        )
```

- `measurement(app="ro-recovery")` selects the output streams of the app `ro-recovery`.
- `lookback = "24h"` tells the server that a value depends on the inputs of the 24 hours before it. When a recovery value arrives or changes, the server reads 24 more hours of input and calculates the 24 hours of output after the change again.

The query only matches when the first app is deployed.
Before that, a check has no bindings:

```python
from ro_recovery_24h import RORecovery24h

acq.check_app(RORecovery24h)["bindings"]
```
```text
[]
```

Deploy the first two apps:

```python
acq.deploy_app(RORecovery)
acq.deploy_app(TotalPumpPower, parameters={"min_power": 1250.0})
```
```text
{'ok': True, 'name': 'ro-recovery', 'status': 'deployed'}
{'ok': True, 'name': 'total-pump-power', 'status': 'deployed'}
```

The server now calculates the outputs for the year of stored data.
This took less than a minute here.
When both apps are `idle` in `list_apps()`, the third app can be checked and deployed:

```python
result = acq.check_app(RORecovery24h, limit=3)
for row in result["bindings"][0]["outputs"]["mean"]["values"]:
    print(row)

acq.deploy_app(RORecovery24h)
```
```text
{'time': '2025-01-01T00:00:00+00:00', 'value': 47.63232968002666}
{'time': '2025-01-01T01:00:00+00:00', 'value': 47.995185848730245}
{'time': '2025-01-01T02:00:00+00:00', 'value': 48.21107311259513}
{'ok': True, 'name': 'ro-recovery-24h', 'status': 'deployed'}
```

## Query the results

```python
for app in acq.list_apps()["apps"]:
    print(app["name"], app["status"], app["binding_count"])
```
```text
ro-recovery idle 1
ro-recovery-24h idle 1
total-pump-power idle 1
```

The outputs are streams, and `where(app=...)` selects the streams of one app:

```python
acq.query().measurement().where(app="total-pump-power").dataframe(limit=3)
```
```text
shape: (3, 2)
┌─────────────────────────┬────────────────────────┐
│ time                    ┆ data__Total pump power │
│ ---                     ┆ ---                    │
│ datetime[μs, UTC]       ┆ f64                    │
╞═════════════════════════╪════════════════════════╡
│ 2025-01-01 00:00:00 UTC ┆ 1293.501427            │
│ 2025-01-01 01:00:00 UTC ┆ 1283.081955            │
│ 2025-01-01 02:00:00 UTC ┆ 1263.095505            │
└─────────────────────────┴────────────────────────┘
```

A named output can also be read by its `source_id` and `stream_name`.
The `source_id` of an output is `derived:` followed by the name of the app:

```python
acq.stream("derived:ro-recovery", "ro-recovery").dataframe(limit=3)
```
```text
shape: (3, 2)
┌─────────────────────────┬───────────┐
│ time                    ┆ value     │
│ ---                     ┆ ---       │
│ datetime[μs, UTC]       ┆ f64       │
╞═════════════════════════╪═══════════╡
│ 2025-01-01 00:00:00 UTC ┆ 47.63233  │
│ 2025-01-01 01:00:00 UTC ┆ 48.358042 │
│ 2025-01-01 02:00:00 UTC ┆ 48.642848 │
└─────────────────────────┴───────────┘
```

Now insert a new value into both inputs of `RORecovery`:

```python
from datetime import datetime, timezone

t = datetime(2026, 1, 1, tzinfo=timezone.utc)
acq.insert_timeseries("csv-data", "RO-in-flow-mass-water", [(t, 300.0)])
acq.insert_timeseries("csv-data", "RO-out-flow-mass-water", [(t, 150.0)])
```

A few seconds later the recovery has the new hour, and the 24 hour mean that reads it has it too:

```python
start = datetime(2025, 12, 31, 22, tzinfo=timezone.utc)

print(acq.query().measurement().where(app="ro-recovery").dataframe(start=start))
print(acq.query().measurement().where(app="ro-recovery-24h").dataframe(start=start))
```
```text
shape: (3, 2)
┌─────────────────────────┬───────────────────┐
│ time                    ┆ data__RO recovery │
│ ---                     ┆ ---               │
│ datetime[μs, UTC]       ┆ f64               │
╞═════════════════════════╪═══════════════════╡
│ 2025-12-31 22:00:00 UTC ┆ 47.126907         │
│ 2025-12-31 23:00:00 UTC ┆ 47.972396         │
│ 2026-01-01 00:00:00 UTC ┆ 50.0              │
└─────────────────────────┴───────────────────┘
shape: (3, 2)
┌─────────────────────────┬─────────────────────────────────┐
│ time                    ┆ data__RO recovery (ro-recovery… │
│ ---                     ┆ ---                             │
│ datetime[μs, UTC]       ┆ f64                             │
╞═════════════════════════╪═════════════════════════════════╡
│ 2025-12-31 22:00:00 UTC ┆ 46.160203                       │
│ 2025-12-31 23:00:00 UTC ┆ 46.194233                       │
│ 2026-01-01 00:00:00 UTC ┆ 46.320677                       │
└─────────────────────────┴─────────────────────────────────┘
```

## Next

- [Check, deploy, and repair an app](check-deploy-apps.md) shows how to read a check, what to do when an app fails, and how to change and remove an app.
- [How apps work](../explanation/apps.md) describes how the server keeps the outputs up to date.
- The [app reference](../reference/apps/_index.md) describes every setting of an app, `aq.align()` and the outputs.
