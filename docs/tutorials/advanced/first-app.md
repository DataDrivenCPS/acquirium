---
title: Declaring and deploying Apps
---

In this article we'll describe apps.
An app calculates new streams from the existing ones, and Acquirium keeps the result up to date as new data arrives.
This is useful for soft sensors, unit conversions, averages and any other value you would otherwise calculate again and again.

The examples use the plant of [5 - A Full Example](../a-full-example.md).
Run that page first, and run the examples of this page in the same folder.

This page is also available as a [Python script](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/advanced_apps.py) and a [Jupyter notebook](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/advanced_apps.ipynb). The app itself is in [plant_apps.py](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/plant_apps.py).

## What an app is

An app has three parts:

- A **query** that selects its input streams. It is the same kind of query as in the tutorial.
- A **calculation**, which is a plain Python function over dataframes.
- One or more **outputs**. Each output is a new stream, registered by Acquirium.

The difference from doing the calculation in a notebook is that the server runs the app.
It runs when new data arrives, only on what changed, and the result is stored and can be queried like any other stream.

## 1. Write the app

The app below calculates the power of each pump in kilowatts.
Save it as `plant_apps.py`:

```python
import polars as pl

import acquirium as aq


class PumpPowerKW(aq.App):
    name = "pump-power-kw"
    grouping = "per_match"      # one output stream per pump
    backfill = True             # also calculate for the data already stored
    outputs = {
        "power": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/KiloW",
        )
    }

    def build_query(self, plant):
        return plant.query().entity("pump").measurement(alias="power", quantity_kind="power")

    def transform(self, inputs, output, context):
        df = inputs["power"].df()
        output["power"] = df.select("time", pl.col("value") / 1000)
```

- `build_query()` returns the query. `alias="power"` is the name of the input in `transform()`.
- `transform()` does the calculation. `inputs["power"].df()` is a dataframe with the columns `stream_uri`, `time` and `value`.
- An output is assigned as a dataframe with a `time` and a `value` column.
- `grouping = "per_match"` runs the app once for each match of the query. There are two pumps with a power stream, so there will be two output streams.

**Note:** The unit of an output has to be a full URI. Plain text such as "kW" is not matched here.

If the inputs of your app do not share timestamps, `aq.align()` puts them on the same time steps first (see the [app reference](../../reference/apps/inputs.md)).

## 2. Check it

`check_app()` runs the app on the stored data and returns what it calculated.
Nothing is saved.

```python
import acquirium as aq
from plant_apps import PumpPowerKW

acq = aq.init()

result = acq.check_app(PumpPowerKW, limit=2)

for binding in result["bindings"]:
    print(binding["row"]["pump"], binding["error"])
    print(binding["outputs"]["power"]["values"])
```
```text
urn:swro/P1 None
[{'time': '2025-01-01T00:00:00+00:00', 'value': 1187.4115681745568}, {'time': '2025-01-01T01:00:00+00:00', 'value': 1181.6080107814955}]
urn:swro/P2 None
[{'time': '2025-01-01T00:00:00+00:00', 'value': 106.08985852963146}, {'time': '2025-01-01T01:00:00+00:00', 'value': 101.47394375763294}]
```

There is one entry per pump, and no error.
Check that the query matched the streams you expected, and that the values look right.

## 3. Deploy it

`deploy_app()` hands the app to the server:

```python
acq.deploy_app(PumpPowerKW)
```
```text
{'ok': True, 'name': 'pump-power-kw', 'status': 'deployed'}
```

The server starts calculating in the background.
`list_apps()` shows the state of the deployed apps:

```python
acq.list_apps()["apps"][0]
```
```text
{'name': 'pump-power-kw',
 'entrypoint': 'plant_apps:PumpPowerKW',
 'grouping': 'per_match',
 'status': 'idle',
 'binding_count': 2,
 'binding_statuses': {'idle': 2},
 'graph_revision': 12,
 'plan_current': True,
 'error': None}
```

`idle` means the app has caught up with the data.

**Note:** The server runs the app, so it has to be able to import `plant_apps.py`. With `aq.init()` this works when the file is in the folder you run your script from. An app that is defined in the script itself cannot be deployed.

The deployment is kept. The next time Acquirium starts in this folder, the app is running again.

## 4. Query the result

The outputs are streams like any other.
`where(app=...)` selects the streams of an app:

```python
q = acq.query().measurement().where(app="pump-power-kw")
q.dataframe(limit=3)
```
```text
shape: (3, 3)
┌─────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
│ time                    ┆ data__P1 mechanical power (pum… ┆ data__P2 mechanical power (pum… │
│ ---                     ┆ ---                             ┆ ---                             │
│ datetime[μs, UTC]       ┆ f64                             ┆ f64                             │
╞═════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
│ 2025-01-01 00:00:00 UTC ┆ 1187.411568                     ┆ 106.089859                      │
│ 2025-01-01 01:00:00 UTC ┆ 1181.608011                     ┆ 101.473944                      │
│ 2025-01-01 02:00:00 UTC ┆ 1165.345244                     ┆ 97.750261                       │
└─────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘
```

Now insert a new reading into one of the inputs:

```python
from datetime import datetime, timezone

acq.insert_timeseries("csv-data", "P1-mechanical-power", [
    (datetime(2026, 1, 1, tzinfo=timezone.utc), 1_200_000.0),
])
```

A few seconds later the output has the new value, in kilowatts:

```python
q.dataframe(start=datetime(2026, 1, 1, tzinfo=timezone.utc))
```
```text
shape: (1, 2)
┌─────────────────────────┬─────────────────────────────────┐
│ time                    ┆ data__P1 mechanical power (pum… │
│ ---                     ┆ ---                             │
│ datetime[μs, UTC]       ┆ f64                             │
╞═════════════════════════╪═════════════════════════════════╡
│ 2026-01-01 00:00:00 UTC ┆ 1200.0                          │
└─────────────────────────┴─────────────────────────────────┘
```

## Changing and removing an app

After changing the code of an app, deploy it again.
The new code is used for the data that arrives from then on:

```python
acq.deploy_app(PumpPowerKW)
```

To apply the new code to the old data as well, use `reprocess_app()` with a time range:

```python
acq.reprocess_app(
    "pump-power-kw",
    datetime(2025, 1, 1, tzinfo=timezone.utc),
    datetime(2026, 1, 2, tzinfo=timezone.utc),
)
```
```text
{'ok': True, 'name': 'pump-power-kw', 'status': 'reprocessing', 'bindings': 2}
```

`remove_app()` stops an app:

```python
acq.remove_app("pump-power-kw")
```
```text
{'ok': True, 'name': 'pump-power-kw', 'status': 'removed'}
```

The values it already calculated are kept.

## Next

- The [app reference](../../reference/apps/_index.md) describes every setting of an app, its inputs and outputs.
- With a server started by `acquirium server`, the same steps are available as the `acquirium app` commands (see the [reference](../../reference/apps/cli.md)).
- [Check and deploy apps](../../how-to/check-deploy-apps.md) covers checking, deploying and repairing in more detail.
