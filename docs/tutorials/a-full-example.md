---
title: 5 - A Full Example
---

In this article we'll put everything together in a complete example.
The example uses the shipped seawater desalination plant with one year of data.
The example plant is taken from the [WaterTAP seawater RO desalination flowsheet](https://watertap.readthedocs.io/en/latest/technical_reference/flowsheets/seawater_RO_desalination.html).

This tutorial will be helpful to understand the query interface of Acquirium.

The full code of this page is available as a [Python script](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/5_a_full_example.py) and as a [Jupyter notebook](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/5_a_full_example.ipynb), if you want to download or copy it.

## Getting the example files

Run this command in the command line (or PowerShell).
If you're using conda or venv, make sure the environment is active:

```bash
acquirium example init
```

If you're using uv:

```bash
uv run acquirium example init
```
```text
Wrote example/model.ttl
Wrote example/data.csv (8760 rows, 32 streams)
```

It creates a folder named `example` with two files:

- `model.ttl` is the plant model.
- `data.csv` is the data of the plant for the year 2025, one row per hour. It has a `time` column and one column per stream.

This example needs to start without the streams of the earlier pages.
Run `acquirium clear` before the script, every time you run it:

```bash
acquirium clear
```

It erases the data Acquirium keeps in the current folder, after asking you to confirm.

The rest of this page is one Python script, built step by step.
In a script, wrap the lines that show a result in `print()`.
It starts as usual:

```python
import polars as pl
import acquirium as aq

acq = aq.init()
```

## 1. Insert the plant model

```python
acq.insert_graph_file("example/model.ttl", source_id="plant")
```

## 2. Register the streams of the file

Read the file first.
`try_parse_dates=True` turns the `time` column into timestamps:

```python
data = pl.read_csv("example/data.csv", try_parse_dates=True)
stream_names = [column for column in data.columns if column != "time"]
```

Here is how the file maps to streams:

- The file is the source. We name it `"csv-data"`, and use it as the `source_id`.
- Every column other than `time` is one stream, and the column header is its `stream_name`.
- No unit is given. The plant model already has the units, and the streams will get them when they are connected in step 4.

```python
acq.register_streams([
    {"source_id": "csv-data", "stream_name": name} for name in stream_names
])
```

### Inserting the data of the file

`insert_timeseries_batch()` takes a dictionary with one entry per stream: the `stream_name`, and the rows of the stream as a list of `(timestamp, value)` pairs.

The file is a table with one column per stream, so each column is paired with the `time` column:

- `data["time"].to_list()` is the list of the timestamps.
- `data[name].to_list()` is the list of the values of one column.
- `zip()` pairs the two lists row by row.

```python
times = data["time"].to_list()

acq.insert_timeseries_batch("csv-data", {
    name: list(zip(times, data[name].to_list())) for name in stream_names
})["rows_inserted"]
```
```text
280320
```

[`read_csv()`](https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html) and [`to_list()`](https://docs.pola.rs/api/python/stable/reference/series/api/polars.Series.to_list.html) are described in the Polars documentation.
The same can be done with pandas, with [`pd.read_csv()`](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html) and `data[name].tolist()`.

## 3. Find what is not connected

At this point the model and the streams do not know about each other.
`property(has_stream=False)` lists the properties of the model that have no stream:

```python
print(acq.query().property(has_stream=False))
```
```text
Query
  property: property, without a stream
32 matches, first 5:
┌─────────────────────────────────┐
│ property                        │
│ ---                             │
│ str                             │
╞═════════════════════════════════╡
│ wbs:P1-efficiency               │
│ wbs:PXR-brine-out-flow-mass-wa… │
│ wbs:conn-cartridge-filtration-… │
│ wbs:RO-in-flow-mass-water       │
│ wbs:RO-in-temperature-k         │
└─────────────────────────────────┘
```

`where(is_disconnected=True)` lists the streams that are not connected to a property of the model:

```python
print(acq.query().measurement().where(is_disconnected=True))
```
```text
Query
  data: measurement, is_disconnected=True
32 matches, first 5:
┌─────────────────────────────────┬─────────────────────────────────┐
│ data                            ┆ data.label                      │
│ ---                             ┆ ---                             │
│ str                             ┆ str                             │
╞═════════════════════════════════╪═════════════════════════════════╡
│ acq:b7b30bc8-3f8e-5360-a6a0-09… ┆ intake-in-flow-rate-m3s         │
│ acq:1ffc3fe1-a7be-5190-b470-d1… ┆ intake-in-tss-concentration-kg… │
│ acq:83ffcb07-ddf8-5841-8433-37… ┆ P2-efficiency                   │
│ acq:46c80106-cf92-5223-b592-7d… ┆ storage-tank-3-out-flow-rate-m… │
│ acq:b5a98c24-b5fb-5daf-bf3e-9e… ┆ PXR-brine-out-flow-mass-tds     │
└─────────────────────────────────┴─────────────────────────────────┘
```

## 4. Connect the streams to the model

The connection is written down as a mapping: for every stream, the property of the model it belongs to.
The key is the `stream_name`, and the value is the URI of the property:

```python
mapping = {
    "intake-in-flow-rate-m3s":                         "wbs:intake-in-flow-rate-m3s",
    "intake-in-tds-concentration":                     "wbs:intake-in-tds-concentration",
    "intake-in-tss-concentration-kgm3":                "wbs:intake-in-tss-concentration-kgm3",
    "conn-cartridge-filtration-to-S1-temperature-k":   "wbs:conn-cartridge-filtration-to-S1-temperature-k",
    "conn-cartridge-filtration-to-S1-flow-mass-water": "wbs:conn-cartridge-filtration-to-S1-flow-mass-water",
    "conn-cartridge-filtration-to-S1-flow-mass-tds":   "wbs:conn-cartridge-filtration-to-S1-flow-mass-tds",
    "P1-mechanical-power":                             "wbs:P1-mechanical-power",
    "P2-mechanical-power":                             "wbs:P2-mechanical-power",
    "RO-in-temperature-k":                             "wbs:RO-in-temperature-k",
    "RO-in-flow-mass-water":                           "wbs:RO-in-flow-mass-water",
    "RO-in-flow-mass-tds":                             "wbs:RO-in-flow-mass-tds",
    "RO-out-flow-mass-water":                          "wbs:RO-out-flow-mass-water",
    "RO-out-flow-mass-tds":                            "wbs:RO-out-flow-mass-tds",
    "RO-out-retentate-pressure-pa":                    "wbs:RO-out-retentate-pressure-pa",
    "RO-out-retentate-flow-mass-water":                "wbs:RO-out-retentate-flow-mass-water",
    "RO-out-retentate-flow-mass-tds":                  "wbs:RO-out-retentate-flow-mass-tds",
    "PXR-brine-out-flow-mass-water":                   "wbs:PXR-brine-out-flow-mass-water",
    "PXR-brine-out-flow-mass-tds":                     "wbs:PXR-brine-out-flow-mass-tds",
    "storage-tank-3-out-flow-rate-m3s":                "wbs:storage-tank-3-out-flow-rate-m3s",
    "storage-tank-3-out-tds-concentration":            "wbs:storage-tank-3-out-tds-concentration",
    "conn-cartridge-filtration-to-S1-pressure-pa":     "wbs:conn-cartridge-filtration-to-S1-pressure-pa",
    "P1-efficiency":                                   "wbs:P1-efficiency",
    "P1-out-pressure-pa":                              "wbs:P1-out-pressure-pa",
    "P2-efficiency":                                   "wbs:P2-efficiency",
    "PXR-efficiency":                                  "wbs:PXR-efficiency",
    "RO-membrane-area":                                "wbs:RO-membrane-area",
    "RO-in-pressure-pa":                               "wbs:RO-in-pressure-pa",
    "RO-out-pressure-pa":                              "wbs:RO-out-pressure-pa",
    "PXR-brine-out-pressure-pa":                       "wbs:PXR-brine-out-pressure-pa",
    "ferric-chloride-addition-ferric-chloride-dose":   "wbs:ferric-chloride-addition-ferric-chloride-dose",
    "anti-scalant-addition-anti-scalant-dose":         "wbs:anti-scalant-addition-anti-scalant-dose",
    "lime-addition-lime-dose":                         "wbs:lime-addition-lime-dose",
}
```

**Note:** In this example the stream and the property have the same name. With your own data they will rarely match, and an entry looks like `"PT-101": "wbs:P1-out-pressure-pa"`.

Registering a stream again with a `property_uri` connects it to that property:

```python
matches = [
    {"source_id": "csv-data", "stream_name": name, "property_uri": prop}
    for name, prop in mapping.items()
]

acq.register_streams(matches)
```

Both lists are now empty:

```python
print(acq.query().property(has_stream=False))
print(acq.query().measurement().where(is_disconnected=True))
```
```text
Query
  property: property, without a stream
0 matches
Query
  data: measurement, is_disconnected=True
0 matches
```

The mapping can also be kept in a file, which is useful when another script or a driver needs it.
The mapping of this example is available as [mapping.json](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/driver/mapping.json), and is read with:

```python
import json

with open("mapping.json") as f:
    mapping = json.load(f)
```

## 5. Query the plant

### Finding equipment

`entity()` selects the parts of the plant by their class:

```python
acq.query().entity("pump").include("label").metadata()
```
```text
shape: (3, 2)
┌────────────┬─────────────────────────┐
│ pump       ┆ pump.label              │
│ ---        ┆ ---                     │
│ str        ┆ str                     │
╞════════════╪═════════════════════════╡
│ wbs:intake ┆ Seawater Onshore Intake │
│ wbs:P2     ┆ Booster Pump P2         │
│ wbs:P1     ┆ High-pressure Pump P1   │
└────────────┴─────────────────────────┘
```

`related()` follows the connections of the plant.
For instance, the reverse osmosis membrane downstream of each pump:

```python
acq.query().entity("pump").related("Reverse Osmosis Membrane", direction="downstream").metadata()
```
```text
shape: (2, 2)
┌────────┬──────────────────────────┐
│ pump   ┆ Reverse Osmosis Membrane │
│ ---    ┆ ---                      │
│ str    ┆ str                      │
╞════════╪══════════════════════════╡
│ wbs:P1 ┆ wbs:RO                   │
│ wbs:P2 ┆ wbs:RO                   │
└────────┴──────────────────────────┘
```

### Finding streams through equipment

After an entity, `measurement()` selects the streams of that entity.
It takes the same filters as `where()`:

```python
q = acq.query().entity("pump").measurement(quantity_kind="power")
print(q)
```
```text
Query
  pump: entity, Pump
  pump_data: measurement, of pump, quantity_kind=Power
2 matches:
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ pump_data               ┆ pump_data.label     │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power │
└────────┴─────────────────────────┴─────────────────────┘
```

A single part of the plant is selected with `uri=`.
With `direction=`, `measurement()` looks for the nearest stream upstream or downstream of it:

```python
ro = acq.query().entity(uri="wbs:RO", alias="ro")
ro.measurement(direction="upstream", quantity_kind="pressure").metadata()
```
```text
shape: (1, 4)
┌────────┬────────────────────┬───────────────────────┬────────────────────────┐
│ ro     ┆ ro_upstream_entity ┆ ro_upstream_data      ┆ ro_upstream_data.label │
│ ---    ┆ ---                ┆ ---                   ┆ ---                    │
│ str    ┆ str                ┆ str                   ┆ str                    │
╞════════╪════════════════════╪═══════════════════════╪════════════════════════╡
│ wbs:RO ┆ wbs:RO-in          ┆ wbs:RO-in-pressure-pa ┆ RO in pressure pa      │
└────────┴────────────────────┴───────────────────────┴────────────────────────┘
```

### Reading the data

`dataframe()` works the same way as in [2 - Querying Data](querying-data.md):

```python
q.dataframe(
    start="2025-07-01",
    end="2025-07-01 03:00",
)
```
```text
shape: (4, 3)
┌─────────────────────────┬────────────────────────────────┬────────────────────────────────┐
│ time                    ┆ pump_data__P1 mechanical power ┆ pump_data__P2 mechanical power │
│ ---                     ┆ ---                            ┆ ---                            │
│ datetime[μs, UTC]       ┆ f64                            ┆ f64                            │
╞═════════════════════════╪════════════════════════════════╪════════════════════════════════╡
│ 2025-07-01 00:00:00 UTC ┆ 1.0658e6                       ┆ 108456.097041                  │
│ 2025-07-01 01:00:00 UTC ┆ 1.0913e6                       ┆ 116735.330076                  │
│ 2025-07-01 02:00:00 UTC ┆ 1.0295e6                       ┆ 96421.121065                   │
│ 2025-07-01 03:00:00 UTC ┆ 1.0421e6                       ┆ 103118.66318                   │
└─────────────────────────┴────────────────────────────────┴────────────────────────────────┘
```

The model says the power is in watts, so it can be read in kilowatts:

```python
q.data().convert_to("kW").dataframe(limit=3)
```
```text
shape: (3, 3)
┌─────────────────────────┬────────────────────────────────┬────────────────────────────────┐
│ time                    ┆ pump_data__P1 mechanical power ┆ pump_data__P2 mechanical power │
│ ---                     ┆ ---                            ┆ ---                            │
│ datetime[μs, UTC]       ┆ f64                            ┆ f64                            │
╞═════════════════════════╪════════════════════════════════╪════════════════════════════════╡
│ 2025-01-01 00:00:00 UTC ┆ 1187.411568                    ┆ 106.089859                     │
│ 2025-01-01 01:00:00 UTC ┆ 1181.608011                    ┆ 101.473944                     │
│ 2025-01-01 02:00:00 UTC ┆ 1165.345244                    ┆ 97.750261                      │
└─────────────────────────┴────────────────────────────────┴────────────────────────────────┘
```

The script ends by stopping the server:

```python
aq.shutdown()
```

## What's next?

This is the end of the tutorial that shows the core of Acquirium.

From here, there are three advanced features of Acquirium:

- [Writing a driver](advanced/writing-a-driver.md): set up a driver to continuously insert data.
- [Build your first soft sensor](advanced/first-app.md) with a live threshold detector, or [derive a power stream for each pump in this plant](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/advanced_apps.py).
- [Running experiments](advanced/experiments.md): record the inputs and results of simulations and tests in Acquirium.

Every method used in this tutorial is described in the [reference](../reference/_index.md).
