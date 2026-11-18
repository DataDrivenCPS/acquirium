---
title: Writing a driver
---

In the tutorial, the data was inserted by a script that you run by hand.
In this article we'll describe drivers, which do the same work continuously: a driver reads a source, registers its streams and inserts the data.

This page is useful if you have a source that keeps producing data, such as a folder of files, a PLC, an MQTT broker or an API.

The config files, the drivers and the scripts of this page are available in [examples/documentation/driver](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/documentation/driver).

## What a driver does

A driver does the three things you did by hand in [1 - Inserting Data](../inserting-data.md):

- it has a `source_id`,
- it registers its streams,
- it inserts their data.

A driver runs inside the server, and the server calls it on a fixed interval.
It also remembers where it stopped, so a restart continues from there.

## A server that keeps running

So far `aq.init()` started a server that stops with your script.
A driver needs a server that keeps running, which is started with the `acquirium server` command.

The server reads its settings from a file named `acquirium.toml`.
Create it in an empty folder:

```toml
[server]
host       = "127.0.0.1"
port       = 8000
exact_only = true
```

Then start the server in the command line, from the same folder:

```bash
acquirium server
```

The command keeps running until you stop it with Ctrl+C.
Scripts connect to this server with its address:

```python
import acquirium as aq

acq = aq.init(address="http://127.0.0.1:8000")
...
aq.shutdown()       # disconnects, the server keeps running
```

## Starting without code: the CSV driver

Acquirium ships with a driver that reads the CSV files of a folder.
We'll use it on the data of [5 - A Full Example](../a-full-example.md).

Stop the server, and create the example files in the same folder:

```bash
acquirium example init
```

Then add the driver to `acquirium.toml`:

```toml
[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
source_id = "csv-data"
watch_dir = "example"
glob      = "*.csv"
format    = "wide"
```

- `spec` names the driver class.
- `source_id` is the `source_id` of every stream the driver registers.
- `watch_dir` and `glob` select the files to read.
- `format` is `"wide"` when there is one column per stream, which is the case for `data.csv`.

Start the server again.
The driver reads the file, registers one stream per column and inserts the data:

```python
q = acq.query().measurement()
print(q)
```
```text
Query
  data: measurement
32 matches, first 5:
┌─────────────────────────────────┬─────────────────────────────────┐
│ data                            ┆ data.label                      │
│ ---                             ┆ ---                             │
│ str                             ┆ str                             │
╞═════════════════════════════════╪═════════════════════════════════╡
│ acq:c81d82c6-46f8-5396-a488-c7… ┆ intake-in-flow-rate-m3s         │
│ acq:40701b11-c32d-5b45-98aa-21… ┆ P2-mechanical-power             │
│ acq:0a4ab90c-af5c-5fb6-aff2-69… ┆ conn-cartridge-filtration-to-S… │
│ acq:22b1bf04-14cc-5a84-a947-f7… ┆ conn-cartridge-filtration-to-S… │
│ acq:5d7ecdb5-062f-5af3-afbe-96… ┆ RO-in-flow-mass-water           │
└─────────────────────────────────┴─────────────────────────────────┘
```

The driver keeps watching the folder.
When a row is added to the file, or a new file appears, the new rows are inserted within a few seconds.
Rows that were already read are not read again.

## Writing your own

A driver is a Python class.
Pick the base class by how your source behaves:

- `PollingIngestDriver`: you ask the source for its values every few seconds.
- `EventIngestDriver`: the source pushes values to you.
- `FileIngestDriver`: files appear in a folder.

Below is a complete polling driver for a tank sensor.
Save it as `tank_driver.py`, next to `acquirium.toml`:

```python
import random

import acquirium as aq


class TankDriver(aq.PollingIngestDriver):
    def setup(self):
        self.source_id = "tank"
        self.declare("level", unit="m", label="Tank level")
        self.declare("state", value_kind="text")

    def read(self):
        level = 2.0 + random.random()        # ask your sensor here
        self.add("level", level)
        self.add("state", "high" if level > 2.8 else "normal")
```

### Declaring the streams

`setup()` runs once, when the driver starts.

- `self.source_id` is the `source_id` of the driver.
- `declare()` describes one stream. It takes the same keys as `register_streams()`: `unit`, `quantity_kind`, `medium`, `substance`, `label` and `property_uri`.
- `value_kind` is optional, as in the tutorial.

The declared streams are registered by the driver before the first insert.

**Note:** Every stream has to be declared. Adding a value to a stream that was not declared raises an error:

```text
UndeclaredStreamError: observation stream ('tank', 'nope') was not declared
```

### Reading the source

`read()` runs on every interval.
`add()` takes the `stream_name` and the value.
The time of the value is the current time, unless you pass one as the third argument.

The values are collected and inserted together at the end of each interval.

If your source gives you a table instead of single values, `to_observations()` turns a dataframe into rows a driver can insert (see the [driver reference](../../reference/drivers/tabular-helpers.md)).

### Starting the driver

Add the driver to a config file.
We'll use a new file named `tank.toml`:

```toml
[[drivers]]
spec     = "tank_driver.py:TankDriver"
interval = 5.0
```

`interval` is the number of seconds between two reads.

`acquirium driver start` starts the drivers of a file on the running server:

```bash
acquirium driver start tank.toml
```
```text
Started driver 'TankDriver' (tank_driver.py:TankDriver, interval=5.0s)
```

A few seconds later the data is there:

```python
tank = acq.query().measurement().where(label="Tank level")
tank.dataframe()
```
```text
shape: (4, 2)
┌────────────────────────────────┬──────────────────┐
│ time                           ┆ data__Tank level │
│ ---                            ┆ ---              │
│ datetime[μs, UTC]              ┆ f64              │
╞════════════════════════════════╪══════════════════╡
│ 2026-11-18 05:23:41.333397 UTC ┆ 2.111449         │
│ 2026-11-18 05:23:47.340374 UTC ┆ 2.390963         │
│ 2026-11-18 05:23:52.396076 UTC ┆ 2.636132         │
│ 2026-11-18 05:23:57.452671 UTC ┆ 2.439858         │
└────────────────────────────────┴──────────────────┘
```

**Note:** A driver listed in `acquirium.toml` starts together with the server. `acquirium driver start` is for adding one while the server is running.

## Connecting to the plant model

A driver can also insert the plant model, and connect its streams to it.
The streams are connected the same way as in step 4 of the full example: with `property_uri`.

The driver needs to know which property each stream belongs to.
We'll use the mapping of [5 - A Full Example](../a-full-example.md#4-connect-the-streams-to-the-model), kept in a file.
Download [mapping.json](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/driver/mapping.json) and save it next to `acquirium.toml`.
It has one entry per stream:

```json
{
  "intake-in-flow-rate-m3s": "wbs:intake-in-flow-rate-m3s",
  "intake-in-tds-concentration": "wbs:intake-in-tds-concentration",
  ...
}
```

The driver below is the CSV driver with two additions.
Save it as `plant_driver.py`:

```python
import json

import acquirium as aq


class PlantCSV(aq.CSVIngestDriver):
    def setup(self):
        super().setup()
        self.insert_graph_file("example/model.ttl")
        with open("mapping.json") as f:
            self.mapping = json.load(f)

    def declare_stream(self, stream_name):
        self.declare(stream_name, property_uri=self.mapping[stream_name])
```

- `setup()` inserts the model and reads the mapping when the driver starts. `super().setup()` keeps the setup of the CSV driver, which reads the config.
- `insert_graph_file()` of a driver writes with the `source_id` of the driver, so it takes no `source_id`.
- `declare_stream()` is called by the CSV driver for every column it finds. It declares the stream with the property from the mapping.

Stop the CSV driver, which this one replaces:

```bash
acquirium driver stop --name CSVIngestDriver
```

Then start the new driver from a file named `plant.toml`.
It has the same keys as before, with the new `spec`:

```toml
[[drivers]]
spec      = "plant_driver.py:PlantCSV"
source_id = "csv-data"
watch_dir = "example"
glob      = "*.csv"
format    = "wide"
```

```bash
acquirium driver start plant.toml
```

The streams are now connected to the model, and can be found through the equipment:

```python
print(acq.query().entity("pump").measurement(quantity_kind="power"))
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

If your source reports a unit that is different from the model, pass it to `declare()` as well.
It is accepted when it can be converted to the unit of the model.

## Running and checking a driver

`acquirium driver list` shows the drivers of the server:

```bash
acquirium driver list
```
```text
TankDriver  running   interval=5.0s  started=2026-11-18T05:23:41.331341+00:00  spec=tank_driver.py:TankDriver
PlantCSV    running   interval=10.0s  started=2026-11-18T05:24:27.770709+00:00  spec=plant_driver.py:PlantCSV
```

`acquirium driver stop` stops one of them, by the name shown in the list:

```bash
acquirium driver stop --name TankDriver
```

When no data arrives, check these in order:

- Is the driver in `acquirium driver list`, and is it `running`? A driver whose `setup()` fails is not listed.
- The terminal of the server. An error in `read()` is printed there, and the driver tries again on the next interval.
- A query on the label or the `stream_name` you expect, to see if the stream was registered.

## Next

- The [driver reference](../../reference/drivers/_index.md) describes every base class, method and config key, and the other built-in drivers.
- [How drivers work](../../explanation/drivers.md) explains what happens inside the server.
