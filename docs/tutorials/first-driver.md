---
title: Your first driver
---

<!-- TODO: intro -->

A driver is a Python class that collects data from somewhere (a file drop, an
MQTT feed, a simulation, an instrument) and pushes it into the server on a
schedule, for continuous data ingest.

You write three things: the source identity, the streams the source produces,
and the code that reads it.
The platform owns the rest: scheduling, registration, batching, insertion and
shutdown.

In this tutorial we author a driver for the most common source, a CSV export,
by extending the built-in `CSVIngestDriver`.
By the end you have a driver that watches a folder, ingests every new row of
every new file, and binds each column to a point in the plant model with its
unit.

## The source

An RO skid exports one file per day into a folder.
Each file looks like this:

```csv
Date,Time,Feed Flow,Permeate Flow,Feed Pressure,Permeate TDS
8/1/2026,12:00:00 AM,120.5,55.2,801,210
8/1/2026,12:15:00 AM,120.1,55.0,803,208
8/1/2026,12:30:00 AM,NaN,54.9,802,211
```

Three things to notice, because they decide the configuration:

- the layout is **wide**: one column per stream, one row per timestamp;
- the timestamp is split over two columns, `Date` and `Time`, in US format;
- missing values are written as `NaN`.

The column names become the `ref_name` of each stream, unchanged, so
`Feed Flow` is a stream called `Feed Flow`.

## Step 1: run it without code

`CSVIngestDriver` already knows how to walk a folder, read new rows, parse
timestamps and insert.
Describe the file in `acquirium.toml` and it runs as is:

```toml
[[drivers]]
spec        = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
source_id   = "ro-skid"
watch_dir   = "./data/ro-skid"       # relative to this file
glob        = "*.csv"
format      = "wide"
date_col    = "Date"
clock_col   = "Time"
day_first   = false
null_values = ["NaN"]
interval    = 60.0
```

Start the server with this config, or push the entry to a running one:

```bash
acquirium server --config acquirium.toml
acquirium driver start acquirium.toml      # or, on a running server
acquirium driver list
```

Every column is now a stream, `measurement()` on an empty query finds them,
and each one has a placeholder point labelled `ro-skid__Feed Flow` and so on.
What is missing is meaning: nothing says that `Feed Flow` is the flow at the
RO inlet, or that it is in gallons per minute.
That is what the subclass adds.

## Step 2: subclass and keep what works

Create `ro_skid_driver.py` next to the config:

```python
from acquirium import CSVIngestDriver


class ROSkidDriver(CSVIngestDriver):
    pass
```

and point the config at it instead:

```toml
spec = "ro_skid_driver.py:ROSkidDriver"
```

This driver behaves exactly like step 1.
`CSVIngestDriver` gives you `read()`, which reads the file from the saved
cursor, applies the config, reshapes the frame into observations and declares
each column.
Do not override `read()` or `tick()` for a CSV source; the two hooks below are
the places to change behaviour.

## Step 3: declare each column against the model

`declare_stream(ref_name)` is called once per distinct column in every batch.
The default declares the bare identity; override it to say what the column
means:

```python
from acquirium import CSVIngestDriver

POINTS = {
    "Feed Flow":     ("urn:ro-skid/feed-flow",       "gal/min", "volume flow rate"),
    "Permeate Flow": ("urn:ro-skid/permeate-flow",   "gal/min", "volume flow rate"),
    "Feed Pressure": ("urn:ro-skid/feed-pressure",   "psi",     "pressure"),
    "Permeate TDS":  ("urn:ro-skid/permeate-tds",    "mg/L",    "mass concentration"),
}


class ROSkidDriver(CSVIngestDriver):
    def declare_stream(self, ref_name: str) -> None:
        point = POINTS.get(ref_name)
        if point is None:
            self.declare(ref_name)               # a column we do not know: keep it, no point
            return
        point_uri, unit, quantity_kind = point
        self.declare(
            ref_name,
            value_kind="numeric",
            point_uri=point_uri,
            unit=unit,
            quantity_kind=quantity_kind,
        )
```

`unit` and `quantity_kind` take free text; they are resolved to QUDT together,
so the quantity kind disambiguates the unit.
Declaring is idempotent and cheap, so it is fine that this runs on every batch;
only the first call for each column does anything.
A column not in `POINTS` is still ingested, as in step 1.

## Step 4: fix the frame before it is parsed

`prepare_frame(df, path)` runs on the raw frame after the CSV is read and
before timestamps are parsed.
It is the place for source quirks that the config keys cannot express.
Say the skid started adding a `Comment` column with free text in mid-2026:

```python
    def prepare_frame(self, df, path):
        return df.drop("Comment") if "Comment" in df.columns else df
```

(`skip_cols = ["Comment"]` in the config does the same for a fixed name;
use the hook when the fix needs logic.)

## Step 5: insert the model on first start

The points above have to exist in the graph for queries to reach them through
equipment.
A driver can insert its part of the model itself, in `setup()`:

```python
    def setup(self) -> None:
        super().setup()                          # validates source_id, watch_dir, glob
        self.insert_graph_file(self.config_dir() / "ro-skid.ttl")
```

Note that `super().setup()` is required: it reads the file-source keys.
`insert_graph_file()` writes into this driver's own graph, so it cannot
overwrite the plant model or another driver's contribution.

## The whole driver

```python
from acquirium import CSVIngestDriver

POINTS = {
    "Feed Flow":     ("urn:ro-skid/feed-flow",     "gal/min", "volume flow rate"),
    "Permeate Flow": ("urn:ro-skid/permeate-flow", "gal/min", "volume flow rate"),
    "Feed Pressure": ("urn:ro-skid/feed-pressure", "psi",     "pressure"),
    "Permeate TDS":  ("urn:ro-skid/permeate-tds",  "mg/L",    "mass concentration"),
}


class ROSkidDriver(CSVIngestDriver):
    def setup(self) -> None:
        super().setup()
        self.insert_graph_file(self.config_dir() / "ro-skid.ttl")

    def prepare_frame(self, df, path):
        return df.drop("Comment") if "Comment" in df.columns else df

    def declare_stream(self, ref_name: str) -> None:
        point = POINTS.get(ref_name)
        if point is None:
            self.declare(ref_name)
            return
        point_uri, unit, quantity_kind = point
        self.declare(ref_name, value_kind="numeric", point_uri=point_uri,
                     unit=unit, quantity_kind=quantity_kind)
```

Drop a file into `data/ro-skid/`, wait one interval, and check:

```python
acq.query().entity(uri="urn:ro-skid/RO").measurement(quantity_kind="pressure").dataframe().tail(3)
```

## What happened underneath

On every tick the driver lists the folder, and for each file reads from the
row offset it saved last time (the *cursor*), so a file that grows is read
incrementally and a file already fully read costs nothing.
The cursor is saved only after the rows were registered and inserted, so a
failed insert is retried on the next tick with the same rows.
An unreadable file is logged and skipped without stalling the others.

This driver invented its own point URIs and inserted the RDF to back them.
When the plant is already described in the graph, the driver binds to points
that exist instead: see [a driver against an existing plant
model](driver-with-a-plant-model.md).

Every option of the built-in drivers, the other base classes and the full
driver contract are in the [driver reference](../reference/drivers.md).
