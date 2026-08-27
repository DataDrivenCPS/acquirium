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
every new file, and gives every column its unit and quantity kind.

**No plant model is involved.** This is data ingest on its own: getting the
rows in, stored, and described well enough to query by what they measure.
Attaching them to equipment is what a plant model is for, and that is the
[next tutorial](driver-with-a-plant-model.md).
Starting here is deliberate — a driver you can run and watch insert rows is a
much easier thing to debug than one that also has to match a model.

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
and each one has a *placeholder point*: a node minted for the stream, labelled
`ro-skid__Feed Flow` and so on.

```python
acq.query().measurement(alias="m").metadata()
```

The rows are safely stored and reachable. What is missing is meaning: nothing
says that `Feed Flow` is in gallons per minute, or that it is a flow rate at
all, so you cannot ask for it by anything except its name.
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

## Step 3: say what each column measures

`declare_stream(ref_name)` is called once per distinct column in every batch.
The default declares the bare identity; override it to say what the column
means:

```python
from acquirium import CSVIngestDriver

COLUMNS = {
    "Feed Flow":     ("Feed flow",     "gal/min", "volume flow rate"),
    "Permeate Flow": ("Permeate flow", "gal/min", "volume flow rate"),
    "Feed Pressure": ("Feed pressure", "psi",     "pressure"),
    "Permeate TDS":  ("Permeate TDS",  "mg/L",    "mass concentration"),
}


class ROSkidDriver(CSVIngestDriver):
    def declare_stream(self, ref_name: str) -> None:
        described = COLUMNS.get(ref_name)
        if described is None:
            self.declare(ref_name)               # a column we do not know: keep it as is
            return
        label, unit, quantity_kind = described
        self.declare(
            ref_name,
            value_kind="numeric",
            label=label,
            unit=unit,
            quantity_kind=quantity_kind,
        )
```

`unit` and `quantity_kind` take free text; they are resolved to QUDT together,
so the quantity kind disambiguates the unit (`"psi"` alone is ambiguous across
several pressure-like quantities).
These land on the column's placeholder point, so the streams become queryable
by what they measure rather than only by name:

```python
acq.query().measurement(quantity_kind="pressure").metadata()
acq.query().measurement(unit="gal/min").metadata()
```

`label` replaces the default `ro-skid__Feed Flow` and is what result columns
display in place of the URI, which is worth setting for readability alone.

Declaring is idempotent and cheap, so it is fine that this runs on every batch;
only the first call for each column does anything.
A column not in `COLUMNS` is still ingested, as in step 1 — an undescribed
stream is better than a dropped one.

Note that the driver does not pick point URIs here.
Without a plant model there is nothing to attach to, and inventing URIs now
would only make it harder to adopt a real model later: the invented points
would sit beside the real ones rather than becoming them.
Let the platform mint the placeholders and describe them.

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

## The whole driver

Two hooks, no `setup()`, no RDF:

```python
from acquirium import CSVIngestDriver

COLUMNS = {
    "Feed Flow":     ("Feed flow",     "gal/min", "volume flow rate"),
    "Permeate Flow": ("Permeate flow", "gal/min", "volume flow rate"),
    "Feed Pressure": ("Feed pressure", "psi",     "pressure"),
    "Permeate TDS":  ("Permeate TDS",  "mg/L",    "mass concentration"),
}


class ROSkidDriver(CSVIngestDriver):
    def prepare_frame(self, df, path):
        return df.drop("Comment") if "Comment" in df.columns else df

    def declare_stream(self, ref_name: str) -> None:
        described = COLUMNS.get(ref_name)
        if described is None:
            self.declare(ref_name)
            return
        label, unit, quantity_kind = described
        self.declare(ref_name, value_kind="numeric", label=label,
                     unit=unit, quantity_kind=quantity_kind)
```

Drop a file into `data/ro-skid/`, wait one interval, and check that the rows
arrived and carry their meaning:

```python
acq.query().measurement(alias="m", quantity_kind="pressure").dataframe().tail(3)
```

`acquirium driver list` shows the driver as `running`; if it is not there, or
shows `failed:`, the error is in the `acquirium.driver.ROSkidDriver` log.

## What happened underneath

On every tick the driver lists the folder, and for each file reads from the
row offset it saved last time (the *cursor*), so a file that grows is read
incrementally and a file already fully read costs nothing.
The cursor is saved only after the rows were registered and inserted, so a
failed insert is retried on the next tick with the same rows.
An unreadable file is logged and skipped without stalling the others.

## What you have, and what you do not

Every column is stored, described by unit and quantity kind, and findable with
`measurement()` and the attribute filters.

What the placeholder points cannot do is topology. Nothing in the graph says
`Feed Pressure` is measured at the RO inlet, so no query that starts from
equipment reaches it: `entity("pump").measurement()` will not find these
streams, and neither will `direction="upstream"`. They are readings without a
place.

That is not a defect of the driver, it is the absence of a plant model. When
one exists, the same driver binds its columns to the points already in it and
the readings join the plant proper: [a driver against an existing plant
model](driver-with-a-plant-model.md) is this driver again, with that one
change.

Every option of the built-in drivers, the other base classes and the full
driver contract are in the [driver reference](../reference/drivers.md).
