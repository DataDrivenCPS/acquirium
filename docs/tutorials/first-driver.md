---
title: File ingestion driver
---

Plant data lives in many places: CSV exports from a SCADA system, an MQTT
broker, a lab spreadsheet, a simulation. A **driver** is the piece of Python
that reads one of those sources and sends its readings to Acquirium. You
describe the source, which streams it produces, and how to read it. The server
does the rest: it schedules the driver, registers its streams, batches the
inserts, and stops the driver when the server shuts down.

In this tutorial we write a driver for the most common source of all, a folder
of CSV files. The [next tutorial](driver-with-a-plant-model.md) attaches the
same data to a plant model.

## Source files

An RO skid exports one file a day into a folder. Each file looks like this:

```text
Date,Time,Feed Flow,Permeate Flow,Feed Pressure,Permeate TDS
8/1/2026,12:00:00 AM,120.5,55.2,801,210
8/1/2026,12:15:00 AM,120.1,55.0,803,208
8/1/2026,12:30:00 AM,NaN,54.9,802,211
```

Three details matter. The file is *wide*, with one column per signal and one
row per timestamp. The timestamp is split across a `Date` and a `Time` column,
in US order. And a missing value is written as `NaN`.

## Built-in tabular driver

Acquirium ships a `CSVIngestDriver` that already knows how to watch a folder,
read new rows and parse timestamps. Describe the file in `acquirium.toml` and
it runs as it is:

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

Start the server with `acquirium server --config acquirium.toml`. Every minute
the driver looks for new rows, and every column becomes a stream named after
it: `Feed Flow` is a stream called `Feed Flow`. With no plant model to attach
to, each stream gets a *placeholder point*, a node created for it and labelled
with the column name. You can already find them all:

```python
acq.query().measurement(alias="m").metadata()
```

The rows are stored, but the server knows nothing about what they mean. To use
`Feed Flow`, you need to know that it is a volume flow rate in gallons per
minute. Saying so takes a few lines of code.

## Column units and quantity kinds

Create `ro_skid_driver.py` next to the config, subclass the built-in driver,
and point `spec` at your class: `spec = "ro_skid_driver.py:ROSkidDriver"`. The
driver calls `declare_stream()` once for each column it finds. Override it to
describe the column:

```python
from acquirium import CSVIngestDriver

COLUMNS = {
    "Feed Flow":     ("Feed flow",     "gal/min", "volume flow rate"),
    "Permeate Flow": ("Permeate flow", "gal/min", "volume flow rate"),
    "Feed Pressure": ("Feed pressure", "psi",     "pressure"),
    "Permeate TDS":  ("Permeate TDS",  "mg/L",    "mass concentration"),
}

class ROSkidDriver(CSVIngestDriver):
    def declare_stream(self, ref_name):
        if ref_name not in COLUMNS:
            self.declare(ref_name)          # a column we don't recognise: keep it as it is
            return
        label, unit, quantity_kind = COLUMNS[ref_name]
        self.declare(ref_name, value_kind="numeric", label=label,
                     unit=unit, quantity_kind=quantity_kind)
```

The unit and quantity kind are plain text. The server resolves them together,
which matters more than it looks: `"psi"` alone could belong to several kinds
of pressure-like quantity, and the quantity kind settles which one is meant.
The label is what result frames show in place of a URI.

Now the streams can be found by what they measure, not just by name:

```python
acq.query().measurement(alias="p", quantity_kind="pressure").dataframe().tail(3)
```

## Source data preprocessing

Real exports drift. Suppose the skid started adding a free-text `Comment`
column halfway through the year. The driver calls `prepare_frame()` on each
file after reading it and before parsing timestamps, so that is the place to
deal with it. Add this method to the class:

```python
    def prepare_frame(self, df, path):
        return df.drop("Comment") if "Comment" in df.columns else df
```

For a column you always want to skip, `skip_cols = ["Comment"]` in the config
does the same. Use the hook when the fix needs logic.

That is the whole driver: two hooks, and no other code. `acquirium driver list`
shows it as `running`. If it shows `failed:`, the error is in the
`acquirium.driver.ROSkidDriver` log.

## File progress and retry behavior

On every pass the driver lists the folder and reads each file from where it
left off, so a file that grows is read a few rows at a time and a file already
read costs nothing. It saves its position only after the rows are stored. If an
insert fails, the next pass sends the same rows again. A file it cannot read is
logged and skipped, without holding up the others.

## Measurement points in the plant model

Every column is stored, described by its unit and quantity kind, and findable
by what it measures. What the data lacks is a place in the plant. Nothing says
that `Feed Pressure` is measured at the RO inlet, so a query that starts from
equipment, such as `entity("pump").measurement()`, will never reach it, and
neither will a walk upstream from the RO. The
[next tutorial](driver-with-a-plant-model.md) connects the driver to a plant
model.

The [driver reference](../reference/drivers.md) lists every option of the
built-in drivers and the other base classes you can extend.
