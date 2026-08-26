# Building drivers

This is a guide to authoring drivers.
Drivers are the components that feed plant data into acquirium from external sources.
Reading the data back out is covered in the [querying](querying.md) and [data](data.md) guides.

## Introduction

A driver is a Python class that collects data from somewhere (a file drop, an
MQTT feed, a simulation, an instrument) and pushes it into the server on a
schedule, for continuous data ingest.

You write three things: the source identity, the streams the source produces,
and the code that reads it.
The platform owns the rest: scheduling, registration, batching, insertion and
shutdown.

## Where drivers run

Drivers do not run in your process.
The server imports the class and spawns one Ray actor per driver, on the server host.
The actor talks back to the server with an `Acquirium` client.

**TODO:** We'll develop drivers that can run on an edge device and connect to a remote acquirium server to push data.

## The driver lifecycle

After startup the actor runs a fixed loop:

```text
setup()              once, at start; assign source_id, declare streams, open resources
                     the platform registers what was declared
tick()               immediately after setup
loop:
    wait `interval` seconds (or a stop signal)
    graph changed since last look?  ->  on_graph_change()
    tick()                             ->  read + insert
stop()               when the driver is stopped
                     the platform flushes whatever is still buffered
```

`setup()` is where identity and declarations happen; `tick()` is where data is
collected and inserted.
`on_graph_change()` is optional and fires when anything modified the
semantic model, which is how a driver picks up newly declared streams
without restarting.

An exception inside `tick()` is logged and swallowed.
The loop keeps going and the driver still reports as running, so check the
`acquirium.driver.<ClassName>` logs for tick failures; the driver status will
not show them.

## The contract

Every driver subclasses `Driver`, or one of the ingest bases below:

```python
from acquirium import Driver

class MyDriver(Driver):
    def setup(self):
        self.source_id = "my-source"
        ...

    def tick(self):
        ...
```

| method | required | called |
|---|---|---|
| `setup()` | yes | once, at start |
| `tick()` | yes | every `interval` seconds |
| `on_graph_change()` | no | before a tick, when the model changed |
| `stop()` | no | at shutdown, to quiesce producers and close resources |

`source_id` names the data source this driver writes as.
It has no default.
Set it in `setup()`, or under the driver's config as `source_id`.
A multi-source driver can skip it when every observation carries its own
`source_id`.

The base class gives you three things on `self`:

- `self.aq`: the `Acquirium` client, connected to this server.
- `self.config`: the full parsed config file, so a driver can read its own keys.
- `self.state`: a small persistent store, below.

And these helpers: `config_dir()` (the config file's directory, for resolving
relative paths), `data_dir()` (the server's data directory),
`reference_uri(ref_name)` (the canonical URI of one of this driver's streams),
`insert_graph()` / `insert_graph_file()` and `sparql_update()`.

### Persistent state

`self.state` is a key-value store that survives restarts.
Every change is saved to a JSON file under the server's data directory.
The file drivers use it to remember how far into each file they have
ingested; yours can keep watermarks, cursors, or any JSON-serializable value.

```python
self.state.set("watermark", ts.isoformat())
self.state.get("watermark")          # None if never set
```

Note that `DriverState` is not a dict.
It has `get`, `set`, `delete`, `keys`, `update` and `clear`; `state["k"] = v`
raises `TypeError`.

## Declaring streams

A stream is identified by the pair `(source_id, ref_name)`.
Declare every pair before reporting data for it:

```python
self.declare(
    "room 101 temperature",
    value_kind="numeric",
    point_uri="urn:building-a:room-101-temperature",
    unit="http://qudt.org/vocab/unit/DEG_C",
)
```

Note that acquirium does not sanitize or rewrite either value.
A column called `Sample Point 3` becomes exactly that `ref_name`, spaces
included.
Map names yourself if you want different ones, and declare the mapped name.

`declare()` takes `source_id`, `value_kind`, `point_uri`, `label`, `unit`,
`quantity_kind`, `medium`, `substance`, `data_source` and `properties`.
All are optional; `source_id` defaults to the driver's own.
Use `properties` only for extension predicates that have no named argument.

Declaring is idempotent and cheap.
Call it for every stream every time you read the source; only the first call
does anything.
Repeating an identical declaration is a no-op, while changing the metadata of
an existing pair raises an error.
When `point_uri` names a point the graph already has, the declared metadata is
checked against it at registration: a field the point lacks is added, a
conflicting value raises `ValueError`, and a unit that differs from the
point's is accepted only when convertible, in which case it is recorded as
the storage unit and reads convert to the point's unit.
Without a `point_uri`, a placeholder point `<ref_uri>__point` is minted and
labelled `source_id__ref_name`; see the
[lifecycle guide](data-stream-lifecycle.md#what-registration-writes-to-the-graph).

The platform writes declarations to the graph just before the next insert, so
streams always exist before their observations do.
Reporting an undeclared pair raises `UndeclaredStreamError`.

`value_kind` can be left out.
It is then inferred from the first meaningful values and recorded before those
values are inserted.

## Choosing a base class

| base | use when data is | you implement |
|---|---|---|
| `PollingIngestDriver` | pulled every tick | `read()`, reporting values with `add()` |
| `PollingIngestDriver` | pulled every tick, already in bulk | `collect()` returning a frame |
| `EventIngestDriver` | pushed to you by callbacks | the callback, calling `add()` |
| `FileIngestDriver` | in files landing in a directory | `read(path, cursor)` returning a `FileBatch` |

All of them are importable from the package root:

```python
from acquirium import PollingIngestDriver, EventIngestDriver, FileIngestDriver, FileBatch
```

Subclassing `Driver` directly is for drivers that do not ingest timeseries at
all, for instance one that only maintains the semantic model.

## A minimal driver

A polling driver with two streams:

```python
from acquirium import PollingIngestDriver


class ProbeDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "probe-lab"
        self.declare("temperature", value_kind="numeric")
        self.declare("status", value_kind="text")

    def read(self):
        self.add("temperature", read_temperature())
        self.add("status", read_status())
```

That is the whole driver.
Registration, batching and insertion are the platform's job.

Point the server at it:

```toml
[[drivers]]
spec     = "probe_driver.py:ProbeDriver"
interval = 5.0
```

## Reporting observations

`add(ref_name, value, ts=None, *, source_id=None)` buffers one reading.

```python
self.add("temperature", 21.5)                  # ts defaults to now, UTC
self.add("temperature", 21.5, sampled_at)      # naive datetimes are read as UTC
```

`add()` is thread-safe and does no network I/O, so it is safe to call from a
broker callback.
Be aware that a successful `add()` means the row was accepted into memory, not
that it was stored.

The buffer is inserted at the end of every tick.
An insert failure puts the rows back and the next tick retries them, so a
server that is briefly down does not cost you data.
Note that the buffer is memory only.
Rows accepted but not yet inserted are lost if the process crashes.

`max_buffered_rows` (100000 by default) caps the buffer.
`add()` raises `DriverBufferFull` once it is reached, which is the signal to
apply backpressure on the source.

On a graceful stop the platform calls `stop()` first, then flushes what is
left.
A failed final flush makes the shutdown unsuccessful and logs the unsent row
count.

### Bulk frames

A source that already produces a whole batch can skip `add()` and return a
frame from `collect()`:

```python
def collect(self):
    return pl.DataFrame({"ts": timestamps, "ref_name": names, "value": values})
```

The frame needs `ts`, `ref_name` and `value`, plus a `source_id` column when
the driver spans several datasources.
Every pair in it must already be declared.

Values are normalized before upload:
`ts` is cast to `Datetime(us, UTC)`, `ref_name` and `value` are cast to
strings, and rows with a null `ts` or `ref_name` are dropped.
`value` is transferred as a string regardless of the type; the server decides
the storage column from the stream's `value_kind`.

## File drivers

`FileIngestDriver` walks a watched directory and pages through what it finds.
It owns recursive discovery, per-file cursors persisted across restarts,
registration before insertion, and error isolation so one unreadable file
cannot stall the others.

You implement `read(path, cursor)`:

```python
from pathlib import Path
from typing import Any

import polars as pl

from acquirium import FileBatch, FileIngestDriver


class PlantParquetDriver(FileIngestDriver):
    def setup(self):
        super().setup()                       # validates source_id, watch_dir, glob
        self.declare("flow", value_kind="numeric", point_uri="urn:plant:flow")

    def read(self, path: Path, cursor: Any) -> FileBatch:
        offset = cursor or 0
        frame = pl.read_parquet(path).slice(offset)
        if frame.is_empty():
            return FileBatch(None, cursor)
        observations = frame.select(
            pl.col("timestamp").alias("ts"),
            pl.lit("flow").alias("ref_name"),
            pl.col("flow").alias("value"),
        )
        return FileBatch(observations, offset + len(frame))
```

`source_id`, `watch_dir` and `glob` are required in the config; `glob` is one
pattern or a list.
Call `super().setup()` when you override `setup()`, as it reads and validates
those keys.

`cursor` is whatever the previous call returned for that file, or `None` on
first sight.
It must be JSON-serializable: a row offset, a byte position or an ISO
timestamp all work.
Returning the cursor unchanged means there was nothing new.

The cursor is saved only after registration and insertion both succeed.
An exception or a failed insert keeps the old one, so the next tick reads the
same rows again.

## Configuration

Drivers are declared in the server config.
`[driver]` holds defaults shared by all of them; each `[[drivers]]` entry
starts one driver and can override any default:

```toml
[driver]
interval = 10.0

[[drivers]]
spec     = "probe_driver.py:ProbeDriver"
name     = "probe-lab"       # registry name; defaults to the class name
interval = 5.0

[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
source_id = "operator-exports"
watch_dir = "data/incoming"
glob      = ["*.csv", "*.tsv"]
format    = "wide"
```

Every key in an entry is passed through to the driver, and reaches it as
`self.config["driver"]`.
Custom config keys need no declaration; read them in `setup()`.
Two drivers cannot share a `name`.
Attempting to register raises `Driver 'X' is already running`.

### Intervals

`interval` sets the tick cadence, which is also how often buffered rows reach
the server.
`graph_poll_interval` sets how often the driver checks whether the graph
changed, and defaults to `max(interval, 10.0)`.
Set them apart when data arrives fast but the model rarely moves.

Driver methods are serialized, so a slow `tick()` delays both deadlines.

### spec

`spec` points at the class, in one of two forms:

- a file path: `spec = "probe_driver.py:ProbeDriver"`
- an import path: `spec = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"`

File paths resolve relative to the config file's directory, on the server's
filesystem.
Modules next to the spec file are importable, so a driver can be split across
several files in one directory.

### driver_id and state files

The state file from [Persistent state](#persistent-state) is named after
`driver_id`, or derived from the driver spec when `driver_id` is not set.
Set a distinct `driver_id` on each entry when you run the same class twice:


```toml
[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
driver_id = "csv-lab"
watch_dir = "data/lab"

[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
driver_id = "csv-scada"
watch_dir = "data/scada"
```

**TODO:** We can make default driver naming unique to avoid collisions.

### Starting drivers on a running server

`acquirium driver start CONFIG` pushes each `[[drivers]]` entry of a config to
a running server, addressed under `[driver]` (`server_url`, `server_port`).
This is how driver definitions are kept next to a deployment while the server
runs elsewhere.

## Drivers and the graph

A driver can also write to the semantic model.

It is a recommended pattern to insert the plant model on first start, then declare streams against its points.

```python
self.insert_graph_file(model_path)          # or insert_graph(turtle_text)
```

Both helpers write to the driver's own graph, owned by its `source_id`.
They take no owner argument, so a driver cannot write into the plant model or
another driver's graph by accident.
`replace=True` therefore replaces only this driver's contribution.


`on_graph_change()` runs before the next tick whenever anything modified the graph.

Be aware that a graph write inside `on_graph_change()` changes the graph
version again, which might fire the hook again on the next tick.
Acquirium checks if a graph insertion is repetitive by checking the diff however, this guardrail will fail when re-writing the same graph that has Blank Nodes.
In that case the driver will loop the on graph change forever.

Note that `on_graph_change()` never fires for the initial graph.
Do the first query in `setup()`.

## Operations

The CLI manages drivers on a running server:

```bash
acquirium driver list                    # name, status, interval, started, spec
acquirium driver stop --name probe-lab
acquirium driver start acquirium.toml    # push the [[drivers]] entries
```

The reported status is `running`, `stopped`, or `failed: <error>`.
`failed` means the actor died, usually an exception in `setup()`.
Exceptions in `tick()` do not change the status; see
[The driver lifecycle](#the-driver-lifecycle).

Stopping signals the loop, waits up to 10 seconds for the current tick to
finish, then kills the actor.
`stop()` runs in this window; keep cleanup short.

## Built-in driver reference

### Tabular (CSV / XLSX / Parquet)

```toml
[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
source_id = "operator-exports"
watch_dir = "./data/incoming"
glob      = ["*.csv", "*.tsv"]
format    = "wide"          # required: "wide" or "narrow"
time_col  = "time"          # one complete timestamp column
# date_col  = "Date"        # or an explicit date/time pair
# clock_col = "Time"
id_col    = "id"            # narrow only
value_col = "value"         # narrow only
skip_cols = ["notes"]
timezone  = "UTC"           # how to read naive timestamps
```

| key | default | meaning |
|---|---|---|
| `source_id`, `watch_dir`, `glob` | none | required, as for any file driver |
| `format` | none | required; `wide` (one column per stream) or `narrow` (id/value triples) |
| `time_col`, `date_col`, `clock_col` | discovered | timestamp columns; explicit settings win |
| `id_col`, `value_col` | `"id"`, `"value"` | narrow layout only |
| `skip_cols` | `[]` | columns to drop |
| `timezone` | `"UTC"` | interpretation of naive timestamps |
| `date_format`, `day_first` | none, `false` | for unusual or ambiguous dates |
| `skip_rows`, `header_contains`, `encoding`, `ragged_lines` | | CSV only |
| `null_values` | `[]` | CSV only; the source's missing-value sentinels (`["Null", "-"]`) |
| `infer_schema_length` | `100` | CSV only; rows polars samples to type each column; `0` reads every column as text |
| `sheets` | all | XLSX only |

Note that the layout is never inferred; `format` has to be set.
Column names become `ref_name`s unchanged.
Timestamps are discovered from common names (`timestamp`, `ts`, `Date` plus
`Time`, `Sample Date` plus `Sample Time`), and unparseable ones are logged and
dropped.
Set `infer_schema_length = 0` for a source whose columns change shape
mid-file, such as plain numbers early and thousands-separated values later;
every column then reads as text and the value kind is inferred downstream.

`CSVIngestDriver` has two hooks for source-specific quirks, so a deployment
driver subclasses it instead of reimplementing `read()`:

```python
class PlantCSVDriver(CSVIngestDriver):
    def prepare_frame(self, df, path):
        # runs on the raw frame after skip_cols, before timestamps are parsed
        return df.rename({"Date/Time": "time"})

    def declare_stream(self, ref_name):
        # called once per distinct ref_name in each batch; default is self.declare(ref_name)
        self.declare(ref_name, **self.mapping.declaration(ref_name))
```

The two conversions are public, for drivers that read their own files:

```python
from acquirium import to_observations, to_timestamp
```

An ingest driver can also call `self.to_timestamp(...)`.

### MQTT

```toml
[[drivers]]
spec       = "acquirium.Drivers.BuiltInDrivers.mqtt_ingestion:MQTTIngestDriver"
source_id  = "mqtt"
interval   = 1.0
graph_poll_interval = 30.0
mqtt_qos   = 0
```

Subscriptions come from the graph, not the config.
The driver queries for reference nodes carrying broker, topic, time key and
value key, declares each one, and subscribes.
`on_graph_change()` re-runs that query, so adding a stream to the model is
enough to start ingesting it.

### WaterTAP

```toml
[[drivers]]
spec                    = "acquirium.Drivers.BuiltInDrivers.watertap:WaterTAPDriver"
source_id               = "watertap"
watertap_mapping_path   = "mapping.json"
watertap_build_spec     = "model.py:build"
watertap_solve_spec     = "model.py:solve"
watertap_graph_path     = "model.ttl"
watertap_insert_graph   = true
```

| key | default | meaning |
|---|---|---|
| `source_id` | `"watertap"` | datasource for the results |
| `watertap_mapping_path` | none | point URI → pyomo path mapping |
| `watertap_build_spec`, `watertap_solve_spec` | none | `file.py:fn` or `module:fn` |
| `watertap_change_inputs_spec`, `watertap_inputs`, `watertap_build_kwargs` | none | optional input handling |
| `watertap_graph_path`, `watertap_insert_graph`, `watertap_insert_graph_replace` | none, `false`, `false` | insert the model on start |
| `watertap_result_attr` | none | attribute to read results from |

The driver rebuilds and solves the model on every tick, then reports each
mapped value.
