# Building drivers

This is a guide to authoring drivers.
Drivers are the components that feed plant data into acquirium from external sources.
Reading the data back out is covered in the [querying](querying.md) and [data](data.md) guides.

## Introduction

A driver is a Python class that collects data from somewhere (a file drop, an
MQTT feed, a simulation, an instrument) and pushes it into the server on a
schedule, for continuous data ingest.

## Where drivers run

Drivers do not run in your process.
The server imports the class and spawns one Ray actor per driver, on the server host.
The actor talks back to the server with an `Acquirium` client.

**TODO:** We'll develop drivers that can run on an edge device and connect to a remote acquirium server to push data.

## The driver lifecycle

After startup the actor runs a fixed loop:

```text
setup()              once, at start
tick()               immediately after setup
loop:
    wait `interval` seconds (or a stop signal)
    graph changed since last look?  ->  on_graph_change()
    tick()
stop()               when the driver is stopped
```

`setup()` is where registration happens; `tick()` is where data is
collected and inserted.
`on_graph_change()` is optional and fires when anything modified the
semantic model, which is how a driver picks up newly declared streams
without restarting.

An exception inside `tick()` is logged and swallowed.
The loop keeps going and the driver still reports as running, so check the
`acquirium.driver.<ClassName>` logs for tick failures; the driver status will
not show them.

## The contract

Every driver subclasses `Driver`:

```python
from acquirium.Drivers.Driver import Driver

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
| `stop()` | no | at shutdown, for cleanup |

`source_id` names the data source this driver writes as.
It has no default; set it in `setup()`.
A multi-source driver can skip it when every observation row carries its own
`source_id` column.

The base class gives you three things on `self`:

- `self.aq`: the `Acquirium` client, connected to this server.
- `self.config`: the full parsed config file, so a driver can read its own keys.
- `self.state`: a small persistent store, below.

And three helpers: `config_dir()` (the config file's directory, for resolving
relative paths), `data_dir()` (the server's data directory) and
`reference_uri(ref_name)` (the canonical URI of one of this driver's streams).

### Persistent state

`self.state` is a key-value store that survives restarts.
Every change is saved to a JSON file under the server's data directory.
The tabular drivers use it to remember how far into each file they have
ingested; yours can keep watermarks, cursors, or any JSON-serializable value.

```python
self.state.set("watermark", ts.isoformat())
self.state.get("watermark")          # None if never set
```

The full API is `get`, `set`, `delete`, `keys`, `update`, `clear`.
Note it is method-based; `self.state["k"] = v` does not work.

## Choosing a base class

We provide three base classes for authoring common drivers.
Based on your data source (pull-based, push-based or tabular), select one of
these as your parent class.

| base | data arrives by | you implement |
|---|---|---|
| `PollingIngestDriver` | asking for it every tick | `collect()` returning a frame |
| `EventIngestDriver` | being pushed to you (callbacks) | wiring the callback to `insert_observations()` |
| `TabularIngestBase` | files landing in a directory | usually nothing; override hooks to customize |

All three use the same observation frame: a polars frame with columns
`ts`, `ref_name` and `value`.

There are also ready-made drivers that can be used directly from the config,
without writing any code:

| driver | ingests |
|---|---|
| `CSVIngestDriver` | CSV files from a watch directory |
| `XLSXIngestDriver` | Excel files (needs the `acquirium[xlsx]` extra) |
| `ParquetIngestDriver` | parquet files from a watch directory|
| `MQTTIngestDriver` | MQTT topics declared in the graph (needs the `acquirium[mqtt]` extra) |
| `WaterTAPDriver` | a live WaterTAP simulation, solved every tick (needs the `acquirium[watertap]` extra) |
| `SystemMetricsDriver` | CPU/RAM/disk/network of the host|

They all live under `acquirium.Drivers.BuiltInDrivers` and are also the
reference implementations.
For instance, when your source is a CSV drop but the files are formatted
unconventionally, subclassing the closest built-in and overriding one hook is
usually shorter than starting from `TabularIngestBase` or `Driver`.

## A minimal driver

Below is a complete polling driver.
It reads one value per tick from an imaginary instrument and inserts it.

```python
import polars as pl
from datetime import datetime, timezone
from acquirium.Drivers.Driver import PollingIngestDriver


class ProbeDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "probe-1"
        self.aq.register_datasource(self.source_id)
        self.aq.register_streams([{
            "source_id": self.source_id,
            "ref_name": "effluent-tds",
            "value_kind": "numeric",
            "point_uri": "urn:swro/effluent-tds",
            "unit": "mg/L",
            "quantity_kind": "mass concentration",
        }])

    def collect(self) -> pl.DataFrame:
        value = read_probe()          # whatever your source looks like
        return pl.DataFrame({
            "ts": [datetime.now(timezone.utc)],
            "ref_name": ["effluent-tds"],
            "value": [value],
        })
```

`setup()` registers the datasource and the stream, with the same API shown
in the [data guide](data.md).
`collect()` returns the observation frame, and the base class normalizes and
uploads it.

Returning an empty frame is allowed; it means there is nothing new in this
tick.

Run it by listing it under `[[drivers]]` in the config:

```toml
[[drivers]]
spec     = "probe_driver.py:ProbeDriver"
interval = 10.0
```

then `acquirium server --config acquirium.toml`, or push it to a running
server with `acquirium driver start acquirium.toml`.

## The observation frame

Everything a driver ingests goes through a polars frame with
columns `ts`, `ref_name` and `value`.
A missing column will raise an error:

```text
ValueError: Observation frames must include columns ts, ref_name, value; missing ['ts']
```

`insert_observations()` normalizes the frame before uploading:

- `ts` is cast to `Datetime(us, UTC)`. Strings are parsed (pass `date_format=`
  to `normalize_timestamps()` for unusual layouts). Timestamps without a
  timezone are assumed to be UTC.
- `value` is transferred as a `string` regardless of the type. The server restores
  the real type from the stream's registered `value_kind`.
- Rows with a null `ts` or `ref_name` are dropped.

Multi-source drivers push four columns: `ts`, `ref_name`, `value` and `source_id`.
Acquirium splits the frame and inserts per source.


## Tabular drivers

`TabularIngestBase` and its subclasses (`CSVIngestDriver`, `XLSXIngestDriver`,
`ParquetIngestDriver`) watch a directory and ingest new rows from each file.
Each tick they list the files, read past the rows already ingested, and insert
what is new.
The per-file row offset is kept in `self.state`, and it advances only after a
successful upload, so a failed upload is retried in the next tick.

The base implements `setup()` and `tick()` itself.
To customize one, override hooks instead:

| hook | returns | default |
|---|---|---|
| `configure_tabular_driver()` | - | runs before setup; read your config keys here |
| `after_tabular_setup()` | - | runs after setup; extra registration goes here |
| `source_id_for(path)` | the datasource for a file | one datasource per file |
| `stream_name_for(raw_name)` | ref_name for a raw column name | unchanged |
| `stream_specs_for_names(path, source_id, raw_names, ...)` | registration dicts | name-only, no `point_uri` |
| `read_frame(path, row_offset)` | `(frame, rows_read)` | reads per `format` |
| `time_col()`, `id_col()`, `value_col()` | column names | `"time"`, `"id"`, `"value"` |
| `ingest_format()` | `"wide"`, `"narrow"` or `"auto"` | from config |
| `date_format()` | timestamp format string | none |
| `skip_cols(path, col_names)`, `skip_rows_for(path)` | columns/rows to ignore | none |

`stream_specs_for_names()` controls registration.
The default registers streams by name only, without a `point_uri`, so the rows
are stored but no semantic query reaches them.
Override it to attach each column to its model point; the WaterTAP and Benicia
parquet drivers in `deployments/` are complete examples of this pattern.

`read_frame()` controls parsing.
It may return a wide frame (`time_col` plus one column per stream), a narrow
frame (`time_col`, `id_col`, `value_col`), or an already normalized
`(ts, ref_name, value)` frame; the shape is detected automatically.
Override it when the files need cleanup that the `skip_*` hooks cannot
express, for example merging separate date and time columns.

File formats are set in the config:

```toml
[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
watch_dir = "data/incoming"
format    = "wide"          # or "narrow" / "auto"
time_col  = "timestamp"
```

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
watch_dir = "data/incoming"
```

Every key in an entry is passed through to the driver.
Custom config keys need no declaration; read them in `setup()` or `configure_tabular_driver()`.
Two drivers cannot share a `name`. 
Attempting to register raises `Driver 'X' is already running`.

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
`driver_id`, or after the class name when `driver_id` is not set.
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

It is a recommended pattern to insert the plant model on first start, then register streams against its points.

Always pass `replace=False`:

```python
self.aq.insert_graph(model_path, replace=False)
```

There is one main graph, and `replace=True` clears all of it, including
triples written by other drivers and apps.

**TODO:** We will implement named graphs


`on_graph_change()` runs before the next tick whenever anything modified the graph.

Be aware that a graph write inside `on_graph_change()` changes the graph
version again, which might fire the hook again on the next tick.
Acquirium checks if a graph insertion is repetitive by checking the diff however, this guardrail will fail when re-writing the same graph that has Blank Nodes.
In that case the driver will loop the on graph change forever.


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

| key | default | meaning |
|---|---|---|
| `watch_dir` | `"."` | directory to watch, relative to the config file |
| `format` | `"auto"` | `"wide"` (one column per stream), `"narrow"` (id/value columns), or `"auto"` |
| `time_col` | `"time"` | timestamp column |
| `id_col` | `"id"` | stream-name column (narrow format) |
| `value_col` | `"value"` | value column (narrow format) |
| `date_format` | none | explicit timestamp format string |
| `skip_cols` | `[]` | column names to ignore |
| `skip_rows` | `[]` | row indexes to skip |
| `encoding` | `"utf8-lossy"` | CSV only |
| `ragged_lines` | `"ignore"` | CSV only |
| `header_contains` | `[]` | CSV only; find the header row by content when banner lines precede it |
| `sheets` | all | XLSX only; sheet names to read |

### MQTT

| key | default | meaning |
|---|---|---|
| `mqtt_source_id` | `"mqtt"` | datasource the readings are written under |
| `mqtt_qos` | `0` | subscription QoS |
| `mqtt_value_kind` | `"text"` | default value kind for streams that do not declare one |

The MQTT driver takes its subscriptions from the graph, not from the config.
It looks for reference nodes typed `ref:MQTTReference` and reads the broker,
topic and payload keys from that node.
`on_graph_change()` re-reads them, so declaring a new topic in the model is
enough; the driver does not need a restart.

Payloads are decoded as JSON.
For any other wire format, subclass and override `decode_payload(payload,
spec)`; `scripts/custom_mqtt_driver.py` in the repo is a complete MessagePack
example.

### WaterTAP

`WaterTAPDriver` builds a WaterTAP model once, then re-solves it every tick
and reads the mapped Pyomo variables as observations.

| key | default | meaning |
|---|---|---|
| `watertap_source_id` | `"watertap"` | datasource for the results |
| `watertap_mapping_path` | required | JSON mapping of point URIs to Pyomo paths |
| `watertap_build_spec` | required | `file.py:fn` or `module:fn` returning the model |
| `watertap_solve_spec` | required | callable that solves the model |
| `watertap_change_inputs_spec` | none | callable applying new inputs before each solve |
| `watertap_build_kwargs` | `{}` | keyword arguments for the build callable |
| `watertap_inputs` | `{}` | inputs passed to the change-inputs callable |
| `watertap_graph_path` | none | model TTL to insert at setup |
| `watertap_insert_graph` | `false` | insert that TTL at setup |
| `watertap_insert_graph_replace` | `false` | leave this `false`; see [Drivers and the graph](#drivers-and-the-graph) |
| `watertap_register_streams` | `true` | register the mapped points at setup |
| `watertap_result_attr` | none | attribute of the solve result to read values from |

Be aware of the path bases: `watertap_*` paths resolve against the process
working directory (the directory the server was started from), while `spec`
and `watch_dir` resolve against the config file's directory.
The deployment configs under `deployments/WATERTAP/` document this split and
are the reference for a complete setup.
