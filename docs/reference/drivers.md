---
title: Driver reference
---

<!-- TODO: intro -->

This is the reference for authoring and configuring drivers: the class
hierarchy, every hook and helper with its signature, the persistent state
API, the configuration keys, the built-in drivers and the CLI.
For a worked example see [your first driver](../tutorials/first-driver.md);
for why drivers exist see [drivers](../explanation/drivers.md).

## Runtime

Drivers do not run in your process.
The server imports the class and spawns one Ray actor per driver, on the
server host.
The actor talks back to the server with an `Acquirium` client.

**TODO:** We'll develop drivers that can run on an edge device and connect to a remote acquirium server to push data.

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

Driver methods are serialized, so a slow `tick()` delays both deadlines.
An exception inside `tick()` is logged and swallowed; the loop keeps going and
the driver still reports as `running`.
Check the `acquirium.driver.<ClassName>` logs for tick failures.
An exception in `setup()` kills the actor before the driver is registered, so
it never appears in `acquirium driver list` at all; the start call fails with
the error instead.
The `failed: <error>` status is what a driver whose loop has already exited
with an exception reports.

## Class hierarchy

| class | extends | implement | use for |
|---|---|---|---|
| `Driver` | — | `setup()`, `tick()` | drivers that do not ingest timeseries, e.g. one that only maintains the model |
| `IngestDriver` | `Driver` | — | base of the three below; adds declaring, buffering and insertion |
| `PollingIngestDriver` | `IngestDriver` | `read()` or `collect()` | sources you pull from every tick |
| `EventIngestDriver` | `IngestDriver` | a callback that calls `add()` | sources that push to you |
| `FileIngestDriver` | `IngestDriver` | `read(path, cursor)` | files landing in a directory |
| `CSVIngestDriver` | `FileIngestDriver` | optionally `prepare_frame()`, `declare_stream()` | CSV and TSV files |
| `XLSXIngestDriver`, `ParquetIngestDriver` | `FileIngestDriver` | optionally `read()` | Excel and Parquet files |
| `MQTTIngestDriver` | `EventIngestDriver` | — | topics declared in the graph |
| `WaterTAPDriver` | `PollingIngestDriver` | — | a WaterTAP flowsheet solved every tick |
| `SystemMetricsDriver` | `PollingIngestDriver` | — | CPU, RAM, disk and network of the server host |

```python
from acquirium import (Driver, IngestDriver, PollingIngestDriver, EventIngestDriver,
                       FileIngestDriver, FileBatch, CSVIngestDriver,
                       UndeclaredStreamError, DriverBufferFull, to_observations, to_timestamp)
from acquirium.DriverState import DriverState
```

<!-- TODO: `from acquirium import DriverState` silently resolves to the
     acquirium.DriverState *module*, not the class, because the module shadows
     the name. Export the class from `acquirium/__init__.py` (or rename the
     module) and simplify this import. -->

The other built-ins are imported by their module path, as in the `spec`
values below.

## Driver

### Attributes

| attribute | meaning |
|---|---|
| `self.aq` | the `Acquirium` client, connected to this server |
| `self.config` | the full parsed config file; the driver's own entry is `self.config["driver"]` |
| `self.state` | the persistent key-value store, below |
| `self.source_id` | the datasource this driver writes as; set under the config entry or assigned in `setup()`. Reading it before it is set raises `AttributeError`. A multi-source driver may leave it unset when every observation carries a `source_id` |

### Hooks

| method | required | called |
|---|---|---|
| `setup() -> None` | yes | once, at start |
| `tick() -> None` | yes on `Driver`; provided by the ingest bases | every `interval` seconds |
| `on_graph_change() -> None` | no | before a tick, when the server's source generation advanced since the last look; never for the initial graph |
| `stop() -> None` | no | at shutdown, before the final flush; keep it short, the stop window is 10 seconds |

`_after_setup()` and `_shutdown()` are framework hooks; do not override them.

Be aware that a graph write inside `on_graph_change()` advances the graph
version again and can fire the hook on the next tick.
Acquirium skips a rewrite whose diff is empty, but that check fails for a
graph with blank nodes, and the hook then fires on every tick.

### Helpers

| method | meaning |
|---|---|
| `reference_uri(ref_name: str) -> URIRef` | the canonical URI of one of this driver's streams |
| `insert_graph(rdf_graph: str, *, format="turtle", replace=False) -> None` | write RDF into this driver's own graph, owned by `source_id` |
| `insert_graph_file(path, *, format=None, replace=False) -> None` | the same from a file; format from the extension when omitted |
| `sparql_update(update: str) -> dict` | a SPARQL UPDATE against this driver's own graph |
| `config_dir() -> Path` | the directory of the config file, for resolving relative paths |
| `data_dir() -> Path` | the server's data directory |

The graph helpers take no owner argument, so a driver cannot write into the
plant model or another driver's graph; `replace=True` replaces only this
driver's contribution.

### Persistent state

`self.state` is a `DriverState`: a key-value store saved to a JSON file under
the server's data directory after every change, so it survives restarts.
Values must be JSON-serializable.

| method | meaning |
|---|---|
| `get(key: str, default=None) -> Any` | read a value |
| `set(key: str, value: Any) -> None` | write a value |
| `update(data: dict[str, Any]) -> None` | write several |
| `delete(key: str) -> bool` | remove one; returns whether it existed |
| `keys() -> list[str]` | all keys |
| `clear() -> None` | remove everything |

Note that `DriverState` is not a dict; `state["k"] = v` raises `TypeError`.
The file is named after `driver_id`, or derived from `spec` when unset.

## IngestDriver

### Declaring streams

```python
declare(ref_name: str, *, source_id: str | None = None, value_kind: str | None = None,
        point_uri: str | None = None, label: str | None = None,
        unit: str | URIRef | None = None, quantity_kind: str | URIRef | None = None,
        medium: str | URIRef | None = None, substance: str | URIRef | None = None,
        data_source: str | URIRef | None = None, properties: dict | None = None) -> None
```

A stream is identified by `(source_id, ref_name)`; `source_id` defaults to
the driver's own.
`ref_name` is used exactly as given, spaces included.
`value_kind` is `"numeric"` or `"text"`; when omitted it is inferred from the
first observed values before they are inserted.
`unit`, `quantity_kind`, `medium` and `substance` take a URI or free text,
resolved jointly.
`properties` is a mapping of predicate URIs to values for extension triples.

Declaring is idempotent: an identical repeat is a no-op, a changed declaration
for an existing pair raises.
Declarations are written to the graph just before the next insert.
When `point_uri` exists in the graph, the declared metadata is checked against
it; see [units](../explanation/units.md#how-a-unit-gets-recorded) and the
[lifecycle guide](../explanation/stream-lifecycle.md).
Without `point_uri`, a placeholder point `<ref_uri>__point` labelled
`source_id__ref_name` is minted.

| method | meaning |
|---|---|
| `is_declared(ref_name, *, source_id=None) -> bool` | whether this driver has declared the pair |
| `register_declared(observations: pl.DataFrame \| None = None) -> None` | write pending declarations now, inferring value kinds from `observations`; the platform calls this before every insert |

### Reporting observations

| method | meaning |
|---|---|
| `add(ref_name: str, value: Any, ts: datetime \| None = None, *, source_id: str \| None = None) -> None` | buffer one reading; `ts` defaults to now (UTC), naive datetimes are read as UTC. Raises `UndeclaredStreamError` for a pair this driver has not declared. Thread-safe, no network I/O |
| `flush() -> dict` | insert everything buffered and clear the buffer; the platform calls it at the end of every tick and after `stop()` |
| `insert_observations(observations: pl.DataFrame \| None) -> dict` | register what is pending and insert a frame directly |
| `normalize_observations(observations: pl.DataFrame \| None) -> pl.DataFrame` | the normalization applied before upload |
| `to_timestamp(date_or_timestamp: pl.Series, time: pl.Series \| None = None, *, date_format=None, timezone="UTC", day_first=False) -> pl.Series` | parse source timestamp columns |
| `max_buffered_rows = 100000` | class attribute; `add()` raises `DriverBufferFull` once reached |

A successful `add()` means the row was accepted into memory.
A failed insert puts the rows back and the next tick retries them; the buffer
is memory only and is lost if the process crashes.
A failed final flush makes the shutdown unsuccessful and logs the unsent
count.

An observation frame has the columns `ts`, `ref_name`, `value`, plus
`source_id` for a multi-source driver.
Before upload `ts` is cast to `Datetime(us, UTC)`, `ref_name` and `value` to
strings, and rows with a null `ts` or `ref_name` are dropped.
`value` is transferred as a string; the server picks the storage column from
the stream's `value_kind`.
A frame naming an undeclared pair raises `UndeclaredStreamError`.

## PollingIngestDriver

| method | meaning |
|---|---|
| `read() -> None` | sample the source, reporting each value with `add()` |
| `collect() -> pl.DataFrame \| None` | return this tick's observations in bulk instead; `None` means `read()` is used |

`tick()` first flushes rows retained from a failed insert, then calls `read()`, then `collect()`, then flushes again.

## EventIngestDriver

Nothing to implement on the ingest side: subscribe in `setup()`, call `add()`
from the callback, unsubscribe in `stop()`.
`tick()` only flushes the buffer, so `interval` is how often pushed readings
reach the server.

## FileIngestDriver

| method | meaning |
|---|---|
| `setup() -> None` | reads and validates `source_id`, `watch_dir`, `glob`; call `super().setup()` when overriding |
| `read(path: Path, cursor: Any) -> FileBatch` | return the observations from `path` after `cursor`, and the next cursor |

`FileBatch(observations: pl.DataFrame | None, next_cursor: Any)`.
`cursor` is whatever the previous call returned for that file, `None` on first
sight; it must be JSON-serializable.
Returning it unchanged means nothing new.
The cursor is saved only after registration and insertion both succeed.
Discovery is recursive under `watch_dir`; one unreadable file is logged and
skipped.

`CSVIngestDriver` adds two hooks:

| method | meaning |
|---|---|
| `prepare_frame(df: pl.DataFrame, path: Path) -> pl.DataFrame` | transform the raw frame after `skip_cols`, before timestamps are parsed |
| `declare_stream(ref_name: str) -> None` | called once per distinct `ref_name` in each batch; default `self.declare(ref_name)` |

<!-- TODO: `XLSXIngestDriver` and `ParquetIngestDriver` have neither hook —
     they call `self.declare(name)` inline in `read()`, so attaching point
     URIs, units or quantity kinds to an Excel or Parquet source means
     overriding `read()` wholesale. Lift both hooks into a shared tabular base
     and this section covers all three again. -->

The reshaping they use is public:

```python
to_observations(df: pl.DataFrame, *, time_col=None, date_col=None, clock_col=None,
                id_col="id", value_col="value", layout: str,
                date_format=None, timezone="UTC", day_first=False) -> pl.DataFrame
to_timestamp(date_or_timestamp: pl.Series, time: pl.Series | None = None, *,
             date_format=None, timezone="UTC", day_first=False) -> pl.Series
```

## Configuration

`[driver]` holds defaults for every driver and the server address the actors
use; each `[[drivers]]` entry starts one driver and overrides any default.
Every key of an entry reaches the driver as `self.config["driver"]`; custom
keys need no declaration.

| key | default | meaning |
|---|---|---|
| `spec` | required | `file.py:Class` (relative to the config file, on the server host; sibling modules are importable) or `module.path:Class` |
| `name` | the class name | registry name; two drivers cannot share one |
| `interval` | `10.0` | seconds between ticks, and how often buffered rows reach the server |
| `graph_poll_interval` | `max(interval, 10.0)` | seconds between checks for a graph change |
| `driver_id` | derived from `spec` | names the state file; set it when running one class twice |
| `source_id` | none | the driver's datasource; required by the ingest bases unless set in `setup()` |
| `server_url`, `server_port` | `localhost`, the `[server]` port | under `[driver]`: where the actors reach the server |

**TODO:** We can make default driver naming unique to avoid collisions.

<!-- TODO: two more keys the supervisor reads are undocumented above:
     `[driver] use_ssl` (false; it is in the sample acquirium.toml) and
     `[driver] insert_batch_rows` (50000, passed to the actor's client). Add
     them once we decide whether they are public config. -->

## Built-in drivers

### Tabular (CSV, XLSX, Parquet)

`spec` values: `acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver`,
`...xlsx_ingest:XLSXIngestDriver`, `...parquet_ingest:ParquetIngestDriver`.

| key | default | meaning |
|---|---|---|
| `source_id`, `watch_dir`, `glob` | required | as for any file driver; `watch_dir` relative to the config file, `glob` one pattern or a list |
| `format` | required | `"wide"` (one column per stream) or `"narrow"` (`id`/`value` rows); never inferred |
| `time_col` | discovered | one complete timestamp column |
| `date_col`, `clock_col` | discovered | a date column plus a time column |
| `id_col`, `value_col` | `"id"`, `"value"` | narrow layout only |
| `skip_cols` | `[]` | columns dropped before reshaping |
| `timezone` | `"UTC"` | how naive timestamps are read |
| `date_format` | none | strptime format when discovery fails |
| `day_first` | `false` | for ambiguous `d/m` vs `m/d` dates |
| `skip_rows` | `[]` | CSV: row indexes to skip before the header; also takes a per-file map, `{ "subdir/data.csv" = [2, 5] }` |
| `header_contains` | `[]` | CSV: cell values identifying the header row; skips a banner; wins over `skip_rows` |
| `encoding` | `"utf8-lossy"` | CSV |
| `ragged_lines` | `"ignore"` | CSV: `"ignore"` keeps a row with the wrong cell count, `"skip"` drops it, `"error"` fails the file |
| `null_values` | `[]` | CSV: missing-value sentinels |
| `sheets` | first sheet | XLSX: sheet names; several are concatenated |

Timestamp columns are discovered from common names (`timestamp`, `ts`,
`Date` plus `Time`, `Sample Date` plus `Sample Time`); explicit keys win.
Rows with an unparseable timestamp or a null value are dropped.
Column names become `ref_name`s unchanged.

### MQTT

`spec = "acquirium.Drivers.BuiltInDrivers.mqtt_ingestion:MQTTIngestDriver"`

| key | default | meaning |
|---|---|---|
| `source_id` | required | datasource of the ingested streams |
| `mqtt_qos` | `0` | subscription QoS |
| `mqtt_value_kind` | none | value kind applied to streams that do not declare one in the graph |

Subscriptions come from the graph, not the config: reference nodes typed as
MQTT references carry broker, topic, time key and value key.
`on_graph_change()` re-runs that query.

### WaterTAP

`spec = "acquirium.Drivers.BuiltInDrivers.watertap:WaterTAPDriver"`

| key | default | meaning |
|---|---|---|
| `source_id` | `"watertap"` | datasource for the results |
| `watertap_mapping_path` | required | JSON mapping of point URI to Pyomo variable path |
| `watertap_build_spec`, `watertap_solve_spec` | required | `file.py:fn` or `module:fn` |
| `watertap_change_inputs_spec` | none | function that varies the inputs per tick |
| `watertap_inputs`, `watertap_build_kwargs` | `{}` | passed to the input and build functions |
| `watertap_graph_path`, `watertap_insert_graph`, `watertap_insert_graph_replace` | none, `false`, `false` | insert the model on start |
| `watertap_result_attr` | none | attribute to read results from |

The model is rebuilt and solved on every tick, then each mapped value is
reported.

### System metrics

`spec = "acquirium.Drivers.BuiltInDrivers.system_metrics:SystemMetricsDriver"`,
no keys.

## Operations

```bash
acquirium driver start CONFIG          # push the [[drivers]] entries to a running server
acquirium driver list                  # name, status, interval, started, spec
acquirium driver stop --name NAME
```

Status is `running`, `stopped`, or `failed: <error>`.
Stopping signals the loop, waits up to 10 seconds for the current tick, runs
`stop()`, flushes, then kills the actor.
