---
name: acquirium-drivers
description: Write and operate Acquirium drivers — the classes that feed plant data into the server on a schedule.
load_when: The task involves ingesting data into an Acquirium server (writing a driver, configuring one, or debugging ingestion).
human_doc: ../reference/drivers.md
---

# Acquirium drivers

A driver is a class the SERVER runs as a Ray actor on its own host. It talks
back over HTTP via `self.aq`, a normal `Acquirium` client. Lifecycle:
`setup()` once → platform registers what was declared → `tick()` immediately
→ then two independent clocks: every `interval` s `tick()`; every
`graph_poll_interval` s check the server's source version and call
`on_graph_change()` if it advanced → on stop `stop()` then a final flush.

You provide identity + declarations + reading code. The platform owns
scheduling, registration, batching, insertion and shutdown.

## Contract

```python
from acquirium import (Driver, IngestDriver, PollingIngestDriver,
                       EventIngestDriver, FileIngestDriver, FileBatch,
                       CSVIngestDriver, DriverBufferFull, UndeclaredStreamError,
                       to_observations, to_timestamp)

class MyDriver(PollingIngestDriver):        # pick an ingest base, not Driver
    def setup(self):                        # required; declare here
        self.source_id = "src"              # no default; set it (or config source_id)
        self.declare("temp", value_kind="numeric", point_uri="urn:p:temp")
    def read(self):                         # report values one at a time
        self.add("temp", read_sensor())
```

Provided on `self`: `aq` (client), `config` (full parsed toml; entry keys
under `config["driver"]`), `state` (persistent store), `source_id`
(property; reading it unset raises `AttributeError`). Helpers:
`config_dir()`, `data_dir()`, `reference_uri(ref_name)`, `insert_graph()`,
`insert_graph_file()`, `sparql_update()`, `declare()`, `is_declared()`,
`add()`, `flush()`, `insert_observations(frame)`, `register_declared()`,
`normalize_observations(frame)`, `to_timestamp(...)`.

`self.state` is METHOD-BASED: `state.get(k, default=None)`, `state.set(k, v)`,
`delete(k) -> bool`, `keys()`, `update(dict)`, `clear()`, `k in state`.
`state[k] = v` raises TypeError. Values must be JSON-serializable; every
mutation writes `<data_dir>/drivers/<id>.json` where `id` is `driver_id`
(sanitized) or `<ClassName>_<sha256(spec)[:16]>`. `data_dir()` =
`$ACQUIRIUM_DATA_DIR`, else `[server] data_dir` (relative to the config
file), else `<config_dir>/.acquirium`.

## declare() / add()

```python
self.declare(ref_name, *, source_id=None, value_kind=None, point_uri=None,
             label=None, unit=None, quantity_kind=None, medium=None,
             substance=None, data_source=None, properties=None)
self.is_declared(ref_name, *, source_id=None) -> bool
self.add(ref_name, value, ts=None, *, source_id=None)
```

- Stream identity is the exact pair `(source_id, ref_name)`. NOT sanitized,
  normalized or rewritten — `"Sample Point 3"` stays exactly that.
- `declare` is idempotent and cheap: call it every read. Identical repeat =
  no-op; changed metadata for an existing pair = `ValueError`. Only
  non-`None` kwargs count as metadata.
- `value_kind` is `"numeric"` or `"text"`; omit it to have it inferred from
  the first meaningful (non-null, non-blank) values at insert time.
- `add` is thread-safe, does NO network I/O — safe from broker callbacks.
  `ts` defaults to now UTC; naive datetimes are read as UTC.
- Declarations are registered right after `setup()` and again just before
  every insert (`register_declared()`); datasources are registered the same
  way.

## Base choice

| source | base | implement |
|---|---|---|
| no timeseries (model-only) | `Driver` | `setup()` + `tick()` |
| pulled every tick | `PollingIngestDriver` | `read()` + `add()` |
| pulled every tick, already bulk | `PollingIngestDriver` | `collect() -> frame` |
| pushed via callback | `EventIngestDriver` | the callback, calling `add()`; `tick()` only flushes |
| files in a directory | `FileIngestDriver` | `read(path, cursor) -> FileBatch` |

`PollingIngestDriver.tick()` = flush (retry) → `read()` → `collect()` →
insert → flush. A subclass implementing neither `read` nor `collect` fails
at construction.

Ready-made, config-only (`spec = "acquirium.Drivers.BuiltInDrivers.<module>:<Class>"`;
only `CSVIngestDriver` is also exported from `acquirium`):

| class | module | base | required keys |
|---|---|---|---|
| `CSVIngestDriver` | `csv_ingest` | File | `source_id`, `watch_dir`, `glob`, `format` |
| `XLSXIngestDriver` | `xlsx_ingest` | File | same |
| `ParquetIngestDriver` | `parquet_ingest` | File | same |
| `MQTTIngestDriver` | `mqtt_ingestion` | Event | `source_id` |
| `WaterTAPDriver` | `watertap` | Polling | `watertap_mapping_path`, `watertap_build_spec`, `watertap_solve_spec` |
| `SystemMetricsDriver` | `system_metrics` | Polling | none (`source_id` = `<hostname>-system-metrics`) |

Every key lives on the `[[drivers]]` entry (or `[driver]` defaults); there
are no `[csv]`/`[mqtt]`/`[watertap]` sections.

Tabular optional keys: `time_col` | `date_col`+`clock_col` (else discovered
from `timestamp`/`datetime`/`ts`/`time`/`date` or a `Date`+`Time` pair),
`id_col`="id", `value_col`="value" (narrow), `skip_cols`, `date_format`,
`timezone`="UTC", `day_first`=false. CSV only: `skip_rows` (list or
per-file map), `header_contains`, `encoding`="utf8-lossy",
`ragged_lines`="ignore"|"skip"|"error", `null_values`,
`infer_schema_length`=100. XLSX only: `sheets`. Column names become
`ref_name`s unchanged; rows with unparseable `ts` or null value are dropped.
`CSVIngestDriver` hooks: `prepare_frame(df, path)` (before reshaping) and
`declare_stream(ref_name)` (default `self.declare(ref_name)`); XLSX/Parquet
have no hooks — override `read()`.

MQTT optional: `mqtt_qos`=0, `mqtt_value_kind`. Subscriptions come from
MQTT reference nodes in the graph, re-queried in `on_graph_change()`.
WaterTAP optional: `source_id`="watertap", `watertap_change_inputs_spec`,
`watertap_inputs`, `watertap_build_kwargs`, `watertap_graph_path`,
`watertap_insert_graph`=false, `watertap_insert_graph_replace`=false,
`watertap_result_attr`. Model is rebuilt and solved every tick.

## The observation frame (collect / insert_observations / FileBatch)

Columns `ts`, `ref_name`, `value` (+ optional `source_id` for multi-source;
frame is partitioned per source and inserted per source). Normalization on
insert: `ts` → Datetime(us, UTC) via `to_timestamp` (naive = UTC);
`ref_name`/`value`/`source_id` → Utf8; float NaN value → null; rows with
null `ts`/`ref_name` dropped. `value` is transferred as a string regardless
of type; the server picks the storage column from the registered
`value_kind`. Missing column: `ValueError: Observation frames must include
columns ts, ref_name, value; missing [...]`.

`to_observations(df, *, layout="wide"|"narrow", time_col=None, date_col=None,
clock_col=None, id_col="id", value_col="value", date_format=None,
timezone="UTC", day_first=False)` reshapes a table into that frame; `layout`
is required.

## Hard rules

- Declare BEFORE reporting. An undeclared pair raises
  `UndeclaredStreamError`, on `add()` and on frame insert alike.
- Use `self.insert_graph()` / `self.insert_graph_file()` /
  `self.sparql_update()`, NEVER `self.aq.insert_graph(...)`. The helpers are
  scoped to the driver's own graph (`source_id`) and default
  `replace=False`; the raw client call requires a `source_id` kwarg and
  defaults `replace=True` for that owner's graph.
- `add()` accepted ≠ stored. Rows sit in memory until the tick's flush.
  A failed insert puts them back and the NEXT tick retries. A process crash
  loses them. A failed final flush at shutdown raises and logs the unsent
  count.
- `max_buffered_rows` (class attr, 100_000) caps pending + in-flight rows;
  `add()` then raises `DriverBufferFull` — apply backpressure, do not
  swallow it.
- Tick exceptions are logged and SWALLOWED; status stays `running`. Check
  the `acquirium.driver.<ClassName>` logs, not `/drivers/list`.
  `on_graph_change()` exceptions are swallowed the same way.
- `setup()` raising kills the actor before registration: `driver start` /
  `POST /drivers/start` returns the error and the driver never appears in
  the list. `failed: <error>` status means the run loop itself exited with
  an exception (e.g. `stop()` or the final flush raised).
- `FileIngestDriver`: call `super().setup()` when overriding `setup()` (it
  validates `source_id`/`watch_dir`/`glob`, sets `self.watch_dir` and
  `self.file_patterns`). Discovery is recursive (`rglob`) under
  `watch_dir`. A `read()` exception is logged and that file skipped; an
  insert failure raises out of the tick. `FileBatch(observations,
  next_cursor)`: `next_cursor` must be JSON-serializable (checked with
  `json.dumps`) and is saved to `state["cursors"]` ONLY after registration +
  insertion succeed. Return the cursor unchanged (or `FileBatch(None,
  cursor)`) to signal nothing new.
- Tabular built-ins REQUIRE `format = "wide"|"narrow"`; layout is never
  inferred.
- Two `[[drivers]]` entries with the same `spec` string share one state file
  unless each sets a distinct `driver_id`. Two entries of the same class
  also need distinct `name`s or the second start fails with
  `Driver '<name>' is already running`.
- File specs (`spec = "file.py:Class"`) resolve on the SERVER's filesystem,
  relative to the config file's directory (`__config_dir`, injected by the
  config loader). Sibling modules are importable (the file's directory goes
  on the actor's `PYTHONPATH`). `spec` must contain `:`; the class must
  subclass `Driver`.
- `watertap_*` paths and `file.py:fn` build/solve specs resolve against the
  process CWD, not the config dir. `watch_dir` resolves against the config
  dir.
- `on_graph_change()` never fires for the initial graph or for the driver's
  own `setup()` writes — do the first query in `setup()`. It is polled on
  `graph_poll_interval`, not every tick. A graph write inside it advances
  the version and re-triggers it on the next poll; guard or the driver
  loops.
- Never `while True` / `sleep()` in a driver; the runner owns scheduling.
  `stop()` is for quiescing producers only; stop waits 10 s then kills the
  actor.

## Error → cause

| symptom | cause |
|---|---|
| `UndeclaredStreamError: observation stream ('src', 'x') was not declared` | `add()` for a pair never passed to `declare()` |
| `UndeclaredStreamError: observations contain undeclared streams: ('src', 'x')` | a frame (`collect`/`insert_observations`/`FileBatch`) names an undeclared pair |
| `ValueError: conflicting declaration for stream ('src', 'x'): was {...}, now {...}` | same pair declared twice with different metadata |
| `ValueError: stream declarations require non-empty source_id and ref_name` | empty `ref_name`, or empty `source_id` |
| `DriverBufferFull: X buffer is full (max_buffered_rows=100000)` | source outruns the flush; raise `max_buffered_rows` or slow the source |
| `AttributeError: X.source_id is not set yet. ...` | no `self.source_id` and no `source_id` in config |
| `ValueError: file ingest drivers require driver.source_id` / `driver.watch_dir` / `driver.glob as a string or list` | missing key, or `setup()` overridden without `super().setup()` |
| `TypeError: X.read() must return FileBatch` | returned a bare frame (logged; file skipped) |
| `ValueError: CSV ingestion requires driver.format = 'wide' or 'narrow'` (also `XLSX ...`, `Parquet ...`) | `format` not set |
| `ValueError: layout must be explicitly set to 'wide' or 'narrow'` | `to_observations` called without `layout` |
| `ValueError: could not identify timestamp columns in [...]` | tabular: set `time_col` or `date_col`+`clock_col` |
| `TypeError: X must implement read() (reporting values with self.add) or collect() (returning an observation frame)` | `PollingIngestDriver` subclass implements neither |
| `ValueError: driver interval must be greater than zero` / `graph_poll_interval must be greater than zero` | non-positive config value |
| `ValueError: Driver 'N' is already running` | duplicate `name` (default: class name) |
| `ValueError: MQTT ingestion requires driver.source_id` | MQTT entry without `source_id` |
| `ValueError: Missing required config key: watertap_*` / `FileNotFoundError: watertap_* not found: <abs path>` | WaterTAP key missing, or path relative to the wrong CWD |
| rows stored but no semantic query finds them | declared without `point_uri` (a placeholder point is minted) |
| status `running`, no data | tick raising; read the driver logs |

## Config

```toml
[driver]                 # defaults for all drivers + where actors reach the server
interval = 10.0
graph_poll_interval = 30.0     # default max(interval, 10.0)
# server_url = "localhost"; server_port = <[server] port>; use_ssl = false
# insert_batch_rows = 50000    # client batch size for the actor

[[drivers]]              # one driver; any key overrides [driver] and is
spec      = "probe_driver.py:ProbeDriver"   # visible as config["driver"][key]
name      = "probe-lab"  # registry name, unique; defaults to class name
source_id = "probe-lab"
driver_id = "probe-lab"  # state file name; default derived from spec
interval  = 5.0
```

Start: `acquirium server --config X.toml` (runs server + drivers; with
`[server] enabled = false` it only pushes the entries to the server named in
`[driver]`) or `acquirium driver start X.toml` (push entries to a running
server). Manage: `acquirium driver list` / `acquirium driver stop --name N`;
all `driver` commands take `--server-url` / `--server-port`
(HTTP: `GET /drivers/list`, `POST /drivers/start {spec, config, name?,
interval?}`, `POST /drivers/stop {name}`). Status: `running` | `stopped` |
`failed: <error>`.

## Canonical snippets

Polling:

```python
from acquirium import PollingIngestDriver

class ProbeDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "probe-1"
        self.declare("effluent-tds", value_kind="numeric",
                     point_uri="urn:swro/effluent-tds",
                     unit="mg/L", quantity_kind="mass concentration")

    def read(self):
        self.add("effluent-tds", read_probe())
```

File:

```python
from pathlib import Path
from typing import Any
import polars as pl
from acquirium import FileBatch, FileIngestDriver

class PlantParquetDriver(FileIngestDriver):
    def setup(self):
        super().setup()                       # validates source_id/watch_dir/glob
        self.declare("flow", value_kind="numeric", point_uri="urn:plant:flow")

    def read(self, path: Path, cursor: Any) -> FileBatch:
        offset = cursor or 0
        frame = pl.read_parquet(path).slice(offset)
        if frame.is_empty():
            return FileBatch(None, cursor)
        obs = frame.select(pl.col("timestamp").alias("ts"),
                           pl.lit("flow").alias("ref_name"),
                           pl.col("flow").alias("value"))
        return FileBatch(obs, offset + len(frame))
```

Reference implementations: `deployments/WATERTAP/scripts/parquet_driver.py`,
`deployments/BENICIA/scripts/parquet_driver.py` (both build on
`acquirium.Drivers.BuiltInDrivers.parquet_ingest.read_parquet_batch`).
Shared helpers: `from acquirium import to_observations, to_timestamp`.

## Anti-patterns

```python
self.aq.insert_graph(model, source_id=self.source_id)  # WRONG: replace=True
self.insert_graph_file(model_path)                     # right

self.state["k"] = v                          # WRONG: TypeError
self.state.set("k", v)                       # right

def read(self):
    self.add("temp", 21.5)                   # WRONG if never declared
def setup(self):
    self.declare("temp", value_kind="numeric")   # right

class Mine(FileIngestDriver):
    def setup(self):
        self.declare("x")                    # WRONG: skipped super().setup()
    def setup(self):
        super().setup(); self.declare("x")   # right
```
