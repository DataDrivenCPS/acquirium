---
name: acquirium-drivers
description: Write and operate Acquirium drivers — the classes that feed plant data into the server on a schedule.
load_when: The task involves ingesting data into an Acquirium server (writing a driver, configuring one, or debugging ingestion).
human_doc: ../drivers.md
---

# Acquirium drivers

A driver is a class the SERVER runs as a Ray actor on its own host. It talks
back over HTTP via `self.aq`, a normal `Acquirium` client. Lifecycle:
`setup()` once → platform registers declarations → `tick()` immediately →
every `interval` s: [graph changed → `on_graph_change()`] → `tick()` → on
shutdown `stop()` then a final flush.

You provide identity + declarations + reading code. The platform owns
scheduling, registration, batching, insertion and shutdown.

## Contract

```python
from acquirium import (Driver, PollingIngestDriver, EventIngestDriver,
                       FileIngestDriver, FileBatch,
                       DriverBufferFull, UndeclaredStreamError)

class MyDriver(PollingIngestDriver):        # pick an ingest base, not Driver
    def setup(self):                        # required; declare here
        self.source_id = "src"              # no default; set it (or config source_id)
        self.declare("temp", value_kind="numeric", point_uri="urn:p:temp")
    def read(self):                         # report values one at a time
        self.add("temp", read_sensor())
```

Provided on `self`: `aq` (client), `config` (full parsed toml; entry keys
under `config["driver"]`), `state` (persistent store). Helpers:
`config_dir()`, `data_dir()`, `reference_uri(ref_name)`, `insert_graph()`,
`insert_graph_file()`, `sparql_update()`, `declare()`, `add()`,
`is_declared()`, `flush()`.

`self.state` is METHOD-BASED: `state.set(k, v)`, `state.get(k)`, `delete`,
`keys`, `update`, `clear`. `state[k] = v` raises TypeError. Persists to JSON
per `driver_id` (default: derived from the spec).

## declare() / add()

```python
self.declare(ref_name, *, source_id=None, value_kind=None, point_uri=None,
             label=None, unit=None, quantity_kind=None, medium=None,
             substance=None, data_source=None, properties=None)
self.add(ref_name, value, ts=None, *, source_id=None)
```

- Stream identity is the exact pair `(source_id, ref_name)`. NOT sanitized,
  normalized or rewritten — `"Sample Point 3"` stays exactly that.
- `declare` is idempotent and cheap: call it every read. Identical repeat =
  no-op; changed metadata for an existing pair = `ValueError`.
- Omit `value_kind` to have it inferred from the first meaningful values.
- `add` is thread-safe, does NO network I/O — safe from broker callbacks.
  `ts` defaults to now UTC; naive datetimes are read as UTC.
- Declarations reach the graph just before the next insert.

## Base choice

| source | base | implement |
|---|---|---|
| pulled every tick | `PollingIngestDriver` | `read()` + `add()` |
| pulled every tick, already bulk | `PollingIngestDriver` | `collect() -> frame` |
| pushed via callback | `EventIngestDriver` | the callback, calling `add()` |
| files in a directory | `FileIngestDriver` | `read(path, cursor) -> FileBatch` |

Ready-made, config-only: `CSVIngestDriver`, `XLSXIngestDriver` (`[xlsx]`),
`ParquetIngestDriver`, `MQTTIngestDriver` (`[mqtt]`), `WaterTAPDriver`
(`[watertap]`), `SystemMetricsDriver` — all under
`acquirium.Drivers.BuiltInDrivers`.

## The observation frame (collect / insert_observations)

Columns `ts`, `ref_name`, `value` (+ optional `source_id` for multi-source;
frame is split per source). Normalization on insert: `ts` → Datetime(us, UTC);
`ref_name`/`value` → Utf8; null `ts`/`ref_name` rows dropped. `value` is
transferred as a string regardless of type; the server picks the storage
column from the registered `value_kind`. Missing column: `ValueError:
Observation frames must include columns ts, ref_name, value; missing [...]`.

## Hard rules

- Declare BEFORE reporting. An undeclared pair raises
  `UndeclaredStreamError`, on `add()` and on frame insert alike.
- Use `self.insert_graph()` / `self.insert_graph_file()`, NEVER
  `self.aq.insert_graph(...)`. The helpers are scoped to the driver's own
  graph and default `replace=False`; the raw client call takes a `source_id`
  and defaults `replace=True` for that owner's graph.
- `add()` accepted ≠ stored. Rows sit in memory until the tick's flush.
  A failed insert puts them back and the NEXT tick retries. A process crash
  loses them.
- `max_buffered_rows` (100_000) caps the buffer; `add()` then raises
  `DriverBufferFull` — apply backpressure, do not swallow it.
- Tick exceptions are logged and SWALLOWED; status stays `running`. Check
  the `acquirium.driver.<ClassName>` logs, not `/drivers/list`.
- `failed: <error>` status means the actor died (usually `setup()`).
- `FileIngestDriver`: call `super().setup()` when overriding `setup()` (it
  validates `source_id`/`watch_dir`/`glob`). `FileBatch.next_cursor` must be
  JSON-serializable and is saved ONLY after registration + insertion succeed.
  Return the cursor unchanged to signal nothing new.
- Tabular built-ins REQUIRE `format = "wide"|"narrow"`; layout is never
  inferred.
- Two `[[drivers]]` entries of the same class share one state file unless
  each sets a distinct `driver_id`.
- File specs (`spec = "file.py:Class"`) resolve on the SERVER's filesystem,
  relative to the config file's directory. Sibling modules are importable.
- `watertap_*` paths resolve against the process CWD, not the config dir.
  `watch_dir` resolves against the config dir.
- `on_graph_change()` never fires for the initial graph — do the first query
  in `setup()`. A graph write inside it re-triggers it next tick; guard or
  the driver loops.
- Never `while True` / `sleep()` in a driver; the runner owns scheduling.
  `stop()` is for quiescing producers only.

## Error → cause

| symptom | cause |
|---|---|
| `UndeclaredStreamError: observation stream ('src','x') was not declared` | `add()`/frame for a pair never passed to `declare()` |
| `ValueError: conflicting declaration for stream ...` | same pair declared twice with different metadata |
| `DriverBufferFull` | source outruns the flush; raise `max_buffered_rows` or slow the source |
| `AttributeError: X.source_id is not set yet` | no `self.source_id` and no `source_id` in config |
| `ValueError: file ingest drivers require driver.watch_dir` (or `source_id`/`glob`) | missing key, or `setup()` overridden without `super().setup()` |
| `TypeError: X.read() must return FileBatch` | returned a bare frame |
| `ValueError: CSV ingestion requires driver.format = 'wide' or 'narrow'` | `format` not set |
| `TypeError: ... must implement read() or collect()` | `PollingIngestDriver` subclass implements neither |
| rows stored but no semantic query finds them | declared without `point_uri` |
| status `running`, no data | tick raising; read the driver logs |

## Config

```toml
[driver]                 # defaults for all drivers
interval = 10.0
graph_poll_interval = 30.0     # default max(interval, 10.0)

[[drivers]]              # one driver; any key overrides [driver] and is
spec      = "probe_driver.py:ProbeDriver"   # visible as config["driver"][key]
name      = "probe-lab"  # registry name, unique; defaults to class name
source_id = "probe-lab"
interval  = 5.0
```

Start: `acquirium server --config X.toml` (runs server + drivers) or
`acquirium driver start X.toml` (push entries to a running server).
Manage: `acquirium driver list` / `acquirium driver stop --name N`
(HTTP: `GET /drivers/list`, `POST /drivers/start`, `POST /drivers/stop`).

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
`deployments/BENICIA/scripts/parquet_driver.py`.
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
