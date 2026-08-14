---
name: acquirium-drivers
description: Write and operate Acquirium drivers — the classes that feed plant data into the server on a schedule.
load_when: The task involves ingesting data into an Acquirium server (writing a driver, configuring one, or debugging ingestion).
human_doc: ../drivers.md
---

# Acquirium drivers

A driver is a class the SERVER runs as a Ray actor on its own host. It talks
back over HTTP via `self.aq`, a normal `Acquirium` client. Lifecycle:
`setup()` once → `tick()` immediately → every `interval` s: [graph changed →
`on_graph_change()`] → `tick()` → on shutdown `stop()`.

## Contract

```python
from acquirium.Drivers.Driver import Driver, PollingIngestDriver, EventIngestDriver
from acquirium.Drivers.BuiltInDrivers.tabular_base import TabularIngestBase

class MyDriver(PollingIngestDriver):        # pick a base, not Driver directly
    def setup(self):                        # required; register here
        self.source_id = "src"              # no default; set it
    def collect(self) -> pl.DataFrame:      # PollingIngestDriver's hook
        ...
```

Provided on `self`: `aq` (client), `config` (full parsed toml; entry keys
under `config["driver"]`), `state` (persistent store). Helpers:
`config_dir()`, `data_dir()`, `reference_uri(ref_name)`.

`self.state` is METHOD-BASED: `state.set(k, v)`, `state.get(k)`, `delete`,
`keys`, `update`, `clear`. `state[k] = v` raises TypeError. Persists to JSON
per `driver_id` (default: class name).

## Base choice

| source | base | implement |
|---|---|---|
| poll every tick | `PollingIngestDriver` | `collect() -> frame` |
| pushed via callback | `EventIngestDriver` | call `insert_observations(frame)` from the callback |
| files in a directory | `TabularIngestBase` (or CSV/XLSX/Parquet built-ins) | hooks only |

Ready-made, config-only: `CSVIngestDriver`, `XLSXIngestDriver` (`[xlsx]`),
`ParquetIngestDriver`, `MQTTIngestDriver` (`[mqtt]`), `WaterTAPDriver`
(`[watertap]`), `SystemMetricsDriver` — all under
`acquirium.Drivers.BuiltInDrivers`.

## The observation frame

Columns `ts`, `ref_name`, `value` (+ optional `source_id` for multi-source;
frame is split per source). Normalization on insert: `ts` → Datetime(us, UTC),
strings parsed, naive assumed UTC; `value` → string on transfer, real type
restored from the registered `value_kind`; null `ts`/`ref_name` rows dropped.
Missing column: `ValueError: Observation frames must include columns ts,
ref_name, value; missing [...]`.

## Hard rules

- NEVER `insert_graph(..., replace=True)` from a driver. One main graph;
  `replace=True` clears everyone's triples. The client default IS `True`, so
  always pass `replace=False` explicitly.
- Register streams BEFORE the first insert (`register_datasource` +
  `register_streams` in `setup()`), with `point_uri` set; otherwise rows are
  stored but unreachable by semantic queries. Insert to an unregistered
  stream → HTTP 400 `stream urn:acquirium#<uuid> is not registered`.
- Tick exceptions are logged and SWALLOWED; status stays `running`. Check
  the `acquirium.driver.<ClassName>` logs, not `/drivers/list`.
- `failed: <error>` status means the actor died (usually `setup()`).
- Only tabular drivers replay after a failed upload (file offset advances on
  success only). A polling driver's failed tick is lost unless `collect()`
  re-reads from a watermark kept in `self.state`.
- Two `[[drivers]]` entries of the same class share one state file unless
  each sets a distinct `driver_id`.
- File specs (`spec = "file.py:Class"`) resolve on the SERVER's filesystem,
  relative to the config file's directory. Sibling modules are importable.
- `watertap_*` paths resolve against the process CWD, not the config dir.
- A graph write inside `on_graph_change()` re-triggers it next tick; guard
  or the driver loops.
- Do not override `setup()`/`tick()` on `TabularIngestBase`; use
  `configure_tabular_driver()` / `after_tabular_setup()` and the hooks.

## Config

```toml
[driver]                 # defaults for all drivers
interval = 10.0

[[drivers]]              # one driver; any key overrides [driver] and is
spec      = "probe_driver.py:ProbeDriver"   # visible as config["driver"][key]
name      = "probe-lab"  # registry name, unique; defaults to class name
interval  = 5.0
```

Start: `acquirium server --config X.toml` (runs server + drivers) or
`acquirium driver start X.toml` (push entries to a running server).
Manage: `acquirium driver list` / `acquirium driver stop --name N`
(HTTP: `GET /drivers/list`, `POST /drivers/start`, `POST /drivers/stop`).

## Canonical snippet

```python
import polars as pl
from datetime import datetime, timezone
from acquirium.Drivers.Driver import PollingIngestDriver

class ProbeDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "probe-1"
        self.aq.register_datasource(self.source_id)
        self.aq.register_streams([{
            "source_id": self.source_id, "ref_name": "effluent-tds",
            "value_kind": "numeric", "point_uri": "urn:swro/effluent-tds",
            "unit": "mg/L", "quantity_kind": "mass concentration",
        }])

    def collect(self) -> pl.DataFrame:
        return pl.DataFrame({"ts": [datetime.now(timezone.utc)],
                             "ref_name": ["effluent-tds"],
                             "value": [read_probe()]})
        # empty frame = nothing new this tick
```

Tabular customization: override `stream_specs_for_names()` to attach
`point_uri`s (default registers name-only, semantically unreachable) and
`read_frame()` for file cleanup (may return wide, narrow, or normalized
`(ts, ref_name, value)`). Reference implementations:
`deployments/WATERTAP/scripts/parquet_driver.py`,
`deployments/BENICIA/scripts/parquet_driver.py`.

## Anti-patterns

```python
self.aq.insert_graph(model)                  # WRONG: replace defaults to True
self.aq.insert_graph(model, replace=False)   # right

self.state["k"] = v                          # WRONG: TypeError
self.state.set("k", v)                       # right

class Mine(TabularIngestBase):
    def setup(self): ...                     # WRONG: breaks the base's setup
    def configure_tabular_driver(self): ...  # right
```
