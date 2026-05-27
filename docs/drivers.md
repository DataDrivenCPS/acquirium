# Drivers

A **Driver** connects an external source to Acquirium. The runner calls
`setup()` once, then calls `tick()` on a fixed interval until shutdown.

Most drivers should not implement insertion themselves. Use one of the ingest
base classes:

- `PollingIngestDriver`: the runner pulls observations by calling `collect()`
- `EventIngestDriver`: callbacks push observations by calling
  `insert_observations()`
- `TabularIngestBase`: file-oriented drivers can reuse paging, wide/narrow
  normalization, and generic insertion while specializing stream naming and
  registration

Both ingest bases use the same canonical observation frame:

```text
ts | ref_name | value
```

## Stream Identity

Every stream is identified by `(source_id, ref_name)`. `source_id` scopes
source-local stream names, so two sources can both report `ref_name="temp"`
without colliding. Single-source drivers usually set `self.source_id` in
`setup()`. Multi-source drivers can omit `self.source_id` if every observation
row includes a `source_id` column.

This identity rule applies to all drivers, regardless of lifecycle. Polling
drivers, event drivers, graph-configured drivers, and custom `Driver`
subclasses must all register and insert streams with the same `(source_id,
ref_name)` pair. If a driver bypasses `insert_observations()` and calls the
client directly, it must still pass `source_id` explicitly.

Drivers must register each stream before reporting observations for it. Stream
registration declares `value_kind`: use `"numeric"` for numeric streams and
`"text"` for status, state, enum, JSON/event, or other non-numeric streams.
Numeric samples are written to `numeric_value`; text samples are written to
`text_value`.

If a numeric stream receives a value that cannot be converted to float,
Acquirium stores that row in `text_value` instead of failing the insert. This
is intended for exceptional status values in otherwise numeric streams. Query
callers can use `value_mode` to read numeric-only rows, text-only rows, or a
coalesced mixed stream; see [`data-api.md`](data-api.md).

Keep `value` in its native Python/Polars type when possible. CSV-like drivers
may emit numeric values as strings, but they must still register those streams
with `value_kind="numeric"` so storage parses those strings into numeric
columns. `value_kind` should not be included in observation dataframes.

When a driver infers stream metadata from observed data, use
`assign_stream_value_kind()` so its behavior matches built-in ingestion:

```python
from acquirium.Storage.values import assign_stream_value_kind

value_kind = assign_stream_value_kind(
    observed_values,
)

aq.register_stream(
    source_id=source_id,
    ref_name=ref_name,
    value_kind=value_kind,
)
```

The helper treats `value_kind` as the stream's preferred/default storage
column. If any numeric value is observed, the stream is assigned `"numeric"`;
otherwise it is assigned `"text"`. Unparseable rows in numeric streams are
still preserved in `text_value`.

## Which Method Do I Implement?

In most drivers, you do **not** implement `tick()` directly.

Use this rule:

- Polling source: subclass `PollingIngestDriver` and implement `collect()`.
- Event source: subclass `EventIngestDriver` and call `insert_observations()`
  from callbacks or subscription handlers.
- Special lifecycle source: subclass `Driver` or `IngestDriver` and implement
  `tick()` directly only when neither built-in lifecycle fits.

`tick()` is the runner hook. The framework calls it on each interval. For
polling drivers, `PollingIngestDriver.tick()` is already implemented as:

```python
def tick(self):
    self.insert_observations(self.collect())
```

## Polling Drivers

Use `PollingIngestDriver` when the source is sampled on each tick, such as a
file directory, system metrics, an HTTP API, or a model solve.

```python
from datetime import datetime, timezone

import polars as pl

from acquirium import PollingIngestDriver


class TemperatureDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "sensors"
        self.aq.register_datasource(self.source_id)
        self.aq.register_stream(
            source_id=self.source_id,
            ref_name="temp/room1",
            value_kind="numeric",
        )

    def collect(self):
        return pl.DataFrame({
            "ts": [datetime.now(timezone.utc)],
            "ref_name": ["temp/room1"],
            "value": [read_sensor()],
        })
```

The base `tick()` implementation calls `collect()` and passes its frame to
`insert_observations()`. `insert_observations()` normalizes the frame and calls
`insert_timeseries_arrow()`, which serializes the data as Arrow IPC and sends it
to the server in one round-trip.

Rules:

- Do not put `while True` or `time.sleep` in a driver; the runner owns timing.
- Register datasources and streams before reporting observations for them.
- Use `stop()` to release resources on shutdown.

## Tabular Drivers

Use `TabularIngestBase` for drivers that ingest CSV/XLSX-like tabular files
and want the framework to own:

- file discovery under `watch_dir`
- row-offset paging via driver state, allowing efficient ingestion of
  "growing" files; this is a common pattern when recorded data is
  batch-uploaded to a fileshare
- wide/narrow normalization into `(ts, ref_name, value)`
- observation insertion

The public base lives at
`acquirium.BuiltinDrivers.tabular_base:TabularIngestBase`. The built-in
`CSVIngestDriver` and `XLSXIngestDriver` are thin specializations of it.

### Describing Wide/Narrow Schemas

Tabular drivers use five config/method hooks to describe the input schema:

- `ingest_format()` / `format`
- `time_col()` / `time_col`
- `id_col()` / `id_col`
- `value_col()` / `value_col`
- `skip_cols(path, col_names)` / `skip_cols`

You can set these in configuration:

```toml
[[drivers]]
spec = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
format = "narrow"
time_col = "timestamp"
id_col = "sensor_id"
value_col = "reading"
skip_cols = ["notes", "operator_comment"]
```

Or override the methods in a subclass when the schema is fixed:

```python
class MyDriver(CSVIngestDriver):
    def ingest_format(self) -> str:
        return "narrow"

    def time_col(self) -> str:
        return "timestamp"

    def id_col(self) -> str:
        return "tag"

    def value_col(self) -> str:
        return "reading"

    def skip_cols(self, path: Path, col_names: list[str]) -> tuple[str, ...]:
        if path.name.startswith("debug_"):
            return tuple(col_names)
        return tuple(name for name in col_names if name.startswith("debug_"))
```

When each field matters:

- wide format:
  - `time_col` matters
  - `id_col` and `value_col` are ignored (ids are column names, values are cell values)
- narrow format:
  - `time_col`, `id_col`, and `value_col` all matter

`skip_cols(path, col_names)` is applied before wide/narrow parsing. Use it to
drop columns that are not part of the stream schema at all, such as note
fields, debug columns, or report metadata.

If timestamps or stream IDs must be derived from multiple columns, or if the
file shape is otherwise more specialized than the built-in wide/narrow
handlers, override `read_frame(...)` and return either:

- a standard wide/narrow frame that these hooks describe, or
- a fully normalized frame with `ts`, `ref_name`, and `value`

`TabularIngestBase` is intentionally generic. Subclasses are expected to
specialize their own stream semantics and registration policy through hooks
such as:

- `stream_name_for(raw_name)`
- `stream_specs_for_names(path, source_id, raw_names, value_kinds)`
- `ensure_streams_registered(path, source_id, df, value_kinds)`
- `skip_cols(path, col_names)`

That means source-specific header parsing and metadata inference should live in
the subclass, not in the base frame format.

### Tabular Hooks

These hooks let subclasses specialize stream identity and registration without
reimplementing paging or file parsing.

#### `stream_name_for(raw_name)`

What it does:
- Maps a raw source-local stream/header name to the canonical `ref_name` that
  Acquirium stores and uses for stream identity.

When it is called:
- During wide/narrow normalization, when the base converts parsed tabular data
  into `(ts, ref_name, value)`.
- During default registration logic, if a subclass derives stream specs from
  raw names and wants them normalized consistently.

How to use it:
- Override when the default URI-safe normalization is not the right stream
  identity for your source.
- Good uses include preserving source-specific conventions, collapsing aliases,
  or applying a custom sanitization rule.

Default behavior:
- Returns a URI-safe version of `raw_name` (not necessarily a URI, just safe
  for use as one path segment). For example, `"Temp (°C)"` could become
  `"Temp_C"`.

#### `stream_specs_for_names(path, source_id, raw_names, value_kinds)`

What it does:
- Builds the `register_streams()` payload for a set of raw source-local stream
  or header names.
- This is where a subclass can add metadata such as `unit`, `medium`,
  `quantity_kind`, `substance`, `point_uri`, or other stream-registration
  fields.

When it is called:
- By the default `ensure_streams_registered(...)` implementation, only for
  streams that have not already been registered for that `source_id`.

How to use it:
- Override when stream registration depends on source-specific knowledge such
  as raw headers, sidecar config files, workbook sheet structure, or file-local
  metadata.
- Return one spec dict per stream in `raw_names`.
- Set `ref_name` explicitly in each returned spec, typically by calling
  `stream_name_for(raw_name)`.

Default behavior:
- Registers only `source_id`, `ref_name`, and normalized `value_kind`.

#### `ensure_streams_registered(path, source_id, df, value_kinds)`

What it does:
- Ensures that streams for the current file are registered before observation
  rows are inserted.
- It is the highest-level registration hook for tabular drivers.
- Receives the parsed per-file frame `df`, so subclasses can derive raw stream
  names directly from the data they are already ingesting.

When it is called:
- Once per file during `tick()`, after the file has been parsed and value kinds
  inferred, but before `insert_observations(...)`.

How to use it:
- Override when registration should be eager, file-aware, or otherwise more
  specialized than “register newly seen names lazily”.
- This is the right place to:
  - inspect raw file headers
  - inspect parsed `df["ref_name"]` values
  - perform one-time registration for a whole file or datasource
  - skip registration entirely because setup already handled it
  - maintain custom registration state

What `value_kinds` is:
- `value_kinds` is a `dict[str, str]` mapping canonical `ref_name` to the
  inferred or declared Acquirium value kind for that stream, usually
  `"numeric"` or `"text"`.
- The base computes it from the parsed frame before registration so subclasses
  can reuse the same classification when building registration specs.

Default behavior:
- Registers only newly seen stream names for the given `source_id`, using the
  raw names in `df["ref_name"]`, then records the resulting canonical
  `ref_name`s in the base class's `_registered` cache.

#### `skip_cols(path, col_names)`

What it does:
- Returns source-local column names that should be ignored entirely before the
  built-in wide/narrow handling runs.

When it is called:
- By the built-in CSV and XLSX drivers, after column names are known but before
  the parsed frame is handed to the base wide/narrow normalization logic.

How to use it:
- Set `skip_cols = [...]` in config when the file always has junk columns you
  want to ignore.
- Override it in a subclass when the choice depends on the available source
  columns or on which file is being ingested.
- Use it for dropping columns, not combining them. If you need `Date` + `Time`
  or a composite ID built from several columns, override `read_frame(...)`
  instead.

Default behavior:
- Reads `driver.skip_cols` from config and skips any matching names present in
  `col_names`.

## Event Drivers

Use `EventIngestDriver` when data arrives asynchronously, such as MQTT messages
or serial callbacks. `tick()` is a no-op; the driver pushes observations when
events arrive.

```python
import polars as pl

from acquirium import EventIngestDriver


class CallbackDriver(EventIngestDriver):
    def setup(self):
        self.source_id = "events"
        self.aq.register_datasource(self.source_id)
        self.client.on_message = self.on_message
        self.client.start()

    def on_message(self, ts, name, value):
        self.aq.register_stream(
            source_id=self.source_id,
            ref_name=name,
            value_kind="text",
        )
        self.insert_observations(pl.DataFrame({
            "ts": [ts],
            "ref_name": [name],
            "value": [value],
        }))

    def stop(self):
        self.client.stop()
```

`register_stream()` is expected to be idempotent, so drivers can call it when a
stream is discovered instead of coordinating stream declarations through a
separate framework callback.

## Graph Changes

Override `on_graph_change()` when the driver depends on graph-declared
configuration. The runner polls `GET /graph_version` before each tick and calls
`on_graph_change()` only when the version changes after `setup()`.

MQTT is the main example: it queries the graph for new `ref:MQTTReference`
nodes, registers the associated streams, and subscribes to newly discovered
topics.

```python
class GraphConfiguredDriver(EventIngestDriver):
    def setup(self):
        self.source_id = "mqtt"
        self.aq.register_datasource(self.source_id)
        self.on_graph_change()

    def on_graph_change(self):
        for spec in self.query_graph_for_specs():
            if spec.key in self.known:
                continue
            self.aq.register_stream(
                source_id=self.source_id,
                ref_name=spec.ref_name,
                value_kind=spec.value_kind,
            )
            self.subscribe(spec)
            self.known.add(spec.key)
```

## Config

`self.config` is the complete parsed TOML dict. Drivers conventionally read
their keys from `self.config["driver"]`. When a driver is started from a
`[[drivers]]` entry, that entry is merged over `[driver]` before construction.

```python
def setup(self):
    cfg = self.config.get("driver", {})
    self.source_id = cfg.get("my_source_id", "default-source")
    self.aq.register_datasource(self.source_id)
```

## Persistent State

Drivers can persist state across restarts using `self.state`, a key-value store
backed by JSON files. State is stored in `.acquirium/drivers/<driver-id>.json`
relative to the config directory.

### State API

```python
# Get a value (with optional default)
offset = self.state.get("last_offset", default=0)

# Set a value (auto-saves to disk)
self.state.set("last_offset", 150)

# Delete a key
self.state.delete("temp_data")

# List all keys
keys = self.state.keys()

# Update multiple keys at once
self.state.update({"offset": 200, "cursor": "abc123"})

# Check if a key exists
if "last_offset" in self.state:
    ...
```

### Driver Identifier

The state file is named based on a driver identifier, determined as follows:

1. **Explicit `driver_id`** (recommended for human-readable names):
   ```toml
   [[drivers]]
   spec = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
   driver_id = "my-csv-monitor"
   ```
   → State file: `.acquirium/drivers/my-csv-monitor.json`

2. **Derived from the driver config block** (if `driver_id` not provided):
   - A hash of the merged `driver` config block combined with the class name
   - This avoids collisions when two `[[drivers]]` entries use the same
     driver class with different settings
   - Example: `CSVIngestDriver_a1b2c3d4e5f6g7h8.json`

### Example: CSV Driver with `driver_id`

```toml
[[drivers]]
spec = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
interval = 5.0
watch_dir = "./data/incoming"
driver_id = "sensor-data-ingest"  # Human-readable state file name
```

Row offsets are automatically persisted, so restarting the driver resumes
from where it left off.

### Example: Custom Driver with API Cursor

```python
from acquirium import PollingIngestDriver
import polars as pl

class APIPollingDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "api-source"
        self.aq.register_datasource(self.source_id)
        self.aq.register_stream(self.source_id, "metric/value", "numeric")

    def collect(self):
        # Load cursor from persistent state
        cursor = self.state.get("api_cursor", default=None)

        # Fetch data from API using cursor
        data = fetch_api_page(cursor=cursor)

        # Save new cursor for next tick
        if data.next_cursor:
            self.state.set("api_cursor", data.next_cursor)

        return pl.DataFrame({
            "ts": data.timestamps,
            "ref_name": data.names,
            "value": data.values,
        })
```

### Example: Custom Checkpointing

```python
class ProcessingDriver(PollingIngestDriver):
    def collect(self):
        # Load checkpoint
        checkpoint = self.state.get("processing_checkpoint", default={"id": 0})

        # Process batch starting from checkpoint
        batch = fetch_batch(after_id=checkpoint["id"])

        # Update checkpoint after successful processing
        if batch:
            self.state.set("processing_checkpoint", {"id": batch.last_id})

        return batch.to_dataframe()
```

## Reference URIs

Drivers should use the canonical reference helper instead of inventing their
own node names:

```python
ref_uri = self.reference_uri(ref_name)
```

Rules:

- Use `self.reference_uri(ref_name)` to compute the canonical reference URI.
- Use that URI as the object of `ref:hasExternalReference` in the graph.
- Write `acq:sourceId` and `acq:refName` on the same reference node.
- Attach driver-specific provenance metadata to that node.
- Insert samples by source-local `ref_name`; Acquirium computes the storage
  `ref_uri` internally from `(source_id, ref_name)`.

If you are not inside a driver, use `aq.reference_uri(source_id, ref_name)`.

## Running Drivers

Drivers are declared in `acquirium.toml` under `[[drivers]]` and started by the
CLI. There are two deployment modes.

### Server + drivers (default)

Start the FastAPI server with drivers running in the same process:

```bash
acquirium server --config acquirium.toml
```

```toml
[server]
host = "0.0.0.0"
port = 8000

[[drivers]]
spec = "scripts/temp_driver.py:TemperatureDriver"
interval = 5.0

[[drivers]]
spec = "acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver"
interval = 10.0
mqtt_source_id = "mqtt"
```

In-process drivers call the server manager directly — no HTTP round-trip. This
is the default for single-host deployments.

### Driver-only mode

Set `enabled = false` under `[server]` to run drivers against a remote Acquirium
instance without starting a local server:

```bash
acquirium server --config driver.toml
```

```toml
[server]
enabled = false

[driver]
server_url = "acquirium.example.com"
server_port = 8000
use_ssl = true
interval = 10.0
insert_batch_rows = 50000

[[drivers]]
spec = "scripts/temp_driver.py:TemperatureDriver"
interval = 5.0
```

Drivers connect to the server declared in `[driver]` and communicate over HTTP.
Use this when drivers run on a separate machine from the server, or when you
want to scale out data collection independently.

### Driver spec format

`spec` is `path/to/file.py:ClassName` or `my.module:ClassName`:

```toml
[[drivers]]
spec = "scripts/temp_driver.py:TemperatureDriver"

[[drivers]]
spec = "mypackage.drivers:TemperatureDriver"
```

Each `[[drivers]]` entry requires `spec` and may override any `[driver]` key:

```toml
[driver]
interval = 10.0
server_url = "localhost"
server_port = 8000
use_ssl = false
insert_batch_rows = 50000

[[drivers]]
spec = "scripts/fast_driver.py:FastDriver"
interval = 2.0            # overrides [driver].interval for this entry only
```

`insert_batch_rows` caps the number of rows sent in one insert request.

## Built-In MQTT Driver

`acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver` is an event driver.
It subscribes to MQTT topics declared in the graph and pushes observations from
the MQTT message callback.

```toml
[[drivers]]
spec = "acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver"
interval = 5.0
mqtt_source_id = "mqtt"
mqtt_qos = 0
mqtt_value_kind = "text"     # default for graph refs without acq:valueKind
```

Each stream is discovered from `ref:MQTTReference` nodes. The reference declares
the broker and topic; optional `ref:timeKey` and `ref:valueKey` fields identify
payload keys, defaulting to `"Timestamp"` and `"Value"`. Optional
`acq:valueKind` declares `"numeric"` or `"text"` for that stream.

Override `decode_payload()` to support another wire format. The stream identity
is already known from `MQTTStreamSpec`; the method returns only timestamp and
value.

```python
from datetime import datetime, timezone
from typing import Any

import msgpack

from acquirium.BuiltinDrivers.mqtt_ingestion import (
    MQTTIngestDriver,
    MQTTStreamSpec,
    parse_mqtt_timestamp,
)


class MyCustomMQTTIngestDriver(MQTTIngestDriver):
    def decode_payload(self, payload: bytes, spec: MQTTStreamSpec) -> tuple[datetime, Any]:
        obj = msgpack.unpackb(payload, raw=False)
        if not isinstance(obj, dict):
            raise ValueError(f"msgpack payload is not a map: {type(obj)}")
        raw_ts = obj.get(spec.time_key)
        raw_val = obj.get(spec.value_key)
        ts = parse_mqtt_timestamp(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
        return ts, raw_val
```

## Built-In WaterTAP Driver

`acquirium.BuiltinDrivers.watertap:WaterTAPDriver` is a polling driver. Each
tick runs a configured WaterTAP build/solve callable and returns observations
for RDF-mapped Pyomo variables.

```toml
[[drivers]]
spec = "acquirium.BuiltinDrivers.watertap:WaterTAPDriver"
interval = 30.0
watertap_source_id = "watertap"
watertap_graph_path = "deployments/WATERTAP2/models/test-model.ttl"
watertap_build_spec = "deployments/WATERTAP2/scripts/example_watertap.py:build_and_solve"
watertap_insert_graph = true
watertap_build_kwargs = { flow_vol = 0.001, salt_mass_conc = 0.035 }
```

Optional keys:

- `watertap_insert_graph_replace` replaces the main graph when inserting it
- `watertap_register_streams` defaults to `true`
- `watertap_result_attr` extracts the model from an attribute when the build
  function returns a wrapper object

## Built-In CSV/XLSX Drivers

CSV and XLSX drivers are polling tabular drivers. They watch a directory,
normalize file rows to canonical observations, register streams as columns or
IDs are discovered, and return rows for insertion by the common ingest base.

```toml
[[drivers]]
spec = "acquirium.BuiltinDrivers.csv_ingest:CSVIngestDriver"
interval = 5.0
watch_dir = "./data/incoming"
format = "auto"        # "auto" | "wide" | "narrow"
time_col = "time"
id_col = "id"          # narrow only
value_col = "value"    # narrow only
skip_cols = ["notes"]  # optional columns to ignore entirely
date_format = "%m/%d/%Y"
skip_rows = [1, 3]     # or { "subdir/data.csv" = [2, 5] }
encoding = "utf8-lossy"
```

Wide format:

```csv
time,temp,rh,flow
2024-01-01T00:00Z,22.5,55.0,1.2
```

Narrow format:

```csv
time,id,value
2024-01-01T00:00Z,sensor/temp,22.5
2024-01-01T00:00Z,sensor/rh,55.0
```

Behavior:

- `watch_dir` is scanned recursively
- row offsets are tracked in memory
- files are not moved or deleted
- each file gets its own datasource; datasource IDs are derived from absolute file paths
- stream `ref_name`s come from wide column names or narrow `id` values
- stream `value_kind`s are inferred per stream: any observed numeric value
  means `"numeric"`; text-only streams are `"text"`
- value-kind inference uses `assign_stream_value_kind()` from
  `acquirium.Storage.values`, which custom drivers can call too

### Customising the tabular base

The base class exposes a set of overridable methods for the common knobs.
Override any of them to hard-code behaviour without touching config:

**Config hooks** — override to hard-code behaviour without touching TOML:

| Method | Default (from config) | Purpose |
|---|---|---|
| `time_col()` | `"time"` | Timestamp column name |
| `id_col()` | `"id"` | Stream-ID column (narrow only) |
| `value_col()` | `"value"` | Value column (narrow only) |
| `skip_cols(path, col_names)` | `driver.skip_cols` | Drop source-local columns before parsing |
| `ingest_format()` | `"auto"` | `"wide"`, `"narrow"`, or `"auto"` |
| `date_format()` | `None` | strptime format for non-ISO timestamps |
| `skip_rows_for(path)` | from config | 1-indexed row numbers to skip |
| `source_id_for(path)` | sanitised absolute path | Datasource ID for a file |

**`read_frame(path, row_offset)`** is the main override point for custom file
layouts. It receives the current row offset (rows already ingested from this
file) and returns `(df, rows_read)`. Two return shapes are accepted:

- **Wide or narrow** — the base class melts it using `time_col()`,
  `ingest_format()`, etc.
- **Already normalized** — a frame with `ts`, `ref_name`, and `value` columns
  is detected automatically and passed through without further melting.

**`read_df(path, row_offset, schema_overrides=None)`** is the CSV I/O helper
(defined on `CSVIngestDriver`). Call it from `read_frame` to get the raw
frame with offset slicing, skip-row filtering, skip-column filtering, and
encoding already handled.

```python
import polars as pl
from pathlib import Path

from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver

DATE_COL = "Date"
TIME_COL = "Time"
DATE_FMT = "%m/%d/%Y %I:%M:%S %p"


class MyCSVDriver(CSVIngestDriver):
    def setup(self):
        super().setup()
        self.source_id = "my-source"

    def skip_rows_for(self, path: Path) -> tuple[int, ...]:
        return (1,)  # row 1 is a header banner, not data

    def source_id_for(self, path: Path) -> str:
        return self.source_id  # all files share one datasource

    def read_frame(self, path: Path, row_offset: int = 0):
        df = self.read_df(path, row_offset,
                          schema_overrides={DATE_COL: pl.Utf8, TIME_COL: pl.Utf8})
        rows_read = len(df)
        if rows_read == 0:
            return df, 0

        combined_ts = self.normalize_timestamps(
            df.with_columns(
                pl.concat_str([pl.col(DATE_COL), pl.lit(" "), pl.col(TIME_COL)]).alias("__ts")
            )["__ts"],
            date_format=DATE_FMT,
        )
        stream_cols = [c for c in df.columns if c not in (DATE_COL, TIME_COL)]
        rows = [
            (ts, col, val)
            for col in stream_cols
            for ts, val in zip(combined_ts.to_list(), df[col].to_list())
            if val is not None and ts is not None
        ]
        return pl.DataFrame(
            rows,
            schema={"ts": pl.Datetime("us", "UTC"), "ref_name": pl.Utf8, "value": pl.Object},
            orient="row",
        ), rows_read
```

## Lifecycle

```text
setup()
  |
  v
repeat until stopped:
  |
  +--> check graph version
  |      |
  |      +--> on_graph_change() if changed
  |
  +--> tick()
  |
  +--> sleep interval
  |
  v
stop()
```

For polling ingest drivers, `tick()` calls `collect()` and inserts the returned
observations.

Event-based drivers do not implement `collect()`. They connect to an external
event source in `setup()` and push observations from callbacks, background
client threads, or subscription handlers:

```python
class MyEventDriver(EventIngestDriver):
    def setup(self):
        self.source_id = "events"
        self.aq.register_datasource(self.source_id)
        self.client.on_message = self.on_message
        self.client.start()

    def on_message(self, ts, ref_name, value):
        self.aq.register_stream(source_id=self.source_id, ref_name=ref_name, value_kind="text")
        self.insert_observations(pl.DataFrame({
            "ts": [ts],
            "ref_name": [ref_name],
            "value": [value],
        }))
```

For these drivers, `tick()` is intentionally empty. The runner still calls it
on the configured interval so graph-change checks and shutdown behavior stay
uniform, but data flow is driven by the external event source rather than by
the tick loop.

The graph-change hook is runner-driven, not pushed by the server into the
driver. Before each tick, the runner polls the graph version and calls
`on_graph_change()` only if that version has advanced.
