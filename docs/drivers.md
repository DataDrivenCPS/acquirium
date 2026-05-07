# Drivers

A **Driver** connects an external source to Acquirium. The runner calls
`setup()` once, then calls `tick()` on a fixed interval until shutdown.

Most drivers should not implement insertion themselves. Use one of the ingest
base classes:

- `PollingIngestDriver`: the runner pulls observations by calling `collect()`
- `EventIngestDriver`: callbacks push observations by calling
  `insert_observations()`

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
`insert_timeseries_polars()`.

Rules:

- Do not put `while True` or `time.sleep` in a driver; the runner owns timing.
- Register datasources and streams before reporting observations for them.
- Use `stop()` to release resources on shutdown.

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

```bash
acquirium run path/to/driver.py:ClassName --config acquirium.toml --interval 5
```

Driver specs must include the class name after a colon. File paths and dotted
module paths are accepted:

```bash
acquirium run scripts/temp_driver.py:TemperatureDriver --config acquirium.toml
acquirium run mypackage.drivers:TemperatureDriver --config acquirium.toml
```

The `[driver]` section sets connection defaults:

```toml
[driver]
server_url = "localhost"
server_port = 8000
use_ssl = false
interval = 10.0
insert_batch_rows = 50000
```

`insert_batch_rows` caps samples sent in one insert request. The Acquirium
client splits large Polars inserts automatically.

## Auto-Started Drivers

Add `[[drivers]]` entries to `acquirium.toml` to start drivers inside the
server process.

```toml
[[drivers]]
spec = "scripts/temp_driver.py:TemperatureDriver"
interval = 5.0
```

Each entry requires `spec` and may override any `[driver]` key:

```toml
[driver]
interval = 10.0

[[drivers]]
spec = "acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver"
interval = 5.0
mqtt_source_id = "mqtt"
```

In-process drivers receive the same `self.aq` interface as `acquirium run`, but
calls are dispatched directly to the server manager instead of over HTTP.
MQTT ingestion is configured this way too: the server does not start MQTT
subscribers implicitly from the graph.

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
- each file gets its own datasource
- datasource ids are derived from absolute file paths
- stream `ref_name`s come from wide column names or narrow `id` values
- stream `value_kind`s are inferred per stream: any observed numeric value
  means `"numeric"`; text-only streams are `"text"`
- value-kind inference uses `assign_stream_value_kind()` from
  `acquirium.Storage.values`, which custom drivers can call too

Custom tabular parsing should return normalized observations from
`read_frame()` when the built-in wide/narrow formats do not fit:

```python
import polars as pl

from acquirium.BuiltinDrivers.csv_ingest import CSVIngestDriver


class MyCSVDriver(CSVIngestDriver):
    def read_frame(self, path, row_offset=0):
        df = self._read_df(path, row_offset)
        raw_ts = df.select(
            (pl.col("Date") + pl.lit(" ") + pl.col("Time")).alias("ts")
        )["ts"]
        out = pl.DataFrame({
            "ts": self.normalize_timestamps(
                raw_ts, date_format="%m/%d/%Y %I:%M:%S %p"
            ),
            "ref_name": ["Temperature"] * len(df),
            "value": df["Temperature"],
        })
        return out, len(df)
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
