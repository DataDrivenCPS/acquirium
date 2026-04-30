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

An optional `source_id` column may be included when one driver emits rows for
multiple datasources. If omitted, `self.source_id` is used.

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
        self.aq.register_stream(source_id=self.source_id, ref_name="temp/room1")

    def collect(self):
        return pl.DataFrame({
            "ts": [datetime.now(timezone.utc)],
            "ref_name": ["temp/room1"],
            "value": [read_sensor()],
        })
```

The base `tick()` implementation normalizes the frame and calls
`insert_timeseries_polars()`.

Rules:

- Do not put `while True` or `time.sleep` in a driver; the runner owns timing.
- Set `self.source_id` in `setup()` for single-source drivers.
- Register datasources and streams where they are discovered.
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
        self.aq.register_stream(source_id=self.source_id, ref_name=name)
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
            self.aq.register_stream(source_id=self.source_id, ref_name=spec.ref_name)
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
```

Each stream is discovered from `ref:MQTTReference` nodes. The reference declares
the broker and topic; optional `ref:timeKey` and `ref:valueKey` fields identify
payload keys, defaulting to `"Timestamp"` and `"Value"`.

Override `decode_payload()` to support another wire format. The stream identity
is already known from `MQTTStreamSpec`; the method returns only timestamp and
value.

```python
from datetime import datetime, timezone
from typing import Any

import msgpack

from acquirium.BuiltinDrivers.mqtt_ingestion import MQTTIngestDriver, MQTTStreamSpec


class MyCustomMQTTIngestDriver(MQTTIngestDriver):
    def decode_payload(self, payload: bytes, spec: MQTTStreamSpec) -> tuple[datetime, Any]:
        obj = msgpack.unpackb(payload, raw=False)
        if not isinstance(obj, dict):
            raise ValueError(f"msgpack payload is not a map: {type(obj)}")
        raw_ts = obj.get(spec.time_key)
        raw_val = obj.get(spec.value_key)
        ts = parse_ts(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
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
            "ref_name": "Temperature",
            "value": df["Temperature"],
        })
        return out, len(df)
```

## Lifecycle

```text
setup()
  |
  v
check graph version -> on_graph_change() if changed
  |
  v
tick()  <---- interval ----
tick()
tick()
  |
  v
stop()
```

For polling ingest drivers, `tick()` calls `collect()` and inserts the returned
observations. For event ingest drivers, `tick()` is intentionally empty and
callbacks call `insert_observations()` as data arrives.
