# Drivers

A driver connects one or more external sources to Acquirium. Driver authors
provide source identity, stream declarations, and source-reading code. The
platform owns scheduling, datasource and stream registration, batching,
insertion, retry retention, and orderly shutdown.

The normal lifecycle is:

1. `setup()` assigns `source_id`, declares known streams, and opens resources.
2. The platform registers declared datasources and streams.
3. Each tick calls `read()` or `collect()` and inserts reported observations.
4. `stop()` closes external producers; the platform then flushes accepted rows.

## The Author Contract

Every observation is identified by the exact `(source_id, ref_name)` pair.
Acquirium does not sanitize, normalize, or otherwise rewrite either value.
Declare every pair before reporting data for it:

```python
from acquirium import PollingIngestDriver


class TemperatureDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "building-a"
        self.declare(
            "room 101 temperature",
            value_kind="numeric",
            point_uri="urn:building-a:room-101-temperature",
            unit="http://qudt.org/vocab/unit/DEG_C",
        )

    def read(self):
        self.add("room 101 temperature", read_sensor())
```

`declare()` accepts explicit stream metadata:

```python
self.declare(
    ref_name,
    source_id=None,
    value_kind=None,
    point_uri=None,
    label=None,
    unit=None,
    quantity_kind=None,
    medium=None,
    substance=None,
    data_source=None,
    properties=None,
)
```

- `source_id` defaults to `self.source_id`.
- The platform idempotently registers the datasource and stream.
- Identical repeated declarations are free; conflicting declarations raise.
- If `value_kind` is omitted, Acquirium infers `"numeric"` or `"text"` from
  the first observation batch and records the result before inserting it.
- Reporting an undeclared pair raises `UndeclaredStreamError`.
- Use `properties` only for extension predicates not represented by a named
  argument.

Use `self.insert_graph(rdf_text, format="turtle")` for RDF content and
`self.insert_graph_file(path, format=None)` for a file. File format inference is
limited to known RDF suffixes; content is never guessed to be a path.

## Polling Drivers

For scalar or small samples, implement `read()` and call `add()`:

```python
class MetricsDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "host-a"
        self.declare("cpu_percent", value_kind="numeric")
        self.declare("state", value_kind="text")

    def read(self):
        self.add("cpu_percent", read_cpu())
        self.add("state", read_state())
```

`add(ref_name, value, ts=None, *, source_id=None)` is thread-safe and performs
no network I/O. `ts` defaults to the current UTC time; naive datetimes are
interpreted as UTC. A successful return means the row was accepted into the
in-memory buffer, not durably stored.

When a source already produces a whole batch, implement `collect()` instead:

```python
def collect(self):
    return pl.DataFrame({
        "ts": timestamps,
        "ref_name": names,
        "value": values,
    })
```

Bulk frames require `ts`, `ref_name`, and `value`, plus `source_id` for a
multi-source frame. Every pair in the frame must already be declared.

The framework retries a retained failed batch before sampling again. It never
silently drops accepted rows. If the in-memory queue reaches
`max_buffered_rows`, a new `add()` raises `DriverBufferFull`; the callback or
source integration decides how to apply backpressure or recover.

Buffers are not persisted across process crashes. On a graceful stop, the
platform first calls `stop()` to quiesce producers and then performs a final
flush. A failed final flush makes shutdown unsuccessful and reports the unsent
row count.

## Event Drivers

Use `EventIngestDriver` when a callback receives observations. Declare streams
before starting the producer, then call `add()` from the callback:

```python
from acquirium import EventIngestDriver


class CallbackDriver(EventIngestDriver):
    def setup(self):
        self.source_id = "events"
        self.declare("alarm", value_kind="text")
        self.client.on_message = self.on_message
        self.client.start()

    def on_message(self, ts, value):
        self.add("alarm", value, ts)

    def stop(self):
        self.client.stop()
```

`interval` is the batching and arrival-to-queryable cadence for event drivers.
Choose it independently from `graph_poll_interval`.

## File Drivers

`FileIngestDriver` owns recursive discovery, persisted per-file checkpoints,
registration-before-insertion, and checkpoint advancement after success. A
specialized driver must make its read operation visible:

```python
from pathlib import Path
from typing import Any

import polars as pl

from acquirium import FileBatch, FileIngestDriver


class PlantParquetDriver(FileIngestDriver):
    def setup(self):
        super().setup()  # validates source_id, watch_dir, and glob
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

`source_id`, `watch_dir`, and `glob` are required for file drivers; `glob` is
either one pattern or a list. `FileBatch.next_cursor` must be
JSON-serializable. The platform saves it only after registration and insertion
return successfully; exceptions and `{ "ok": false }` retain the old cursor.
An empty successful batch may advance its cursor.

For source-specific name mapping, perform the mapping visibly in `read()` and
declare the mapped name. Acquirium preserves raw column and ID values by
default, including spaces and punctuation.

## Built-In Tabular Drivers

CSV, XLSX, and Parquet drivers use the same explicit schema configuration:

```toml
[[drivers]]
spec = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
interval = 5.0
source_id = "operator-exports"
watch_dir = "./data/incoming"
glob = ["*.csv", "*.tsv"]
format = "wide"       # required: "wide" or "narrow"
# Timestamp columns are discovered from common names when omitted.
time_col = "time"     # one complete timestamp column
# date_col = "Date"   # or an explicit split date/time pair
# clock_col = "Time"
id_col = "id"         # narrow only
value_col = "value"   # narrow only
skip_cols = ["notes"]
timezone = "UTC"      # interpretation of naive timestamps
# date_format = "%m/%d/%Y"  # optional override
# day_first = true            # resolve ambiguous 03/04/2025 as 3 April
```

Wide data has timestamp columns and one column per stream. Narrow data has
timestamp columns, a stream-ID column, and a value column. Layout is never
inferred.

`to_timestamp()` accepts native `Date`/`Datetime`, ISO/RFC3339 text, common
delimited formats, integer Unix epochs, and separate date/time columns. The
tabular drivers discover unambiguous names such as `timestamp`, `ts`, `Date`
plus `Time`, and matching pairs such as `Sample Date` plus `Sample Time`.
Explicit `time_col`, or `date_col` plus `clock_col`, wins over discovery.
`date_format` is available for unusual formats, `timezone` says how to read
naive values, and `day_first` resolves otherwise ambiguous numeric dates.
Unparseable timestamps are logged and dropped.

Both reusable conversions are public driver-author helpers:

```python
from acquirium import to_observations, to_timestamp
```

An ingest driver can equivalently call `self.to_timestamp(...)`.

CSV additionally supports `skip_rows`, `header_contains`, `encoding`, and
`ragged_lines`. XLSX supports `sheets`. These are format-reader conveniences,
not part of the general driver contract.

`to_timestamp()`, `to_observations()`, and `read_parquet_batch()` are plain
mechanical helpers. They know nothing about datasource identity, graph
semantics, or source-specific name mapping. Specialized drivers such as
Benicia and WaterTAP explicitly call the Parquet helper from their own
`read(path, cursor)` implementations.

## Graph Changes and Scheduling

Override `on_graph_change()` when subscriptions or source configuration depend
on the graph. It is not called for the initial graph; perform the initial query
in `setup()`.

```toml
[[drivers]]
spec = "acquirium.Drivers.BuiltInDrivers.mqtt_ingestion:MQTTIngestDriver"
interval = 1.0
graph_poll_interval = 30.0
source_id = "mqtt"
```

`interval` controls ticks and buffered insertion. `graph_poll_interval`
controls graph version checks independently and defaults to
`max(interval, 10.0)`. Driver methods are serialized, so a long-running
synchronous method can delay either deadline.

## Configuration and State

`self.config` is the complete merged TOML dictionary; driver-specific keys live
under `self.config["driver"]`. `config_dir()` resolves relative configuration
assets, and `data_dir()` resolves the runtime data directory.

Persistent driver state is available through `self.state`:

```python
cursor = self.state.get("cursor", 0)
self.state.set("cursor", cursor + rows_read)
self.state.update({"cursor": cursor, "token": token})
self.state.delete("token")
```

Set `driver_id` when multiple instances of one class need stable, distinct
state files. Otherwise the identifier is derived from the driver spec.

## Running Drivers

Driver specs use `module:Class`, `path/to/file.py:Class`, or
`./relative.py:Class`. Relative paths resolve from the configuration file.

```toml
[driver]
interval = 10.0
server_url = "localhost"
server_port = 8000

[[drivers]]
spec = "scripts/temperature.py:TemperatureDriver"
interval = 2.0
```

Run the server and configured drivers with:

```bash
uv run acquirium server --config acquirium.toml
```

Do not put `while True` or `sleep()` in a driver; the runner owns scheduling.
Use `stop()` only to release or quiesce external resources. The platform owns
the final observation flush.
