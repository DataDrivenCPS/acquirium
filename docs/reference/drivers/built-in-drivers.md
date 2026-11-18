---
title: Built-in drivers
---

Acquirium ships with six drivers. Each is started by naming it in the `spec` of a `[[drivers]]` entry. The keys in the tables below go in that entry, next to the keys that apply to [every driver](configuration.md).

| Item | Reads | `spec` |
|---|---|---|
| [`CSVIngestDriver`](#csvingestdriver) | CSV and TSV files in a directory | `acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver` |
| [`CSVIngestDriver.prepare_frame`](#csvingestdriverprepare_frame) | hook of the CSV driver | |
| [`CSVIngestDriver.declare_stream`](#csvingestdriverdeclare_stream) | hook of the CSV driver | |
| [`XLSXIngestDriver`](#xlsxingestdriver) | Excel files in a directory | `acquirium.Drivers.BuiltInDrivers.xlsx_ingest:XLSXIngestDriver` |
| [`ParquetIngestDriver`](#parquetingestdriver) | Parquet files in a directory | `acquirium.Drivers.BuiltInDrivers.parquet_ingest:ParquetIngestDriver` |
| [`MQTTIngestDriver`](#mqttingestdriver) | MQTT topics named in the graph | `acquirium.Drivers.BuiltInDrivers.mqtt_ingestion:MQTTIngestDriver` |
| [`MQTTIngestDriver.decode_payload`](#mqttingestdriverdecode_payload) | hook of the MQTT driver | |
| [`WaterTAPDriver`](#watertapdriver) | a WaterTAP model solved on every tick | `acquirium.Drivers.BuiltInDrivers.watertap:WaterTAPDriver` |
| [`SystemMetricsDriver`](#systemmetricsdriver) | CPU, memory, disk and network of the server machine | `acquirium.Drivers.BuiltInDrivers.system_metrics:SystemMetricsDriver` |

## Keys shared by the file drivers

The CSV, XLSX and Parquet drivers extend [`FileIngestDriver`](base-classes.md#fileingestdriver) and share these keys. The reshaping is done by [`to_observations`](tabular-helpers.md#to_observations).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `source_id` | `str` | required | The `source_id` of every stream read from the files |
| `watch_dir` | `str` | required | Directory to watch, relative to the configuration file. Subdirectories are searched too |
| `glob` | `str` or list of `str` | required | File name pattern or patterns, for example `["*.csv", "*.tsv"]` |
| `format` | `str` | required | `"wide"`: one column per stream, one row per time. `"narrow"`: one row per value, with a column for the stream name and a column for the value |
| `time_col` | `str` | found by name | Name of the column that holds the full timestamp |
| `date_col` | `str` | found by name | Name of the date column, when date and time are in two columns |
| `clock_col` | `str` | found by name | Name of the time-of-day column. Requires `date_col` |
| `id_col` | `str` | `"id"` | Narrow format only: column that holds the stream name |
| `value_col` | `str` | `"value"` | Narrow format only: column that holds the value |
| `skip_cols` | `str` or list of `str` | `[]` | Columns to ignore |
| `date_format` | `str` | none | `strptime` format of the timestamps, for example `"%m/%d/%Y %H:%M"` |
| `timezone` | `str` | `"UTC"` | Time zone of timestamps that carry none |
| `day_first` | `bool` | `false` | See [`to_timestamp`](tabular-helpers.md#to_timestamp) |

Column names in the wide format, and the values of `id_col` in the narrow format, become the `stream_name` unchanged. Each stream is declared without metadata, and the server infers its value kind. Rows whose timestamp cannot be read, and cells without a value, are dropped.

Files are never moved or deleted. Each driver remembers how many rows of each file it has read and inserts only the rows after that.

## `CSVIngestDriver`

```python
CSVIngestDriver(aq, config)
```

Reads CSV files from a directory. Files ending in `.tsv` are read with a tab as the separator. Every cell is read as text. It is exported as `acquirium.CSVIngestDriver`.

**Configuration keys**

The [shared keys](#keys-shared-by-the-file-drivers), and:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `skip_rows` | `int`, list of `int`, or table | `[]` | Lines of the file to remove before it is parsed. Line 1 is the first line of the file. A table maps a file path relative to `watch_dir` to its own list: `{ "sub/data.csv" = [2, 5] }` |
| `header_contains` | `str` or list of `str` | `[]` | Cell values that identify the header line. Every line above the first line that contains all of them is removed. Only the first 100 lines are searched. When set, `skip_rows` is ignored |
| `encoding` | `str` | `"utf8-lossy"` | Text encoding of the files, for example `"utf8"` or `"latin1"`. `"utf8-lossy"` replaces bytes that are not valid UTF-8 |
| `ragged_lines` | `str` | `"ignore"` | What to do with a row whose number of cells differs from the header. `"ignore"`: keep the row, drop extra cells, leave missing cells empty. `"skip"`: drop the row. `"error"`: fail the file |
| `null_values` | `str` or list of `str` | `[]` | Cell texts that mean "no value", for example `["Null", "-"]` |

**Raises**

- `ValueError`: from `setup()`, when `source_id`, `watch_dir` or `glob` is missing.
- `ValueError`: while reading a file, when `format` is missing, `ragged_lines` has another value, or no header line contains the `header_contains` values. The file is logged and tried again on the next tick.

**Example**

```toml
[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"
source_id = "csv-files"
watch_dir = "incoming"
glob      = "*.csv"
format    = "wide"
```

With this file in `incoming/a.csv`:

```text
time,temp,rh
2026-01-01T00:00:00Z,22.5,55
2026-01-01T00:01:00Z,22.6,
```

the first read of the file returns:

```text
FileBatch(observations=shape: (3, 3)
┌─────────────────────────┬─────────────┬───────┐
│ ts                      ┆ stream_name ┆ value │
│ ---                     ┆ ---         ┆ ---   │
│ datetime[μs, UTC]       ┆ str         ┆ str   │
╞═════════════════════════╪═════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ temp        ┆ 22.5  │
│ 2026-01-01 00:01:00 UTC ┆ temp        ┆ 22.6  │
│ 2026-01-01 00:00:00 UTC ┆ rh          ┆ 55    │
└─────────────────────────┴─────────────┴───────┘, next_cursor=2)
```

## `CSVIngestDriver.prepare_frame`

```python
CSVIngestDriver.prepare_frame(df, path)
```

Hook. Changes the table read from a file before it is reshaped. It runs after `skip_cols` are dropped and before timestamps are parsed. Override it in a subclass to repair a source-specific layout. The default returns `df` unchanged.

**Parameters**

- `df` (`polars.DataFrame`): the new rows of the file, every column as text.
- `path` (`Path`): the file being read.

**Returns**

`polars.DataFrame`. The number of rows is used to advance the cursor, so do not add or remove rows here.

**Example**

```python
import polars as pl
import acquirium as aq


class PlantCSV(aq.CSVIngestDriver):
    def prepare_frame(self, df, path):
        return df.rename({"Sample Timestamp (local)": "time"})
```

## `CSVIngestDriver.declare_stream`

```python
CSVIngestDriver.declare_stream(stream_name)
```

Hook. Declares one stream found in a file. It is called once for each stream name in each batch. The default calls `self.declare(stream_name)`. Override it to attach metadata.

**Parameters**

- `stream_name` (`str`): a column name (wide format) or a value of `id_col` (narrow format).

**Returns**

`None`.

**Example**

```python
import acquirium as aq

UNITS = {"temp": "http://qudt.org/vocab/unit/DEG_C"}


class PlantCSV(aq.CSVIngestDriver):
    def declare_stream(self, stream_name):
        self.declare(stream_name, unit=UNITS.get(stream_name))
```

## `XLSXIngestDriver`

```python
XLSXIngestDriver(aq, config)
```

Reads Excel files from a directory. Import it from `acquirium.Drivers.BuiltInDrivers.xlsx_ingest`. It needs the `xlsx` extra, which is installed with `pip install "acquirium[xlsx]"`.

**Configuration keys**

The [shared keys](#keys-shared-by-the-file-drivers), and:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `sheets` | list of `str` | first sheet | Names of the sheets to read. Several sheets are joined into one table before reshaping |

**Raises**

- `ValueError`: from `setup()`, when `source_id`, `watch_dir` or `glob` is missing.
- `ValueError`: while reading a file, when `format` is missing.

**Example**

```toml
[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.xlsx_ingest:XLSXIngestDriver"
source_id = "lab-sheets"
watch_dir = "incoming"
glob      = "*.xlsx"
format    = "wide"
sheets    = ["Sheet1"]
```

## `ParquetIngestDriver`

```python
ParquetIngestDriver(aq, config)
```

Reads Parquet files from a directory. Parquet keeps column types, so timestamps usually need no parsing. Import it from `acquirium.Drivers.BuiltInDrivers.parquet_ingest`.

**Configuration keys**

The [shared keys](#keys-shared-by-the-file-drivers). There are no other keys.

**Raises**

- `ValueError`: from `setup()`, when `source_id`, `watch_dir` or `glob` is missing.
- `ValueError`: while reading a file, when `format` is missing.

**Example**

```toml
[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.parquet_ingest:ParquetIngestDriver"
source_id = "parquet-exports"
watch_dir = "incoming"
glob      = ["*.parquet", "*.pq"]
format    = "wide"
time_col  = "timestamp"
```

## `MQTTIngestDriver`

```python
MQTTIngestDriver(aq, config)
```

Subscribes to MQTT topics and inserts the messages. The topics are not listed in the configuration. They are read from the graph, and read again whenever the graph changes. Import it from `acquirium.Drivers.BuiltInDrivers.mqtt_ingestion`. It needs the `mqtt` extra, which is installed with `pip install "acquirium[mqtt]"`.

**Configuration keys**

| Key | Type | Default | Meaning |
|---|---|---|---|
| `source_id` | `str` | required | The `source_id` of the streams |
| `mqtt_qos` | `int` | `0` | Quality-of-service level of the subscriptions |
| `mqtt_value_kind` | `str` | `"text"` | Value kind of streams whose graph node does not state one: `"numeric"` or `"text"` |

**Graph statements**

The driver subscribes once for every property that is linked by `ref:hasExternalReference` to a node of type `ref:MQTTReference`. The prefix `ref:` is `https://brickschema.org/schema/Brick/ref#` and `acq:` is `urn:acquirium#`.

| Predicate on the `ref:MQTTReference` node | Default | Meaning |
|---|---|---|
| `acq:refName` | required | The `stream_name` |
| `ref:MQTTTopic` | required | Topic to subscribe to |
| `ref:MQTTBroker` | `localhost` | Broker as `host`, `host:port`, `mqtt://host[:port]` or `mqtts://host[:port]`. The default port is 1883, or 8883 for `mqtts` |
| `acq:timeKey` | `Timestamp` | Key of the message that holds the time |
| `acq:valueKey` | `Value` | Key of the message that holds the value |
| `acq:valueKind` | `mqtt_value_kind` | `numeric` or `text` |

Note that the URI of the `ref:MQTTReference` node must be the stream URI for this `source_id` and `stream_name`, as returned by [`reference_uri`](helpers.md#driverreference_uri). Other nodes are skipped with a warning.

A message is a JSON object, for example `{"Timestamp": "2026-01-01T00:00:00Z", "Value": 21.5}`. The time may be an ISO string, or a number of seconds or milliseconds since 1970. A message without the time key gets the time of arrival.

**Raises**

- `ValueError`: from `setup()`, when `source_id` is missing or `mqtt_value_kind` is not a known kind.

**Example**

```toml
[[drivers]]
spec      = "acquirium.Drivers.BuiltInDrivers.mqtt_ingestion:MQTTIngestDriver"
interval  = 5.0
source_id = "mqtt"
```

## `MQTTIngestDriver.decode_payload`

```python
MQTTIngestDriver.decode_payload(payload, spec)
```

Hook. Turns one raw message into a time and a value. Override it in a subclass to read a format other than JSON.

**Parameters**

- `payload` (`bytes`): the message as received.
- `spec` (`MQTTStreamSpec`): the stream the message belongs to. Its fields are `property_uri`, `stream_uri`, `stream_name`, `broker`, `port`, `topic`, `time_key`, `value_key` and `value_kind`.

**Returns**

`tuple[datetime, Any]`: the time, with a time zone, and the value.

**Raises**

- `ValueError`: when the message cannot be decoded. The driver logs it and continues.

**Example**

```python
from datetime import datetime, timezone
from acquirium.Drivers.BuiltInDrivers.mqtt_ingestion import MQTTIngestDriver


class PlainNumberMQTT(MQTTIngestDriver):
    def decode_payload(self, payload, spec):
        return datetime.now(timezone.utc), float(payload)
```

## `WaterTAPDriver`

```python
WaterTAPDriver(aq, config)
```

Builds and solves a WaterTAP model on every tick and inserts the values of the mapped model variables. Import it from `acquirium.Drivers.BuiltInDrivers.watertap`. It needs the `watertap` extra, which is installed with `pip install "acquirium[watertap]"`.

**Configuration keys**

| Key | Type | Default | Meaning |
|---|---|---|---|
| `watertap_mapping_path` | `str` | required | JSON file with a `properties` table that maps a property URI to the path of a Pyomo variable, and an optional `namespace` |
| `watertap_build_spec` | `str` | required | Function that builds the model, as `file.py:function` or `module:function`. Called as `build(**watertap_build_kwargs)` |
| `watertap_solve_spec` | `str` | required | Function that solves the model. Called as `solve(model)` |
| `source_id` | `str` | `"watertap"` | The `source_id` of the streams |
| `watertap_build_kwargs` | table | `{}` | Keyword arguments for the build function |
| `watertap_change_inputs_spec` | `str` | none | Function that sets model inputs. Called as `change_inputs(model, watertap_inputs)` after the build |
| `watertap_inputs` | table | `{}` | Inputs for `watertap_change_inputs_spec`. When empty, the function is not called |
| `watertap_graph_path` | `str` | none | RDF file of the plant model |
| `watertap_insert_graph` | `bool` | `false` | Insert `watertap_graph_path` into the driver's graph in `setup()` |
| `watertap_insert_graph_replace` | `bool` | `false` | Replace the driver's graph when inserting |
| `watertap_result_attr` | `str` | none | Name of the attribute of the build result that holds the model. Without it, the result itself is the model, or its first item when it is a tuple or a list |

One numeric stream is declared for each entry of `properties`. Its `stream_name` is the property URI with the `namespace` prefix removed. A variable that is not found in the solved model is skipped with a warning.

Note that relative paths in these keys are resolved against the working directory of the server, not against the configuration file.

**Raises**

- `ValueError`: from `setup()`, when a required key is missing or the mapping has no `properties`.
- `FileNotFoundError`: from `setup()`, when a named file does not exist.

**Example**

```toml
[[drivers]]
spec                  = "acquirium.Drivers.BuiltInDrivers.watertap:WaterTAPDriver"
interval              = 60.0
watertap_mapping_path = "models/ro/watertap-mapping.json"
watertap_build_spec   = "models/ro/build-and-solve.py:build"
watertap_solve_spec   = "models/ro/build-and-solve.py:solve"
```

## `SystemMetricsDriver`

```python
SystemMetricsDriver(aq, config)
```

Reports the load of the machine the server runs on. It has no configuration keys of its own. Import it from `acquirium.Drivers.BuiltInDrivers.system_metrics`.

Its `source_id` is `<hostname>-system-metrics`. In `setup()` it writes a node for the machine, `urn:host:<hostname>`, to its graph and declares these numeric streams:

| `stream_name` | Unit | Meaning |
|---|---|---|
| `cpu_percent` | percent | CPU use |
| `memory_percent` | percent | Memory use |
| `memory_used_bytes` | bytes | Memory in use |
| `disk_percent` | percent | Use of the disk mounted at `/` |
| `net_bytes_sent` | bytes | Total bytes sent over the network, as counted by the operating system |
| `net_bytes_recv` | bytes | Total bytes received over the network, as counted by the operating system |

**Example**

```toml
[[drivers]]
spec     = "acquirium.Drivers.BuiltInDrivers.system_metrics:SystemMetricsDriver"
interval = 30.0
```
