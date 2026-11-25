---
title: Helpers
---

These methods are available on `self` inside a driver. The ones on `IngestDriver` declare streams and report values. The ones on `Driver` write to the graph and locate directories.

| Item | Purpose |
|---|---|
| [`IngestDriver.declare`](#ingestdriverdeclare) | Declare a stream and its metadata |
| [`IngestDriver.is_declared`](#ingestdriveris_declared) | Check whether a stream was declared |
| [`IngestDriver.register_declared`](#ingestdriverregister_declared) | Send pending declarations to the server |
| [`IngestDriver.add`](#ingestdriveradd) | Report one value |
| [`IngestDriver.flush`](#ingestdriverflush) | Insert the values reported with `add` |
| [`IngestDriver.insert_observations`](#ingestdriverinsert_observations) | Insert a dataframe of values |
| [`IngestDriver.normalize_observations`](#ingestdrivernormalize_observations) | Convert a dataframe to the form that is inserted |
| [`IngestDriver.to_timestamp`](#ingestdriverto_timestamp) | Convert a timestamp column to UTC |
| [`Driver.reference_uri`](#driverreference_uri) | The URI of one of the driver's streams |
| [`Driver.insert_graph`](#driverinsert_graph) | Write RDF text to the driver's graph |
| [`Driver.insert_graph_file`](#driverinsert_graph_file) | Write an RDF file to the driver's graph |
| [`Driver.sparql_update`](#driversparql_update) | Run a SPARQL update on the driver's graph |
| [`Driver.config_dir`](#driverconfig_dir) | Directory of the configuration file |
| [`Driver.data_dir`](#driverdata_dir) | Data directory of the server |

The examples on this page use a driver `d` whose `source_id` is `"demo"`.

## `IngestDriver.declare`

```python
IngestDriver.declare(stream_name, *, source_id=None, value_kind=None,
                     property_uri=None, label=None, unit=None,
                     quantity_kind=None, medium=None, substance=None,
                     data_source=None, properties=None)
```

Declares a stream and its metadata. The declaration is kept in memory and sent to the server before the next insert, so a stream exists before its values do. Calling it again with the same arguments does nothing, so it is safe to call on every tick.

**Parameters**

- `stream_name` (`str`): name of the stream within its `source_id`.
- `source_id` (`str`, optional): the `source_id` the stream belongs to. Defaults to `self.source_id`. Pass it only in a driver that writes to several `source_id` values, and then pass the same value to `add`.
- `value_kind` (`str`, optional): `"numeric"` or `"text"`. When omitted, the server infers it from the values: the stream becomes numeric once it receives a numeric value.
- `property_uri` (`str`, optional): URI of the property in the graph that this stream measures. When omitted, the server creates a property for the stream.
- `label` (`str`, optional): readable name, written on the property.
- `unit` (`str` or `URIRef`, optional): unit of the values. A URI is used as given. Plain text such as `"psi"` is matched to a unit by the server.
- `quantity_kind` (`str` or `URIRef`, optional): quantity kind, as a URI or plain text.
- `medium` (`str` or `URIRef`, optional): medium, as a URI or plain text.
- `substance` (`str` or `URIRef`, optional): substance, as a URI or plain text.
- `data_source` (`str` or `URIRef`, optional): a tag written on the property. Queries can filter on it with `measurement(data_source=...)`.
- `properties` (`dict`, optional): extra graph statements for the stream, as a mapping of predicate URI to value.

**Returns**

`None`.

**Raises**

- `ValueError`: when `stream_name` or the `source_id` is empty.
- `ValueError`: when the stream was already declared with different metadata.
- `AttributeError`: when `source_id` is not passed and `self.source_id` is not set yet.

**Example**

```python
d.declare("temp", unit="http://qudt.org/vocab/unit/DEG_C", label="Tank temperature")
d.declare("temp", label="other")
```

```text
ValueError: conflicting declaration for stream ('demo', 'temp'): was {'label': 'Tank temperature', 'unit': 'http://qudt.org/vocab/unit/DEG_C'}, now {'label': 'other'}
```

## `IngestDriver.is_declared`

```python
IngestDriver.is_declared(stream_name, *, source_id=None)
```

Returns whether this driver instance has declared the stream.

**Parameters**

- `stream_name` (`str`): name of the stream.
- `source_id` (`str`, optional): defaults to `self.source_id`.

**Returns**

`bool`.

**Example**

```python
d.is_declared("temp"), d.is_declared("nope")
```

```text
(True, False)
```

## `IngestDriver.register_declared`

```python
IngestDriver.register_declared(observations=None)
```

Sends the declarations that the server has not received yet. The base classes call it after `setup()` and before every insert. Call it yourself only when a stream must exist on the server before any value is inserted, for example after declaring a stream in `on_graph_change()`.

**Parameters**

- `observations` (`polars.DataFrame`, optional): rows that are about to be inserted. Every stream in them must have been declared.

**Returns**

`None`.

**Raises**

- `UndeclaredStreamError`: when `observations` contain a stream that was not declared.

## `IngestDriver.add`

```python
IngestDriver.add(stream_name, value, ts=None, *, source_id=None)
```

Reports one value. The value is placed in a buffer and inserted at the end of the tick.

**Parameters**

- `stream_name` (`str`): name of a declared stream.
- `value` (any): the value, for example a number or a string.
- `ts` (`datetime`, optional): time of the value. Defaults to the current time. A datetime without a time zone is read as UTC.
- `source_id` (`str`, optional): defaults to `self.source_id`.

**Returns**

`None`.

**Raises**

- [`UndeclaredStreamError`](base-classes.md#undeclaredstreamerror): when the stream was not declared.
- [`DriverBufferFull`](base-classes.md#driverbufferfull): when `max_buffered_rows` rows are already waiting.

**Example**

```python
from datetime import datetime, timezone

d.add("temp", 21.5)
d.add("temp", 20.0, datetime(2025, 12, 31, tzinfo=timezone.utc))
```

## `IngestDriver.flush`

```python
IngestDriver.flush()
```

Inserts every value reported with `add` and empties the buffer. The base classes call it on every tick and once more after `stop()`, so a driver rarely calls it itself.

When the insert fails, the rows are put back in the buffer and the exception is raised. They are sent again on the next tick.

**Returns**

`dict` with the keys `ok` (`bool`) and `rows_inserted` (`int`).

**Raises**

Errors from [`insert_observations`](#ingestdriverinsert_observations).

**Example**

```python
d.add("temp", 20.0)
d.flush()
d.flush()
```

```text
{'ok': True, 'rows_inserted': 1}
{'ok': True, 'rows_inserted': 0}
```

## `IngestDriver.insert_observations`

```python
IngestDriver.insert_observations(observations, *, publication_id=None)
```

Inserts a dataframe of values right away, without the buffer. Use it when the data is already a table. Pending declarations are sent first.

**Parameters**

- `observations` (`polars.DataFrame` or `None`): rows with the columns `ts`, `stream_name` and `value`. A `source_id` column is optional; without it every row belongs to `self.source_id`. Other columns are ignored. `None` and an empty dataframe insert nothing.
- `publication_id` (`str`, optional): an identifier attached to the insert request. `flush` sets it. Driver code can leave it out.

**Returns**

`dict` with the keys `ok` (`bool`) and `rows_inserted` (`int`).

**Raises**

- `ValueError`: when a required column is missing.
- [`UndeclaredStreamError`](base-classes.md#undeclaredstreamerror): when a row names a stream that was not declared.
- `RuntimeError`: when the server reports that the insert failed.

Note that inserting a value for a stream and time that already has one replaces the stored value.

**Example**

```python
from datetime import datetime, timezone
import polars as pl

frame = pl.DataFrame({
    "ts": [datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc)],
    "stream_name": ["temp"],
    "value": [23.5],
})
d.insert_observations(frame)
```

```text
{'ok': True, 'rows_inserted': 1}
```

## `IngestDriver.normalize_observations`

```python
IngestDriver.normalize_observations(observations)
```

Converts a dataframe to the form that is sent to the server. `insert_observations` calls it. It keeps the columns `ts`, `stream_name`, `value` and `source_id` (when present), converts `ts` to UTC, converts `value` to text, and drops rows without a time or a stream name.

**Parameters**

- `observations` (`polars.DataFrame` or `None`): as for [`insert_observations`](#ingestdriverinsert_observations).

**Returns**

`polars.DataFrame` with `ts` as `datetime[μs, UTC]`, and `stream_name` and `value` as strings.

**Raises**

- `ValueError`: when `ts`, `stream_name` or `value` is missing.

Note that values are sent as text and the server stores numeric streams as numbers. A column named `ref_name` is accepted as a deprecated name for `stream_name`.

**Example**

```python
d.normalize_observations(frame)
```

```text
shape: (1, 3)
┌─────────────────────────┬─────────────┬───────┐
│ ts                      ┆ stream_name ┆ value │
│ ---                     ┆ ---         ┆ ---   │
│ datetime[μs, UTC]       ┆ str         ┆ str   │
╞═════════════════════════╪═════════════╪═══════╡
│ 2026-01-01 00:02:00 UTC ┆ temp        ┆ 23.5  │
└─────────────────────────┴─────────────┴───────┘
```

## `IngestDriver.to_timestamp`

```python
IngestDriver.to_timestamp(date_or_timestamp, time=None, *, date_format=None,
                          timezone='UTC', day_first=False)
```

Converts a timestamp column to UTC. This is the function [`to_timestamp`](tabular-helpers.md#to_timestamp) as a method, with the same parameters, return value and errors.

## `Driver.reference_uri`

```python
Driver.reference_uri(stream_name)
```

Returns the URI of the stream `stream_name` under `self.source_id`. The URI is computed from the two names and is the same one the server uses.

**Parameters**

- `stream_name` (`str`): name of the stream.

**Returns**

`rdflib.URIRef`.

**Example**

```python
d.reference_uri("temp")
```

```text
urn:acquirium#ad5dd93c-8801-5bbc-a45f-fa2060adc28d
```

## `Driver.insert_graph`

```python
Driver.insert_graph(rdf_graph, *, format='turtle', replace=False)
```

Writes RDF text to the graph owned by `self.source_id`. See [the graph methods of the client](../client/graph.md).

**Parameters**

- `rdf_graph` (`str`): the RDF content as text.
- `format` (`str`): RDF format of the text: `"turtle"`, `"n3"`, `"xml"` or `"trix"`.
- `replace` (`bool`): `True` replaces everything this `source_id` wrote before. `False` adds to it.

**Returns**

`None`.

**Example**

```python
d.insert_graph('<urn:demo:tank> <http://www.w3.org/2000/01/rdf-schema#label> "Tank" .')
```

## `Driver.insert_graph_file`

```python
Driver.insert_graph_file(path, *, format=None, replace=False)
```

Reads an RDF file and writes it to the graph owned by `self.source_id`.

**Parameters**

- `path` (`str` or `Path`): the file to read.
- `format` (`str`, optional): RDF format of the file. When omitted, it is taken from the file extension.
- `replace` (`bool`): `True` replaces everything this `source_id` wrote before. `False` adds to it.

**Returns**

`None`.

**Raises**

- `FileNotFoundError`: when `path` is not a file.
- `ValueError`: when `format` is omitted and the file extension is not a known RDF extension.

## `Driver.sparql_update`

```python
Driver.sparql_update(update)
```

Runs a SPARQL update on the graph owned by `self.source_id`. Other graphs are not changed.

**Parameters**

- `update` (`str`): the SPARQL update text.

**Returns**

`dict`, the reply of the server.

**Example**

```python
d.sparql_update(
    'INSERT DATA { <urn:demo:tank> <http://www.w3.org/2000/01/rdf-schema#comment> "x" }'
)
```

```text
{'ok': True, 'message': 'update applied', 'changed': True}
```

## `Driver.config_dir`

```python
Driver.config_dir()
```

Returns the directory that holds the configuration file. This is useful for resolving relative paths from the configuration. When the driver was started without a configuration file, it returns the current working directory.

**Returns**

`Path`.

## `Driver.data_dir`

```python
Driver.data_dir()
```

Returns the data directory of the server. It is chosen in this order: the environment variable `ACQUIRIUM_DATA_DIR`; the key `[server] data_dir` of the configuration, resolved against `config_dir()` when relative; `.acquirium` inside `config_dir()`.

**Returns**

`Path`.
