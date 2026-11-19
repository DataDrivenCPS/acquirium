---
title: Reading results
---

These methods run the query. `metadata`, `metadata_objects` and `resolved_nodes` return the matched nodes. `data` and `dataframe` return the time series of the matched streams.
The examples use the [example setup](_index.md#example-setup).

| Method | Description |
| --- | --- |
| [`Query.metadata`](#querymetadata) | The matches as a table |
| [`Query.metadata_objects`](#querymetadata_objects) | The matches as Python dicts |
| [`Query.resolved_nodes`](#queryresolved_nodes) | The URIs of the matched nodes |
| [`Query.data`](#querydata) | A `DataObject` for the matched streams |
| [`Query.dataframe`](#querydataframe) | The time series of the matched streams as one table |

A `Query` object keeps the result of its first run. Calling `metadata()` again on the same object does not ask the server again. Build a new query to see changes made to the model since then.

Several methods take `include_dependencies`. With `True`, the default, the query also reads the ontologies the model imports. Matching a class through its subclasses needs them.

## `Query.metadata`

```python
Query.metadata(*, include_internals=False, include_dependencies=True,
               drop_empty_cols=False)
```

Runs the query and returns the matches as a table. There is one row per combination of matched nodes and one column per node, named by its alias. Node columns hold CURIEs.
A measurement node also has an `<alias>.label` column when at least one matched property has a label. Columns added with [`Query.include`](columns.md#queryinclude) follow. Repeated rows are removed.

**Parameters**

- `include_internals` (`bool`, default `False`): also return the columns used to fetch data. For a measurement node these are the stream URI (column `<alias>_ref`), the `source_id` (`<alias>_source`), the `stream_name` (`<alias>_stream_name`) and the units of the property and the stream.
- `include_dependencies` (`bool`, default `True`): also read the ontologies the model imports.
- `drop_empty_cols` (`bool`, default `False`): leave out columns that are null on every row. An empty result keeps all its columns.

**Returns**

`polars.DataFrame`. [`Query.schema`](columns.md#queryschema) gives its columns and types without running the query.

**Raises**

- `CastError` (a `ValueError`): when a value cannot be cast to the type asked for with `include(type=...)` and `strict` is `True`.

**Example**

```python
q = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
q.metadata()
```

```text
shape: (2, 3)
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ m                       ┆ m.label             │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power │
└────────┴─────────────────────────┴─────────────────────┘
```

```python
acq.query().entity("Pump", alias="pump").include("all").metadata(drop_empty_cols=True)
```

```text
shape: (3, 5)
┌────────────┬─────────────────────────┬────────────────────────────────┬───────────────────────────────┬────────────────────────┐
│ pump       ┆ pump.label              ┆ pump.product_info.manufacturer ┆ pump.tags                     ┆ pump.product_info.year │
│ ---        ┆ ---                     ┆ ---                            ┆ ---                           ┆ ---                    │
│ str        ┆ str                     ┆ str                            ┆ list[str]                     ┆ i64                    │
╞════════════╪═════════════════════════╪════════════════════════════════╪═══════════════════════════════╪════════════════════════╡
│ wbs:intake ┆ Seawater Onshore Intake ┆ null                           ┆ null                          ┆ null                   │
│ wbs:P2     ┆ Booster Pump P2         ┆ Siemens                        ┆ ["booster"]                   ┆ 2012                   │
│ wbs:P1     ┆ High-pressure Pump P1   ┆ Grundfos                       ┆ ["critical", "high-pressure"] ┆ 2019                   │
└────────────┴─────────────────────────┴────────────────────────────────┴───────────────────────────────┴────────────────────────┘
```

## `Query.metadata_objects`

```python
Query.metadata_objects(*, include_dependencies=True)
```

Runs the query and returns the matches as Python dicts. There is one dict per row of [`Query.metadata`](#querymetadata).
Each node alias maps to a dict with the node's `"id"`, its `"label"` when the row has one, and every included attribute in the shape it was saved: a dict stays a dict and a list stays a list.

The differences from the table are:

- `"id"` and other URI values are full URIs, not CURIEs. `"id"` is the id saved on the node, or the URI of the node when no id is saved.
- A key the node does not have is left out. It is not set to `None`.
- A hidden node is left out.

**Parameters**

- `include_dependencies` (`bool`, default `True`): also read the ontologies the model imports.

**Returns**

`list[dict]`: one dict per row, keyed by node alias.

**Example**

```python
q = acq.query().entity("Pump", alias="pump")
q.include("product_info", "tags").metadata_objects()
```

```text
[{'pump': {'id': 'urn:swro/intake'}},
 {'pump': {'id': 'urn:swro/P2', 'tags': ['booster'], 'product_info': {'manufacturer': 'Siemens', 'year': 2012}}},
 {'pump': {'id': 'urn:swro/P1', 'tags': ['critical', 'high-pressure'], 'product_info': {'manufacturer': 'Grundfos', 'year': 2019}}}]
```

## `Query.resolved_nodes`

```python
Query.resolved_nodes(*, alias=None, only_data_nodes=False, include_dependencies=True)
```

Runs the query and returns the URIs of the matched nodes.

**Parameters**

- `alias` (`str | None`, default `None`): return only the matches of this node. The default is every node of the query.
- `only_data_nodes` (`bool`, default `False`): return only the matches of measurement nodes. It is ignored when `alias` is given.
- `include_dependencies` (`bool`, default `True`): also read the ontologies the model imports.

**Returns**

`list[str]`: full URIs, sorted, without repeats.

**Raises**

- `ValueError`: when `alias` is not an alias of the query.

**Example**

```python
q = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
q.resolved_nodes(alias="pump")
```

```text
['urn:swro/P1', 'urn:swro/P2']
```

```python
q.resolved_nodes(only_data_nodes=True)
```

```text
['urn:swro/P1-mechanical-power', 'urn:swro/P2-mechanical-power']
```

## `Query.data`

```python
Query.data(*, start=None, end=None, limit=None, order="asc",
           include_dependencies=True, cast_value=None, value_mode="default")
```

Runs the query and returns a [`DataObject`](data-object.md) for the streams of its measurement nodes. The values are not fetched yet. They are fetched once, when a method of the `DataObject` first needs them.
A query without a measurement node returns an empty `DataObject`.

**Parameters**

- `start` (`datetime | str | None`, default `None`): the earliest timestamp to fetch, inclusive. A string is an ISO 8601 timestamp. `None` means from the first reading.
- `end` (`datetime | str | None`, default `None`): the latest timestamp to fetch, inclusive. `None` means to the last reading.
- `limit` (`int | None`, default `None`): the largest number of readings to fetch per stream.
- `order` (`str`, default `"asc"`): `"asc"` reads from the oldest reading, `"desc"` from the newest. With `limit` it decides which readings are kept.
- `include_dependencies` (`bool`, default `True`): also read the ontologies the model imports.
- `cast_value` (`str | None`, default `None`): how values are typed.
  - `None`: numeric streams give numbers and text streams give text.
  - `"float"`: text that reads as a number becomes a number. Other text stays text.
  - `"int"`: as `"float"`, and number columns are returned as integers. Fractions are cut off, with a `UserWarning`.
  - `"str"`: every value is returned as text.
- `value_mode` (`str`, default `"default"`): which stored values of a stream are read. `"default"` follows the kind of the stream. `"numeric"` reads only numeric values, `"text"` only text values, and `"coalesce"` both, as text.

**Returns**

[`DataObject`](data-object.md).

**Raises**

- `ValueError`: when `cast_value` is not `"float"`, `"int"`, `"str"` or `None`.

**Example**

```python
q = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
d = q.data(start=start, end=end)
d
```

```text
DataObject(lazy, 2 streams, aliases=['m'], entities=['pump'])
```

## `Query.dataframe`

```python
Query.dataframe(shape="wide", *, start=None, end=None, limit=None, order="asc",
                include_dependencies=True, cast_value=None, value_mode="default",
                include_stream=False, compact=True)
```

Runs the query, fetches the time series of the matched streams and returns them as one table. It is the same as `q.data(...).dataframe(...)`.

**Parameters**

- `shape` (`str`, default `"wide"`): the layout of the table.
  - `"wide"`: a `time` column and one column per property. The column is named `<alias>__<label>`, with the CURIE of the property when it has no label.
  - `"narrow"`: one row per reading, with the columns `data_alias`, `property_id`, `time`, `value_numeric` and `value_text`.
- `start`, `end`, `limit`, `order`, `include_dependencies`, `cast_value`, `value_mode`: as in [`Query.data`](#querydata).
- `include_stream` (`bool`, default `False`): in the narrow layout, add a `stream` column with the stream of each reading. `include_ref=` is the deprecated name of this parameter.
- `compact` (`bool`, default `True`): `True` shows CURIEs and labels. With `False`, the narrow layout has full URIs and these columns: `data_alias`, `property_uri`, `stream_uri`, one `entity__<alias>` column per entity node, `time`, `value_numeric` and `value_text`.

**Returns**

`polars.DataFrame`. Timestamps are in UTC. When no reading is found, the table has no rows and the columns of the narrow layout with full URIs, whatever `shape` is.

**Raises**

- `ValueError`: when `cast_value` is not `"float"`, `"int"`, `"str"` or `None`.

**Example**

```python
q = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
q.dataframe(start=start, end=end).head(3)
```

```text
shape: (3, 3)
┌─────────────────────────┬────────────────────────┬────────────────────────┐
│ time                    ┆ m__P1 mechanical power ┆ m__P2 mechanical power │
│ ---                     ┆ ---                    ┆ ---                    │
│ datetime[μs, UTC]       ┆ f64                    ┆ f64                    │
╞═════════════════════════╪════════════════════════╪════════════════════════╡
│ 2026-01-01 00:00:00 UTC ┆ 1.1718e6               ┆ 102904.298764          │
│ 2026-01-01 00:10:00 UTC ┆ 1.1571e6               ┆ 94544.141316           │
│ 2026-01-01 00:20:00 UTC ┆ 1.1594e6               ┆ 100394.429969          │
└─────────────────────────┴────────────────────────┴────────────────────────┘
```

The newest reading of each stream, one row per reading:

```python
q.dataframe("narrow", limit=1, order="desc", include_stream=True)
```

```text
shape: (2, 6)
┌────────────────────────┬─────────────────────────┬────────────────────────────────┬─────────────────────────┬───────────────┬────────────┐
│ data_alias             ┆ property_id             ┆ stream                         ┆ time                    ┆ value_numeric ┆ value_text │
│ ---                    ┆ ---                     ┆ ---                            ┆ ---                     ┆ ---           ┆ ---        │
│ str                    ┆ str                     ┆ str                            ┆ datetime[μs, UTC]       ┆ f64           ┆ str        │
╞════════════════════════╪═════════════════════════╪════════════════════════════════╪═════════════════════════╪═══════════════╪════════════╡
│ m__P1 mechanical power ┆ wbs:P1-mechanical-power ┆ acq:11ce90ae-35c6-5710-92dd-d3 ┆ 2026-01-01 23:50:00 UTC ┆ 1.2228e6      ┆ null       │
│                        ┆                         ┆ 8360c23c15                     ┆                         ┆               ┆            │
│ m__P2 mechanical power ┆ wbs:P2-mechanical-power ┆ acq:fbe9d5ae-ecfa-5abe-8c2a-90 ┆ 2026-01-01 23:50:00 UTC ┆ 111408.133189 ┆ null       │
│                        ┆                         ┆ 40b11f8f82                     ┆                         ┆               ┆            │
└────────────────────────┴─────────────────────────┴────────────────────────────────┴─────────────────────────┴───────────────┴────────────┘
```
