---
title: "DataObject: streams and statistics"
---

These members of `DataObject` describe the properties and streams behind the object. None of them fetches the time series values.
The methods that return values are on [DataObject: values](data-object.md).

The examples use the [example setup](_index.md#example-setup) and this object:

```python
q = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
d = q.data(start=start, end=end)
```

| Name | Description |
| --- | --- |
| [`DataObject.metadata`](#dataobjectmetadata) | One row per matched property |
| [`DataObject.stream_info`](#dataobjectstream_info) | One row per stream |
| [`DataObject.entity_aliases`](#dataobjectentity_aliases) | The entity aliases of the query |
| [`DataObject.is_empty`](#dataobjectis_empty) | Whether the object has any rows |
| [`DataObject.total_rows`](#dataobjecttotal_rows) | Number of stored rows |
| [`DataObject.time_range`](#dataobjecttime_range) | Earliest and latest stored timestamps |
| [`DataObject.bindings`](#dataobjectbindings) | One record per matched stream |
| [`DataObject.refresh_stats`](#dataobjectrefresh_stats) | Forget the cached statistics |

`total_rows`, `time_range` and `bindings` read stream statistics from the server. The statistics are fetched once, on first use, for every stream of the `data()` call. The objects returned by `by()` and `convert_to()` share them.
The statistics describe everything stored in the streams. They do not depend on the `start` and `end` given to `data()`.

## `DataObject.metadata`

```python
DataObject.metadata(*, include_stream_uris=False)
```

Returns one row per matched property and entity. It lists every matched property, including those with no readings in the window.

**Parameters**

- `include_stream_uris` (`bool`, default `False`): add the columns `stream_uri`, `source_id` and `stream_name`, with one row per stream. `include_ref_uris=` is the deprecated name of this parameter.

**Returns**

`polars.DataFrame` with the columns `data_alias`, `property_label`, `property_uri`, `unit` and one `entity__<alias>` column per entity node of the query. All values are text, and URIs are full URIs. `unit` is the unit the values are in, including a conversion asked for with `convert_to()`. It is null for a property with no unit.

**Example**

```python
d.metadata()
```

```text
shape: (2, 5)
┌────────────┬─────────────────────┬──────────────────────────────┬──────────────────────────────┬──────────────┐
│ data_alias ┆ property_label      ┆ property_uri                 ┆ unit                         ┆ entity__pump │
│ ---        ┆ ---                 ┆ ---                          ┆ ---                          ┆ ---          │
│ str        ┆ str                 ┆ str                          ┆ str                          ┆ str          │
╞════════════╪═════════════════════╪══════════════════════════════╪══════════════════════════════╪══════════════╡
│ m          ┆ P1 mechanical power ┆ urn:swro/P1-mechanical-power ┆ http://qudt.org/vocab/unit/W ┆ urn:swro/P1  │
│ m          ┆ P2 mechanical power ┆ urn:swro/P2-mechanical-power ┆ http://qudt.org/vocab/unit/W ┆ urn:swro/P2  │
└────────────┴─────────────────────┴──────────────────────────────┴──────────────────────────────┴──────────────┘
```

## `DataObject.stream_info`

```python
DataObject.stream_info(alias=None)
```

Returns one row per stream behind the object.

**Parameters**

- `alias` (`str | None`, default `None`): list only the streams of this data alias. The stream numbers stay the same as in the full table.

**Returns**

`polars.DataFrame` with the columns `stream`, `alias`, `property_uri`, `stream_uri`, `source_id` and `stream_name`. `stream` is a number that identifies the stream within this object. Pass it as `streams=` to [`DataObject.dataframe`](data-object.md#dataobjectdataframe) or [`DataObject.latest`](data-object.md#dataobjectlatest).

**Raises**

- `KeyError`: when `alias` is not a data alias of the object.

**Example**

```python
d.stream_info()
```

```text
shape: (2, 6)
┌────────┬───────┬──────────────────────────────┬────────────────────────────────────────────────────┬────────────┬─────────────────────┐
│ stream ┆ alias ┆ property_uri                 ┆ stream_uri                                         ┆ source_id  ┆ stream_name         │
│ ---    ┆ ---   ┆ ---                          ┆ ---                                                ┆ ---        ┆ ---                 │
│ u32    ┆ str   ┆ str                          ┆ str                                                ┆ str        ┆ str                 │
╞════════╪═══════╪══════════════════════════════╪════════════════════════════════════════════════════╪════════════╪═════════════════════╡
│ 0      ┆ m     ┆ urn:swro/P1-mechanical-power ┆ urn:acquirium#11ce90ae-35c6-5710-92dd-d38360c23c15 ┆ plant-data ┆ P1-mechanical-power │
│ 1      ┆ m     ┆ urn:swro/P2-mechanical-power ┆ urn:acquirium#fbe9d5ae-ecfa-5abe-8c2a-9040b11f8f82 ┆ plant-data ┆ P2-mechanical-power │
└────────┴───────┴──────────────────────────────┴────────────────────────────────────────────────────┴────────────┴─────────────────────┘
```

## `DataObject.entity_aliases`

```python
DataObject.entity_aliases
```

The aliases of the entity nodes of the query. This is a property, not a method. These are the names [`DataObject.by`](data-object.md#dataobjectby) accepts.

**Returns**

`list[str]`.

**Example**

```python
d.entity_aliases
```

```text
['pump']
```

## `DataObject.is_empty`

```python
DataObject.is_empty()
```

Reports whether the object has no rows. The answer depends on whether the values were fetched:

- Before the values are fetched, it reports whether the matched streams store no rows at all. The window given to `data()` is not considered.
- After the values are fetched, it reports whether the fetched window has no rows.

**Parameters**

None.

**Returns**

`bool`.

**Example**

```python
d.is_empty()
```

```text
False
```

## `DataObject.total_rows`

```python
DataObject.total_rows
```

The number of rows stored in the matched streams. This is a property, not a method.

**Returns**

`int`.

**Example**

```python
d.total_rows
```

```text
288
```

## `DataObject.time_range`

```python
DataObject.time_range
```

The earliest and the latest timestamp stored in the matched streams. This is a property, not a method.

**Returns**

`tuple[datetime | None, datetime | None]`: `(earliest, latest)`. Both are `None` when the streams store no rows.

**Example**

```python
d.time_range
```

```text
(datetime.datetime(2026, 1, 1, 0, 0, tzinfo=TzInfo(0)), datetime.datetime(2026, 1, 1, 23, 50, tzinfo=TzInfo(0)))
```

## `DataObject.bindings`

```python
DataObject.bindings
```

One `BindingInfo` record per matched stream, with its statistics. This is a property, not a method.

**Returns**

`list[BindingInfo]`. A `BindingInfo` has these fields:

- `alias` (`str`): the data alias.
- `property_uri` (`str`): the URI of the property.
- `stream_uri` (`str`): the URI of the stream.
- `source_id` (`str | None`) and `stream_name` (`str | None`): the names the stream was registered with.
- `property_label` (`str | None`): the label of the property.
- `property_unit` (`str | None`): the unit URI of the property.
- `stream_unit` (`str | None`): the unit URI of the stream, when it was registered with one.
- `entity_contexts` (`list[dict]`): the entities the property was matched with, as `{"entity__<alias>": uri}`.
- `row_count` (`int`), `earliest` (`datetime | None`), `latest` (`datetime | None`): the statistics of the stream.
- `nid` (`int`): the number of the measurement node in the query.

**Example**

```python
d.bindings[0]
```

```text
BindingInfo(nid=1, property_uri='urn:swro/P2-mechanical-power', stream_uri='urn:acquirium#fbe9d5ae-ecfa-5abe-8c2a-9040b11f8f82', alias='m', entity_contexts=[{'entity__pump': 'urn:swro/P2'}], row_count=144, earliest=datetime.datetime(2026, 1, 1, 0, 0, tzinfo=TzInfo(0)), latest=datetime.datetime(2026, 1, 1, 23, 50, tzinfo=TzInfo(0)), property_unit='http://qudt.org/vocab/unit/W', stream_unit=None, property_label='P2 mechanical power', source_id='plant-data', stream_name='P2-mechanical-power')
```

## `DataObject.refresh_stats`

```python
DataObject.refresh_stats()
```

Forgets the cached statistics. The next read of `total_rows`, `time_range`, `bindings` or `is_empty()` asks the server again. Use it after new data was inserted.
The objects returned by `by()` and `convert_to()` share the cache, so they are refreshed too.

**Parameters**

None.

**Returns**

`None`.

**Example**

```python
d.refresh_stats()
d.total_rows
```

```text
288
```
