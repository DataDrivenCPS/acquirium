---
title: "DataObject: values"
---

A `DataObject` holds the time series of the streams a query matched. [`Query.data`](reading.md#querydata) returns it.
It fetches nothing when it is created. The values are fetched once, for the window given to `data()`, the first time a method on this page needs them.
The methods that describe the streams without fetching values are on [DataObject: streams and statistics](data-object-info.md).

The examples use the [example setup](_index.md#example-setup) and this object:

```python
q = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
d = q.data(start=start, end=end)
```

| Name | Description |
| --- | --- |
| [`DataObject.aliases`](#dataobjectaliases) | The data aliases in the object |
| [`DataObject[alias]`](#dataobjectalias) | The time series of one alias |
| [`DataObject.dataframe`](#dataobjectdataframe) | All time series as one table |
| [`DataObject.latest`](#dataobjectlatest) | The newest reading of each property |
| [`DataObject.by`](#dataobjectby) | Split the object by entity |
| [`DataObject.units`](#dataobjectunits) | The units of each alias |
| [`DataObject.convert_to`](#dataobjectconvert_to) | Convert values to another unit |

A data alias is the alias of a measurement node of the query. One alias can cover several properties, and so several streams.

## `DataObject.aliases`

```python
DataObject.aliases
```

The data aliases in the object, sorted. This is a property, not a method. It does not fetch values.

The object also works like a collection of its aliases: `len(d)` is the number of aliases, `alias in d` tests for one, and `for alias in d` goes through them.

**Returns**

`list[str]`.

**Example**

```python
d.aliases
```

```text
['m']
```

```python
len(d), "m" in d, list(d)
```

```text
(1, True, ['m'])
```

## `DataObject[alias]`

```python
DataObject[alias]
```

Returns the time series of one data alias.

**Parameters**

- `alias` (`str`): a data alias of the object.

**Returns**

`polars.DataFrame` with the columns `time`, `value` and `property_uri`, sorted by `time`. There is one row per reading and property. `value` is a number column, or a text column when the alias has text values. An alias with no readings in the window gives a table with no rows.

**Raises**

- `KeyError`: when `alias` is not a data alias of the object.

**Example**

```python
d["m"].head(3)
```

```text
shape: (3, 3)
┌─────────────────────────┬───────────────┬──────────────────────────────┐
│ time                    ┆ value         ┆ property_uri                 │
│ ---                     ┆ ---           ┆ ---                          │
│ datetime[μs, UTC]       ┆ f64           ┆ str                          │
╞═════════════════════════╪═══════════════╪══════════════════════════════╡
│ 2026-01-01 00:00:00 UTC ┆ 1.1718e6      ┆ urn:swro/P1-mechanical-power │
│ 2026-01-01 00:00:00 UTC ┆ 102904.298764 ┆ urn:swro/P2-mechanical-power │
│ 2026-01-01 00:10:00 UTC ┆ 1.1571e6      ┆ urn:swro/P1-mechanical-power │
└─────────────────────────┴───────────────┴──────────────────────────────┘
```

## `DataObject.dataframe`

```python
DataObject.dataframe(shape="wide", *, start=None, end=None, limit=None,
                     order="asc", include_stream=False, compact=True, streams=None)
```

Returns all time series of the object as one table. The parameters shared with [`Query.dataframe`](reading.md#querydataframe) mean the same.
Here `start`, `end`, `limit` and `order` select from the values the object already fetched. They cannot reach outside the window given to `data()`.

**Parameters**

- `shape` (`str`, default `"wide"`): the layout of the table.
  - `"wide"`: a `time` column and one column per property. The column is named `<alias>__<label>`, with the CURIE of the property when it has no label.
  - `"narrow"`: one row per reading, with the columns `data_alias`, `property_id`, `time`, `value_numeric` and `value_text`.
- `start` (`datetime | str | None`, default `None`): keep readings at or after this time.
- `end` (`datetime | str | None`, default `None`): keep readings at or before this time.
- `limit` (`int | None`, default `None`): keep at most this many readings per stream, counted in the direction of `order`.
- `order` (`str`, default `"asc"`): `"asc"` sorts oldest first, `"desc"` newest first.
- `include_stream` (`bool`, default `False`): in the narrow layout, add a `stream` column with the stream of each reading. `include_ref=` is the deprecated name of this parameter.
- `compact` (`bool`, default `True`): `True` shows CURIEs and labels. With `False`, the narrow layout has full URIs and these columns: `data_alias`, `property_uri`, `stream_uri`, one `entity__<alias>` column per entity node, `time`, `value_numeric` and `value_text`.
- `streams` (`int | str | Stream | list | None`, default `None`): keep only these streams. A stream is given by its number in the `stream` column of [`DataObject.stream_info`](data-object-info.md#dataobjectstream_info), its stream URI, or a `Stream` from `acq.stream(source_id, stream_name)`. A list can mix them.

**Returns**

`polars.DataFrame`. When no reading is left, the table has no rows and the columns of the narrow layout with full URIs, whatever `shape` is.

**Raises**

- `KeyError`: when `streams` has a number or URI that is not in the object.
- `TypeError`: when an entry of `streams` is not a number, a string or a `Stream`.

**Example**

```python
d.dataframe().head(3)
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

One stream, by its number:

```python
d.dataframe(streams=[0], limit=2)
```

```text
shape: (2, 2)
┌─────────────────────────┬────────────────────────┐
│ time                    ┆ m__P1 mechanical power │
│ ---                     ┆ ---                    │
│ datetime[μs, UTC]       ┆ f64                    │
╞═════════════════════════╪════════════════════════╡
│ 2026-01-01 00:00:00 UTC ┆ 1.1718e6               │
│ 2026-01-01 00:10:00 UTC ┆ 1.1571e6               │
└─────────────────────────┴────────────────────────┘
```

## `DataObject.latest`

```python
DataObject.latest(alias=None, *, streams=None)
```

Returns the newest reading of each property. It asks the server for one reading per stream, so it does not fetch the whole window.
The `start` and `end` given to `data()` still apply: the result is the newest reading inside them. The `limit` and `order` given to `data()` do not affect it.

**Parameters**

- `alias` (`str | None`, default `None`): return only the properties of this data alias. The default is every alias.
- `streams` (`int | str | Stream | list | None`, default `None`): use only these streams, as in [`DataObject.dataframe`](#dataobjectdataframe).

**Returns**

`polars.DataFrame`. Without `alias` the columns are `data_alias`, `property_uri`, `time` and `value`. With `alias` they are `time`, `value` and `property_uri`.

**Raises**

- `KeyError`: when `alias` is not a data alias of the object, or `streams` has a number or URI that is not in the object.

**Example**

```python
d.latest()
```

```text
shape: (2, 4)
┌────────────┬──────────────────────────────┬─────────────────────────┬───────────────┐
│ data_alias ┆ property_uri                 ┆ time                    ┆ value         │
│ ---        ┆ ---                          ┆ ---                     ┆ ---           │
│ str        ┆ str                          ┆ datetime[μs, UTC]       ┆ f64           │
╞════════════╪══════════════════════════════╪═════════════════════════╪═══════════════╡
│ m          ┆ urn:swro/P1-mechanical-power ┆ 2026-01-01 23:50:00 UTC ┆ 1.2228e6      │
│ m          ┆ urn:swro/P2-mechanical-power ┆ 2026-01-01 23:50:00 UTC ┆ 111408.133189 │
└────────────┴──────────────────────────────┴─────────────────────────┴───────────────┘
```

## `DataObject.by`

```python
DataObject.by(entity_alias)
```

Splits the object by the matches of one entity node. Use it in a `for` loop to handle the data of each entity on its own.

**Parameters**

- `entity_alias` (`str`): the alias of an entity node of the query. [`DataObject.entity_aliases`](data-object-info.md#dataobjectentity_aliases) lists them.

**Returns**

An iterator of `(entity_uri, DataObject)` pairs, sorted by entity URI. Each `DataObject` holds only the streams of that entity.

**Raises**

- `KeyError`: when `entity_alias` is not an entity alias of the object. The error is raised when the loop starts.

**Example**

```python
for uri, sub in d.by("pump"):
    print(uri, sub.aliases, sub.total_rows)
```

```text
urn:swro/P1 ['m'] 144
urn:swro/P2 ['m'] 144
```

## `DataObject.units`

```python
DataObject.units()
```

Returns the units the values of each data alias are in. It does not fetch values.

**Parameters**

None.

**Returns**

`dict[str, list[str | None]]`: each data alias mapped to the unit URIs of its properties, without repeats. `None` stands for a property with no unit.

**Example**

```python
d.units()
```

```text
{'m': ['http://qudt.org/vocab/unit/W']}
```

## `DataObject.convert_to`

```python
DataObject.convert_to(to_unit, *, from_unit=None, alias=None)
```

Returns a new `DataObject` with values converted to another unit. The original object is not changed. Each property is converted from its own unit, so an alias with properties in different units ends up in one unit.

**Parameters**

- `to_unit` (`str`): the target unit. A unit URI, or text such as a label or symbol (`"kW"`).
- `from_unit` (`str | None`, default `None`): the unit the values are in now. The default is the unit of each property. Give it when a property has no unit.
- `alias` (`str | None`, default `None`): convert only this data alias. The default is every alias.

**Returns**

`DataObject`.

**Raises**

- `KeyError`: when `alias` is not a data alias of the object.
- `ValueError`: when `from_unit` is `None` and a property has no unit.
- `ValueError`: when the units cannot be converted into each other, or the unit text cannot be matched.
- `ValueError`: when the object was created with `cast_value="str"`.

**Example**

```python
kw = d.convert_to("kW")
kw.units()
```

```text
{'m': ['http://qudt.org/vocab/unit/KiloW']}
```

```python
kw.dataframe().head(2)
```

```text
shape: (2, 3)
┌─────────────────────────┬────────────────────────┬────────────────────────┐
│ time                    ┆ m__P1 mechanical power ┆ m__P2 mechanical power │
│ ---                     ┆ ---                    ┆ ---                    │
│ datetime[μs, UTC]       ┆ f64                    ┆ f64                    │
╞═════════════════════════╪════════════════════════╪════════════════════════╡
│ 2026-01-01 00:00:00 UTC ┆ 1171.789545            ┆ 102.904299             │
│ 2026-01-01 00:10:00 UTC ┆ 1157.083238            ┆ 94.544141              │
└─────────────────────────┴────────────────────────┴────────────────────────┘
```
