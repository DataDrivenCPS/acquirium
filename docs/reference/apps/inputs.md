---
title: Inputs
---

`transform(inputs, output, context)` receives its data in `inputs` and a description of the call in `context`. `inputs` is a dictionary with one `StreamSet` for each alias of the query. `context` is an `InputBatch`. The function `align` turns `inputs` into one table with a shared time column.

| Item | Purpose |
|---|---|
| [`StreamSet`](#streamset) | The data and streams of one query alias |
| [`StreamSet.df`](#streamsetdf) | The data as a dataframe |
| [`StreamSet.collect`](#streamsetcollect) | The data as a PyArrow table |
| [`StreamSet.batches`](#streamsetbatches) | The data in pieces |
| [`StreamSet.in_unit`](#streamsetin_unit) | The data converted to one unit |
| [`StreamDescriptor`](#streamdescriptor) | Description of one input stream |
| [`InputBatch`](#inputbatch) | The `context` argument: matched rows and time windows |
| [`TimeWindow`](#timewindow) | A span of time with a start and an end |
| [`align`](#align) | Resample all inputs onto one clock |

The outputs on this page come from two streams of the `source_id` `demo`: `temp`, labelled "Tank temperature" with unit degrees Celsius, and `flow`, without a label or unit.

## `StreamSet`

```python
inputs[alias]
```

The values and the streams of one query alias for one call. The server creates it. It cannot be changed.

**Attributes**

| Attribute | Type | Meaning |
|---|---|---|
| `alias` | `str` | The query alias |
| `streams` | `tuple[StreamDescriptor, ...]` | The streams of this alias in this call. See [`StreamDescriptor`](#streamdescriptor) |
| `stream` | `StreamDescriptor` | The one stream of this alias. Raises `ValueError` when the alias holds a number of streams other than one, which is the usual case with `grouping = "all_matches"` |
| `window` | [`TimeWindow`](#timewindow) | The span of time that was read |
| `changes` | `pyarrow.Table` | The rows that changed since the previous call, with the same columns as the data |
| `every` | `timedelta` or `None` | The `every` setting of the app |

The data has one row per value and three columns:

| Column | Type | Meaning |
|---|---|---|
| `stream_uri` | string | The stream the value belongs to |
| `time` | timestamp, microseconds, UTC | Time of the value |
| `value` | 64-bit float or string | The value |

**Example**

With `grouping = "per_match"`:

```python
def transform(self, inputs, output, context):
    temp = inputs["input"]
    print(temp.stream.label, temp.stream.unit)
```

```text
Tank temperature http://qudt.org/vocab/unit/DEG_C
```

## `StreamSet.df`

```python
StreamSet.df(library='polars')
```

Returns the data as a dataframe.

**Parameters**

- `library` (`str`): `"polars"` or `"pandas"`.

**Returns**

`polars.DataFrame` or `pandas.DataFrame` with the columns `stream_uri`, `time` and `value`.

**Raises**

- `ValueError`: when `library` is another value.

**Example**

```python
inputs["input"].df()
```

```text
shape: (4, 3)
┌─────────────────────────────────┬─────────────────────────┬───────┐
│ stream_uri                      ┆ time                    ┆ value │
│ ---                             ┆ ---                     ┆ ---   │
│ str                             ┆ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════════════╪═════════════════════════╪═══════╡
│ urn:acquirium#ad5dd93c-8801-5b… ┆ 2026-01-01 00:00:00 UTC ┆ 21.5  │
│ urn:acquirium#ad5dd93c-8801-5b… ┆ 2026-01-01 00:01:00 UTC ┆ 22.5  │
│ urn:acquirium#c640b603-534f-5e… ┆ 2026-01-01 00:00:30 UTC ┆ 3.0   │
│ urn:acquirium#c640b603-534f-5e… ┆ 2026-01-01 00:01:30 UTC ┆ 4.0   │
└─────────────────────────────────┴─────────────────────────┴───────┘
```

## `StreamSet.collect`

```python
StreamSet.collect()
```

Returns the data as a PyArrow table.

**Returns**

`pyarrow.Table` with the columns `stream_uri`, `time` and `value`.

## `StreamSet.batches`

```python
StreamSet.batches()
```

Iterates over the data in pieces of at most 65,536 rows. The data is already in memory; this only splits it.

**Returns**

An iterator of `pyarrow.RecordBatch`.

## `StreamSet.in_unit`

```python
StreamSet.in_unit(unit)
```

Returns a copy with every value converted to `unit`. Each stream is converted from its own unit, so an alias that mixes Celsius and Fahrenheit streams comes out in one unit. The `changes` table is converted too.

**Parameters**

- `unit` (`str`): the target unit, as a URI.

**Returns**

`StreamSet`. Its streams report `unit` as their unit.

**Raises**

- `ValueError`: when a stream of the alias has no unit.
- An error is also raised when a stream's unit cannot be converted to `unit`, for example from a temperature to a pressure.
- `TypeError`: when the values are not numeric.

**Example**

```python
inputs["input"].in_unit("http://qudt.org/vocab/unit/DEG_F").df()
```

```text
shape: (2, 3)
┌─────────────────────────────────┬─────────────────────────┬───────┐
│ stream_uri                      ┆ time                    ┆ value │
│ ---                             ┆ ---                     ┆ ---   │
│ str                             ┆ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════════════╪═════════════════════════╪═══════╡
│ urn:acquirium#ad5dd93c-8801-5b… ┆ 2026-01-01 00:00:00 UTC ┆ 70.7  │
│ urn:acquirium#ad5dd93c-8801-5b… ┆ 2026-01-01 00:01:00 UTC ┆ 72.5  │
└─────────────────────────────────┴─────────────────────────┴───────┘
```

## `StreamDescriptor`

```python
StreamDescriptor(stream_uri, property_uri=None, label=None, value_kind='numeric',
                 unit=None, quantity_kind=None, medium=None, substance=None,
                 properties=<factory>)
```

Description of one input stream. Found in `StreamSet.streams` and `StreamSet.stream`. The server creates it.

**Attributes**

| Attribute | Type | Meaning |
|---|---|---|
| `stream_uri` | `str` | URI of the stream. The same value as in the `stream_uri` column of the data |
| `property_uri` | `str` or `None` | URI of the property the stream measures |
| `label` | `str` or `None` | Label of the property |
| `unit` | `str` or `None` | Unit of the stream, as a URI |

Note that the remaining fields (`value_kind`, `quantity_kind`, `medium`, `substance`, `properties`) are not filled in for inputs. They keep their defaults, so `value_kind` reads `"numeric"` for a text stream too. Check the type of the `value` column to tell the two apart.

The old names `ref_uri` and `point_uri` are accepted for `stream_uri` and `property_uri`; `ref_uri` is deprecated.

**Example**

```python
inputs["input"].streams[0]
```

```text
StreamDescriptor(stream_uri='urn:acquirium#ad5dd93c-8801-5bbc-a45f-fa2060adc28d', property_uri='urn:acquirium#ad5dd93c-8801-5bbc-a45f-fa2060adc28d__point', label='Tank temperature', value_kind='numeric', unit='http://qudt.org/vocab/unit/DEG_C', quantity_kind=None, medium=None, substance=None, properties={})
```

## `InputBatch`

```python
context
```

The `context` argument of `transform`. It says which match of the query the call is for and which spans of time it covers. The server creates it. It cannot be changed.

**Attributes**

| Attribute | Type | Meaning |
|---|---|---|
| `result` | `polars.DataFrame` | Every match of the query, one row per match. The same table in every call of the app |
| `row` | mapping | The match this call is for. Available with `grouping = "per_match"`, and with `"all_matches"` when the query has exactly one match. Otherwise reading it raises `ValueError` |
| `changed_window` | [`TimeWindow`](#timewindow) | Span of the input times that changed |
| `read_window` | [`TimeWindow`](#timewindow) | Span of input times that was read |
| `output_window` | [`TimeWindow`](#timewindow) | Span of output times this call replaces |
| `full_reset` | `bool` | `True` when the call rebuilds the outputs from the start because an input stream was reset |
| `binding_signature` | `str` | Identifier of this combination of app, inputs and outputs |
| `graph_revision`, `from_revision`, `to_revision` | `int` | Version counters, for diagnosis |

The three windows are explained in [Time windows of a call](app-class.md#time-windows-of-a-call).

The columns of `result` and the keys of `row` follow the aliases of the query:

| Column | Meaning |
|---|---|
| `<alias>` | URI of the matched node. For an alias with a stream, the URI of its property |
| `<alias>_ref` | URI of the stream of that alias |
| `<alias>.label` | Label of the property |
| `<alias>.unit` | Unit, as a URI |

**Example**

```python
def transform(self, inputs, output, context):
    print(dict(context.row))
```

```text
{'input': 'urn:acquirium#ad5dd93c-8801-5bbc-a45f-fa2060adc28d__point', 'input_ref': 'urn:acquirium#ad5dd93c-8801-5bbc-a45f-fa2060adc28d', 'input.label': 'Tank temperature', 'input.unit': 'http://qudt.org/vocab/unit/DEG_C'}
```

## `TimeWindow`

```python
TimeWindow(start, end)
```

A span of time. Both ends are part of the span. Import it from `acquirium.Materialization`.

**Parameters**

- `start` (`datetime`): first instant of the span.
- `end` (`datetime`): last instant of the span.

A datetime without a time zone is read as UTC. Both values are stored in UTC and are available as the attributes `start` and `end`.

**Raises**

- `ValueError`: when `end` is before `start`.

**Example**

```python
context.output_window.start, context.output_window.end
```

## `align`

```python
align(inputs, every=None, *, aggregate='mean')
```

Resamples every input stream into time buckets of the same size and returns one wide table. This is useful for combining streams that were recorded at different times. It is available as `aq.align`.

**Parameters**

- `inputs` (`dict[str, StreamSet]`): the `inputs` argument of `transform`, or a part of it.
- `every` (duration, optional): bucket size, as a `timedelta` or a string such as `"1m"`. Defaults to the `every` setting of the app. When the app sets `every`, a value given here must be equal to it.
- `aggregate` (`str`): how the values of one bucket are combined: `"mean"`, `"min"`, `"max"`, `"sum"`, `"first"`, `"last"`, `"median"` or `"count"`.

**Returns**

`polars.DataFrame` with a `time` column that holds the start of each bucket, and one column per stream. An alias with one stream gives a column named after the alias. An alias with several streams gives columns named `alias[label]`, with the stream URI in place of the label when a stream has no label. A bucket in which a stream has no value holds null. Buckets in which no stream has a value are left out.

**Raises**

- `ValueError`: when the app does not set `every`. In a deployed app the bucket size must be declared on the class so that whole buckets are read.
- `ValueError`: when `every` differs from the `every` setting of the app, or is not greater than zero.
- `ValueError`: when `aggregate` is not one of the listed names.

**Example**

In an app with `every = "1m"` and `grouping = "all_matches"`:

```python
aq.align(inputs)
```

```text
shape: (2, 3)
┌─────────────────────────┬─────────────────────────┬─────────────┐
│ time                    ┆ input[Tank temperature] ┆ input[flow] │
│ ---                     ┆ ---                     ┆ ---         │
│ datetime[μs, UTC]       ┆ f64                     ┆ f64         │
╞═════════════════════════╪═════════════════════════╪═════════════╡
│ 2026-01-01 00:00:00 UTC ┆ 21.5                    ┆ 3.0         │
│ 2026-01-01 00:01:00 UTC ┆ 22.5                    ┆ 4.0         │
└─────────────────────────┴─────────────────────────┴─────────────┘
```

With `grouping = "per_match"` and the alias `temp`, the column is named `temp`. This output is from another stream:

```text
shape: (2, 2)
┌─────────────────────────┬──────┐
│ time                    ┆ temp │
│ ---                     ┆ ---  │
│ datetime[μs, UTC]       ┆ f64  │
╞═════════════════════════╪══════╡
│ 2026-01-01 00:00:00 UTC ┆ 21.5 │
│ 2026-01-01 00:01:00 UTC ┆ 24.5 │
└─────────────────────────┴──────┘
```
