---
title: Streams
---

A stream is one series of timestamped values. It is identified by a `source_id` and a `stream_name`, and Acquirium derives its `stream_uri` from the two. A stream must be registered before values are inserted into it. Registration also links the stream to a property in the graph and records metadata such as the unit.

| Item | Purpose |
|---|---|
| [`Acquirium.register_streams`](#acquiriumregister_streams) | Register streams and their metadata |
| [`Acquirium.stream`](#acquiriumstream) | Return a handle for one stream |
| [`Acquirium.reference_uri`](#acquiriumreference_uri) | The `stream_uri` of a `source_id` and `stream_name` pair |
| [`Stream`](#stream) | Handle for one stream |
| [`Stream.between`](#streambetween) | Select a time range of a stream |
| [`Stream.dataframe`](#streamdataframe) | Read the values of a stream |
| [`StreamInterval`](#streaminterval) | A stream between two times |
| [`StreamInterval.dataframe`](#streamintervaldataframe) | Read the values in the interval |

The examples on this page start from a graph that holds one pump with a flow property:

```python
import acquirium as aq

acq = aq.init()
acq.insert_graph("""
@prefix ex: <urn:ex:> .
@prefix s223: <http://data.ashrae.org/standard223#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:pump-1 a s223:Pump ;
    rdfs:label "Pump 1" ;
    s223:hasProperty ex:pump-1-flow .
ex:pump-1-flow a s223:QuantifiableObservableProperty ;
    rdfs:label "Pump 1 flow" .
ex:tank-1 a s223:Equipment ;
    s223:hasProperty ex:tank-1-temp .
ex:tank-1-temp a s223:QuantifiableObservableProperty .
""", source_id="plant")
```

## `Acquirium.register_streams`

```python
Acquirium.register_streams(streams)
```

Registers one or more streams. Each stream is a dictionary. Registering a stream that already exists adds to what is known about it, so the call can be repeated.

**Parameters**

- `streams` (iterable of `dict`): one dictionary per stream, with the keys below.

Keys of a stream dictionary:

- `source_id` (`str`, required): name of the source the stream belongs to, such as a driver or a file.
- `stream_name` (`str`): name of the stream within its `source_id`. The pair identifies the stream.
- `property_uri` (`str`, optional): URI of the property in the graph that this stream measures. When omitted, Acquirium generates a property for the stream and marks it `is_disconnected`. See [Streams without a property](#streams-without-a-property).
- `value_kind` (`str`, optional): `"numeric"` or `"text"`. When omitted, the kind is inferred from the values: the stream becomes numeric once it receives a numeric value, and is read as text until then.
- `label` (`str`, optional): readable name, written on the property. A generated property without a label gets the `stream_name`. Registering again with a `label` replaces the one this source wrote. Registering again without one keeps it.
- `unit`, `quantity_kind`, `medium`, `substance` (`str` or `URIRef`, optional): a URI is used as given. Plain text such as `"L/min"` is matched to a URI, as [`resolve_property_metadata`](metadata.md#acquiriumresolve_property_metadata) does. Text that matches nothing is skipped with a `UserWarning`.
- `data_source` (`str`, optional): a tag written on the property. Queries can filter on it.
- `metadata` (`dict`, optional): more metadata for the property, with the same rules as [`insert_metadata`](metadata.md#acquiriuminsert_metadata). A key that repeats one of the keys above, such as `unit`, must have the same value.
- `properties` (`dict`, optional): extra graph statements for the stream, as a mapping of predicate URI to value.

The old key `point_uri` is accepted for `property_uri`. The old key `ref_name` is deprecated: it is accepted for `stream_name` with a `FutureWarning`.

A stream registered without `property_uri` holds one `unit`, `quantity_kind`, `medium` and `substance`. Registering it again with another value replaces the old one.

When `property_uri` already has metadata in the graph, the values in the call are compared with it:

- A key the property does not have yet is added.
- A `quantity_kind`, `medium`, `substance` or `data_source` that differs from the property's raises `ValueError`, and nothing is written.
- A `unit` that differs is accepted when the two units can be converted into each other. It is then recorded as the unit the stream's values are stored in, and the property keeps its own unit. A unit that cannot be converted raises `ValueError`.

**Returns**

`None`.

**Raises**

- `ValueError`: when a stream has no `source_id`, or has neither a `stream_name` nor a `property_uri`.
- `ValueError`: when `value_kind` is not `"numeric"` or `"text"`.
- `ValueError`: when a value conflicts with the property's metadata, as described above.
- `ValueError`: when `metadata` and the stream give different values for the same key.
- `ValueError`: when `property_uri` is omitted and the stream is already connected to several properties.
- `TypeError`: when a stream gives both an old key and its new name, such as `ref_name` and `stream_name`.

**Example**

```python
acq.register_streams([
    {"source_id": "lab", "stream_name": "flow",
     "property_uri": "urn:ex:pump-1-flow", "unit": "L/min"},
    {"source_id": "lab", "stream_name": "temp", "unit": "degC",
     "label": "Tank temperature"},
    {"source_id": "lab", "stream_name": "state"},
])
```

A unit that cannot be converted to the property's unit:

```python
acq.register_streams([
    {"source_id": "lab", "stream_name": "flow",
     "property_uri": "urn:ex:pump-1-flow", "unit": "degC"},
])
```

```text
ValueError: unit mismatch for point <urn:ex:pump-1-flow>: the point has <http://qudt.org/vocab/unit/L-PER-MIN>, the stream provided <http://qudt.org/vocab/unit/DEG_C> and they are not convertible
```

### Streams without a property

A stream registered without `property_uri` gets a generated property that is marked `is_disconnected`. The stream can be written and read like any other. A query lists such streams with `is_disconnected=True`:

```python
print(acq.query().measurement().where(is_disconnected=True))
```

```text
Query
  data: measurement, is_disconnected=True
2 matches:
┌─────────────────────────────────────────────────┬──────────────────┐
│ data                                            ┆ data.label       │
│ ---                                             ┆ ---              │
│ str                                             ┆ str              │
╞═════════════════════════════════════════════════╪══════════════════╡
│ acq:956809db-a863-541b-b478-1fed2a810cfa__point ┆ Tank temperature │
│ acq:a3a0305e-b76e-5f59-9b80-2c5794f53af8__point ┆ state            │
└─────────────────────────────────────────────────┴──────────────────┘
```

To connect the stream later, register it again with a `property_uri`:

```python
acq.register_streams([
    {"source_id": "lab", "stream_name": "temp",
     "property_uri": "urn:ex:tank-1-temp"},
])
```

The generated property is removed and its metadata moves to `property_uri`:

- The unit, quantity kind and medium are checked as in any registration with a `property_uri`. A mismatch raises `ValueError` and nothing is written.
- The substance, the data source and the keys given in `metadata` are written where `property_uri` has no value. Where it has a different value, it keeps its own and a warning says so.
- The label moves only when `property_uri` has none.
- A value given in the connecting call wins over the value the generated property had.

In the example, `urn:ex:tank-1-temp` now has the unit `DEG_C` and the label `"Tank temperature"`. Once a stream is connected, registering it again without `property_uri` describes the connected property.

## `Acquirium.stream`

```python
Acquirium.stream(source_id_or_uri, name=None)
```

Returns a [`Stream`](#stream) handle. The call does not contact the server and does not check that the stream exists.

**Parameters**

- `source_id_or_uri` (`str` or `URIRef`): the `source_id` when `name` is given. Otherwise the `stream_uri`.
- `name` (`str`, optional): the `stream_name`.

**Returns**

A [`Stream`](#stream).

**Example**

```python
flow = acq.stream("lab", "flow")
print(flow)
print(acq.stream(flow.stream_uri) == flow)
```

```text
urn:acquirium#9c44aa6f-badd-52bd-9edc-558a98c22c26
True
```

## `Acquirium.reference_uri`

```python
Acquirium.reference_uri(source_id, stream_name)
```

Returns the `stream_uri` of a `source_id` and `stream_name` pair. The URI is computed from the two names, so it is the same on every server and the stream does not have to exist.

**Parameters**

- `source_id` (`str`): name of the source.
- `stream_name` (`str`): name of the stream within the source.

**Returns**

An `rdflib.URIRef`.

**Example**

```python
acq.reference_uri("lab", "flow")
```

```text
rdflib.term.URIRef('urn:acquirium#9c44aa6f-badd-52bd-9edc-558a98c22c26')
```

## `Stream`

```python
acquirium.Stream(stream_uri)
```

A handle for one stream. It holds no data: each read asks the server for the current values. Get one from [`Acquirium.stream`](#acquiriumstream). A `Stream` built directly is not bound to a client and cannot read.

Two handles are equal when their `stream_uri` is equal. `str(stream)` returns the `stream_uri`.

**Attributes**

- `stream_uri` (`str`): the URI of the stream. `ref_uri` is a deprecated name for it and emits a `FutureWarning`.

## `Stream.between`

```python
Stream.between(start, end)
```

Selects the values from `start` to `end`. Both ends are included.

**Parameters**

- `start` (`datetime` or `str`): first time of the range. A string is read as an ISO 8601 timestamp. A value without a time zone is taken as UTC.
- `end` (`datetime` or `str`): last time of the range, with the same rules.

**Returns**

A [`StreamInterval`](#streaminterval).

**Raises**

- `ValueError`: when `end` is before `start`.

**Example**

```python
interval = flow.between("2026-01-01T01:00", "2026-01-01T02:00")
print(repr(interval))
```

```text
<StreamInterval urn:acquirium#9c44aa6f-badd-52bd-9edc-558a98c22c26 [2026-01-01T01:00:00+00:00, 2026-01-01T02:00:00+00:00]>
```

## `Stream.dataframe`

```python
Stream.dataframe(*, start=None, end=None, **options)
```

Reads the values of the stream.

**Parameters**

- `start` (`datetime` or `str`, optional): only values at or after this time. A value without a time zone is taken as UTC.
- `end` (`datetime` or `str`, optional): only values at or before this time.
- `limit` (`int`, optional): largest number of rows to return.
- `order` (`str`, default `"asc"`): `"asc"` for oldest first, `"desc"` for newest first.
- `timeout` (`float`, default `60.0`): seconds to wait for the server.
- `value_mode` (`str`, default `"default"`): which values are returned.
  - `"default"`: the values of the stream's kind. A text value in a numeric stream is returned as null.
  - `"numeric"`: only the numeric values.
  - `"text"`: only the text values.
  - `"coalesce"`: every value, as text.

**Returns**

A Polars `DataFrame` with the columns `time` (UTC) and `value`. `value` is `f64` for a numeric stream and `str` for a text stream. A stream with no values, or one that does not exist, gives an empty frame.

**Raises**

- `RuntimeError`: when the handle is not bound to a client.
- `requests.exceptions.HTTPError`: when the server rejects the request, for example an `order` other than `"asc"` or `"desc"`.

**Example**

```python
from datetime import datetime, timezone

acq.insert_timeseries("lab", "flow", [
    (datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), 1.5),
    (datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc), 2.5),
    (datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc), 3.5),
])
print(flow.dataframe())
print(flow.dataframe(limit=1, order="desc"))
```

```text
shape: (3, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ 1.5   │
│ 2026-01-01 01:00:00 UTC ┆ 2.5   │
│ 2026-01-01 02:00:00 UTC ┆ 3.5   │
└─────────────────────────┴───────┘
shape: (1, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 02:00:00 UTC ┆ 3.5   │
└─────────────────────────┴───────┘
```

## `StreamInterval`

```python
acquirium.StreamInterval(stream, start, end)
```

A stream between two times. Get one from [`Stream.between`](#streambetween). It holds no data: a read returns the values present at the time of the read.

**Attributes**

- `stream` ([`Stream`](#stream)): the stream.
- `start` (`datetime`): first time of the range, in UTC.
- `end` (`datetime`): last time of the range, in UTC.
- `stream_uri` (`str`): the URI of the stream.

**Raises**

- `ValueError`: when `end` is before `start`.

## `StreamInterval.dataframe`

```python
StreamInterval.dataframe(**options)
```

Reads the values in the interval. Both ends are included.

**Parameters**

- `limit`, `order`, `timeout`, `value_mode`: as in [`Stream.dataframe`](#streamdataframe).

**Returns**

A Polars `DataFrame` with the columns `time` and `value`.

**Example**

```python
print(flow.between("2026-01-01T01:00", "2026-01-01T02:00").dataframe())
```

```text
shape: (2, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 01:00:00 UTC ┆ 2.5   │
│ 2026-01-01 02:00:00 UTC ┆ 3.5   │
└─────────────────────────┴───────┘
```
