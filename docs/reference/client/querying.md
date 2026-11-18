---
title: Querying
---

A query finds nodes of the plant model and reads the streams connected to them. The client has three methods that start a query. Each returns a `Query`, and the methods of `Query` are described in the [query reference](../query/_index.md).

| Item | Purpose |
|---|---|
| [`Acquirium.query`](#acquiriumquery) | Start an empty query |
| [`Acquirium.explore`](#acquiriumexplore) | Same as `query` |
| [`Acquirium.find_all_data`](#acquiriumfind_all_data) | Start a query over every property that has a stream |

To read one stream by its `source_id` and `stream_name` without a query, use [`Acquirium.stream`](streams.md#acquiriumstream).

## `Acquirium.query`

```python
Acquirium.query()
```

Returns a new, empty query bound to this client. Nothing is sent to the server until the query is printed or read.

**Returns**

A `Query`. See the [query reference](../query/_index.md).

**Example**

```python
print(acq.query())
```

```text
Query (empty)
```

## `Acquirium.explore`

```python
Acquirium.explore()
```

Another name for [`Acquirium.query`](#acquiriumquery). It returns the same thing.

## `Acquirium.find_all_data`

```python
Acquirium.find_all_data(*, _class=None, uri=None)
```

Returns a query that matches every property that has a stream. It is short for `acq.query().measurement(type=_class, uri=uri)`.

**Parameters**

- `_class` (`str`, optional): keep only the properties of this class, given as a URI or as text.
- `uri` (`str` or `URIRef`, optional): keep only the property with this `property_uri`.

**Returns**

A `Query`.

**Example**

```python
print(acq.find_all_data())
```

```text
Query
  data: measurement
3 matches:
┌─────────────────────────────────────────────────┬──────────────────┐
│ data                                            ┆ data.label       │
│ ---                                             ┆ ---              │
│ str                                             ┆ str              │
╞═════════════════════════════════════════════════╪══════════════════╡
│ ex:tank-1-temp                                  ┆ Tank temperature │
│ ex:pump-1-flow                                  ┆ Pump 1 flow      │
│ acq:a3a0305e-b76e-5f59-9b80-2c5794f53af8__point ┆ state            │
└─────────────────────────────────────────────────┴──────────────────┘
```
