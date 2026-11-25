---
title: The stream lifecycle
---

In this article we'll describe what happens to a stream inside Acquirium: how it gets its identity, what registering it writes, how its rows are stored and how a query finds them.
This is useful when you write a driver or when inserted data does not show up in a query.

The examples use the plant of [5 - A Full Example](../tutorials/a-full-example.md).

## Stream identity

A stream is identified by its `source_id` (who writes it) and its `stream_name` (which series).
Acquirium computes the `stream_uri` from the two:

```python
import acquirium as aq

acq = aq.init()

acq.reference_uri("csv-data", "P1-mechanical-power")
```
```text
rdflib.term.URIRef('urn:acquirium#d19c4dbc-0ecd-5223-819c-dd7caba3f790')
```

The same pair always gives the same `stream_uri`, on every server.
This is why an insert only needs the `source_id` and the `stream_name`, and why two sources can use the same `stream_name`.

## What registration writes to the graph

`register_streams()` writes the stream into the graph, in the graph that belongs to its `source_id`.
The stream is a node with its two names:

```text
acq:d19c4dbc-0ecd-5223-819c-dd7caba3f790 ref:storedAt acq:TimescaleDB
acq:d19c4dbc-0ecd-5223-819c-dd7caba3f790 acq:refName 'P1-mechanical-power'
acq:d19c4dbc-0ecd-5223-819c-dd7caba3f790 acq:sourceId 'csv-data'
```

**Note:** `acq:TimescaleDB` is the name of the timeseries store in the graph. It is the same when the data is stored in DuckDB.

A stream is always linked to a property, because queries find streams through properties.

### A stream without `property_uri`

When no `property_uri` is given, Acquirium generates a property for the stream.
For instance, after registering `{"source_id": "lab", "stream_name": "feed-temp", "unit": "degC"}` the graph has:

```text
acq:48888c81-b720-5259-82b0-6d3a15ab213d__point qudt:hasUnit unit:DEG_C
acq:48888c81-b720-5259-82b0-6d3a15ab213d__point rdf:type acq:VirtualPoint
acq:48888c81-b720-5259-82b0-6d3a15ab213d__point rdfs:label 'feed-temp'
acq:48888c81-b720-5259-82b0-6d3a15ab213d__point ref:hasExternalReference acq:48888c81-b720-5259-82b0-6d3a15ab213d
acq:48888c81-b720-5259-82b0-6d3a15ab213d__point acq:isDisconnected 'true'
acq:48888c81-b720-5259-82b0-6d3a15ab213d__point attr:unit.raw 'degC'
```

- The label is the `stream_name`, unless a `label` is given.
- The unit and the other metadata of the stream are written on the generated property. `attr:unit.raw` keeps the text as it was given.
- `acq:isDisconnected` marks the property as not part of the plant model. `where(is_disconnected=True)` lists such streams.

### A stream with `property_uri`

When a `property_uri` is given, the link is written on that property of the plant model:

```text
wbs:P1-mechanical-power ref:hasExternalReference acq:d19c4dbc-0ecd-5223-819c-dd7caba3f790
```

The metadata of the call is compared with the metadata the property already has.
A key the property does not have is added, and a different value raises `ValueError` before anything is written.
The unit is the exception: a different unit that can be converted is accepted, and it is recorded on the stream as the unit its values are stored in (see [Units](units.md)).

### Connecting a stream later

A stream registered without `property_uri` can be connected by registering it again with one.
The generated property is removed and its metadata moves to the plant property.
The `stream_uri` and the stored rows do not change, because they only depend on the `source_id` and the `stream_name`.

The rules for the metadata that moves are in the [streams reference](../reference/client/streams.md#streams-without-a-property).

Note that registration only adds.
Registering a connected stream again without `property_uri` keeps the connection.

## Registration and the streams table

The graph is where streams are defined.
For fast checks the server also keeps a `streams` table next to the timeseries data, with one row per stream: `stream_uri`, `property_uri`, `source_id`, `stream_name` and `value_kind`.
The table is filled from the graph after every graph insert, and again when the server starts.

An insert is checked against this table.
This is why a stream has to be registered before its first row:

```python
from datetime import datetime, timezone

acq.insert_timeseries("lab", "nope", [(datetime(2026, 1, 1, tzinfo=timezone.utc), 1.0)])
```
```text
HTTPError: 400 Client Error: Bad Request for url: http://127.0.0.1:63792/insert_timeseries;
response body: stream urn:acquirium#ad8a6aa9-2db9-528d-8928-fcf7fe2d954c is not registered
```

Be aware that the server also checks the `stream_uri` of every stream it finds in the graph.
A stream node written by hand whose URI is not the one computed from its `source_id` and `stream_name` makes the graph insert fail.
Use `register_streams()` instead of writing stream nodes yourself.

## How rows are written

An insert goes through these steps in the server:

- The `stream_uri` of each row is computed from the `source_id` and the `stream_name`, and checked against the `streams` table.
- Each value is stored as a number or as text, by the `value_kind` of the stream.
- Rows are identified by stream and timestamp. A row that already exists is overwritten.
- The rows of one request are written in one transaction.

When the `value_kind` was not given, the stream becomes numeric when it receives its first numeric value.
Until then its values are read as text:

```python
acq.insert_timeseries("lab", "feed-temp", [(datetime(2026, 1, 1, 0, tzinfo=timezone.utc), "n/a")])
acq.insert_timeseries("lab", "feed-temp", [(datetime(2026, 1, 1, 1, tzinfo=timezone.utc), 21.5)])

acq.stream("lab", "feed-temp").dataframe()
```
```text
shape: (2, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ null  │
│ 2026-01-01 01:00:00 UTC ┆ 21.5  │
└─────────────────────────┴───────┘
```

The text value is still stored.
A numeric stream returns it as null, and `value_mode="coalesce"` returns every value as text (see [Values](values.md)).

Since rows are overwritten by stream and timestamp, running an import twice does not duplicate anything.
`replace=True` goes further and makes the stream contain exactly the rows of the call (see [1 - Inserting Data](../tutorials/inserting-data.md#inserting-the-same-data-twice)).

Note that a driver does not call these methods itself.
It calls `declare()` and `add()`, and the registration and the insert are done for it (see [How drivers work](drivers.md)).

## How a query reads them

A query follows the same links in the other direction:

- The query runs on the graph and finds the properties that match, with the `stream_uri` linked to each of them.
- The rows of those streams are read from the timeseries store.
- When the unit of the stream differs from the unit of the property, the values are converted while they are read (see [Units](units.md#automatic-conversion)).

A property without a stream gives metadata but no data.
`acq.query().property(has_stream=False)` lists such properties.

## Streams written by apps

The outputs of an app are streams as well.
The server registers them when the app is deployed, with `derived:<app name>` as the `source_id`, and queries find them like any other stream.
See [Apps](apps.md).

## Next

- [Streams](../reference/client/streams.md) in the client reference has every key of `register_streams()`.
- [The graph store](graph-backend.md) explains which graph a write goes to.
- [Server internals](server-internals.md) describes the timeseries store.
