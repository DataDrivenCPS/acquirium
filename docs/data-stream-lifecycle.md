# Data Stream Lifecycle

This document describes how a timeseries data stream is created, registered,
and queried in Acquirium.

## Concepts

| Term | Where it lives | Description |
|---|---|---|
| `point_uri` | RDF graph (Oxigraph) | Semantic identity of the measurement. Typed, labelled, linked to equipment in your ontology. |
| `source_id` | TimescaleDB `streams` table + RDF graph | Identifies the datasource (e.g. `"mybox-system-metrics"`). Scopes `ref_name` so two sources with the same stream name don't collide. |
| `ref_name` | TimescaleDB `streams` table + RDF graph | Source-local stream identifier (e.g. `"cpu_percent"`). Unique within a `source_id`. |
| `handle` | TimescaleDB `timeseries` table (storage key) | Deterministic UUID: `acq:{uuid5(namespace, f"{source_id}:{ref_name}")}` in URI format using Acquirium Namespace. Globally unique, stable, never user-visible. |
| Brick ref node | RDF graph | An anonymous node typed as `ref:TimeseriesReference` hanging off `point_uri` via `ref:hasExternalReference`. Carries `ref:hasTimeseriesId` (the handle), `acquirium:sourceId`, `acquirium:refName`, and `ref:storedAt`. |

`point_uri` and (`source_id`, `ref_name`) are intentionally separate. The
semantic identity of a measurement stays stable even if the datasource is
renamed, replaced, or re-ingested — only the external reference needs updating.

The `handle` is an implementation detail of the storage layer. It exists solely
to avoid key collisions when multiple datasources publish the same `ref_name`.
Users never construct or see it directly.

## Lifecycle

### 1. Register the datasource

```python
aq.register_datasource("mybox-system-metrics")
```

Writes a graph node typed as `acquirium:DataSourceRegistry`. Idempotent —
safe to call on every startup.

### 2. Insert data → rows land in TimescaleDB under the handle

```python
aq.insert_timeseries_batch(
    "mybox-system-metrics", # data source name
    {"cpu_percent": [(ts, 42.0)], "memory_percent": [(ts, 61.3)]}, # ref_name → list of (timestamp, value)
)
```

The handle is computed internally to Acquirium:
```
handle = acq:{uuid5(namespace, "mybox-system-metrics:cpu_percent")}
```

Rows are written to TimescaleDB keyed by this handle. The data exists in
storage but has no semantic context yet — the graph knows nothing about it.

### 3. Register metadata → establishes `point_uri` in the graph

```python
aq.register_stream(
    "urn:host:mybox:cpu_percent",   # point_uri
    label="CPU usage",
    unit="%",
    quantity_kind="dimensionless ratio",
    source_id="mybox-system-metrics",
    ref_name="cpu_percent",
)
```

This inserts RDF triples into Oxigraph following the
[Brick timeseries storage spec](https://docs.brickschema.org/metadata/timeseries-storage.html):

```turtle
<urn:host:mybox:cpu_percent>
    a acquirium:VirtualPoint ;
    rdfs:label "CPU usage" ;
    qudt:hasUnit qudt-unit:PERCENT ;
    ref:hasExternalReference <handle> .

<handle>
    a ref:TimeseriesReference ;
    ref:hasTimeseriesId  <handle> ;        # = handle
    acquirium:sourceId   "mybox-system-metrics" ;
    acquirium:refName    "cpu_percent" ;
    ref:storedAt         <urn:acquirium#timescaledb> .

<urn:acquirium#timescaledb>  a <urn:acquirium#Database> .
```

When this graph is inserted, the server scans for `acquirium:sourceId` /
`acquirium:refName` pairs and populates the `streams` table:

```
streams: handle → (point_uri, source_id, ref_name)
```

This is the bridge that lets reads resolve a semantic URI back to data.

At runtime, app outputs are emitted through a single shared output sink used by
both server-side `AppRunner` execution and external app workers:

- `Output.timeseries(...)` inserts the returned `(timestamp, value)` rows into
  the output stream.
- `Output.event(...)` is serialized as one JSON text value in the event stream.
- `Output.trigger(...)` sends the configured HTTP webhook and does not write a
  timeseries row unless the app also returns a timeseries or event output.

The two execution modes provide different insertion transports (`Manager`
directly in the server, `AcquiriumClient` in the worker), but the output
serialization and trigger rules are intentionally shared.

---

### 4. Query → point_uri resolves to handle, then to rows

When `.data()` is called on a query that resolves to
`urn:host:mybox:cpu_percent`, the server:

1. Looks up `point_uri` in the `streams` table → gets `handle`
2. Fetches rows from TimescaleDB where `point_uri = handle`

```
graph:  <point_uri> --ref:hasExternalReference--> <ref_node>
                                                       |
streams table:  point_uri  ──────────────────────>  handle
                                                       |
timescale:                                    rows keyed by handle
```

If a `point_uri` is not in the `streams` table (e.g. data inserted via the
bulk CSV path), the server falls back to querying TimescaleDB directly by
`point_uri`, preserving backwards compatibility with older ingestion paths.

## Example: system metrics script

The `scripts/publish_system_metrics.py` script follows this pattern:

```
source_id  =  "mybox-system-metrics"
ref_name   =  "cpu_percent"
point_uri  =  "urn:host:mybox:cpu_percent"   (s223:Property in the graph)
handle     =  acq:uuid5(ns, "mybox-system-metrics:cpu_percent")  (TimescaleDB key)
```

1. `register_datasource()` registers the datasource node in the graph.
2. `register_host_graph()` inserts the host as `s223:Computer` with
   `s223:hasProperty` links to each `point_uri`, and attaches a Brick
   `ref:TimeseriesReference` node to each stream.
3. `register_stream_metadata()` resolves and attaches QUDT unit / quantity-kind
   triples to each `point_uri`.
4. `collect()` returns samples keyed by `ref_name`.
5. `insert_timeseries_batch()` computes handles and writes rows to TimescaleDB.

## Why keep `point_uri` and `(source_id, ref_name)` separate?

- **MQTT ingestion**: the MQTT topic is the `ref_name`; the sensor URI in your
  ontology is the `point_uri`. Replace the broker or topic without touching
  the ontology.
- **CSV import**: `ref_name` identifies the column; the `point_uri` is the
  sensor. Re-import from a revised file without changing any semantic triples.
- **Multiple sources, same stream name**: Two loggers both publishing
  `"temperature"` get different handles because they have different
  `source_id`s. No collision, no overwritten data.
- **Stable queries**: application code queries by `point_uri`. The storage
  plumbing (`source_id`, `ref_name`, `handle`) can change without breaking
  any query.
