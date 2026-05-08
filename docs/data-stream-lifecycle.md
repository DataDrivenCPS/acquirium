# Acquirium Data Model

Acquirium stores two kinds of things in two different stores:

- **The RDF graph** holds semantics: what a measurement point *is*, what it measures, where it lives in the physical topology, and how it connects to raw data.
- **The timeseries store** (TimescaleDB or DuckDB) holds the raw observations keyed by `ref_uri` and timestamp. Logically this is `(ref_uri, timestamp, value)`, but physically Acquirium stores typed value columns such as `numeric_value` and `text_value`.

These two stores are linked through an *external reference* pattern.

---

## The three identifiers

Every managed stream is described by three identifiers that live at different layers:

### `point_uri`

The semantic URI of a measurement point or computed output. This is what ontologies and application queries talk about. It identifies the *thing being measured*, not the storage location.

Examples:
```
urn:host:mybox:cpu_percent
urn:watertap:pump_1:outlet_pressure
```

`point_uri` lives in the RDF graph. It carries physical meaning: type, unit, quantity kind, medium, connections to other equipment. Applications and SPARQL queries operate on `point_uri` values. It also participates in the ontology, be that [ASHRAE 223](https://open223.info), [WaTr Ontology](https://watermetadata.org) or [Brick](https://brickschema.org).

### `(source_id, ref_name)`

The driver's natural name for a stream. `source_id` identifies the data source (e.g. a sensor network, a file, an MQTT broker, a simulation model) presented through a software process which delivers that data to Acquirium. `ref_name` is the source-local stream identifier: a column name, sensor tag, MQTT topic, etc. Together they form a globally unique, human-readable address for the stream. `source_id`s are unique to an Acquirium instance, but `ref_name`s only need to be unique with respect to the `source_id`.

Examples:
```
source_id = "mybox-system-metrics",  ref_name = "cpu_percent"
source_id = "plant-historian",       ref_name = "TI-101"
source_id = "watertap",              ref_name = "pump_1.outlet.pressure"
```

This is what drivers work with. Drivers do not need to know `point_uri` values to insert data. They only need to know their `source_id` and their source-local `ref_name`.

### `ref_uri` (the canonical reference URI)

A deterministic UUID5 URI minted by Acquirium from `(source_id, ref_name)`:

```python
# from acquirium/internals/models.py
ref_uri = compute_ref_uri(source_id, ref_name)
```

This URI serves double duty:

1. **Graph node**: it is the object of `ref:hasExternalReference` on the `point_uri`, and carries `acq:sourceId` and `acq:refName` predicates so the mapping can be reconstructed from the graph alone.
2. **Storage key**: it is the value stored in the `ref_uri` column of the timeseries table. All writes and reads go through this key.

Because `ref_uri` is derived deterministically from `(source_id, ref_name)`, drivers, the server, and the graph all agree on the same value without any coordination. A driver that computes `ref_uri` offline will get the same key as the server. A graph that was inserted before the first data row arrives will already contain the correct `ref_uri`. The UUID5 construction means two sources with the same `ref_name` can never produce the same `ref_uri`.

There is no separate `handle` concept. Older notes and code used `handle` for this same value; the current model calls it `ref_uri` everywhere because it is both the external-reference URI and the physical timeseries storage key.

---

## The streams table

The timeseries store maintains a `streams` table that records the mapping:

```
ref_uri  →  (point_uri, source_id, ref_name, value_kind)
```

This table is populated three ways:

- **On stream registration** (`register_stream` / `register_streams`): the client writes the graph triple `point_uri → ref:hasExternalReference → ref_uri` when `point_uri` is known, and the server's `_sync_stream_refs_from_graph` method scans for these triples and upserts them into the streams table.
- **On data insert** (`insert_timeseries`, `insert_timeseries_batch`, `insert_timeseries_polars`): the server computes `ref_uri` from `(source_id, ref_name)` and upserts a streams row even when no `point_uri` is known yet. In that case `streams.point_uri` is `NULL`.
- **On graph insert**: any time RDF is inserted, the server re-scans the graph for managed reference patterns.

The streams table lets the server answer: *given a `point_uri`, what storage key should I read from?* Without it, reading by `point_uri` would require a SPARQL query on every data request.

The table also records streams discovered only from data insertion. Those rows have a `ref_uri`, `source_id`, and `ref_name`, with `point_uri = NULL` until a semantic graph link is inserted later.

`value_kind` records the stream-level storage type. Drivers declare it as `"numeric"` or `"text"` when registering streams or inserting observations. It defaults to `"text"` when omitted. Numeric telemetry is stored in `timeseries.numeric_value`; text/log-like samples are stored in `timeseries.text_value`; the other value column is normally `NULL`.

Numeric streams can still contain occasional nonnumeric rows. If a value in a numeric stream cannot be converted to a float, Acquirium stores that row in `timeseries.text_value` rather than rejecting the whole insert. Read APIs expose `value_mode` to choose default behavior, numeric-only rows, text-only rows, or a coalesced mixed stream. See [`data-api.md`](data-api.md) for the read modes and examples.

---

## The graph shape

The RDF graph for a managed stream looks like this:

```turtle
@prefix acq:  <urn:acquirium#> .
@prefix ref:  <https://brickschema.org/schema/Brick/ref#> .
@prefix qudt: <http://qudt.org/schema/qudt/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

# The semantic point — what is being measured
<urn:host:mybox:cpu_percent>
    a acq:VirtualPoint ;
    rdfs:label "CPU usage" ;
    qudt:hasUnit      <http://qudt.org/vocab/unit/PERCENT> ;
    qudt:hasQuantityKind <http://qudt.org/vocab/quantitykind/DimensionlessRatio> ;
    ref:hasExternalReference <urn:acquirium#01234567-89ab-cdef-0123-456789abcdef> .

# The external reference node — links semantics to storage
<urn:acquirium#01234567-89ab-cdef-0123-456789abcdef>
    acq:sourceId "mybox-system-metrics" ;
    acq:refName  "cpu_percent" ;
    acq:valueKind "numeric" ;
    ref:storedAt <urn:acquirium#TimescaleDB> .

# The datasource node — makes the source discoverable
<urn:acquirium:datasource:mybox-system-metrics>
    a acq:DataSourceRegistry ;
    rdfs:label "mybox-system-metrics" .
```

The `ref_uri` node (the UUID5 URI) is intentionally thin. It is an indirection node, not a description of the measurement itself. Semantic metadata goes on `point_uri`; provenance and routing metadata goes on `ref_uri`.

---

## How data flows in

### Step 1 — register the datasource

```python
aq.register_datasource("mybox-system-metrics")
```

Writes the `acq:DataSourceRegistry` node to the graph. This is a discovery mechanism — it makes the source visible to SPARQL queries that enumerate known sources. Safe to call on every startup; the graph write is idempotent.

### Step 2 — declare stream metadata

`point_uri` is **optional**. Drivers that have a meaningful semantic URI for the stream should provide it; drivers that only know their source-local identity can omit it.

**With a `point_uri`** — creates the semantic point node, links it to the ref node, and writes any metadata (unit, quantity kind, label) on the point:

```python
aq.register_stream(
    "urn:host:mybox:cpu_percent",       # optional — omit if unknown
    label="CPU usage",
    unit="%",                           # resolved to a QUDT URI via server
    quantity_kind="dimensionless ratio", # resolved to a QUDT URI via server
    source_id="mybox-system-metrics",
    ref_name="cpu_percent",
    value_kind="numeric",
)
```

**Without a `point_uri`** — writes only the external reference node. The stream is immediately usable for data insertion; a semantic point URI can be linked later by inserting an RDF graph that declares `<point_uri> ref:hasExternalReference <ref_uri>`:

```python
aq.register_stream(
    source_id="mybox-system-metrics",
    ref_name="cpu_percent",
    value_kind="numeric",
)
```

Or in bulk, which is preferred when a driver discovers many streams at once (e.g. columns in a CSV file):

```python
aq.register_streams([
    {"source_id": "mybox-system-metrics", "ref_name": "cpu_percent", "value_kind": "numeric"},
    {"source_id": "mybox-system-metrics", "ref_name": "memory_percent", "value_kind": "numeric"},
    # point_uri is optional per entry
    {"point_uri": "urn:host:mybox:disk", "source_id": "mybox-system-metrics", "ref_name": "disk_percent", "value_kind": "numeric"},
])
```

Registration:
- writes the external reference node with `acq:sourceId`, `acq:refName`, `acq:valueKind`, and `ref:storedAt`
- if `point_uri` is provided, also creates the point node and links it to the ref node
- resolves plain-text unit/quantity_kind strings to QUDT URIs via the server's embedding matcher
- triggers `_sync_stream_refs_from_graph`, which upserts the mapping into the streams table

Stream registration is purely a metadata operation. No timeseries rows are written.

### Step 3 — insert data

```python
aq.insert_timeseries_batch(
    "mybox-system-metrics",
    {
        "cpu_percent":    [(ts, 42.1)],
        "memory_percent": [(ts, 71.3)],
        "disk_percent":   [(ts, 18.5)],
    },
)
```

The server:
1. computes `ref_uri = compute_ref_uri(source_id, ref_name)` for each `ref_name` key
2. uses the explicit stream-level `value_kind`, defaulting to `"text"`
3. builds a Polars DataFrame with `(ref_uri, timestamp, value, value_kind)` rows
4. bulk-inserts into typed timeseries columns via the Arrow bridge
5. upserts each stream into the `streams` table with `point_uri = NULL` if no semantic point has been registered yet

The driver never touches `ref_uri` directly. The mapping from `ref_name` to storage key is entirely internal.

For large batches, the `Acquirium` client facade splits the input into `insert_batch_rows`-sized chunks and issues multiple requests. Drivers do not need to chunk manually.

---

## Reading data back

Reads go by either `point_uri` or `ref_uri`:

**By `point_uri`**: the server looks up `ref_uri` in the streams table, then queries the timeseries store for rows where `timeseries.ref_uri = ref_uri`.

**By `ref_uri`** (direct): used internally and by API consumers that already know the canonical reference URI.

---

## Logs use `point_uri`

Logs are intentionally different from managed timeseries rows.

The `logs` table is keyed by `point_uri`, not `ref_uri`, because logs describe semantic equipment, points, alarms, observations, and app outputs. A log entry is about the physical or modeled thing in the ontology, not about a particular storage stream. That means:

- timeseries samples are stored by `ref_uri`
- logs are stored by `point_uri`
- the graph links a `point_uri` to one or more `ref_uri` values with `ref:hasExternalReference`

This keeps logs stable even if a point gets multiple external references over time.

---

## Driver-specific metadata on reference nodes

Each driver type adds its own provenance triples to the `ref_uri` node. This records how the data was collected and what the source format looked like, so a consumer reading the graph can understand the origin of a stream.

### Tabular drivers (CSV, XLSX)

Written by `_TabularIngestBase._stream_registration_properties`:

```turtle
<ref_uri>
    a ref:FileReference ;
    acq:dataSource "CSV" ;
    ref:fileLocation "subdir/data.csv" ;   # relative to watch_dir
    ref:timeColumnID "Date" ;
    ref:valueColumnID "Temperature" .
```

### MQTT driver

Written when `register_stream` is called from `_sync_subscriptions`:

```turtle
<ref_uri>
    acq:sourceId "my_mqtt_data" ;
    acq:refName  "sensors/room1/temp" .
```

The broker, topic, and payload-key configuration lives on separate `ref:MQTTReference` nodes declared in the user's graph, not on the managed `ref_uri`. The MQTT driver reads those nodes via SPARQL and subscribes accordingly.

### WaterTAP driver

The user-supplied RDF model file declares both `ref:hasExternalReference` (used to identify the managed stream) and `acq:hasPyomoVar` (used to extract the value from the solved WaterTAP model):

```turtle
<urn:watertap:pump1:outlet_pressure>
    ref:hasExternalReference <ref_uri> ;
    acq:hasPyomoVar "m.fs.pump.outlet.pressure[0]" .
```

The driver reads these triples at startup and uses them to map Pyomo variable paths to `ref_name` values for insertion.

### Soft-sensor apps (computed outputs)

App outputs get the same managed-stream treatment, but the reference node also carries type and dependency metadata:

```turtle
<ref_uri>
    a acq:Stream, acq:TimeseriesStream ;
    acq:sourceId "my-app" ;
    acq:refName  "urn:output:predicted_flow" ;
    acq:storageBackend "timescale" .
```

The `acq:produces` and `acq:isCalculatedFrom` triples on the output point URI record the app that generated the stream and the inputs it depended on.

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

## The streams table vs the graph

A natural question is why there is both a `streams` table in the timeseries store *and* `acq:sourceId`/`acq:refName` triples in the graph. They appear to contain the same information.

They serve different roles:

| | RDF graph | streams table |
|---|---|---|
| **Purpose** | semantic description and discovery | fast lookup for data reads and writes |
| **Query interface** | SPARQL | SQL key lookup |
| **Updated by** | `register_stream`, `insert_graph` | `_sync_stream_refs_from_graph` and data insertion |
| **Authoritative for** | meaning, metadata, topology | storage key resolution |

The graph is authoritative for semantic meaning. The streams table is a fast lookup table and can also contain source-local streams that do not have a semantic `point_uri` yet. When the graph does contain a managed reference, `_sync_stream_refs_from_graph` re-derives that row from the graph, and the server rejects any managed reference node whose URI does not match the canonical `compute_ref_uri` value for its `(source_id, ref_name)` pair.

---

## External references vs managed streams

Not all references are live read targets. The `ref:hasExternalReference` pattern can also record provenance for driver-managed streams:

- **File references**: `a ref:FileReference` with `ref:fileLocation`, written by file-based drivers as stream provenance.
- **MQTT references**: `a ref:MQTTReference` with `ref:MQTTBroker` and `ref:MQTTTopic`, queried by the MQTT driver.
- **Database references**: connection/table/query metadata belongs in a driver configuration; the driver ingests rows into managed streams.

The distinction between a managed stream and a provenance-only external reference is structural: managed streams have `acq:sourceId` and `acq:refName` on the reference node.

---

## Summary

| Identifier | Where it lives | Who creates it | What it means |
|---|---|---|---|
| `point_uri` | RDF graph | user / driver | semantic identity of a measurement point |
| `source_id` | RDF graph, streams table, driver config | driver | datasource namespace |
| `ref_name` | RDF graph, streams table, driver | driver | source-local stream name |
| `ref_uri` | RDF graph, streams table, timeseries store | Acquirium (deterministic) | graph + storage identity, derived from `(source_id, ref_name)` |

Data enters through `(source_id, ref_name)`. The system resolves that to `ref_uri` for storage. Applications query by `point_uri`. The graph connects all three.
