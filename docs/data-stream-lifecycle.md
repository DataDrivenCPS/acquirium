# Acquirium Data Model

Acquirium stores two kinds of things in two different stores:

- **The RDF graph** holds semantics: what a measurement point *is*, what it measures, where it lives in the physical topology, and how it connects to raw data.
- **The timeseries store** (TimescaleDB or DuckDB) holds the raw observations: a table of `(storage_key, timestamp, value)` triples.

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
handle = uuid.uuid5(_HANDLE_NAMESPACE, f"{source_id}:{ref_name}")
ref_uri = URIRef(f"urn:acquirium#{handle}")
```

This URI serves double duty:

1. **Graph node**: it is the object of `ref:hasExternalReference` on the `point_uri`, and carries `acq:sourceId` and `acq:refName` predicates so the mapping can be reconstructed from the graph alone.
2. **Storage key**: it is the value stored in the `point_uri` column of the timeseries table. All writes and reads go through this key.

Because `ref_uri` is derived deterministically from `(source_id, ref_name)`, drivers, the server, and the graph all agree on the same value without any coordination. A driver that computes `ref_uri` offline will get the same key as the server. A graph that was inserted before the first data row arrives will already contain the correct `ref_uri`. The UUID5 construction means two sources with the same `ref_name` can never produce the same `ref_uri`.

---

## The streams table

The timeseries store maintains a `streams` table that records the mapping:

```
handle (= ref_uri)  →  (point_uri, source_id, ref_name)
```

This table is populated two ways:

- **On stream registration** (`register_stream` / `register_streams`): the client writes the graph triple `point_uri → ref:hasExternalReference → ref_uri` and the server's `_sync_stream_handles_from_graph` method scans for these triples and upserts them into the streams table.
- **On graph insert**: any time RDF is inserted, the server re-scans the graph for managed reference patterns.

The streams table lets the server answer: *given a `point_uri`, what storage key should I read from?* Without it, reading by `point_uri` would require a SPARQL query on every data request.

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
)
```

**Without a `point_uri`** — writes only the external reference node. The stream is immediately usable for data insertion; a semantic point URI can be linked later by inserting an RDF graph that declares `<point_uri> ref:hasExternalReference <ref_uri>`:

```python
aq.register_stream(
    source_id="mybox-system-metrics",
    ref_name="cpu_percent",
)
```

Or in bulk, which is preferred when a driver discovers many streams at once (e.g. columns in a CSV file):

```python
aq.register_streams([
    {"source_id": "mybox-system-metrics", "ref_name": "cpu_percent"},
    {"source_id": "mybox-system-metrics", "ref_name": "memory_percent"},
    # point_uri is optional per entry
    {"point_uri": "urn:host:mybox:disk", "source_id": "mybox-system-metrics", "ref_name": "disk_percent"},
])
```

Registration:
- writes the external reference node with `acq:sourceId`, `acq:refName`, and `ref:storedAt`
- if `point_uri` is provided, also creates the point node and links it to the ref node
- resolves plain-text unit/quantity_kind strings to QUDT URIs via the server's embedding matcher
- triggers `_sync_stream_handles_from_graph`, which upserts the mapping into the streams table

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
1. computes `ref_uri = compute_handle(source_id, ref_name)` for each `ref_name` key
2. builds a Polars DataFrame with `(ref_uri, timestamp, value)` rows
3. bulk-inserts into the timeseries store via the Arrow bridge

The driver never touches `ref_uri` directly. The mapping from `ref_name` to storage key is entirely internal.

For large batches, the `Acquirium` client facade splits the input into `insert_batch_rows`-sized chunks and issues multiple requests. Drivers do not need to chunk manually.

---

## Reading data back

Reads go by either `point_uri` or `ref_uri`:

**By `point_uri`**: the server looks up `ref_uri` in the streams table, then queries the timeseries store for rows where `storage_key = ref_uri`.

**By `ref_uri`** (direct): used internally and by API consumers that already know the canonical reference URI.

For external Postgres historians, the lookup takes a different path: the server checks whether the `point_uri` is registered as a `PGReferenceInfo` and, if so, queries the external database directly using the DSN and table/query stored on the reference node.

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

---

## The streams table vs the graph

A natural question is why there is both a `streams` table in the timeseries store *and* `acq:sourceId`/`acq:refName` triples in the graph. They appear to contain the same information.

They serve different roles:

| | RDF graph | streams table |
|---|---|---|
| **Purpose** | semantic description and discovery | fast lookup for data reads and writes |
| **Query interface** | SPARQL | SQL key lookup |
| **Updated by** | `register_stream`, `insert_graph` | `_sync_stream_handles_from_graph` (triggered by graph inserts) |
| **Authoritative for** | meaning, metadata, topology | storage key resolution |

The graph is authoritative. The streams table is a derived cache. If they disagree, the graph wins — `_sync_stream_handles_from_graph` re-derives the table from the graph on every graph mutation, and the server rejects any managed reference node whose URI does not match the canonical `compute_handle` value for its `(source_id, ref_name)` pair.

---

## External references vs managed streams

Not all streams are managed by Acquirium. The `ref:hasExternalReference` pattern also covers:

- **External Postgres historians**: `ref:storedAt` is a literal DSN string (`postgresql://...`). The server detects this and routes reads to `PGReferenceRegistry`.
- **File references**: `a ref:FileReference` with `ref:fileLocation`, used by the file-upload ingestion endpoint.
- **MQTT references**: `a ref:MQTTReference` with `ref:MQTTBroker` and `ref:MQTTTopic`, queried by the MQTT driver.

The distinction between a managed stream and an external reference is structural: managed streams have `acq:sourceId` and `acq:refName` on the reference node; external references do not.

---

## Summary

| Identifier | Where it lives | Who creates it | What it means |
|---|---|---|---|
| `point_uri` | RDF graph | user / driver | semantic identity of a measurement point |
| `source_id` | RDF graph, streams table, driver config | driver | datasource namespace |
| `ref_name` | RDF graph, streams table, driver | driver | source-local stream name |
| `ref_uri` | RDF graph, streams table, timeseries store | Acquirium (deterministic) | graph + storage identity, derived from `(source_id, ref_name)` |

Data enters through `(source_id, ref_name)`. The system resolves that to `ref_uri` for storage. Applications query by `point_uri`. The graph connects all three.
