# Data Stream Lifecycle

This document describes how a timeseries data stream is created, registered,
and queried in Acquirium.

## Concepts

| Term | Where it lives | Description |
|---|---|---|
| `point_uri` | RDF graph (Oxigraph) | Semantic identity of the measurement. This is the URI your ontology knows about — typed, labelled, linked to equipment. |
| `ref_uri` | TimescaleDB + RDF graph | Storage key. Data rows are written and read using this URI. Also appears in the graph as the object of `acquirium:hasExternalReference`. |
| External reference node | RDF graph | The `ref_uri` itself, typed as `acquirium:TimeseriesStream`. Describes *where* the data lives. |

`point_uri` and `ref_uri` are intentionally separate. The semantic identity of
a measurement can remain stable even if the backing store, ingestion method, or
data source changes — only the external reference needs updating.

## Lifecycle

### 1. Push data → establishes `ref_uri` in TimescaleDB

```
POST /insert_timeseries_batch
{ "urn:host:mybox:cpu_percent:acquirium-ref": [["2024-01-01T00:00:00Z", 42.0]] }
```

This writes a row to TimescaleDB keyed by the `ref_uri`.  At this point the
data exists in storage but has no semantic context — the graph knows nothing
about it yet.

### 2. Register metadata → establishes `point_uri` in the graph

```python
aq.register_stream(
    "urn:host:mybox:cpu_percent",          # point_uri
    label="CPU usage",
    unit="%",
    external_reference="urn:host:mybox:cpu_percent:acquirium-ref",  # ref_uri
)
```

This inserts RDF triples into Oxigraph:

```turtle
<urn:host:mybox:cpu_percent>
    a acquirium:VirtualPoint ;
    rdfs:label "CPU usage" ;
    qudt:hasUnit qudt-unit:PERCENT ;
    acquirium:hasExternalReference <urn:host:mybox:cpu_percent:acquirium-ref> .

<urn:host:mybox:cpu_percent:acquirium-ref>
    a acquirium:Stream, acquirium:TimeseriesStream ;
    acquirium:storageBackend "timescale" .
```

The `ref_uri` is the bridge: it is the graph node *and* the TimescaleDB key.

### 3. Query → graph traversal joins to TimescaleDB

When `.data()` is called on a query that resolves to `urn:host:mybox:cpu_percent`,
the query layer:

1. Finds `point_uri` via SPARQL against the graph
2. Follows `acquirium:hasExternalReference` to get `ref_uri`
3. Fetches rows from TimescaleDB where `point_uri = ref_uri`

```
graph:  <point_uri> --hasExternalReference--> <ref_uri>
                                                  |
timescale:                             rows keyed by ref_uri
```

## Example: system metrics script

The `scripts/publish_system_metrics.py` script follows this pattern:

```
urn:host:mybox:cpu_percent              ← point_uri  (graph, s223:Property)
urn:host:mybox:cpu_percent:acquirium-ref ← ref_uri   (TimescaleDB key + graph node)
```

1. `register_host_graph()` inserts the host as `s223:Computer` with
   `s223:hasProperty` links to each `point_uri`, and wires each
   `point_uri → hasExternalReference → ref_uri`.
2. `register_stream_metadata()` resolves and attaches QUDT unit/quantity-kind
   triples to each `point_uri`.
3. `collect()` returns data keyed by `ref_uri` — the TimescaleDB storage key.
4. `insert_timeseries_batch()` writes rows to TimescaleDB under those keys.

## When `point_uri` and `ref_uri` can differ

Keeping them separate pays off when the data source changes:

- **MQTT ingestion**: `ref_uri` is the MQTT reference node carrying broker/topic.
  The `point_uri` (the sensor in your ontology) never changes.
- **CSV import**: `ref_uri` is a `acquirium:CSVReference` node with a file path.
  Reimport from a new file by updating the reference, not the ontology.
- **Direct push**: For simple cases (like this script) they can share a base name
  but should still be distinct URIs so the graph's external reference pattern
  remains consistent and queryable.
