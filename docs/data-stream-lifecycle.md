# Data Stream Lifecycle

## Summary

This document describes the managed-stream path in Acquirium.

Managed streams use:

- `point_uri` as the semantic point in the RDF graph
- `(source_id, ref_name)` as the source-local stream name
- `compute_handle(source_id, ref_name)` as the canonical external-reference URI
- that canonical external-reference URI as the timeseries storage key

The graph-visible external-reference URI and the storage key are the same.

## Terms

| Term | Location | Meaning |
|---|---|---|
| `point_uri` | RDF graph | Semantic identity of the measurement or output |
| `source_id` | RDF graph, streams table | Datasource namespace |
| `ref_name` | RDF graph, streams table | Source-local stream identifier |
| `ref_uri` | RDF graph, streams table, timeseries table | Canonical external-reference URI computed from `(source_id, ref_name)` |

## Canonical reference URI

For managed streams:

```python
ref_uri = compute_handle(source_id, ref_name)
```

Equivalent helpers:

- in a driver: `self.reference_uri(ref_name)`
- on the client: `aq.reference_uri(source_id, ref_name)`

## Managed graph shape

Minimal pattern:

```turtle
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .

<POINT_URI> ref:hasExternalReference <REF_URI> .

<REF_URI>
    acq:sourceId "SOURCE_ID" ;
    acq:refName "REF_NAME" .
```

Typical pattern:

```turtle
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix qudt: <http://qudt.org/schema/qudt/> .

<urn:host:mybox:cpu_percent>
    a acq:VirtualPoint ;
    rdfs:label "CPU usage" ;
    qudt:hasUnit <http://qudt.org/vocab/unit/PERCENT> ;
    ref:hasExternalReference <urn:acquirium#01234567-89ab-cdef-0123-456789abcdef> .

<urn:acquirium#01234567-89ab-cdef-0123-456789abcdef>
    acq:sourceId "mybox-system-metrics" ;
    acq:refName "cpu_percent" ;
    ref:storedAt <urn:acquirium#TimescaleDB> .
```

Driver- or app-specific provenance metadata is written on the same `ref_uri`.

## Lifecycle

### 1. Register the datasource

```python
aq.register_datasource("mybox-system-metrics")
```

This makes the datasource discoverable in the graph.

### 2. Register stream metadata

```python
aq.register_stream(
    "urn:host:mybox:cpu_percent",
    label="CPU usage",
    unit="%",
    quantity_kind="dimensionless ratio",
    source_id="mybox-system-metrics",
    ref_name="cpu_percent",
)
```

Effects:

- computes `ref_uri = compute_handle("mybox-system-metrics", "cpu_percent")`
- writes `point_uri -> ref:hasExternalReference -> ref_uri`
- writes `acq:sourceId` and `acq:refName` on `ref_uri`
- writes stream metadata on `point_uri`

### 3. Insert timeseries rows

```python
aq.insert_timeseries_batch(
    "mybox-system-metrics",
    {"cpu_percent": [(ts, 42.0)]},
)
```

Effects:

- computes the same `ref_uri`
- writes rows to the timeseries store under `ref_uri`

### 4. Sync point-to-stream mapping

When graph data is inserted, the server scans for:

```turtle
<POINT_URI> ref:hasExternalReference <REF_URI> .
<REF_URI> acq:sourceId "SOURCE_ID" ; acq:refName "REF_NAME" .
```

For each such row, the server:

- recomputes `compute_handle(source_id, ref_name)`
- requires it to equal `ref_uri`
- records `(point_uri, source_id, ref_name, ref_uri)` in the streams table

Non-canonical managed refs are rejected.

### 5. Read timeseries by point or by ref URI

Point-based reads:

1. resolve `point_uri` to `ref_uri` through the streams table
2. fetch rows where the storage key is `ref_uri`

Ref-based reads:

1. use `ref_uri` directly
2. fetch rows where the storage key is `ref_uri`

## Invariants

For managed streams:

- `ref_uri == compute_handle(source_id, ref_name)`
- graph identity and storage identity are the same
- inserts use `ref_name`
- graph links use `ref_uri`
- the server rejects managed refs whose graph URI does not match the canonical value

## Example: system metrics

Example values:

```text
source_id = "mybox-system-metrics"
ref_name  = "cpu_percent"
point_uri = "urn:host:mybox:cpu_percent"
ref_uri   = compute_handle(source_id, ref_name)
```

The driver:

1. registers the datasource
2. inserts host / point topology
3. registers each stream, which creates the canonical `ref_uri`
4. inserts samples keyed by `ref_name`

## Notes

- A point may have multiple external references if it has multiple distinct streams.
- External Postgres/file/MQTT/app-specific metadata may also live on the same `ref_uri`.
- Acquirium does not require `rdf:type ref:TimeseriesReference` for managed streams.
