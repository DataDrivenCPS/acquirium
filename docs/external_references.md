# External References

## Summary

Acquirium uses the object of `ref:hasExternalReference` as the canonical stream
identifier in the RDF graph and in the timeseries store.

For Acquirium-managed streams:

- the reference URI is computed from `(source_id, ref_name)`
- the reference node stores `acq:sourceId` and `acq:refName`
- driver- or app-specific source metadata is attached to that same node
- timeseries rows are stored under that reference URI

Acquirium does not require the node to be typed as `ref:TimeseriesReference`.
Acquirium also does not maintain a separate `handle`; the old handle idea is now
just `ref_uri`.

## Terms

`point_uri`
: RDF node for the semantic point, property, or output stream.

`ref_name`
: Source-local stream name within one datasource. Examples: MQTT topic alias,
  sensor tag, file column key, derived output name.

`source_id`
: Datasource namespace. Examples: `mqtt`, `watertap`, `host-a-system-metrics`.

`ref_uri`
: Canonical external-reference URI computed from `(source_id, ref_name)`. This
  is the value stored in `timeseries.ref_uri` and `streams.ref_uri`.

`handle`
: Obsolete name for `ref_uri`. Do not add new APIs, tables, or docs that treat
  it as a separate identifier.

## Canonical URI

Canonical reference URIs are computed deterministically:

```python
ref_uri = compute_ref_uri(source_id, ref_name)
```

Equivalent helpers:

- inside a driver: `self.reference_uri(ref_name)`
- outside a driver: `aq.reference_uri(source_id, ref_name)`

## Required graph pattern

Minimal managed-stream shape:

```turtle
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .

<POINT_URI> ref:hasExternalReference <REF_URI> .

<REF_URI>
    acq:sourceId "SOURCE_ID" ;
    acq:refName "REF_NAME" .
```

Recommended managed-stream shape:

```turtle
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .

<urn:point:temp>
    ref:hasExternalReference <urn:acquirium#01234567-89ab-cdef-0123-456789abcdef> .

<urn:acquirium#01234567-89ab-cdef-0123-456789abcdef>
    acq:sourceId "mqtt" ;
    acq:refName "temp-room-1" ;
    ref:storedAt <urn:acquirium#TimescaleDB> ;
    ref:MQTTBroker "broker.local:1883" ;
    ref:MQTTTopic "plant/temp/room1" .
```

## Invariants

For Acquirium-managed streams, the following are expected to hold:

- `point_uri -> ref:hasExternalReference -> ref_uri`
- `ref_uri == compute_ref_uri(source_id, ref_name)`
- `ref_uri` has `acq:sourceId`
- `ref_uri` has `acq:refName`
- inserts for `(source_id, ref_name)` resolve to the same `ref_uri`

## Storage semantics

Writers insert rows by `ref_name`, not by `ref_uri`:

```python
aq.insert_timeseries_batch(source_id, {
    ref_name: rows,
})
```

Acquirium computes `compute_ref_uri(source_id, ref_name)` internally and stores
the rows under that URI in the `timeseries.ref_uri` column.

Implications:

- the graph uses `ref_uri`
- inserts use `ref_name`
- the deterministic mapping keeps graph identity and storage identity aligned
- the `streams` table is upserted on insert, even if there is no semantic
  `point_uri` yet; those rows have `point_uri = NULL`

## Driver responsibilities

Drivers are responsible for:

- choosing `ref_name`
- computing `ref_uri` when they need to write RDF graph references
- writing `ref:hasExternalReference`
- writing `acq:sourceId`
- writing `acq:refName`
- attaching source/provenance metadata to the same `ref_uri`

Drivers should not mint ad hoc reference URIs when the stream is intended to be
managed by Acquirium.

## Driver metadata

The reference node is the location for source-specific metadata.

Typical examples:

- MQTT:
  - `ref:MQTTBroker`
  - `ref:MQTTTopic`
  - `acq:timeKey`
  - `acq:valueKey`
- file-based ingestion:
  - `ref:fileLocation`
  - `ref:timeColumnID`
  - `ref:valueColumnID`
- external Postgres:
  - `ref:storedAt` as a DSN literal
  - `acq:timeseriesTable`
  - `acq:timeseriesQuery`
  - `acq:timeseriesTimeColumn`
  - `acq:timeseriesValueColumn`
- apps / derived streams:
  - storage backend
  - lineage predicates
  - app-specific metadata

## Relationship between Acquirium and the data source

The reference node is a stream identifier first and a provenance/configuration
carrier second.

Operationally:

- Acquirium uses the reference URI as the stream key
- the driver or app uses the same node to describe where the stream comes from
  or how it is produced

Acquirium does not maintain a separate hidden stream identifier for managed
streams.

## Single-node model

Acquirium uses one node per managed stream for:

- graph identity
- storage identity
- provenance/configuration metadata

This is the intended model for built-in drivers and apps.

## Timeseries vs logs

Timeseries samples and logs intentionally use different identifiers:

- managed timeseries samples are stored by `ref_uri`
- `streams.ref_uri` maps the storage key back to `(source_id, ref_name)` and,
  when known, `point_uri`
- logs are stored by `point_uri`

Logs use `point_uri` because they describe the semantic point, equipment,
alarm, or app output in the ontology. A point may have multiple external
references over time, but the log remains about the point.

## Recognition rules used by Acquirium

Acquirium recognizes a managed stream by:

- presence of `ref:hasExternalReference`
- presence of `acq:sourceId` on the reference node
- presence of `acq:refName` on the reference node

Acquirium does not require:

- `rdf:type ref:TimeseriesReference`
- `ref:hasTimeseriesId`

## Driver example

```python
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF

from acquirium import Driver
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    HAS_EXTERNAL_REFERENCE,
    MQTT_BROKER,
    MQTT_REFERENCE,
    MQTT_TOPIC,
)

class ExampleDriver(Driver):
    def setup(self):
        self._source_id = "mqtt"
        self.aq.register_datasource(self._source_id)

        point_uri = URIRef("urn:point:temp")
        ref_name = "temp-room-1"
        ref_uri = self.reference_uri(ref_name)

        g = Graph()
        g.add((point_uri, HAS_EXTERNAL_REFERENCE, ref_uri))
        g.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(self.source_id())))
        g.add((ref_uri, ACQUIRIUM_REF_NAME, Literal(ref_name)))
        g.add((ref_uri, RDF.type, MQTT_REFERENCE))
        g.add((ref_uri, MQTT_BROKER, Literal("broker.local:1883")))
        g.add((ref_uri, MQTT_TOPIC, Literal("plant/temp/room1")))
        self.aq.insert_graph(g.serialize(format="turtle"), format="turtle", replace=False)

    def loop(self):
        rows = [...]
        self.aq.insert_timeseries_batch(self.source_id(), {
            "temp-room-1": rows,
        })
```

## Notes

- A point may have multiple external references if it has multiple distinct
  streams.
- For managed streams, the reference URI should still follow the canonical
  `(source_id, ref_name)` mapping.
- For non-managed references such as external Postgres readers, Acquirium may
  use the same predicates but not store the data locally.
