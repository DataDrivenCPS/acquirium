# Glossary

This is a reference for the vocabulary of the other guides.

## RDF terms

| term | meaning | example |
|---|---|---|
| URI | the full, globally unique name of a thing; every class, predicate, entity, point and unit has one | `urn:swro/RO`, `http://qudt.org/vocab/unit/PSI` |
| CURIE | a compact URI, written `prefix:local` | `wbs:RO` is `urn:swro/` + `RO` |
| free text | a plain phrase the server resolves to a URI, against one vocabulary (its *kind*: class, predicate, substance, process, unit or quantity kind) | `"ro membrane"` resolves to `nawi:ReverseOsmosisMembrane` |
| literal | a plain value in a triple rather than a name for something; matched verbatim, never resolved | `"SCADA"`, `42.0` |
| prefix | the short name standing in for a namespace | `wbs` |
| namespace | the URI a prefix expands to | `urn:swro/` |
| triple | one statement: subject, predicate, object | `wbs:RO s223:hasConnectionPoint wbs:RO-in` |
| predicate | the middle of a triple; the kind of edge | `s223:hasConnectionPoint` |
| class | a category a node belongs to, via `rdf:type` | `nawi:ReverseOsmosisMembrane` |
| instance | one actual thing of a class | `wbs:RO` |
| ontology | the shared vocabulary the model is written in | s223, NAWI water, QUDT |
| model | the RDF description of one plant | `deployments/WATERTAP/models/seawater-ro/model.ttl` |
| Turtle | the text format RDF is written in (`.ttl` files, graph inserts) | `model.ttl` |
| SPARQL | the query language for RDF graphs; every `Query` compiles to it | `q.to_sparql()` |


## The plant model

| term | meaning | example |
|---|---|---|
| entity | a thing in the plant: equipment, a system, a connection point | `wbs:RO`, `wbs:RO-in` |
| point | a node representing one measured or computed quantity | `wbs:P1-out-pressure` |
| connection point | the inlet or outlet of a piece of equipment; most points hang off one | `wbs:RO-in` |
| reference node | the RDF node linking a point to its stream, through `ref:hasExternalReference` | `urn:acquirium#399ce39c-...` |
| stream | the timeseries behind a point | the rows stored under that reference node |
| source | the owner of a set of streams and of its own graph | a driver, an app, `plant` |
| datasource | who writes a set of streams; the graph-side record of a source | `watertap-seawater-ro` |
| deployment graph | the inferred union of every source's data, which queries run against | — |
| dependencies | the resolved ontology and shape triples queried alongside it | `include_dependencies=True` |
| medium | what flows at a connection point, or what a property is measured in | `nawi:Water-Brine` |
| substance | the constituent a measurement is about | `nawi:Constituent-Salt` |
| quantity kind | what a measurement measures, in QUDT's vocabulary | `qudtqk:MassConcentration` |
| process | the treatment process an entity performs | `nawi:Process-ReverseOsmosis` |
| unit | the QUDT unit a measurement carries | `unit:MilliGM-PER-L` |

### Stream identifiers

A stream carries three identifiers, and the third is computed from the first
two.

| identifier | meaning | example |
|---|---|---|
| `source_id` | who writes the stream | `watertap-seawater-ro` |
| `ref_name` | which series, unique within that source | `P1-out-pressure` |
| `ref_uri` | the canonical URI of the stream | `urn:acquirium#399ce39c-e18d-5ad5-bd5c-9c9d053fe04d` |

## Querying

| term | meaning | example |
|---|---|---|
| query | a description of what you are looking for, built by chaining verbs | `acq.query().entity("pump")` |
| verb | one step in that chain | `entity()`, `related()`, `measurement()` |
| pattern | the whole shape the server matches against the plant | a pump, a tank near it, the pressures on both |
| node | one position in the pattern | the pump position |
| entity node | a node holding equipment, systems or connection points | `entity("pump")` |
| data node | a node holding measurements | `measurement()` |
| alias | the name of a node, used as its column name and as its handle in `frm=`, `target=` and `of=` | `entity("pump", alias="p1")` |
| attribute | a property of a node the interface exposes by name instead of by predicate | `unit`, `medium`, `substance`, `quantity_kind`, `process`, `type`, `cp_type`, `enumeration_kind`, `data_source` |
| pointer | the node the chain is currently on, which is what a bare `where()` or `measurement()` applies to | moved by `refocus()` |
| hidden predicate | an edge that generic traversal never follows, because it describes a node rather than connects the plant | `rdf:type`, `s223:hasProperty`, `s223:hasConnectionPoint`, `s223:cnx` |
| `max_depth` | how many hops a traversal may take | `related("tank", max_depth=1)` |
| `nearest` | keep only the closest match per source instead of every match in range | `related("pump", nearest=False)` |


## Data

| term | meaning | example |
|---|---|---|
| observation | one reading: a timestamp and a value on one stream | `(2026-08-08 05:51:47, 7e6)` |
| `value_kind` | whether a stream was registered as numeric or text | `"numeric"` |
| `value_mode` | which of the two value columns a read returns | `"default"`, `"numeric"`, `"text"`, `"coalesce"` |
| `cast_value` | a client-side cast applied after the values arrive | `"float"`, `"int"` |
| wide | one column per point, aligned on the timestamp | `time, m__wbs:RO-in-pressure, ...` |
| narrow | one row per reading, with its point in its own column | `data_alias, point_uri, time, value_numeric, ...` |


## The platform

| term | meaning | example |
|---|---|---|
| driver | a class the server runs on a schedule to ingest data | `PollingIngestDriver` |
| tick | one scheduled run of a driver | `interval = 10.0` |
| app | a class the server runs to compute on data and write results back | `App` |
| graph store | the embedded Oxigraph store holding the model and the ontologies | `data_dir/.oxigraph` |
| timeseries store | where observations are stored | duckdb (default) or timescale |
| logbook | the event log stored alongside the timeseries | `POST /insert_log` |
