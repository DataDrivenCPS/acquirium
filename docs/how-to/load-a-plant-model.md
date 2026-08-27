---
title: Load a plant model
---

A fresh server starts empty: it knows the ontologies, but nothing about your
plant. This guide loads the semantic model (the equipment, the piping, the
points and their meaning) so queries have something to match against.

Do this once per deployment, before pointing any driver at the server.
Writing timeseries into that model is [inserting data](write-data.md); having a
driver contribute its own fragment is [your first driver](../tutorials/first-driver.md).

## The model file

A plant model is an RDF document, normally Turtle, describing the plant with
the ASHRAE 223 and NAWI water ontologies. The seawater-ro model shipped with
the repo is a worked example:

```turtle
@prefix nawi: <urn:nawi-water-ontology#> .
@prefix s223: <http://data.ashrae.org/standard223#> .
@prefix wbs:  <urn:swro/> .

wbs: a owl:Ontology ;
    owl:imports nawi: .

wbs:P1  a s223:Pump ;
    s223:hasConnectionPoint wbs:P1-out .

wbs:P1-out-pressure  a s223:QuantifiableObservableProperty ;
    qudt:hasQuantityKind qudtqk:Pressure .
```

Two things matter for querying to work later.

Declare the imports. `owl:imports nawi:` is what pulls the water ontology's
class hierarchy into the query view, and that hierarchy is why
`entity("pump")` matches `wbs:P1` without the file ever saying the word
"equipment".

Give every instance a stable URI. Those URIs are what drivers bind their
streams to, so renaming one later orphans its data.

## Load it

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)

acq.insert_graph_file(
    "deployments/WATERTAP/models/seawater-ro/model.ttl",
    source_id="plant",
)
```

The format is taken from the file extension; pass `format=` (`turtle`, `n3`,
`xml`, `trig`, `nquads`) when the extension does not say. `insert_graph()`
takes the RDF as a string instead, for a model you built in memory.

### source_id decides who owns it

`source_id` is required on every graph write, and it is an ownership
boundary. The reserved `plant` source is the shared model that everything else
hangs off. A driver or app writes under its own source ID instead, and cannot
touch `plant` — its `self.insert_graph()` helper supplies its own owner and
takes no `source_id` argument.

Use `plant` for the model that describes the physical plant. Use a source ID
for RDF that belongs to one component, such as a driver contributing the
points it happens to produce.

### replace= decides whether you are adding or replacing

**`replace` defaults to `True` on `insert_graph_file()` and
`insert_graph()`**, which clears the `plant` graph before inserting. That is
what you want when reloading a corrected model, and not what you want when
adding a second building to an existing one:

```python
acq.insert_graph_file("plant-a.ttl", source_id="plant")                 # replaces
acq.insert_graph_file("plant-b.ttl", source_id="plant", replace=False)  # appends
```

Either way the blast radius is one owner's graph. `replace=True` on a driver's
source never touches the plant model, and vice versa.

## Check that it landed

The call returns nothing on success and raises on a parse or validation
failure, so the first check is that it returned at all. Then ask the server
what it now knows:

```python
acq.query().entity("Equipment").metadata()
```
```text
shape: (50, 1)
┌─────────────────────────────────┐
│ Equipment                       │
╞═════════════════════════════════╡
│ wbs:PXR-efficiency-sensor       │
│ wbs:RO-membrane-area-sensor     │
│ wbs:storage-tank-3-out-flow-ra… │
│ wbs:PXR                         │
│ …                               │
└─────────────────────────────────┘
```

`Equipment` is subclass-closed, so this catches the sensors too. Narrow it
with `options("type")` to see which classes the model actually uses.

If that comes back empty, work down the [debugging
guide](debug-an-empty-query.md); an empty result right after a load usually
means the file parsed but declared no `owl:imports`, so nothing matched the
ontology class you asked for.

`facets()` on a fresh model is the fastest way to see what vocabulary it
actually uses:

```python
acq.query().measurement().facets()
```

Two more server-side checks:

```python
acq.graph_version()    # 23   — advances on every write
acq.graph_status()     # {'source_version': 23, 'published_version': 23,
                       #  'is_current': True, 'rebuild_in_progress': False}
```

A write makes the derived query cache stale, and the server rebuilds it in the
background. `is_current: False` with `rebuild_in_progress: True` means
inference is still running and queries are answering from the previous
complete version; see [the graph backend](../explanation/graph-backend.md#when-derived-data-is-refreshed).

## Validate it against the shapes

`validate_graph()` runs the ontologies' SHACL shapes over everything loaded:

```python
report = acq.validate_graph()
report["conforms"]        # False
print(report["results_text"])
```
```text
Validation Report
Conforms: False
Results (364):
Constraint Violation in ClassConstraintComponent
  Severity: sh:Violation
  Focus Node: <urn:swro/intake-in-tds-concentration-sensor>
  Value: <urn:swro/intake-in-tds-concentration-sensor>
  Message: Value <urn:swro/intake-in-tds-concentration-sensor> is not an
           instance of class <http://data.ashrae.org/standard223#Property>
```

`report["report"]` is the same thing as a Turtle SHACL report, for feeding to
another tool.

Be aware that `conforms: False` is not a reason to stop. Acquirium queries the
model whether or not it satisfies every s223 shape, and the shipped
seawater-ro model does not. Treat the report as a review of modelling quality:
a violation on a node you care about is worth fixing, and one on an ontology
term you never query is noise.

## Load order

Nothing enforces an order, but this one avoids avoidable churn:

1. **Ontologies first**, if you have extensions of your own. They go in
   `[ontologies] sources`, not through `insert_graph`; see the
   [server configuration](../reference/server-config.md#the-ontologies-section).
2. **The plant model**, as above, in one call.
3. **Drivers**, once the points they bind to exist. A driver that declares a
   `point_uri` the graph already has gets its metadata checked against that
   point; a driver started first mints placeholder points instead, and those
   do not become the real ones later.

Prefer one insert over a loop of small ones. Every `insert_graph` request
waits for a fresh inferred view so stream registrations can be synced against
it, so twenty small inserts pay for twenty inference passes.

## Updating a model that is already loaded

Reload the whole file with the default `replace=True`. It is the same call as
the first load, and it is scoped to the `plant` graph, so driver and app
graphs survive it.

For a targeted edit, `sparql_update()` avoids a full reload:

```python
acq.sparql_update(
    'INSERT DATA { <urn:swro/P1> <http://www.w3.org/2000/01/rdf-schema#label> "Feed pump 1" }',
    source_id="plant",
)
```

Note that a point's `rdfs:label` is what result columns display in place of
its CURIE, so labels are worth adding even when nothing else changes.

Existing timeseries are untouched by any of this. Rows live in the timeseries
store keyed by `ref_uri`, and the graph only says which streams to read; a
reload that keeps the point URIs keeps the data reachable. A reload that
renames a point leaves its rows stored but unreachable through that point, as
described in [the stream lifecycle](../explanation/stream-lifecycle.md#the-read-path).
