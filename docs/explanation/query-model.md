---
title: The query model
---

This is an explanation of the ideas behind the `Query` interface: what a
query is, why we built it this way, how free text turns into ontology terms,
and how a query is executed.
Using the interface is covered in the [querying tutorial](../tutorials/querying.md).

## The mental model of Querying in Acquirium

A `Query` is a description of what you are looking for.
You chain verbs to describe a pattern (a pump, a tank near it, the measurements on either one, etc.), and the server finds every place in the plant model where that
pattern matches.

`acq.query()` starts an empty `Query`.
Every verb returns a new immutable `Query`, so you can hold on to a partial
query and extend it in different directions.

The `Query` interface is an opinionated view over the underlying plant knowledge graph.
It is different from querying a raw RDF graph.
This view treats certain edges as attributes of the source nodes.
For instance a measurement's unit is a separate triple in RDF: `:source qudt:hasUnit :unit`, but here, it is just the measurement's `unit` attribute.
For the same reason, edges whose meaning is not plant topology (class hierarchies, external references etc.) are hidden from generic traversal.

This interface classifies nodes into two main categories.
Every concept, system, equipment and connection is treated as an `Entity` node,
while every measurement (i.e. `s223:Property`) is treated as a `Data` node.

The `Query` interface is lazy.
The actual query execution will happen after the user executes certain functions like `.metadata()`, `.dataframe()` or `.facets()`.
This is what lets you build a query step by step and check `.metadata()` after every step, without paying for data you do not need yet.

### Why?

The main reasons we designed this interface are:
- SPARQL and other declarative languages are not designed for incremental querying, where users can visualize intermediate results while building queries
- We see the advantages of *dataframe* systems compared to SQL databases for data science and data exploration tasks
- Previous systems that combine data and metadata access (e.g. Mortar, Energon, Chrontext) all depend on declarative languages and do not focus on data-metadata co-exploration
- We do not expect our intended users to be familiar with RDF and SPARQL

Read [our paper](https://dl.acm.org/doi/abs/10.1145/3744256.3812557) for more info.


## Free text and what it resolves to

Every place that takes a class, a predicate or an attribute value also takes
free text.
This is a design element for us towards our goal to avoid/reduce RDF exposure to our users.
The server matches it against the ontologies (exact match on a label first,
then semantic similarity) and picks the best hit.
`"ozone unit"` becomes `nawi:OzonationUnit`, `"mg/L"` becomes `unit:MilliGM-PER-L`.
Each slot is matched against its own vocabulary, so `unit="PSI"` searches
units, not classes.

Be aware that free text queries might result in incorrect results.
The match is a best guess, and a wrong guess would still generate a working query but with
the wrong answer.
In such case, try to change the text you enter, use the exact URI of the equipment you're searching or check the text matching results:

```python
acq.client.resolve("ro membrane", "class", top_k=3, min_score=0.3)
```
```text
[{'uri': 'urn:nawi-water-ontology#ReverseOsmosisMembrane',        'score': 0.839, ...},
 {'uri': 'urn:nawi-water-ontology#ElectricallyConductingMembrane', 'score': 0.788, ...},
 {'uri': 'urn:nawi-water-ontology#MolybdenumSulfideBasedMembrane', 'score': 0.786, ...}]
```

Note that only the ontologies are searchable this way, not your plant's own
instance labels.
`entity("pump")` resolves the *class* `Pump`; it will not find the item named "pump-1" in the plant by
its label.


## How Queries are executed

The Acquirium `Query` object converts its content to an optimized SPARQL query (or queries) and runs that in the graph database embedded in the server.
Completely removing this step and executing `Query` objects on the graph database directly is our current research area.

`to_sparql()` returns the SPARQL the query compiles to, without running it.
It is the surest way to see what a chain actually asks for, and to check that a filter applies to the node you intended.

`execute()` runs that query and returns the raw bindings as `{"columns": [...], "rows": [[...]]}`.
`metadata()` is this plus the alias-to-column naming, so you rarely need it directly.

Traversal queries (i.e. finding the *nearest* related equipment or measurement point) are executed at both the Python level and the SPARQL level:

### How traversal runs

SPARQL handles multi-hop wildcard traversal poorly: every hop multiplies joins, and an unbounded path can stall the store.
Acquirium therefore resolves these steps client side.
When a step uses `via="any"` or a repeatable predicate, the client resolves it at execute time with a breadth-first walk: fetch one layer of neighbors, step, repeat, up to `max_depth`.
The walk also makes `nearest` exact: distance is counted per source, and ties survive.
The final SPARQL only ever receives the concrete pairs the walk found.
Predicate lists and `direction=` compile to SPARQL directly, without the walk.
Layer results are cached until the graph changes, so repeating a query is cheap.
You can always inspect what will run with `.to_sparql()`.


### Hidden Predicates

We use [s223](https://open223.info/) to model the connections in WaTr graphs.
However, s223 graphs contain some predicates that are not useful or meaningful for our querying.
Additionally, certain edges in our graphs are attributes of the node.
These predicates describe a node rather than connect the plant, and walking them returns ontology terms instead of nearby equipment.
That's why we hide these during traversals.

`via="any"` walks every predicate except a default hidden set: the ones backing
the attributes (`rdf:type`, `hasUnit`, `hasQuantityKind`, `ofMedium`,
`hasMedium`, `ofSubstance`, `hasEnumerationKind`, `hasProcess`, `dataSource`),
plus `rdfs:subClassOf`, `s223:hasProperty`, `s223:hasConnectionPoint`,
`s223:cnx` and `ref:hasExternalReference`.

```python
from acquirium.Client.explore import hidden_predicates, hide, unhide

sorted(hidden_predicates())
```

The set is global and adjustable: `hide(uri)` adds to it, `unhide(uri)` takes
one out, and a bare `unhide()` resets to the defaults.
Naming a predicate in `via=` always overrides the set, so hiding never blocks
an explicit request.
Measurement edges are exempt too, which is why `measurement()` still reaches
points through `hasProperty` and `hasConnectionPoint`.

### The step patterns behind direction=

`direction="upstream"` and `"downstream"` are shorthand for four fixed step
patterns each, exported as constants:

```python
from acquirium.Client.explore import (
    UPSTREAM_EQUIPMENT, DOWNSTREAM_EQUIPMENT,
    UPSTREAM_PROPERTY, DOWNSTREAM_PROPERTY,
)
```

The `EQUIPMENT` pair walks equipment to equipment through connection points and
connections; the `PROPERTY` pair ends on the measurements attached along the
way.
`related()` and `measurement()` pick the right one for you.
Each constant is a readable tuple of chains.
Passing one to `via=` runs the same traversal as a client-side walk, so
`nearest=True` follows the process flow instead of raw graph hops:

```python
q.related("pump", via=UPSTREAM_EQUIPMENT, nearest=True)
```

The module docstring of `acquirium.Client.explore.directions` documents
every step.



## Miscellaneous

`measurement(frm=...)` also accepts a list of aliases, attaching one
measurement node per named entity.
This is the middle ground between a single `frm=` and `frm="*"`:

```python
(acq.query().entity(uri="wbs:P1", alias="p1").related("Pressure Exchanger", alias="px")
 .measurement(frm=["p1", "px"], quantity_kind="pressure"))
# columns: p1, px, p1_data, px_data
```

`alias()` renames the current node, but the previous alias keeps working as
an alternative handle in `target=`, `frm=` and `of=`; display uses the latest
name.

**TODO:** This might be a bad idea. Remove old aliases

The terminals take `include_dependencies=` (default `True`).
`True` queries the deployment data together with the resolved ontology and
shape triples, which is what makes subclass matching work.
`False` restricts the query to the deployment data alone; it is faster and
correct only for queries that need no ontology terms.
See the [graph backend guide](graph-backend.md#query-and-export-semantics).
