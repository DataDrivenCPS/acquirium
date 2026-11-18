---
title: The query model
---

In this article we'll describe the ideas behind the query interface: what a query is, how it sees the plant model, and how it is run.
Using the interface is covered in the tutorial, starting from [2 - Querying Data](../tutorials/querying-data.md).
The examples use the plant of [5 - A Full Example](../tutorials/a-full-example.md).

## A query is a pattern

A query describes a pattern of nodes in the plant model.
The server finds every place in the model where the pattern matches, and each match is one row of the result.

There are three kinds of nodes:

- An **entity** node is a part of the plant, such as a pump, a tank or a pipe. It is added with `entity()` and `related()`.
- A **property** node is one measured quantity of the plant. It is added with `property()`.
- A **measurement** node is a property that has a stream connected. It is added with `measurement()`, and it is the only kind of node that returns data.

For instance, the pattern below has two nodes: a pump, and a power measurement of that pump.

```python
pumps = acq.query().entity("pump")
power = pumps.measurement(quantity_kind="power")
print(power)
```
```text
Query
  pump: entity, Pump
  pump_data: measurement, of pump, quantity_kind=Power
2 matches:
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ pump_data               ┆ pump_data.label     │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power │
└────────┴─────────────────────────┴─────────────────────┘
```

Every method returns a new query and leaves the original unchanged.
This is useful for keeping a part of a query in a variable and extending it in several ways.
For instance, `pumps` is still the query with one node, and it can be extended again:

```python
tanks = pumps.related("tank", direction="downstream")
print(tanks)
```
```text
Query
  pump: entity, Pump
  tank: entity, Tank, downstream of pump
2 matches:
┌────────┬────────────────────┐
│ pump   ┆ tank               │
│ ---    ┆ ---                │
│ str    ┆ str                │
╞════════╪════════════════════╡
│ wbs:P1 ┆ wbs:storage-tank-2 │
│ wbs:P2 ┆ wbs:storage-tank-2 │
└────────┴────────────────────┘
```

Building a query does not search the plant model.
The search runs when a result is asked for, with `print()`, `metadata()`, `dataframe()` or `options()`.
So a query can be built step by step and checked after every step, and the data is read only at the end.

**Note:** The result of a search is kept on the query object.
If the plant model or the streams change, build the query again to see the change.

## Attributes and links

The plant model is an RDF graph, where every fact is a link between two nodes.
A query does not show the graph this way.
It sorts the links into two groups:

- **Attributes** describe one node: its class, label, unit, quantity kind, medium, and the keys written with `insert_metadata()`. They are used with `where()`, `include()` and `options()`.
- **Links** connect the parts of the plant, such as a pipe between a pump and a tank. They are followed with `related()`.

For instance, the unit of a property is a separate node in the graph.
In a query it is a value of the property:

```python
power.include("unit").metadata()
```
```text
shape: (2, 4)
┌────────┬─────────────────────────┬─────────────────────┬────────────────┐
│ pump   ┆ pump_data               ┆ pump_data.label     ┆ pump_data.unit │
│ ---    ┆ ---                     ┆ ---                 ┆ ---            │
│ str    ┆ str                     ┆ str                 ┆ str            │
╞════════╪═════════════════════════╪═════════════════════╪════════════════╡
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power ┆ unit:W         │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power ┆ unit:W         │
└────────┴─────────────────────────┴─────────────────────┴────────────────┘
```

For the same reason, `related()` does not follow the attributes.
Otherwise two pumps would be related because they have the same class or the same unit.
The links that `related()` skips are called hidden predicates, and the list can be changed (see [Traversal settings](../reference/query/traversal.md)).

## Why not SPARQL

RDF graphs are normally queried with SPARQL.
We designed this interface for the following reasons:

- SPARQL and other declarative languages are not designed for building a query step by step and looking at the results in between.
- Dataframe libraries work better than SQL databases for data exploration, and we wanted the same for plant models.
- Earlier systems that combine data and metadata (Mortar, Energon, Chrontext) depend on declarative languages, and do not focus on exploring the data and the metadata together.
- We do not expect our users to know RDF and SPARQL.

See [our paper](https://dl.acm.org/doi/abs/10.1145/3744256.3812557) for more.

## Plain text in a query

Classes and attribute values can be written as plain text, such as "pump" or "power" above.
The text matcher of the server turns the text into a term of the ontologies when the method is called.
A text that matches nothing raises a `ValueError`:

```python
acq.query().entity("pmp")
```
```text
ValueError: Could not resolve 'pmp' as class
```

Note that the text matcher only knows the ontologies.
It does not search the names in your plant model, so `entity("High-pressure Pump P1")` does not find the pump with that label.
Use `where(label=...)` or `uri=` for that.
See [Text matcher](text-resolution.md) for how the matching works.

## How a query is run

The client turns the query into SPARQL and sends it to the server, which runs it on the graph store.
`to_sparql()` shows this text:

```python
print(pumps.to_sparql())
```
```text
SELECT DISTINCT ?v0
WHERE {
  ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v0_typ .
  { SELECT DISTINCT ?v0_typ WHERE { ?v0_typ <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <https://watermetadata.org/ontology/watr#Pump> . } }
}
```

The second line of the pattern is the reason a class also matches its subclasses.
The query reads the plant model together with the ontologies it imports, so it knows which classes are a kind of pump.

### Searching over links

A search that follows any link for several steps is slow in SPARQL.
For this reason the client does this part itself.
With `related()` and no `direction`, the client reads the links one step at a time, starting from the nodes found so far.
It stops after `max_depth` steps, which is 3 by default.
By default it keeps only the nearest matches of each start node:

```python
p1 = acq.query().entity(uri="wbs:P1", alias="p1")
p1.related("pump", alias="other").metadata()
```
```text
shape: (1, 2)
┌────────┬────────┐
│ p1     ┆ other  │
│ ---    ┆ ---    │
│ str    ┆ str    │
╞════════╪════════╡
│ wbs:P1 ┆ wbs:P2 │
└────────┴────────┘
```

With `nearest=False` and `max_depth=0`, which means no limit, the same query also returns the intake pump `wbs:intake`.

The links that were read are kept in the client until the graph changes, so the next search is faster.

### Searching along the flow

With `direction="upstream"` or `direction="downstream"`, only the connections of the plant are followed: from an entity to its connection points, to the pipe, and to the next entity.

When the nearest match is wanted, the client searches place by place.
For a measurement, the first place is the inlets or outlets of the entity itself.
The next places are the connected pipe, then the next entity with its connection points, and so on.
The search of a start node stops at the first place that has a match.
For instance, the nearest pressure upstream of the reverse osmosis stage is at its own inlet:

```python
ro = acq.query().entity(uri="wbs:RO", alias="ro")
ro.measurement(direction="upstream", quantity_kind="pressure").metadata()
```
```text
shape: (1, 4)
┌────────┬────────────────────┬───────────────────────┬────────────────────────┐
│ ro     ┆ ro_upstream_entity ┆ ro_upstream_data      ┆ ro_upstream_data.label │
│ ---    ┆ ---                ┆ ---                   ┆ ---                    │
│ str    ┆ str                ┆ str                   ┆ str                    │
╞════════╪════════════════════╪═══════════════════════╪════════════════════════╡
│ wbs:RO ┆ wbs:RO-in          ┆ wbs:RO-in-pressure-pa ┆ RO in pressure pa      │
└────────┴────────────────────┴───────────────────────┴────────────────────────┘
```

With `nearest=False`, the places further upstream are included as well:

```python
ro.measurement(direction="upstream", quantity_kind="pressure", nearest=False).metadata()
```
```text
shape: (3, 4)
┌────────┬────────────────────┬───────────────────────────────┬───────────────────────────┐
│ ro     ┆ ro_upstream_entity ┆ ro_upstream_data              ┆ ro_upstream_data.label    │
│ ---    ┆ ---                ┆ ---                           ┆ ---                       │
│ str    ┆ str                ┆ str                           ┆ str                       │
╞════════╪════════════════════╪═══════════════════════════════╪═══════════════════════════╡
│ wbs:RO ┆ wbs:P1             ┆ wbs:P1-out-pressure-pa        ┆ P1 out pressure pa        │
│ wbs:RO ┆ wbs:PXR            ┆ wbs:PXR-brine-out-pressure-pa ┆ PXR brine out pressure pa │
│ wbs:RO ┆ wbs:RO-in          ┆ wbs:RO-in-pressure-pa         ┆ RO in pressure pa         │
└────────┴────────────────────┴───────────────────────────────┴───────────────────────────┘
```

### From an entity to its measurements

`measurement()` after an entity does not search over links.
It matches the properties that the model puts on the entity or on one of its connection points.
`context()` is the reverse: it starts from a measurement and adds the entity it belongs to.

```python
acq.query().measurement(quantity_kind="power").context().metadata()
```
```text
shape: (2, 3)
┌─────────────────────────┬─────────────────────┬─────────────┐
│ data                    ┆ data.label          ┆ data_entity │
│ ---                     ┆ ---                 ┆ ---         │
│ str                     ┆ str                 ┆ str         │
╞═════════════════════════╪═════════════════════╪═════════════╡
│ wbs:P2-mechanical-power ┆ P2 mechanical power ┆ wbs:P2      │
│ wbs:P1-mechanical-power ┆ P1 mechanical power ┆ wbs:P1      │
└─────────────────────────┴─────────────────────┴─────────────┘
```

Note that a sensor is not the entity of a property.
A property that a sensor observes is found through the equipment or the connection point that has it.

## Further reading

- [Query reference](../reference/query/_index.md): every method of a query, with its parameters.
- [Traversal settings](../reference/query/traversal.md): the hidden predicates and the relations of `context()`.
- [Text matcher](text-resolution.md): how plain text is matched to the terms of the ontologies.
- [Units](units.md) and [Values](values.md): what happens to the data when it is read.
