---
title: The graph store
---

In this article we'll describe how the server stores the graph: which graph a write goes to, what a query reads, and when that is updated.
This is useful when you write a driver that inserts graph data, or when a query does not see what you inserted.

The graph store belongs to the server.
Scripts, drivers and apps reach it through the client, and nothing else opens its files (see [Server internals](server-internals.md)).

## Every write has an owner

The store does not keep one big graph.
It keeps one graph per owner, and the `source_id` of a write selects the graph:

| `source_id` | Graph | Written by |
|---|---|---|
| `"plant"` | the plant model | `insert_graph(..., source_id="plant")` |
| the `source_id` of a driver or a script | the streams of that source and the graph data it inserts | `register_streams()`, `insert_graph()`, `self.insert_graph()` in a driver |
| `"metadata"` | the values attached to nodes | `insert_metadata()` |
| `"experiments"` | studies, runs and variables | the experiment methods |

The server keeps two more kinds of graph that you do not write to: one for its own records, such as the outputs of apps and the logs, and one per ontology.

`replace=True` replaces the graph of one owner only.
For instance, a script adds a pump under its own `source_id`:

```python
import acquirium as aq

acq = aq.init()

acq.insert_graph("""
@prefix ex: <urn:ex/> .
@prefix watr: <https://watermetadata.org/ontology/watr#> .
ex:P8 a watr:Pump .
""", source_id="lab-notes")

print(acq.query().entity("pump").metadata())
```
```text
shape: (4, 1)
┌────────────┐
│ pump       │
│ ---        │
│ str        │
╞════════════╡
│ wbs:intake │
│ wbs:P2     │
│ ex:P8      │
│ wbs:P1     │
└────────────┘
```

Replacing the graph of `lab-notes` removes `ex:P8`, and the pumps of the plant model stay:

```python
acq.insert_graph("""
@prefix ex: <urn:ex/> .
@prefix watr: <https://watermetadata.org/ontology/watr#> .
ex:P9 a watr:Pump .
""", source_id="lab-notes", replace=True)

print(acq.query().entity("pump").metadata())
```
```text
shape: (4, 1)
┌────────────┐
│ pump       │
│ ---        │
│ str        │
╞════════════╡
│ ex:P9      │
│ wbs:intake │
│ wbs:P2     │
│ wbs:P1     │
└────────────┘
```

This is why a driver can insert and replace its own graph data without touching the plant model or another driver.
A driver calls `self.insert_graph()`, which always uses the `source_id` of the driver.

**Note:** `acq.insert_graph()` replaces by default. Pass `replace=False` to add to the graph of the owner.

Be aware that a `source_id` says who owns the data, and is not a permission.
Any client can write with any `source_id`.

## What a query reads

Queries do not read the graphs above directly.
The server builds one graph for queries from them:

- It joins the graphs of all owners. A query sees the plant model, the streams and the metadata together.
- It applies the rules of the ontologies to the result, and adds what they infer.

This graph is a copy.
It is never edited, and it is built again when something changes.

The ontologies that the plant model imports with `owl:imports` are kept ready next to it, so that a query can also read class hierarchies and definitions.

## When the query graph is rebuilt

Every write increases the version of the stored graphs and starts a rebuild of the query graph.

- Only one rebuild runs at a time. Writes that arrive during a rebuild are covered by one more rebuild after it.
- A query that arrives during a rebuild reads the last complete query graph. It never sees a half-built one.
- `insert_graph()`, `register_streams()` and `sparql_update()` return after the rebuild, because the server reads the streams from the new query graph (see [The stream lifecycle](stream-lifecycle.md#registration-and-the-streams-table)). A query that follows one of these calls sees the change.

`graph_status()` shows the two versions:

```python
print(acq.graph_status())
```
```text
{'source_version': 18, 'published_version': 18, 'is_current': True, 'rebuild_in_progress': False}
```

- `source_version` increases with every write.
- `published_version` is the version the query graph was built from.

Be aware that each of these calls waits for one rebuild, and a rebuild goes over the whole graph.
Insert a model in one call, and register many streams in one `register_streams()` call, instead of looping over small ones.

## Query and export semantics

Queries have an `include_dependencies` option, which is `True` by default:

- `True`: the query reads the query graph together with the imported ontologies.
- `False`: the query reads the query graph only. The inferred statements are still there.

For instance, the parent class of `s223:Pump` is a statement of the ontology, so it is only found with the default:

```python
q = """
PREFIX s223: <http://data.ashrae.org/standard223#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?parent WHERE { s223:Pump rdfs:subClassOf ?parent }
"""

print(acq.client.sparql_query(q)["rows"])
print(acq.client.sparql_query(q, include_dependencies=False)["rows"])
```
```text
[['http://data.ashrae.org/standard223#Equipment']]
[]
```

The server can also export the graph, with `GET /export_graph` (see the [HTTP API reference](../reference/server/http-api.md)).
The export contains what was inserted by all owners, without the inferred statements.
With `include_dependencies=true` the imported ontologies are added.

`acq.client.sparql_query()` has one more option, `wait_for_fresh=True`.
It makes the query wait for a rebuild that is running.

## Validation

`acq.validate_graph()` checks everything that was inserted against the shapes of the ontologies.
It returns `conforms`, a report and a readable text of the results (see the [graph reference](../reference/client/graph.md)).
The check does not change the graph.

## Limits

- One server process owns the graph store. Do not start two servers on the same data folder.
- There is no method that removes an owner. `insert_graph("", source_id=..., replace=True)` empties its graph.
- Metadata written with `insert_metadata()` is removed when the node it describes is removed from the other graphs.

## Next

- [Graph](../reference/client/graph.md) in the client reference has the methods that read and write the graph.
- [The query model](query-model.md) explains how a query is built and executed.
- [The stream lifecycle](stream-lifecycle.md) explains what registering a stream writes.
