---
title: Graph updates and queries
---

A plant model, driver registrations, app outputs, and experiment metadata all
contribute to the same graph. A query can follow relationships across those
contributions, while each writer keeps ownership of its own statements.

The server owns the graph store. Drivers and applications use the client API;
Python calculation functions receive their selected inputs and do not open the
graph themselves.

## Source ownership

Every RDF write names a `source_id`. The reserved `plant` source holds the
shared plant model. A driver writes to its own source, an app writes output
metadata under `derived:<app-name>`, and the experiment interface creates the
sources it needs for Studies and runs.

Replacing a source's graph clears only that source's statements. For example,
reloading the plant model leaves driver registrations and app outputs in place:

```python
client.insert_graph_file("plant.ttl", source_id="plant")
```

Use `replace=False` to append. The [plant-model guide](../how-to/load-a-plant-model.md)
explains inserts and source-scoped SPARQL updates.

Values added with `insert_metadata()` live in the reserved `metadata` source.
When a node disappears from all other source graphs, its attached metadata is
pruned. Source ownership identifies who maintains statements; it is not an
authentication credential.

## Ontologies and inference

Ontologies supply class hierarchies, vocabulary, shapes, and inference rules.
The server resolves `owl:imports` from configured ontology sources and bundled
files. Rules run over the combined deployment data, so they can connect a plant
entity to measurements registered by a driver.

The asserted source graphs and ontologies are persistent. The inferred query
view is rebuilt from them. Validation uses the asserted data and resolved
shapes directly; see [load and validate a model](../how-to/load-a-plant-model.md).

## Inference refresh and query freshness

A graph write schedules inference. Queries normally read the last complete
view while the rebuild runs. They see a complete view throughout, although it
may briefly lag behind a write.

When a query must include the latest graph changes, wait for a fresh view:

```python
client.client.sparql_query(query, wait_for_fresh=True)
```

RDF insertion also waits for the fresh view when synchronizing stream references,
so ontology rules can complete stream-to-point associations before registration
finishes.

`GET /graph_version` reports `source_version`, `published_version`, `is_current`,
and `rebuild_in_progress`. This is a status snapshot; freshness for a particular
query comes from that query's `wait_for_fresh` option.

## Query and export semantics

`include_dependencies=True`, the default, adds ontology and shape triples to
the inferred deployment view. This supports queries that need class hierarchies
or vocabulary definitions. With `False`, queries see inferred deployment facts
alone.

`export_graph(include_dependencies=False)` exports asserted deployment data.
With `True`, it adds resolved imports. Export describes the stored model;
query results can also include inferred facts.

The `/sparql` endpoint returns standard SPARQL JSON results. `/sparql_json`
returns the client's tabular `columns` and `rows` response. Both use the same
query views.

## Storage and recovery

Oxigraph stores the source graphs under the configured `graph_path`. One server
process opens those files; drivers connect over HTTP. Back up the graph together
with the experiment ledger and Escapement store, as described in
[storage upgrades and recovery](../reference/storage-migrations.md).

Experiment metadata documents live in the graph alongside their queryable
attributes. The ledger holds pending graph writes until delivery succeeds.
See [experiment storage](experiments.md#storage) for how recording coordinates
those writes with time-series data.
