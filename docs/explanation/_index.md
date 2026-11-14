---
title: Explanation
---

The pages in this section explain how Acquirium works and why it is designed this way: the query model, values and units, drivers, apps, the stream lifecycle, experiments, text resolution and the server internals.
They are background reading. For step-by-step instructions, see the [tutorials](../tutorials/_index.md) and the [how-to guides](../how-to/_index.md).

- [The query model](query-model.md) — what a query is, how free text becomes ontology terms and how a query is executed
- [Values](values.md) — how numeric and text readings are stored and returned
- [Units](units.md) — where a unit is recorded and how values are converted
- [Drivers](drivers.md) — why drivers exist and where the design comes from
- [Apps](apps.md) — how apps keep derived values up to date as inputs change
- [The data stream lifecycle](stream-lifecycle.md) — how a stream is registered, how its rows are stored and how a query finds them
- [Experiments](experiments.md) — studies, runs and what an experiment records
- [Text resolution](text-resolution.md) — how free text is matched to ontology URIs
- [Materialization internals](materialization-internals.md) — how derived values and processing progress are stored and resumed
- [Server internals](server-internals.md) — the storage backends, the graph store and the embedding indexes
- [Graph backend architecture](graph-backend.md) — the persistent graphs, the derived query views and the update rules
