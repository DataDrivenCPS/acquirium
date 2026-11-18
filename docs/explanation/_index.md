---
title: Explanation
---

The pages in this section explain how Acquirium works and why.
They are background reading. For step-by-step instructions, see the [tutorial](../tutorials/_index.md) and the [how-to guides](../how-to/_index.md).

## Queries and data

- [The query model](query-model.md): what a query is and how it is executed.
- [Text matcher](text-resolution.md): how plain text is matched to the terms of the ontologies.
- [Values](values.md): how numbers and text are stored and returned.
- [Units](units.md): where a unit is recorded and how values are converted.
- [The stream lifecycle](stream-lifecycle.md): what registration writes, how readings are stored and how a query finds them.

## Drivers, apps and experiments

- [How drivers work](drivers.md): what the server does when a driver runs.
- [How apps work](apps.md): how the calculated streams are kept up to date.
- [How experiments work](experiments.md): studies, runs and what is stored.

## Inside the server

- [Server internals](server-internals.md): the data folder, the stores and the startup order.
- [The graph store](graph-backend.md): the graphs of each source, what a query reads and when it is rebuilt.
- [Materialization internals](materialization-internals.md): how the progress of apps is stored and resumed.
