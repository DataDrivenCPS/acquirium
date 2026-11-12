---
title: How-to guides
---

Each guide answers one practical question. They assume you have worked through
the [tutorials](../tutorials/_index.md).

**Running Acquirium**

- [Acquirium in Python scripts](local-runtime.md): `aq.init()`, sharing one server between scripts, and shutdown.
- [Server startup](run-the-server.md): the server command, startup, and Docker.

**The plant model**

- [Plant model loading](load-a-plant-model.md): insert it, check it landed, and validate it.
- [Model exploration](explore-a-model.md): find out what a model holds with `options()` and `facets()`.
- [Query troubleshooting](debug-an-empty-query.md): the usual causes, and how to check each.
- [Node metadata](attach-metadata.md): your own values on equipment and points, queryable like the built-in attributes.
- [Text resolution to URIs](resolve-text.md): see what free text matches, and convert units.

**Data**

- [Direct data insertion](write-data.md): register streams and insert rows directly.
- [Streaming change feed](consume-streaming-changes.md): follow every committed change, resuming where you left off.

**Soft sensors**

- [Soft sensor examples](soft-sensor-examples.md): calculations, cleaning pipelines, multiple outputs, parameters, and profiling.
- [App deployment and recovery](check-deploy-apps.md): start soft sensors, inspect their state, update calculations, recover from failures, and remove apps.

**Experiments**

- [Experiment recording](record-experiments.md): record an analysis's inputs and results, and find runs again.
- [Recording complete models](record-a-whole-model.md): declare a model's components once and record snapshots of them.
- [Compare runs with formulas](compare-experiment-runs.md): one row per run, and formulas across runs.
