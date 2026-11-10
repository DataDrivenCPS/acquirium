---
title: How-to guides
---

Each guide answers one practical question. They assume you have worked through
the [tutorials](../tutorials/_index.md).

**Running Acquirium**

- [Start from a script](local-runtime.md): `aq.init()`, sharing one server between scripts, and shutdown.
- [Run the server](run-the-server.md): the server command, startup, and Docker.

**The plant model**

- [Load a plant model](load-a-plant-model.md): insert it, check it landed, and validate it.
- [Explore a model](explore-a-model.md): find out what a model holds with `options()` and `facets()`.
- [Debug a query with an unexpected result](debug-an-empty-query.md): the usual causes, and how to check each.
- [Attach metadata to a node](attach-metadata.md): your own values on equipment and points, queryable like the built-in attributes.
- [Resolve text to URIs](resolve-text.md): see what free text matches, and convert units.

**Data**

- [Write data without a driver](write-data.md): register streams and insert rows directly.
- [Consume streaming changes](consume-streaming-changes.md): follow every committed change, resuming where you left off.

**Apps**

- [Calculate a derived stream](write-soft-sensors.md): built-in calculations, your own Python functions, windows, state, and speed.
- [Deploy, change and repair apps](check-deploy-apps.md): previews, inspection, new code, recovery, and removal.
- [Profile app execution](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/profiling): measure where an epoch spends its time, with [initial results](../profiling/2026-10-18/README.md).

**Experiments**

- [Record experiments](record-experiments.md): record an analysis's inputs and results, and find runs again.
- [Record a whole model](record-a-whole-model.md): declare a model's components once and record snapshots of them.
- [Compare runs with formulas](compare-experiment-runs.md): one row per run, and formulas across runs.
