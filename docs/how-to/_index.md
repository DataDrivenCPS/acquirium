---
title: How-to guides
---

Each guide in this section shows how to do one task with Acquirium, such as loading a plant model, inserting data or running the server.
The guides assume a running server. If you do not have one, start with [getting started](../tutorials/getting-started.md).

- [Load a plant model](load-a-plant-model.md) — insert the semantic model of a plant, check it and validate it
- [Explore a model](explore-a-model.md) — build a query step by step and list the values that occur in the matches
- [Debugging queries for an unexpected result](debug-an-empty-query.md) — common causes of an empty or unexpected result, each with a way to check it
- [Calculate derived streams](calculate-derived-streams.md) — write an app that calculates new streams from existing readings
- [Check, deploy, and repair an app](check-deploy-apps.md) — check an app, deploy it, apply code changes to retained history and remove it
- [Inserting data](write-data.md) — register streams and insert readings without a driver
- [Attach metadata to a node](attach-metadata.md) — your own values on equipment and points, queryable like built-in attributes
- [Record a whole model](record-a-whole-model.md) — declare model components and record snapshots across runs
- [Record experiments](record-experiments.md) — record the inputs, outputs and events of a script as an experiment
- [Compare runs with formulas](compare-experiment-runs.md) — read the scalar results of a study as one row per run and compute formulas from them
- [Run the server](run-the-server.md) — the server command, its flags, startup order and Docker
- [Start Acquirium from a script](local-runtime.md) — start a local server with `aq.init()` and share it between scripts
- [Resolve text to URIs](resolve-text.md) — use `resolve()` directly and check a bad match
