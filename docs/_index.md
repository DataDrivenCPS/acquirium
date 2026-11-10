---
title: Acquirium documentation
---

Acquirium connects a plant model to its sensor streams. Describe the equipment,
measurements, or relationships you need, then read the matching data through the
query interface. Apps keep derived streams current as readings arrive or change.

The plant vocabulary uses [WaTr](https://github.com/DataDrivenCPS/water-ontology)
and ASHRAE 223P. Escapement stores readings and runs app calculations.

The design is described in [Acquirium: Toward Interoperable Data Driven
Applications in Water Treatment Systems](https://dl.acm.org/doi/abs/10.1145/3744256.3812557)
(BuildSys '26). If you use Acquirium in your work, cite:

```bibtex
@inproceedings{saka2026acquirium,
  title     = {Acquirium: Toward Interoperable Data Driven Applications in Water Treatment Systems},
  author    = {Saka, Umut Mete and Chapin, Fletcher T. and Paul, Lazlo and Anwar, Avia and Struck, Scott and Mauter, Meagan S. and Fierro, Gabe},
  booktitle = {Proceedings of the 13th ACM International Conference on Systems for Energy-Efficient Buildings, Cities, and Transportation (BuildSys '26)},
  year      = {2026},
  publisher = {ACM},
  address   = {Banff, AB, Canada},
  doi       = {10.1145/3744256.3812557}
}
```

## Architecture

```text
                     ┌──────────────── Acquirium server ────────────────┐
 SCADA, historians,  │                                                  │
 CSV, MQTT,          │  Plant model                Readings             │  query
 lab sheets          │  equipment, sensors  ◀───▶  every value from     │ ◀────── your code,
     │               │  and what each one          every sensor, with   │ ──────▶ notebooks
     ▼               │  measures                   its corrections      │  data
  drivers ─────────▶ │  (WaTr, ASHRAE 223P)          │          ▲       │
  points, readings   │                               ▼          │       │
                     │                            Apps: averages,       │
                     │                            alarms, soft sensors, │
                     │                            kept current          │
                     └──────────────────────────────────────────────────┘
```

Drivers read from plant systems and send readings to the server, along with
the points they belong to in the plant model. Queries name equipment and
measurements in the model's terms and return the matching readings. Apps
compute derived streams from readings and update them when a reading arrives
late or is corrected. [Server internals](explanation/server-internals.md)
shows the parts of the server and where each one keeps its data.

## Tutorials

Tutorials walk through the capabilities of Acquirium. We recommend starting here.

- [Getting started](tutorials/getting-started.md) — install, start a server, run a first query
- [Querying](tutorials/querying.md) — accessing plant metadata and timeseries data with the incremental query interface
- [Query cookbook](tutorials/query-cookbook.md) — example queries for common domain questions
- [Working with data](tutorials/data.md) — accessing and preprocessing timeseries data
- [Your first driver](tutorials/first-driver.md) — authoring a CSV driver step by step without a plant model
- [A driver against an existing plant model](tutorials/driver-with-a-plant-model.md) — the same driver with a plant model, binding its streams to the model's points
- [Your first app](tutorials/first-app.md) — derive a new stream from stored data and watch it stay current
- [Follow a turbidity alarm](tutorials/turbidity.md) — combine apps and follow a late reading through an alarm
- [Your first experiment](tutorials/first-experiment.py) — optimize a pump schedule with SciPy, record each scenario, and compare runs

The notebooks under [`notebooks/watertap/`](https://github.com/DataDrivenCPS/acquirium/tree/main/notebooks/watertap) are runnable tutorials too: a quick start, the query interface feature by feature, a regulatory-compliance check and a soft sensor.

## How-to guides

- [Calculate derived streams](how-to/calculate-derived-streams.md) — built-in averages and thresholds
- [Write a Python calculation](how-to/write-soft-sensors.md) — supply a function and choose its input window
- [Consume streaming changes](how-to/consume-streaming-changes.md) — resumable signed changes, epoch completion, and a sustained workload
- [Profile soft-sensor execution](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/profiling) — native epoch and Python stage measurements, with [initial results](profiling/2026-10-18/README.md)
- [Check, deploy, and repair an app](how-to/check-deploy-apps.md) — dry runs, deployment, removal, and execution controls
- [Compare runs with formulas](how-to/compare-experiment-runs.md) — one row per run, formulas in Polars or plain Python
- [Record experiments](how-to/record-experiments.md) — record inputs and results, find runs by metadata, and annotate stream intervals
- [Record a whole model](how-to/record-a-whole-model.md) — explicit general and Pyomo adapters, JSON metadata, and sampling
- [Run the server](how-to/run-the-server.md) — the server command, startup, Docker
- [Start from a script](how-to/local-runtime.md) — `aq.init()`, config discovery, and local server ownership
- [Debugging queries for an unexpected result](how-to/debug-an-empty-query.md) — the five usual causes
- [Explore a model](how-to/explore-a-model.md) — build a query step by step; `options()` and `facets()`
- [Load a plant model](how-to/load-a-plant-model.md) — insert the model, check it landed, validate it against the shapes
- [Inserting data](how-to/write-data.md) — register streams and write rows without a driver
- [Resolve text to URIs](how-to/resolve-text.md) — `resolve()`, units and conversion, tuning

## Reference

The reference guides contain complete interface of each module:

- [Acquirium Client API](reference/client-api.md) — every method of `Acquirium`, `Query`, `DataObject`, `AcquiriumClient`
- [Driver reference](reference/drivers.md) — class hierarchy, hooks, state, config keys, built-in drivers, CLI
- [App reference](reference/apps.md) — the app API, output requirements, and processing rules
- [Experiment reference](reference/experiments.md) — Studies, variables, runs, stream annotations, and endpoints
- [Server configuration](reference/server-config.md) — `[server]`, environment variables, `[ontologies]`, the endpoint table
- [Storage upgrades and recovery](reference/storage-migrations.md) — schema upgrades, backups, and restore
- [HTTP API](reference/http-api.md) — the raw endpoints
- [Glossary](reference/glossary.md) — URIs, CURIEs, free text, the plant model, querying and data terms

## Explanation

These contain explanation behind the motivation of key design choices we made while building acquirium:

- [The query model](explanation/query-model.md) — what a query is, why we built it, free text, how it executes
- [Values](explanation/values.md) — numeric and text storage, `value_mode`, `cast_value`
- [Units](explanation/units.md) — point units, storage units, compatibility, automatic and requested conversion
- [Drivers](explanation/drivers.md) — how sources hand readings to the engine
- [How derived streams stay current](explanation/apps.md) — what a correction changes and why
- [Experiments](explanation/experiments.md) — how Studies, runs, stream intervals, and artifacts preserve analysis context
- [The data stream lifecycle](explanation/stream-lifecycle.md) — how streams are identified, stored and found again
- [Text resolution](explanation/text-resolution.md) — how matching works
- [Escapement core](explanation/escapement-core.md) — execution, history, snapshots, and recovery
- [Server internals](explanation/server-internals.md) — what one server process runs: the engine, graph store, experiment ledger, indexes and drivers
- [How graph changes reach queries](explanation/graph-backend.md) — ownership, inference, and fresh reads

## Citation

The design is described in [Acquirium: Toward Interoperable Data Driven
Applications in Water Treatment Systems](https://dl.acm.org/doi/abs/10.1145/3744256.3812557)
(BuildSys '26). If you use Acquirium in your work, cite:

```bibtex
@inproceedings{saka2026acquirium,
  title     = {Acquirium: Toward Interoperable Data Driven Applications in Water Treatment Systems},
  author    = {Saka, Umut Mete and Chapin, Fletcher T. and Paul, Lazlo and Anwar, Avia and Struck, Scott and Mauter, Meagan S. and Fierro, Gabe},
  booktitle = {Proceedings of the 13th ACM International Conference on Systems for Energy-Efficient Buildings, Cities, and Transportation (BuildSys '26)},
  year      = {2026},
  publisher = {ACM},
  address   = {Banff, AB, Canada},
  doi       = {10.1145/3744256.3812557}
}
```
