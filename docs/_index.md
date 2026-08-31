---
title: Acquirium documentation
---

Acquirium is a data management platform for water treatment plants. It
pairs with [WaTr](https://github.com/DataDrivenCPS/water-ontology), an
ontology for water treatment operations that extends ASHRAE 223P with
treatment processes, substance flows, chemical media and references to
external data sources. 

The server stores two things: the semantic model of the plant (equipment,
piping, sensors and what they measure, described with WaTr and 223P) and
the timeseries of every measured point, whatever its source. Queries
describe what you are looking for in domain terms and are refined step by
step; the server finds the matching points and returns their data, without
SPARQL or knowledge of the graph's structure.

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
                                  ┌──────────────────────────────────────────────┐
                                  │               acquirium server               │
 ┌───────────┐   ┌────────────┐   │                                              │
 │ your code │──▶│  Acquirium │   │   graph store        semantic model + WaTr   │
 │ notebooks │◀──│   client   │◀─▶│   timeseries store   duckdb or timescale     │
 └───────────┘   └────────────┘   │   text resolution    embedding indexes       │
                       HTTP       │                    ▲                         │
                                  │                    │ write                   │
                                  │             ┌──────┴──────┐                  │
                                  │             │   drivers   │                  │
                                  │             │ (Ray actor) │                  │
                                  │             └──────▲──────┘                  │
                                  └────────────────────┼─────────────────────────┘
                                                       │ poll on a schedule
                                     SCADA · historians · CSV · MQTT · lab sheets
```

## Tutorials

Tutorials walk through the capabilities of Acquirium. We recommend starting here.

- [Getting started](tutorials/getting-started.md) — install, start a server, run a first query
- [Querying](tutorials/querying.md) — accessing plant metadata and timeseries data with the incremental query interface
- [Query cookbook](tutorials/query-cookbook.md) — example queries for common domain questions
- [Working with data](tutorials/data.md) — accessing and preprocessing timeseries data
- [Your first driver](tutorials/first-driver.md) — authoring a CSV driver step by step without a plant model
- [A driver against an existing plant model](tutorials/driver-with-a-plant-model.md) — the same driver with a plant model, binding its streams to the model's points
- [Your first app](tutorials/first-app.md) — coming soon

The notebooks under [`notebooks/watertap/`](https://github.com/DataDrivenCPS/acquirium/tree/main/notebooks/watertap) are runnable tutorials too: a quick start, the query interface feature by feature, a regulatory-compliance check and a soft sensor.

<!-- TODO: that sentence covers four of the five notebooks in the folder;
     `watertap-1.ipynb` is unlisted. Name it here or drop it from the repo. -->

## How-to guides

- [Run the server](how-to/run-the-server.md) — the server command, startup, Docker
- [Debugging queries for an unexpected result](how-to/debug-an-empty-query.md) — the five usual causes
- [Explore a model](how-to/explore-a-model.md) — build a query step by step; `options()` and `facets()`
- [Load a plant model](how-to/load-a-plant-model.md) — insert the model, check it landed, validate it against the shapes
- [Inserting data](how-to/write-data.md) — register streams and write rows without a driver; the logbook
- [Resolve text to URIs](how-to/resolve-text.md) — `resolve()`, units and conversion, tuning

<!-- TODO: `generate_grafana_dashboard()` and the Grafana service in
     compose.yaml are undocumented everywhere except one row of the client-API
     table. Either write a how-to or drop it from the public API list. -->

## Reference

The reference guides contain complete interface of each module:

- [Acquirium Client API](reference/client-api.md) — every method of `Acquirium`, `Query`, `DataObject`, `AcquiriumClient`
- [Driver reference](reference/drivers.md) — class hierarchy, hooks, state, config keys, built-in drivers, CLI
- [App reference](reference/apps.md) — coming soon
- [Server configuration](reference/server-config.md) — `[server]`, environment variables, `[ontologies]`, the endpoint table
- [HTTP API](reference/http-api.md) — the raw endpoints
- [Glossary](reference/glossary.md) — URIs, CURIEs, free text, the plant model, querying and data terms

## Explanation

These contain explanation behind the motivation of key design choices we made while building acquirium:

- [The query model](explanation/query-model.md) — what a query is, why we built it, free text, how it executes
- [Values](explanation/values.md) — numeric and text storage, `value_mode`, `cast_value`
- [Units](explanation/units.md) — point units, storage units, compatibility, automatic and requested conversion
- [Drivers](explanation/drivers.md) — why drivers, and the sMAP inspiration
- [Apps](explanation/apps.md) — coming soon
- [The data stream lifecycle](explanation/stream-lifecycle.md) — how streams are identified, stored and found again
- [Text resolution](explanation/text-resolution.md) — how matching works
- [Server internals](explanation/server-internals.md) — storage backends, the graph store, the embedding indexes
- [Graph backend architecture](explanation/graph-backend.md) — graph ownership, inference, query views

<!-- TODO: this page used to end by pointing at `docs/agents/`, the compact
     per-topic references written for coding agents. That directory lives on
     the `ums-agents` branch and does not exist here, so the paragraph was
     removed. Restore it when the agent references land, and update the
     "Working with a coding agent?" line in the top-level README with it. -->
