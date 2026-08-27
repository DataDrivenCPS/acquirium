---
title: Acquirium documentation
---

Acquirium is a data platform for water treatment plants.
The server stores two things: a semantic model of the plant (equipment,
piping, sensors and their meaning, described with the ASHRAE 223 and NAWI
water ontologies) and the timeseries of every measured point.
Queries describe what you are looking for; the server finds the matching
points and returns their data.

## Architecture

```text
your code ── Acquirium client ──HTTP──▶ acquirium server
                                          ├─ graph store        (semantic model + ontologies)
                                          ├─ timeseries store   (duckdb or timescale)
                                          ├─ text resolution    (embedding indexes)
                                          └─ Ray actors         (drivers and apps)

drivers: feed data in, on a schedule        apps: compute on data, write results back
```

## Tutorials

Learning by doing, on the public WaterTAP seawater-ro model.

- [Getting started](tutorials/getting-started.md) — install, start a server, run a first query
- [Querying](tutorials/querying.md) — the `Query` verbs: entities, topology, measurements, filters, columns
- [Query cookbook](tutorials/query-cookbook.md) — domain questions, how to phrase them, the query
- [Working with data](tutorials/data.md) — lazy fetching, shapes, units, taking a result apart
- [Your first driver](tutorials/first-driver.md) — authoring a CSV driver step by step
- [Your first app](tutorials/first-app.md) — pending the app rework

The notebooks under [`notebooks/watertap/`](https://github.com/DataDrivenCPS/acquirium/tree/main/notebooks/watertap) are runnable tutorials too: a quick start, the query interface feature by feature, a regulatory-compliance check and a soft sensor.

<!-- TODO: that sentence covers four of the five notebooks in the folder;
     `watertap-1.ipynb` is unlisted. Name it here or drop it from the repo. -->

## How-to guides

One task each.

- [Explore a model](how-to/explore-a-model.md) — build a query step by step; `options()` and `facets()`
- [Debugging queries for an unexpected result](how-to/debug-an-empty-query.md) — the five usual causes
- [Inserting data](how-to/write-data.md) — register streams and write rows without a driver; the logbook
- [Run the server](how-to/run-the-server.md) — the server command, startup, Docker
- [Resolve text to URIs](how-to/resolve-text.md) — `resolve()`, units and conversion, tuning

<!-- TODO: two tasks have no how-to yet and are only described inside
     explanation/graph-backend.md:
       - load a plant model: `insert_graph_file(..., source_id="plant")`,
         what `replace=True` does, how to check it landed;
       - validate a model: `validate_graph()` and how to read its SHACL report.
     Both are the first thing a new deployment does. -->
<!-- TODO: `generate_grafana_dashboard()` and the Grafana service in
     compose.yaml are undocumented everywhere except one row of the client-API
     table. Either write a how-to or drop it from the public API list. -->

## Reference

Facts, no narrative.

- [Acquirium Client API](reference/client-api.md) — every method of `Acquirium`, `Query`, `DataObject`, `AcquiriumClient`
- [Driver reference](reference/drivers.md) — class hierarchy, hooks, state, config keys, built-in drivers, CLI
- [App reference](reference/apps.md) — pending the app rework
- [Server configuration](reference/server-config.md) — `[server]`, environment variables, `[ontologies]`, the endpoint table
- [HTTP API](reference/http-api.md) — the raw endpoints
- [Glossary](reference/glossary.md) — URIs, CURIEs, free text, the plant model, querying and data terms

## Explanation

Why things are the way they are.

- [The query model](explanation/query-model.md) — what a query is, why we built it, free text, how it executes
- [Values](explanation/values.md) — numeric and text storage, `value_mode`, `cast_value`
- [Units](explanation/units.md) — point units, storage units, compatibility, automatic and requested conversion
- [Drivers](explanation/drivers.md) — why drivers, and the sMAP inspiration
- [Apps](explanation/apps.md) — pending the app rework
- [The data stream lifecycle](explanation/stream-lifecycle.md) — how streams are identified, stored and found again
- [Text resolution](explanation/text-resolution.md) — how matching works
- [Server internals](explanation/server-internals.md) — storage backends, the graph store, the embedding indexes
- [Graph backend architecture](explanation/graph-backend.md) — graph ownership, inference, query views

<!-- TODO: this page used to end by pointing at `docs/agents/`, the compact
     per-topic references written for coding agents. That directory lives on
     the `ums-agents` branch and does not exist here, so the paragraph was
     removed. Restore it when the agent references land, and update the
     "Working with a coding agent?" line in the top-level README with it. -->
