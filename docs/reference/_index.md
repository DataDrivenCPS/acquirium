---
title: Reference
---

This section describes every public interface of Acquirium: each function, method and class with its signature, parameters, return value and errors.
The pages are grouped by what you are working with.

## Python

| section | what it covers |
|---|---|
| [Client](client/_index.md) | `aq.init()`, `aq.shutdown()` and the methods of the client: registering streams, inserting data, the graph, metadata, logs |
| [Query](query/_index.md) | the query builder returned by `acq.query()`, filters, columns, reading metadata and data |
| [Drivers](drivers/_index.md) | the driver base classes, the hooks you override, the helpers, configuration keys and the built-in drivers |
| [Apps](apps/_index.md) | the `App` class, its inputs and outputs, checking and deploying |
| [Actors](actors.md) | `aq.Actor`, the handler methods, timer and graph-change messages, snapshots and transitions |
| [Experiments](experiments/_index.md) | studies, runs, variables, recording and reading results, model adapters |

## Server

| section | what it covers |
|---|---|
| [Command line](server/cli.md) | the `acquirium` command and every subcommand |
| [Server configuration](server/configuration.md) | every key of `acquirium.toml` |
| [HTTP API](server/http-api.md) | the HTTP endpoints of the server |
| [Storage upgrades and backups](server/storage-migrations.md) | move to Escapement or back up and restore an existing deployment |

## Terms

The [glossary](glossary.md) defines the terms used across the documentation.

## How to read an entry

Every entry has the same parts:

- the **signature**, with the default value of each parameter,
- **Parameters**, one line per parameter with its type,
- **Returns**, the type and the shape of the result,
- **Raises**, the errors you can expect and when,
- an **Example**, with its output.

Parameters after a `*` in a signature must be given by name.
