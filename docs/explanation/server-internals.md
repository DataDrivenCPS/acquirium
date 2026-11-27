---
title: Server internals
---

In this article we'll describe what the Acquirium server consists of, what it keeps on disk and what it does when it starts.
The config keys are in the [server configuration reference](../reference/server/configuration.md).

## One process

The server is one process that answers HTTP requests.
Every client, driver and command talks to it over HTTP, and nothing else opens the stored data.

The server holds three things:

- the graph store, with the plant model, the streams and the metadata,
- the timeseries store, with the rows of the streams,
- the indexes of the text matcher.

It also runs the drivers and the apps.
Each driver runs in its own process and talks to the server like a client (see [How drivers work](drivers.md)).
The server compiles plans and Escapement runs calculations with up to `engine_workers` workers.

Be aware that only one server can use a data folder.
A second server on the same folder does not start, because the graph store can be opened by one process only.
For the same reason `workers` has to stay 1.

## Two ways to start it

`aq.init()` starts a local server for the current folder:

```python
import acquirium as aq

acq = aq.init()
```
```text
Acquirium: starting a local server for .../.acquirium
Acquirium: ready at http://127.0.0.1:61590
```

- It listens on this computer only, on a free port.
- It keeps its data in `./.acquirium`.
- The script that started it stops it with `aq.shutdown()`, or when the script ends. The data stays.
- A second script started in the same folder connects to the running server instead of starting a new one.

`acquirium server` starts a server that keeps running until you stop it, on the address given in `acquirium.toml`.
This is the one to use for drivers and for a plant that is always on.
Scripts connect to it with `aq.init(address=...)`.

Both are the same server and store the data the same way.

## The data folder

Everything the server stores is in one folder, `.acquirium` by default:

```text
.acquirium/
  .oxigraph/           the graph store
  .ontoenv/            the catalog of the loaded ontologies
  escapement-epoch.duckdb  the time-series engine store
  experiments.duckdb     the experiment and logbook ledger
  drivers/             the state files of the drivers
  embedding_cache/     the embeddings of the text matcher
  .runtime/            the log and lock files of a server started by aq.init()
```

`drivers`, `embedding_cache` and `.runtime` only exist when they are used.

`acquirium clear` erases the data of the folder and keeps the embeddings, because they take long to build and do not depend on your data.
It refuses to run while a server is using the folder.

## The time-series store

[Escapement](escapement-core.md) stores readings, the durable write queue, published
epochs, and app plans. The default is a DuckDB file. Set `engine_path` to a
PostgreSQL URL to store the same engine state in PostgreSQL.

The separate SQL ledger stores experiment records and logbook entries. The graph
stores stream registrations and logbook links.

## The graph store

The graph is stored with Oxigraph, in the `.oxigraph` folder.
It holds what you inserted, and a second copy that is prepared for queries.
[The graph store](graph-backend.md) explains the two and when the copy is rebuilt.

The prefixes that an inserted model declares are saved next to the graph, in `.oxigraph/acquirium_prefixes.json`:

```text
{
  "wbs": "urn:swro/"
}
```

They are loaded again when the server starts.
This is why `wbs:P1` is still printed as `wbs:P1` after a restart.

## The text matcher

The text matcher turns text such as `"pump"` or `"degC"` into a URI.
It uses two indexes that the server builds when it starts:

- one over the water and s223 ontologies, for classes, relations and substances,
- one over QUDT, for units and quantity kinds.

The indexes are built from the ontologies only.
The plant model and the labels you insert are not part of them.

With `exact_only = true` the text has to match a name, a label or a symbol exactly.
This is the default of `aq.init()`.
Without it the indexes also hold embeddings, so that text that is close to a name is matched as well.
Building the embeddings can take several minutes the first time.
They are saved in `embedding_cache`, and later starts reuse them.

The state of the two indexes can be read with the lower-level client:

```python
acq.client.embedding_status()
```
```text
{'semantic': False,
 'graph': {'state': 'ready', 'concepts': 1480, 'surfaces': 2502, 'error': None, 'last_built': '2026-11-17T23:33:34.668763', 'duration_s': 0.73},
 'qudt': {'state': 'ready', 'concepts': 4143, 'surfaces': 5341, 'error': None, 'last_built': '2026-11-17T23:33:37.415838', 'duration_s': 2.75}}
```

`semantic` is `False` when the server runs with `exact_only`.
See [the text matcher](text-resolution.md) for how a match is made.

## What happens at startup

The server does these in order:

- It opens the two stores. When the data folder is new, the ontologies that ship with Acquirium are loaded into the graph store.
- It builds the indexes of the text matcher.
- It fills the `streams` table from the graph, and prepares the deployed apps.
- It starts answering requests.
- It starts the drivers of `acquirium.toml`, and then deploys the apps of `acquirium.toml`.

In our runs a server with `exact_only = true` answered requests about 45 seconds after the command on a new data folder, and about 15 seconds on later starts.

Note that a stream node in the graph with a wrong `stream_uri` makes the third step fail, and the server does not start (see [Registration and storage](stream-lifecycle.md#registration-and-storage)).

## Next

- [Server configuration](../reference/server/configuration.md) lists every key of `acquirium.toml`.
- [Command line](../reference/server/cli.md) describes `acquirium server` and `acquirium clear`.
- [HTTP API](../reference/server/http-api.md) lists the endpoints.
