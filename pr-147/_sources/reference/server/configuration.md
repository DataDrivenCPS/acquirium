---
title: Server configuration
---

This is the reference for `acquirium.toml`: every key of the `[server]`
section, the environment variables that override them, the `[ontologies]`
section, and the endpoint map.
The driver sections are in the [driver reference](../drivers/configuration.md).
For starting the server, see [run the server](cli.md#acquirium-server).

Relative paths in a config file always resolve against that file's own
directory, never the working directory.

All servers use one embedded Escapement engine and one embedded Oxigraph graph.
Paths resolve relative to the configuration file.

```toml
[server]
host = "127.0.0.1"
port = 8000
data_dir = ".acquirium"
exact_only = true
recreate = false
engine_workers = 2
epoch_requests = 128
max_pending_requests = 100000
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `data_dir` | `.acquirium` | Graph, indexes, cached calculation artifacts, and default engine database |
| `engine_path` | `data_dir/escapement-epoch.duckdb` | Escapement store, either a DuckDB file or a PostgreSQL URL |
| `engine_schema` | URL `search_path` when provided, otherwise `acquirium_epoch` | PostgreSQL schema for Escapement |
| `experiments_path` | `engine_path` if it is a `postgres://` URL, else `data_dir/experiments.duckdb` | Experiment ledger: a DuckDB file or a `postgres://` URL. Holds Studies, runs, observations, artifact bytes, and logbook entries; their samples are Escapement streams |
| `engine_workers` | 2 | Python workers available to synchronous app calculations |
| `epoch_requests` | 128 | Maximum accepted requests processed per epoch |
| `max_pending_requests` | 100000 | Maximum accepted requests awaiting publication. A full queue returns HTTP 503 for new requests |
| `history_epochs` | unlimited | Optional number of recent epochs to retain. Older snapshots and feed cursors can expire, including epochs referenced by experiments |
| `drain_timeout` | 10 | Seconds shutdown waits for pending writes to publish; what's left is processed at the next start |
| `graph_path` | `data_dir/.oxigraph` | Embedded RDF storage |
| `exact_only` | false | Resolve exact ontology surfaces without downloading an embedding model |
| `recreate` | false | Explicitly erase the configured data directory and the experiment ledger at startup |
| `read_batch_size` | 50000 | Arrow response page size |
| `workers` | 1 | HTTP processes; must remain one |
| `host`, `port` | `0.0.0.0`, 8000 | Standalone bind address |

`aq.init()` uses loopback, an automatically selected port, persistent storage,
and exact-only resolution by default. Its first caller owns the server lifetime.

Environment overrides include `ACQUIRIUM_DATA_DIR`, `ACQUIRIUM_ENGINE_PATH`, `ACQUIRIUM_ENGINE_SCHEMA`,
`ACQUIRIUM_EXPERIMENTS_PATH`,
`ACQUIRIUM_ENGINE_WORKERS`,
`ACQUIRIUM_EPOCH_REQUESTS`,
`ACQUIRIUM_MAX_PENDING_REQUESTS`, `ACQUIRIUM_HISTORY_EPOCHS`, `ACQUIRIUM_DRAIN_TIMEOUT`, `ACQUIRIUM_GRAPH_PATH`,
`ACQUIRIUM_EXACT_ONLY`, `ACQUIRIUM_EMBEDDING_MODEL`, `ACQUIRIUM_RECREATE`, and
`ACQUIRIUM_WORKERS`.

## The [driver] and [[drivers]] sections

These declare the drivers the server starts; they are documented in the
[driver reference](../drivers/configuration.md).
One key concerns the server itself: `[driver] server_url` and `server_port`
are the address driver actors use to reach the server. The server also uses
that address for the client which deploys configured `[[apps]]` after startup.
The default (`127.0.0.1` and the `[server]` port) is right for a single-host
setup; set them when the bind address is not reachable under that name.
Note that `127.0.0.1` is preferred over `localhost`: the server listens on IPv4
only, `localhost` resolves to `::1` first, and on Windows every new connection
waits about 2 s for the IPv6 attempt to fail. The server closes idle
connections after 5 s, so an actor ticking every 10 s reconnects on each tick
and, with `localhost`, pays that delay every time.

## The [[actors]] section

Each `[[actors]]` entry names an actor declaration for the server to start,
for example `spec = "turbidity_alarm.py:alarm"`. See the
[actor reference](../actors.md#actors-configuration).

## The [ontologies] section

The package ships five ontologies, registered under canonical IRIs at
startup:

| ontology | canonical IRI |
|---|---|
| NAWI Water | `https://watermetadata.org/ontology/watr` |
| ASHRAE 223P | `http://data.ashrae.org/standard223/1.0/model/all` |
| QUDT units | `https://qudt.org/vocab/unit` |
| QUDT quantity kinds | `https://qudt.org/vocab/quantitykind` |
| Brick ref-schema | `https://brickschema.org/schema/Brick/ref` |

`[ontologies] sources` adds to or replaces this set:

```toml
[ontologies]
sources = [
  "./local-extensions.ttl",                       # add under its declared IRI
  { source = "https://qudt.org/3.3.0/vocab/unit", # replace a bundled graph
    as     = "https://qudt.org/vocab/unit" },
]
```

A plain string loads the file or URL and registers it under whatever IRI its
`owl:Ontology` declaration uses.
The `{ source, as }` form rewrites that IRI to `as` and replaces any graph
already registered there.
Replacing a bundled ontology requires the `as` form; a plain string pointing
at an already-populated IRI is skipped.

Note that `[ontologies]` is read from the config file only, with no
environment variable.

## The HTTP API

The Python client covers all of these; the raw endpoints are listed for
scripting against the server directly.
This table is the map; each endpoint's parameters and response shape are in
the [HTTP API reference](http-api.md).
A running server also serves the generated OpenAPI schema at `/docs` and
`/openapi.json`.

| area | endpoints |
|---|---|
| liveness | `GET /health`, `GET /graph_version`, `GET /embedding_status` |
| graph | `POST /insert_graph`, `GET /export_graph`, `POST /sparql_update`, `POST /validate_graph`, `GET /namespace/list` |
| sparql | `GET /sparql` (standards-compatible), `GET\|POST /sparql_json` (legacy `{columns, rows}`) |
| timeseries | `POST /insert_timeseries`, `POST /insert_timeseries_arrow`, `GET /timeseries`, `POST /timeseries_info` |
| resolution | `GET /resolve_text`, `POST /resolve_record`, `POST /resolve_unit`, `POST /resolve_conversion`, `POST /conversion_factors` |
| drivers | `POST /drivers/start`, `POST /drivers/stop`, `GET /drivers/list` |
| apps | `GET /apps`, `GET /apps/{name}`, `PUT /apps/{name}`, `DELETE /apps/{name}` |
| logbook | `POST /insert_log`, `GET /query_logs`, `DELETE /delete_logs` |

The conventions across all of them:

- Almost every failure is an HTTP 400 with the reason in `detail`.
  404 is reserved for an unknown driver or app.
- `GET /timeseries` streams Arrow record batches, not JSON.
- `/sparql_json` accepts POST with a JSON body because resolved traversal
  queries exceed URL length limits; the client always POSTs.
- `GET /sparql` is the SPARQL 1.1 endpoint for outside tools, with content
  negotiation on `Accept`. `/sparql_json` stays for the Python client's
  `{columns, rows}` contract.
- Graph reads take `include_dependencies` (default true) and `wait_for_fresh`
  (default false).
