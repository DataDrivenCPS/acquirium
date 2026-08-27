---
title: Server configuration
---

<!-- TODO: intro -->

## The [server] section

```toml
[server]
host               = "0.0.0.0"
port               = 8000
data_dir           = ".acquirium"
timeseries_backend = "duckdb"        # or "timescale" (requires pg_dsn)
```

| key | default | meaning |
|---|---|---|
| `host` | `"0.0.0.0"` | bind host |
| `port` | `8000` | bind port |
| `data_dir` | `".acquirium"` | root directory for everything the server stores |
| `timeseries_backend` | `"duckdb"` | `"duckdb"` or `"timescale"` |
| `pg_dsn` | none | Postgres DSN; required for the timescale backend |
| `duckdb_path` | `data_dir/timeseries.duckdb` | duckdb file location |
| `graph_path` | `data_dir/.oxigraph` | graph store location |
| `embedding_model` | `"BAAI/bge-small-en-v1.5"` | model for the text-resolution index |
| `recreate` | `false` | wipe the data directory and start fresh; see below |
| `read_batch_size` | `50000` | rows per Arrow batch on timeseries reads |
| `workers` | `1` | must stay 1 |
| `enabled` | `true` | `false` skips the HTTP server and pushes `[[drivers]]` to a remote server |

Relative paths resolve against the config file's directory.

`recreate = true` deletes the following contents of the data directory at
startup: graph, timeseries, embedding caches, app sources, driver state.

### Environment variables

| variable | replaces |
|---|---|
| `ACQUIRIUM_DATA_DIR` | `[server] data_dir` |
| `PG_DSN` | `[server] pg_dsn` |
| `ACQUIRIUM_DUCKDB_PATH` | `[server] duckdb_path` |
| `ACQUIRIUM_TIMESERIES_BACKEND` | `[server] timeseries_backend` |
| `ACQUIRIUM_GRAPH_PATH` | `[server] graph_path` |
| `ACQUIRIUM_EMBEDDING_MODEL` | `[server] embedding_model` |
| `ACQUIRIUM_RECREATE` | `[server] recreate` |
| `ACQUIRIUM_WORKERS` | `[server] workers` |

An environment variable has higher precedence compared to the toml variables.
Check the environment when a config edit appears to have no effect.

## The [driver] and [[drivers]] sections

These declare the drivers the server starts; they are documented in the
[driver reference](drivers.md#configuration).
One key concerns the server itself: `[driver] server_url` and `server_port`
are the address driver and app actors use to reach the server.
The default (`localhost` and the `[server]` port) is right for a single-host
setup; set them when the bind address is not reachable under that name.

## The [ontologies] section

The package ships five ontologies, registered under canonical IRIs at
startup:

| ontology | canonical IRI |
|---|---|
| NAWI Water | `urn:nawi-water-ontology` |
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

| area | endpoints |
|---|---|
| liveness | `GET /health`, `GET /graph_version`, `GET /embedding_status` |
| graph | `POST /insert_graph`, `GET /export_graph`, `POST /sparql_update`, `POST /validate_graph`, `GET /namespace/list` |
| sparql | `GET /sparql` (standards-compatible), `GET\|POST /sparql_json` (legacy `{columns, rows}`) |
| timeseries | `POST /register_datasource`, `POST /insert_timeseries`, `POST /insert_timeseries_arrow`, `GET /timeseries`, `POST /timeseries_info` |
| resolution | `GET /resolve_text`, `POST /resolve_record`, `POST /resolve_unit`, `POST /resolve_conversion`, `POST /conversion_factors` |
| drivers | `POST /drivers/start`, `POST /drivers/stop`, `GET /drivers/list` |
| apps | `POST /apps/register`, `POST /apps/delete`, `POST /apps/run`, `POST /apps/stop`, `GET /apps/list` |
| logbook | `POST /insert_log`, `GET /query_logs`, `DELETE /delete_logs` |

Three conventions:

- Almost every failure is an HTTP 400 with the reason in `detail`.
  404 is reserved for an unknown driver or app, 409 for registering an app
  name that already exists.
- `GET /timeseries` streams Arrow record batches, not JSON.
- `/sparql_json` accepts POST with a JSON body because resolved traversal
  queries exceed URL length limits; the client always POSTs.
- `GET /sparql` is the SPARQL 1.1 endpoint for outside tools, with content
  negotiation on `Accept`. `/sparql_json` stays for the Python client's
  `{columns, rows}` contract.
- Graph reads take `include_dependencies` (default true) and `wait_for_fresh`
  (default false).
