# Running the server

This is a guide to running and configuring the acquirium server: the config
file, storage backends, ontologies, and the HTTP API.

## The server command

```bash
acquirium server --config acquirium.toml
```

The server hosts everything: the graph store, the timeseries store, the HTTP
API, and the Ray actors that run drivers and apps.
Without `--config` it looks for `acquirium.toml` in the working directory,
and starts with defaults when there is none.

| flag | meaning |
|---|---|
| `--config`, `-c` | path to the config file |
| `--host`, `--port` | bind address; override the `[server]` section |
| `--verbose`, `-v` | DEBUG logs for `acquirium.*` loggers only |
| `--reload` | uvicorn auto-reload, for development |
| `--workers`, `-w` | must stay `1`, below |

The worker count must be 1.
The embedded graph store is single-process on every backend, so a second
worker (or a second server on the same data directory) fails at startup.

The CLI also has a `driver` group (`start`, `list`, `stop`), covered in the
[drivers guide](drivers.md#operations).

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
[drivers guide](drivers.md#configuration).
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

## Storage backends

Both backends store the same logical schema: a `timeseries` table (one row
per stream and timestamp, with a `numeric_value` and a `text_value` column),
the `streams` reference table, and the logbook.
They key the rows differently.
Timescale uses `ref_uri` directly; duckdb uses an integer `ref_id` and maps it
back through a `ref_ids` table.
Reads expose `ref_uri` either way.

`duckdb` is the default: one file under the data directory, with no extra
services to install or run.
Reads run on their own connections, so a long scan does not block a driver's
inserts.
There is no compression or retention; the file grows with the data.

`timescale` stores the same tables in Postgres with the TimescaleDB
extension, addressed by `pg_dsn`.
The `timeseries` table is a hypertable, and chunks older than 7 days are
compressed automatically.

The graph store is embedded either way.
Switching the timeseries backend does not lift the single-process constraint.

## The graph store

The semantic model lives in an embedded Oxigraph store under `graph_path`.
Two datasets are kept: the source of record (one graph per data owner plus one
per ontology) and a query dataset holding the inferred deployment data and the
resolved ontology and shape triples.
Queries run against both by default; the derived data is rebuilt in the
background when something changes, and a reader gets the last complete version
until it is ready.
Pass `wait_for_fresh=True` when a query must see the current generation.
The [graph backend guide](graph-backend-architecture.md) covers this in
full.

The store keeps a source-data generation, exposed as `GET /graph_version`
along with the state of the derived query cache (`source_version`,
`published_version`, `is_current`, `rebuild_in_progress`).
`source_version` advances on every mutation.
Clients poll it to invalidate caches; drivers and apps use it for their
graph-change hooks.

The store is guarded by a single lock, so a heavy SPARQL query delays other
graph operations until it finishes.
The [querying guide](querying.md#when-a-query-returns-nothing) covers how to
keep queries bounded.

## The embedding indexes

Free-text resolution is served by two vector indexes built at startup: one
over the water and s223 ontologies (classes, predicates, substances,
processes) and one over QUDT (units, quantity kinds).
They are built from the ontologies only.
Inserted plant data is never indexed.
This is why free text resolves classes and units but not instance labels.

The first build is the expensive part of a first start; the QUDT index alone
takes minutes.
The result is cached under `data_dir/embedding_cache`, keyed by ontology
content, so later starts reuse it and a changed ontology triggers a rebuild
automatically.
`GET /embedding_status` reports the state of both indexes.

## Startup and health

Startup runs in this order:

1. Read the config, open both stores, load the ontologies.
2. Build or load the embedding indexes.
3. Sync the `streams` table from the graph.
   A reference node failing the canonical-URI check aborts startup; see the
   [lifecycle guide](data-stream-lifecycle.md#registration-and-the-streams-table).
4. Serve HTTP. `/health` answers from this point.
5. In the background: restore registered apps, then start the `[[drivers]]`
   entries.

Note that `/health` only means the core is up; drivers and apps may still be
starting.
Check `GET /drivers/list` and `GET /apps/list` for those.
The `Acquirium()` client constructor waits for `/health` (60 seconds by
default) so scripts can start before the server finishes booting.

A cold first start builds the embedding indexes and loads the ontologies,
and can take minutes.
A warm restart with an intact data directory is much faster.

## Docker

`compose.yaml` runs the server with TimescaleDB, Mosquitto and Grafana;
`compose.minimal.yaml` is the same stack without profiles, hardcoded to
`acquirium.docker.toml`.
Postgres credentials and `PG_DSN` come from `.env` (see `.env.example`).
Since these are environment variables, they override the toml keys.
`compose.testing.yaml` runs the same services on offset ports (server 8010,
Postgres 55432, MQTT 11883) for the integration test suite; the Makefile's
`testing-up`, `test` and `wait-health` targets wrap it.

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
