---
name: acquirium-server
description: Run, configure and debug an Acquirium server — config keys, storage backends, startup, HTTP API.
load_when: The task involves starting a server, editing acquirium.toml, choosing a backend, or debugging server behavior.
human_doc: ../reference/server-config.md
---

# Acquirium server ops

One process hosts everything: graph store, timeseries store, HTTP API, Ray
actors for drivers and apps. `acquirium server --config acquirium.toml`.
Without `--config` it uses `./acquirium.toml` if present, else defaults.
`--host`/`--port` override `[server] host`/`port`; `--reload` forces
`workers=1`; `--verbose`/`-v` sets `ACQUIRIUM_VERBOSE=1`.

## Hard rules

- `workers` must be 1. The CLI refuses `workers > 1` (`--workers`,
  `ACQUIRIUM_WORKERS`, or `[server] workers`). A second server on the same
  `graph_path` fails at startup with "Cannot open Oxigraph store" — the
  embedded graph store is single-process on every backend.
- Environment variables BEAT the config file. The CLI copies `[server]` keys
  into the environment with `setdefault`, so an existing value wins:
  `ACQUIRIUM_DATA_DIR`, `PG_DSN`, `ACQUIRIUM_DUCKDB_PATH`,
  `ACQUIRIUM_TIMESERIES_BACKEND`, `ACQUIRIUM_GRAPH_PATH`,
  `ACQUIRIUM_EMBEDDING_MODEL`, `ACQUIRIUM_RECREATE`, `ACQUIRIUM_WORKERS`.
  `host`, `port`, `read_batch_size`, `enabled` have no env var. Env-only
  knobs: `ACQUIRIUM_ONTOENV_ROOT` (default `data_dir`),
  `ACQUIRIUM_APP_STORAGE_ROOT` (default `data_dir/apps`),
  `FASTEMBED_CACHE_PATH` (default `data_dir/embedding_cache/models`).
  When a config edit has no effect, check the environment first.
- `recreate = true` DELETES the data directory contents (graph, timeseries
  file, embedding caches, app sources under `apps/`, driver state under
  `drivers/`) and drops the timescale tables when that backend is used. Never
  set it on a real deployment. It logs a WARNING on every start.
- `/health` means the core is up, NOT that drivers/apps run. Those start in
  the background after; check `GET /drivers/list` and `GET /apps/list`.
- A reference node failing the canonical-URI check
  (`ref URI != compute_ref_uri(source_id, ref_name)`) aborts startup and
  fails every later `insert_graph`. Fix the model file; do not mint ref URIs
  by hand.
- `[ontologies]` is read from the file named by `ACQUIRIUM_CONFIG`, which the
  CLI sets. Bare uvicorn skips the `[server]`-to-env step and, without
  `ACQUIRIUM_CONFIG`, drops user ontology sources silently — always use the
  CLI.
- Replacing a bundled ontology requires the `{ source, as }` form (loads with
  overwrite); a plain string at an already-populated IRI is skipped at DEBUG.
  A failing user source logs a WARNING and startup continues.
- Relative paths resolve against the config file's directory: `data_dir`,
  `duckdb_path`, `graph_path` in `[server]`, file entries in
  `[ontologies] sources`, and `path.py:Class` driver specs.
- `insert_graph` / `sparql_update` REQUIRE a `source_id`; triples land in that
  owner's graph (`plant` for the shared model, `app:<name>`, or a driver's
  source). `replace` defaults to `True` on the HTTP body and in the Python
  client, and replaces only that owner's graph. Driver and app code uses
  `self.insert_graph(...)` / `self.sparql_update(...)`, which fix the owner
  and default `replace=False`.
- Graph reads take `include_dependencies` (default true: inferred deployment
  data + ontology/shape triples) and `wait_for_fresh` (default false: serve the
  last complete cache while a rebuild runs; every caller waits for the first
  cache).

## Config skeleton

```toml
[server]
host = "0.0.0.0"
port = 8000
data_dir = ".acquirium"
timeseries_backend = "duckdb"      # or "timescale" + pg_dsn (or PG_DSN env)
# duckdb_path = "<data_dir>/timeseries.duckdb"
# graph_path  = "<data_dir>/.oxigraph"
# embedding_model = "BAAI/bge-small-en-v1.5"
# recreate = false
# read_batch_size = 50000          # rows per Arrow batch on /timeseries; toml-only
# enabled = true                   # false: skip HTTP, push [[drivers]] to [driver] server

[ontologies]
# sources = ["./extensions.ttl", { source = "...", as = "<canonical IRI>" }]

[driver]
server_url  = "localhost"          # address actors dial back on ("0.0.0.0" becomes localhost)
server_port = 8000                 # default: the port the server actually bound

[[drivers]]
spec = "..."
```

## Backend choice

duckdb (default): one file, zero ops, own connection per read, no
compression; keys `timeseries` rows by an `INTEGER ref_id` via a `ref_ids`
table (sequence-assigned, never exposed; the `timeseries_streams` view joins
`ref_uri` back). timescale: Postgres + TimescaleDB via `pg_dsn`, hypertable
with compression (segment by `ref_uri`, `ts DESC`) and a 7-day compression
policy, keyed by `ref_uri` directly, needed when other tools must reach the
data. Same logical schema either way; the API speaks `ref_uri` on both.

## Startup order (what to expect in logs)

config → `[server]` copied to env → (recreate wipe) → timeseries store →
graph store + ontologies → embedding indexes (cold build takes minutes,
cached on disk by a hash of the concept set) → streams-table sync (can abort)
→ `ray.init` + supervisors → HTTP serves (`/health` up) → background: poll
`/health`, restore persisted apps, start `[[drivers]]` (each serial, setup
included; a failing entry logs and is skipped).

The streams-table sync queries the FRESH inferred graph, so every
`insert_graph` request pays for one inference pass. Batch registrations into
one call rather than looping. Inserted data is not embedded: the embedding
corpus is the ontologies only, and `/insert_graph` always answers
`embedding_ready: true`.

Client side: `Acquirium()` blocks on `/health` up to 60 s
(`health_timeout=None` or `0` to skip) and raises `ConnectionError` after.

## Debugging

- `--verbose` / `-v`: DEBUG for `acquirium.*` loggers only; root stays INFO.
  Every HTTP request then logs one `acquirium.api` line with status + ms.
  Logger names: `acquirium.api`, `.manager`, `.graph_store`, `.storage`,
  `.config`, `.ontologies`, `.embedding_matcher`,
  `acquirium.Storage.duckdb_store` / `.timescale_store` (module names),
  `acquirium.driver.<ClassName>` (driver instance), `acquirium.driver.runner`,
  `acquirium.driver.supervis` (sic), `acquirium.app.<name>`,
  `acquirium.apps.runner`, `.apps.supervisor`.
- `GET /embedding_status`: `{graph, qudt}` each with `state`
  (`idle|building|ready|error`), `concepts`, `surfaces`, `error`,
  `last_built`, `duration_s`.
- `GET /graph_version`: returns `source_version`, `published_version`,
  `is_current`, `rebuild_in_progress`. `source_version` bumps on every
  mutation; unchanged = the write did not happen. The cache is fresh when
  `published_version == source_version` and `is_current`.
- Nearly every API failure is a 400 with the reason in `detail`; 404 =
  unknown driver (`/drivers/stop`) or app (`/apps/delete`), 409 = duplicate
  app name on `/apps/register` without `?replace=true`, 500 = `/delete_logs`
  store failure. `/resolve_text` and `/resolve_record` have no handler
  wrapper, so their errors surface as 500.
- `GET /timeseries` streams Arrow IPC (`application/vnd.apache.arrow.stream`,
  schema `ts, value, uri`), not JSON; an empty result is still a valid
  stream.
- Long SPARQL goes to `POST /sparql_json` (URL length limits on GET).
  `GET /sparql` is the standards-compatible endpoint for outside tools
  (SELECT/ASK/CONSTRUCT/DESCRIBE, `Accept`-negotiated, read-only);
  `/sparql_json` keeps the client's `{columns, rows}` contract.
- `SIGTERM` is handled like Ctrl-C: drivers and apps are stopped, Ray shut
  down, stores closed.

## Endpoints

| area | endpoints |
|---|---|
| liveness | `GET /health`, `GET /graph_version`, `GET /embedding_status` |
| graph | `POST /insert_graph` (needs `source_id`), `GET /export_graph`, `POST /sparql_update`, `POST /validate_graph`, `GET /namespace/list` |
| sparql | `GET /sparql` (standards), `GET\|POST /sparql_json` (legacy `{columns, rows}`) |
| timeseries | `POST /register_datasource`, `POST /insert_timeseries`, `POST /insert_timeseries_arrow`, `GET /timeseries`, `POST /timeseries_info` |
| resolution | `GET /resolve_text`, `POST /resolve_record`, `POST /resolve_unit`, `POST /resolve_conversion`, `POST /conversion_factors` |
| drivers | `POST /drivers/start`, `POST /drivers/stop`, `GET /drivers/list` |
| apps | `POST /apps/register` (`?replace=true` to overwrite), `POST /apps/delete`, `POST /apps/run`, `POST /apps/stop`, `GET /apps/list` |
| logbook | `POST /insert_log`, `GET /query_logs`, `DELETE /delete_logs` |

FastAPI also serves `/docs` and `/openapi.json`.
