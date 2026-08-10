---
name: acquirium-server
description: Run, configure and debug an Acquirium server — config keys, storage backends, startup, HTTP API.
load_when: The task involves starting a server, editing acquirium.toml, choosing a backend, or debugging server behavior.
human_doc: ../server.md
---

# Acquirium server ops

One process hosts everything: graph store, timeseries store, HTTP API, Ray
actors for drivers and apps. `acquirium server --config acquirium.toml`.

## Hard rules

- `workers` must be 1. A second worker or a second server on the same
  `data_dir` fails at startup (embedded graph store is single-process, on
  every backend).
- Environment variables BEAT the config file (`ACQUIRIUM_DATA_DIR`,
  `PG_DSN`, `ACQUIRIUM_TIMESERIES_BACKEND`, `ACQUIRIUM_GRAPH_PATH`,
  `ACQUIRIUM_RECREATE`, ...). When a config edit has no effect, check the
  environment first.
- `recreate = true` DELETES the data directory contents (graph, timeseries,
  embedding caches, app sources, driver state). Never set it on a real
  deployment.
- `/health` means the core is up, NOT that drivers/apps run. Those start in
  the background after; check `GET /drivers/list` and `GET /apps/list`.
- A reference node failing the canonical-URI check aborts startup. Fix the
  model file; do not mint ref URIs by hand.
- `[ontologies]` is read from the config file only. Starting via bare
  uvicorn drops user ontology sources silently — always use the CLI.
- Replacing a bundled ontology requires the `{ source, as }` form; a plain
  string at an already-populated IRI is skipped.
- Relative paths in `[server]` resolve against the config file's directory.

## Config skeleton

```toml
[server]
host = "0.0.0.0"
port = 8000
data_dir = ".acquirium"
timeseries_backend = "duckdb"      # or "timescale" + pg_dsn
# read_batch_size = 50000          # toml-only, no env var

[ontologies]
# sources = ["./extensions.ttl", { source = "...", as = "<canonical IRI>" }]

[driver]
server_url  = "localhost"          # address actors dial back on
server_port = 8000

[[drivers]]
spec = "..."
```

## Backend choice

duckdb (default): one file, zero ops, own connection per read, no
compression. timescale: Postgres + TimescaleDB extension via `pg_dsn`,
hypertable with automatic 7-day chunk compression, needed when other tools
must reach the data. Same schema either way.

## Startup order (what to expect in logs)

config → stores open + ontologies load → embedding indexes (cold build takes
minutes, cached by ontology content afterward) → streams-table sync (can
abort) → HTTP serves (`/health` up) → background: app restore, `[[drivers]]`
start.

Client side: `Acquirium()` blocks on `/health` up to 60 s
(`health_timeout=None` to skip).

## Debugging

- `--verbose` / `-v`: DEBUG for `acquirium.*` loggers only. Logger names:
  `acquirium.api`, `.manager`, `.graph_store`, `.storage`,
  `acquirium.driver.<ClassName>`, `.apps.supervisor`.
- `GET /embedding_status`: per-index build state when resolution misbehaves.
- `GET /graph_version`: bumps on every graph mutation; unchanged version =
  the write did not happen.
- Nearly every API failure is a 400 with the reason in `detail`; 404 =
  unknown driver/app, 409 = duplicate app name.
- `GET /timeseries` streams Arrow, not JSON; do not parse it as JSON.
- Long SPARQL goes to `POST /sparql_json` (URL length limits on GET).

## Endpoints

| area | endpoints |
|---|---|
| liveness | `GET /health`, `/graph_version`, `/embedding_status` |
| graph | `POST /insert_graph`, `GET /export_graph`, `GET\|POST /sparql_json`, `POST /sparql_update`, `GET /namespace/list` |
| timeseries | `POST /register_datasource`, `POST /insert_timeseries`, `POST /insert_timeseries_arrow`, `GET /timeseries`, `POST /timeseries_info` |
| resolution | `GET /resolve_text`, `POST /resolve_record`, `/resolve_unit`, `/resolve_conversion`, `/conversion_factors` |
| drivers | `POST /drivers/start`, `POST /drivers/stop`, `GET /drivers/list` |
| apps | `POST /apps/register`, `/apps/delete`, `/apps/run`, `/apps/stop`, `GET /apps/list` |
| logbook | `POST /insert_log`, `GET /query_logs`, `DELETE /delete_logs` |
