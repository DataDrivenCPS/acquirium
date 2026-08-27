---
title: Run the server
---

<!-- TODO: intro -->


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
[driver reference](../reference/drivers.md#operations).

## Startup and health

Startup runs in this order:

1. Read the config, open both stores, load the ontologies.
2. Build or load the embedding indexes.
3. Sync the `streams` table from the graph.
   A reference node failing the canonical-URI check aborts startup; see the
   [lifecycle guide](../explanation/stream-lifecycle.md#registration-and-the-streams-table).
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
