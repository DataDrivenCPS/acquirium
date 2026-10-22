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

The server hosts the graph store, the timeseries store, the HTTP API, the
bounded in-process worker pool that runs apps, and the child processes that
run drivers.
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
3. Read the registered streams from the graph.
   A reference node failing the canonical-URI check aborts startup.
4. Restore app deployments. If the engine runs a plan that `apps.json` does not
   record, startup fails instead of retracting every derived output.
5. Serve HTTP. `/health` answers from this point.
6. In the background, start the `[[drivers]]` entries and then deploy the
   `[[apps]]` entries. Drivers start first because their setup may load the
   graph that app queries use.

Note that `/health` only means the core is up; drivers and apps may still be
starting.
Check `GET /drivers/list` for configured drivers and
`GET /apps` and `GET /engine/status` for app bindings and execution state.
The `Acquirium()` client constructor waits for `/health` (60 seconds by
default) so scripts can start before the server finishes booting.

A cold first start builds the embedding indexes and loads the ontologies,
and can take 5-10 minutes.
A warm restart with an intact data directory is much faster.

## Docker

`compose.yaml` runs the server with Mosquitto. `compose.minimal.yaml` is the
same stack without profiles, hardcoded to `acquirium.docker.toml`. Overrides
come from `.env` (see `.env.example`); they are environment variables, so they
override the toml keys.
`compose.testing.yaml` runs the same services on offset ports (server 8010,
MQTT 11883) for the integration test suite; the Makefile's `testing-up`, `test`
and `wait-health` targets wrap it.
