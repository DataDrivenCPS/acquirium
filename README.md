# Acquirium
A Data-Metadata Framework for Water Treatment Plants

Acquirium is a framework for storing, managing, querying, and integrating data and metadata for water treatment systems. It combines knowledge graphs and time series data to support analysis, monitoring, and experimentation.

## Installation

From PyPI:

```bash
pip install acquirium
```

Optional extras for specific drivers:

```bash
pip install "acquirium[mqtt]"       # MQTT ingestion driver
pip install "acquirium[xlsx]"       # Excel ingestion driver
pip install "acquirium[watertap]"   # WaterTAP simulation driver
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install acquirium
```

For development from a clone:

```bash
git clone https://github.com/DataDrivenCPS/acquirium.git
cd acquirium
python -m venv .venv && source .venv/bin/activate
pip install -e .
# or: uv sync
```

## Quickstart

**Easiest way to experiment with acquirium is through an example. We strongly recommend following the steps in [watertap readme](./deployments/WATERTAP/readme.md). Watertap is a simulation tool that has integration to acquirium. If you follow the steps there, you'll be able to run acquirium as if it's connected to a live plant that generates data or generate historical data that has realistic (abides physical formulations) ranges and try building applications over it.**


Acquirium ships a single CLI entry point. Start the server and any configured drivers with:

```bash
acquirium server --config acquirium.toml
```

A sample `acquirium.toml` is included at the repository root. Key sections:

- `[server]` — bind host/port, choice of timeseries backend (DuckDB or TimescaleDB), data directory.
- `[driver]` — connection defaults applied to all drivers (server URL, port, tick interval).
- `[[drivers]]` — drivers to start alongside the server.

By default the server stores data on local disk — an embedded Oxigraph RDF store and a single DuckDB file under `data_dir`. **No external services are required for a fresh install.** For multi-worker or production deployments, switch the config to `timeseries_backend = "timescale"` and point `pg_dsn` at a Postgres + TimescaleDB instance.

Override the bind host/port from the CLI if needed:

```bash
acquirium server --config acquirium.toml --host 127.0.0.1 --port 8000
acquirium server --config acquirium.toml --reload          # uvicorn auto-reload
```

## Driver-only mode

To run only `[[drivers]]` against a remote Acquirium server (no FastAPI on this host), set:

```toml
[server]
enabled = false
```

and configure `[driver].server_url` / `server_port` to point at the remote instance. Then:

```bash
acquirium server --config acquirium.toml
```

When `enabled = false`, the `server` subcommand starts only the drivers.

## Docker stack (optional)

A `compose.yaml` is provided for an all-in-one local stack (Acquirium + TimescaleDB + Grafana):

```bash
make up                              # start
make up ACQUIRIUM_RECREATE=true      # wipe data + start
make down                            # stop
```

> By default each Docker run resets the system. To preserve data across runs, set `ACQUIRIUM_RECREATE=false` in `compose.yaml`.

## WaterTAP integration

**The [WaterTAP deployment](./deployments/WATERTAP/readme.md) is the recommended
starting point** — it walks you through cloning the repo, installing (uv **or**
pip), and running Acquirium against physically realistic simulated plant data,
with example notebooks. Start there.

In short, the `watertap` extra installs the Python packages needed for the
built-in WaterTAP driver, plus a one-time install of native solver extensions:

```bash
pip install "acquirium[watertap]"
idaes get-extensions                        # native IDAES/IPOPT solver binaries
# with uv: uv sync --extra watertap && uv run idaes get-extensions
acquirium server --config acquirium.toml    # with a [[drivers]] entry for WaterTAP
```

For a full demo (WaterTAP + streaming simulator + API examples):

```bash
make watertap-up
uv run scripts/api_example.py
# or open the notebooks in notebooks/watertap/
make watertap-down
```

## Logging

Acquirium supports user logs attached to entities in the system. See [scripts/logging_example.py](./scripts/logging_example.py):

```bash
acquirium server --config acquirium.toml &
python scripts/logging_example.py
```

## Text Matcher

Acquirium uses a text matcher to map natural-language input to ontology URIs (classes, predicates, units, quantity kinds). The match algorithm uses **semantic embedding similarity** powered by [FastEmbed](https://github.com/qdrant/fastembed) (default model: `BAAI/bge-small-en-v1.5`). Each ontology concept is represented by one or more surface strings, embedded and stored in an in-memory vector index. At query time the input phrase is embedded and compared against the index using cosine similarity.

There are two separate matchers, each with its own index:

1. **Graph matcher** — indexes classes and predicates from user-inserted RDF graphs. Surface strings are derived from `rdfs:label` values and CamelCase/underscore-split local names.
2. **QUDT matcher** — indexes units and quantity kinds from the QUDT ontology, which ships bundled inside the `acquirium` package and is registered at the versionless canonical IRIs `https://qudt.org/vocab/unit` and `https://qudt.org/vocab/quantitykind`. Override either by adding a `{ source = "...", as = "<canonical IRI>" }` entry to `[ontologies] sources` in `acquirium.toml`. Surface strings include `rdfs:label`, `skos:prefLabel`, `skos:altLabel`, symbols, UCUM codes, and split local names.

Both indexes are cached to disk and updated incrementally when graphs change. Results can be filtered by `kind` (`class`, `predicate`, `unit`, `quantity_kind`) and are ranked by cosine similarity, deduplicated to the highest-scoring surface per URI. See [scripts/text_matcher_example.py](./scripts/text_matcher_example.py) for usage.

## Tests

```bash
pytest tests/unit            # unit tests only
make test                    # full suite (Docker required)
```

## Status

Acquirium is under active development. Planned work is tracked in [improvements.md](./improvements.md). Bug reports and feature requests are welcome — please open an issue.
