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
uv sync
```

## Quickstart

**The easiest way to experiment with acquirium is through an example. We strongly recommend following the steps in the [WaterTAP readme](./deployments/WATERTAP/readme.md).** WaterTAP is a simulation tool with an acquirium integration; following the steps there runs acquirium as if it were connected to a live plant, generating physically realistic data you can query and build applications on.

The server and its configured drivers start from one command:

```bash
acquirium server --config acquirium.toml
```

A sample `acquirium.toml` is included at the repository root. By default everything is stored on local disk (an embedded Oxigraph RDF store and a DuckDB file under `data_dir`); no external services are required. The first start builds the text-resolution indexes and can take minutes; later starts reuse the cache.

Querying is a Python client:

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)
acq.query().entity("pump").measurement(quantity_kind="pressure").dataframe(shape="wide")
```

## Documentation

The guides live in [docs/](./docs/index.md):

| guide | covers |
|---|---|
| [Querying](./docs/querying.md) | finding equipment, topology and measurements |
| [Working with data](./docs/data.md) | fetching timeseries, shapes, units, writing data |
| [Building drivers](./docs/drivers.md) | feeding plant data in on a schedule |
| [Building apps](./docs/apps.md) | computing on plant data server-side, writing results back |
| [The data stream lifecycle](./docs/data-stream-lifecycle.md) | how streams are identified, stored and found |
| [Running the server](./docs/server.md) | config, storage backends, ontologies, HTTP API |
| [Text resolution](./docs/resolution.md) | free text to URIs; unit conversion |
| [Graph backend](./docs/graph-backend-architecture.md) | graph ownership, inference, query views |
| [HTTP API](./docs/http-api.md) | the raw endpoints, for scripting against the server |

Working with a coding agent? Point it at [AGENTS.md](./AGENTS.md). (Coming Soon!)

## Docker stack (optional)

A `compose.yaml` is provided for an all-in-one local stack (Acquirium + TimescaleDB + Grafana):

```bash
make up                              # start
make up ACQUIRIUM_RECREATE=true      # wipe data + start
make down                            # stop
```

> By default each Docker run resets the system. To preserve data across runs, set `ACQUIRIUM_RECREATE=false` in `compose.yaml`.

## WaterTAP integration

The `watertap` extra installs the Python packages for the built-in WaterTAP driver, plus a one-time install of native solver extensions:

```bash
pip install "acquirium[watertap]"
idaes get-extensions                        # native IDAES/IPOPT solver binaries
# with uv: uv sync --extra watertap && uv run idaes get-extensions
acquirium server --config deployments/WATERTAP/scripts/acquirium.toml
```

The [WaterTAP deployment readme](./deployments/WATERTAP/readme.md) covers the models, the data generator, and the example notebooks under [notebooks/watertap/](./notebooks/watertap/).

## Text resolution

Free text anywhere in the API (class names, units, quantity kinds) is matched to ontology URIs by embedding similarity, using two indexes built from the bundled ontologies at server start. See the [resolution guide](./docs/resolution.md).

## Tests

```bash
uv run pytest tests/unit     # unit tests only (or: make unit-test)
make test                    # full suite (Docker required)
```

## Status

Acquirium is under active development. Planned work is tracked in [improvements.md](./improvements.md). Bug reports and feature requests are welcome — please open an issue.
