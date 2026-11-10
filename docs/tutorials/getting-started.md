---
title: Getting started
---

This tutorial installs Acquirium from PyPI, generates a config file, starts a
server and runs one query.
The starter config collects CPU, memory, disk and network readings of your
machine, so there is data to query without a plant model.

## 1. Install

Acquirium requires Python 3.11 or newer.
Create a directory for the project and install the package with either pip or
uv.

With pip and a virtual environment:

```bash
mkdir my-plant && cd my-plant
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install acquirium
```

With uv:

```bash
uv init my-plant && cd my-plant
uv add acquirium
```

Note that with uv, the `acquirium` and `python` commands below are run as
`uv run acquirium ...` and `uv run python ...`.

Some drivers need extra dependencies, which are installed as extras:
`acquirium[mqtt]`, `acquirium[xlsx]`, `acquirium[watertap]`.

## 2. Generate a config file

```bash
acquirium config init
```
```text
Wrote acquirium.toml
Start the server with: acquirium server --config acquirium.toml
```

This writes `acquirium.toml` into the current directory.
An existing file is not overwritten unless `--force` is passed.

## 3. Start Acquirium

There are two ways to start Acquirium:

### Option A: run the server

```bash
acquirium server
```

Default `--config`, is `acquirium.toml` at the working directory.
It keeps running until you stop it with Ctrl+C.
This is useful when drivers should keep collecting data, or when several
scripts and notebooks use the same server.

Connect to it from a second terminal or a notebook:

```python
from acquirium import Acquirium

acq = Acquirium(server_url="127.0.0.1", server_port=8000)
```

See [run the server](../how-to/run-the-server.md) for the command's flags and
the startup order.

### Option B: start it from Python

```python
import acquirium as aq

acq = aq.init()      # reads ./acquirium.toml and starts a local server
# ... use acq ...
aq.shutdown()
```

`aq.init()` starts the server in the background and returns the same client
as option A.
The server stops at `aq.shutdown()` or when the script exits. The data stays
in `data_dir`.
This is useful for a single script or notebook.

See [start Acquirium from a script](../how-to/local-runtime.md) for the
arguments of `aq.init()` and for sharing one server between scripts.

## 4. Run a query

The system metrics driver in the starter config describes your machine as a
`Computer` with six measurement points, and takes a reading every 30 seconds.
Wait about a minute after the start, then run:

```python
(acq.query()
 .entity("Computer")
 .measurement(alias="m", quantity_kind="dimensionless ratio")
 .dataframe()
 .tail(2))
```
```text
shape: (2, 4)
┌────────────────────────────────┬──────────────┬──────────────────────┬──────────────┐
│ time                           ┆ m__CPU usage ┆ m__Disk usage (root) ┆ m__RAM usage │
│ ---                            ┆ ---          ┆ ---                  ┆ ---          │
│ datetime[μs, UTC]              ┆ f64          ┆ f64                  ┆ f64          │
╞════════════════════════════════╪══════════════╪══════════════════════╪══════════════╡
│ 2026-11-10 21:50:48.834927 UTC ┆ 79.8         ┆ 5.1                  ┆ 75.8         │
│ 2026-11-10 21:51:20.641139 UTC ┆ 70.3         ┆ 5.1                  ┆ 76.5         │
└────────────────────────────────┴──────────────┴──────────────────────┴──────────────┘
```

The query selects every `Computer` in the graph, then the measurements of it
whose quantity kind is a dimensionless ratio, which are the three percentages.
`dataframe()` fetches their readings as a polars frame.
The [querying tutorial](querying.md) covers the query interface.

## The config file

These are the keys of the starter `acquirium.toml` that matter first.
Relative paths resolve against the directory of the config file.

| key | starter value | meaning |
|---|---|---|
| `[server] host` | `"127.0.0.1"` | The address the server listens on. `127.0.0.1` accepts connections from this machine only; `"0.0.0.0"` accepts connections from other machines. `aq.init()` always uses `127.0.0.1`. |
| `[server] port` | `8000` | The port the server listens on. Pass the same value as `server_port` to `Acquirium(...)`. |
| `[server] data_dir` | `".acquirium"` | The directory that holds the graph store, the DuckDB timeseries file and the embedding cache. |
| `[server] timeseries_backend` | `"duckdb"` | Where readings are stored. `"duckdb"` is a local file under `data_dir`. `"timescale"` uses TimescaleDB and requires `pg_dsn`. |
| `[server] exact_only` | `true` | How free text such as `"dimensionless ratio"` is matched to ontology terms. `true` matches labels exactly. `false` also matches by meaning; it downloads an embedding model and builds indexes on the first start, which can take several minutes. See [text resolution](../explanation/text-resolution.md). |
| `[server] recreate` | `false` | `true` deletes all stored data every time the server starts. |
| `[driver] interval` | `10.0` | Seconds between two polls of a driver. This is the default for every `[[drivers]]` entry. |
| `[[drivers]] spec` | system metrics driver | The driver class to start with the server, as `module:Class` or `path/to/file.py:Class`. Each `[[drivers]]` entry starts one driver and can override the `[driver]` defaults. |

The [server configuration reference](../reference/server-config.md) lists
every `[server]` key, and the
[driver reference](../reference/drivers.md#configuration) lists the driver
keys and the built-in drivers.

## Next steps

- [Load a plant model](../how-to/load-a-plant-model.md) to describe your own
  equipment and points.
- [Write a first driver](first-driver.md) to ingest your own data.
- [Querying](querying.md) and [working with data](data.md) for the query
  interface. Their examples use the WaterTAP seawater-ro model; the
  [WaterTAP deployment guide](https://github.com/DataDrivenCPS/acquirium/blob/main/deployments/WATERTAP/readme.md)
  shows how to run it.
