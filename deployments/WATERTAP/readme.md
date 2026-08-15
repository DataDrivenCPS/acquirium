# WaterTAP × Acquirium deployment

This deployment wires [WaterTAP](https://watertap.readthedocs.io/) process models
into Acquirium as live data sources. Each model is a steady-state flowsheet
(built on IDAES/Pyomo) that is re-solved over time — either against synthetic
operating conditions or live user input — and its mapped variables are ingested
as timeseries streams, semantically described by an `s223`/`nawi` RDF model.

There are two models and a set of generic, model-agnostic tools that drive any
of them.

## Quick Start

> **New to Acquirium?** Acquirium is a framework for storing, querying, and
> integrating time-series data together with the knowledge graph (metadata)
> that describes it — see the [top-level README](../../README.md). This
> deployment is the recommended starting point: it runs Acquirium as if it were
> connected to a live water-treatment plant, using WaterTAP simulations to
> produce physically realistic data.

These examples live in the Acquirium repository (config files, models, and
notebooks), so the first step for **both** setups below is to clone it:

```bash
git clone https://github.com/DataDrivenCPS/acquirium.git
cd acquirium
```

Everything after this assumes you are in the repo root. Pick **one** of the two
setups below.

### Option A — using [uv](https://docs.astral.sh/uv/) (recommended)

uv manages the virtual environment and the right Python version for you.

1. Install Acquirium (uv fetches Python 3.12 if needed):
    ```bash
    uv sync
    ```
    The WaterTAP driver's own dependencies (watertap, pyomo, the native
    IDAES/IPOPT solvers) are declared in its `[[drivers]]` config entry via
    `env = { pip = [...], setup_commands = ["idaes get-extensions"] }` — the
    server builds the driver its own environment on first start, so nothing
    WaterTAP-specific is installed here.
2. Start the server (plus any drivers listed in the config):
    ```bash
    uv run acquirium server --config deployments/WATERTAP/scripts/acquirium.toml
    ```
3. Run the example notebooks (in a second terminal, repo still the working dir):
    ```bash
    uv run --with jupyter jupyter lab
    ```
    JupyterLab opens in your browser (`http://localhost:8888` by default).

### Option B — using pip + venv

**Requires Python 3.12+** already installed.

1. Create and activate a fresh virtual environment:
    ```bash
    python3.12 -m venv .venv
    source .venv/bin/activate          # Linux/macOS
    # .venv\Scripts\activate           # Windows (PowerShell/cmd)
    ```
2. Install Acquirium (from the clone):
    ```bash
    pip install -e .
    ```
    As in Option A, WaterTAP's packages and the native IDAES/IPOPT solver
    install are declared in the driver's `[[drivers]]` config entry
    (`env = { pip = [...], setup_commands = ["idaes get-extensions"] }`) and
    installed into the driver's own environment on first start.
3. Start the server (plus any drivers listed in the config):
    ```bash
    acquirium server --config deployments/WATERTAP/scripts/acquirium.toml
    ```
4. Run the example notebooks. With the `.venv` active:
    ```bash
    pip install jupyterlab
    jupyter lab
    ```
    If you prefer VS Code, just open a notebook and select the `.venv`
    interpreter when prompted. To register the venv as a named Jupyter kernel:
    ```bash
    pip install ipykernel
    python -m ipykernel install --user --name=venv --display-name "Python (venv)"
    ```

### Verify it's running

Once the server is up (either option), confirm it:

- Open [`http://localhost:8000/docs`](http://localhost:8000/docs) in a browser
  (or whichever host/port your config sets), **or**
- `curl localhost:8000/health` from another terminal

### Notebooks

Example notebooks live in [`notebooks/watertap/`](../../notebooks/watertap/).
We add new ones as we demonstrate new features — **we recommend starting with quickstart.ipynb**


## Models

Every model folder under `models/<name>/` exposes the **same interface**, so one
generator and one set of drivers work for all of them:

| File | Role |
|------|------|
| `build-and-solve.py` | `build() -> m`, `change_inputs(m, d)`, `solve(m)` — the Pyomo model lifecycle |
| `generate-values.py` | `generate_new_values(ts, rng) -> dict` — realistic, deterministic feed inputs for a timestamp |
| `watertap-mapping.json` | maps each ontology point URI (`urn:.../...`) to a Pyomo variable path; the driver contract |
| `model.ttl` | the `s223`/`nawi` semantic model: equipment, connection points, and sensor properties |

### `seawater-ro` — full seawater RO desalination plant
The WaterTAP `seawater_RO_desalination` flowsheet (pretreatment → RO with
pressure-exchanger energy recovery → posttreatment). **29 mapped properties.**
Varying inputs: seawater **temperature**, **TDS** salinity, **TSS** turbidity,
and demand-following intake **flow**. The high-pressure pump is held at 70 bar,
so recovery and permeate quality float with conditions.

### `seawater-ro-fouled` — seawater RO with a fouling membrane
Same flowsheet as `seawater-ro`, but the RO membrane's water permeability
(`A_comp`) is no longer fixed: `generate-values.py` holds it at its pristine
value for the first `FOULING_ONSET_DAYS` (a clean operating baseline), then
drives it down a slow exponential decline (cake layer / biofilm resistance
building up over time), and `build-and-solve.py:change_inputs` re-fixes it
before every re-solve — so one continuous time series covers both a
known-clean period and a fouling event, mirroring a single real plant's
operating history rather than two parallel plants. Permeate flow visibly
degrades once fouling begins, at constant feed pressure. **30 mapped
properties** (adds `RO-membrane-water-permeability`). Uses its own
`urn:swro-fouled/` namespace so it can be ingested alongside the healthy
`seawater-ro` model without colliding ontology points.

### `simple-pipe` — single pump
A minimal one-unit flowsheet (a `Pump` on a seawater stream) — the smallest
end-to-end example. **5 mapped properties.** Varying inputs: inlet
**temperature**, **pressure**, fresh-water and saltwater **mass flows**; the
mapped output of interest is the pump's mechanical work (`Pump1-Work`).

## Layout

```
deployments/WATERTAP/
├── models/
│   ├── seawater-ro/          build-and-solve.py · generate-values.py · model.ttl · watertap-mapping.json
│   ├── seawater-ro-fouled/   build-and-solve.py · generate-values.py · model.ttl · watertap-mapping.json
│   └── simple-pipe/          build-and-solve.py · generate-values.py · model.ttl · watertap-mapping.json
└── scripts/
    ├── data-generator.py     batch: write parquet snapshots over time
    ├── simulation_driver.py  live: auto-generate inputs, solve, ingest
    ├── parquet_driver.py     live: watches data directory of the model and loads
    ├── gui_driver.py         live: solve on GUI input changes, ingest
    ├── input_gui.py          generic Streamlit GUI for any model's inputs
    └── acquirium.toml        driver configuration (two active blocks)
```

## Generating data (batch → parquet)

`scripts/data-generator.py` builds a model once, then walks `--N` timestamps,
re-solving at each, and writes **wide** parquet snapshots (one column per mapped
property) into `models/<name>/data/`. For instance the following command will generate 1 week of data with 1 hour intervals:

```bash
.venv/bin/python deployments/WATERTAP/scripts/data-generator.py seawater-ro --N 168 -T 1h
```

| Flag | Default | Meaning |
|------|---------|---------|
| `model` (positional) | — | model folder name (e.g. `seawater-ro`) or a path |
| `--N` | 24 | number of data points |
| `-T`, `--interval` | `1h` | spacing (`30m`, `1h`, `2d`, `90s`) |
| `-X`, `--points-per-file` | 50000 | max rows per parquet file |
| `--start` | `2025-01-01T00:00:00` | ISO start timestamp |
| `--seed` | 42 | RNG seed (reproducible) |

Same `--seed` + `--start` → identical output. Files are named
`<model>_<first>-<last>_<timestamp>.parquet` and sort chronologically.

We provide one example pq file in seawater-ro model folder, you can add more data to the same folder using the generator. 


## Drivers

All drivers ingest into a running Acquirium server. The WaterTAP drivers read
points from a model's `watertap-mapping.json`; registering a stream per point
writes its external reference and Pyomo variable
(`ref:hasExternalReference` / `acq:hasPyomoVar`) via Acquirium's insert-graph
interface — no hand-authored reference graph needed.

| Driver | Spec | What it does |
|--------|------|--------------|
| **Simulation** | `simulation_driver.py:SimulationDriver` | Each tick calls `generate_new_values`, applies them via `change_inputs`, solves, and ingests. Fully autonomous. |
| **GUI** | `gui_driver.py:GuiDriver` | Launches `input_gui.py` (Streamlit) for the model's inputs; solves + ingests **only when a value changes**. Decoupled via a shared JSON file. |
| **Parquet** | `parquet_driver.py:WaterTAPParquetDriver` | Watches `models/<name>/data/` and replays generated parquet snapshots into Acquirium. |

Both WaterTAP drivers inherit `WaterTAPDriver` and so share its
`build → change_inputs → solve → read` pipeline. The GUI is **model-agnostic**:
it introspects whatever model folder it is pointed at (via
`generate_new_values`) to build one field per input variable.

Run the configured drivers (from the repo root, against a running server):

```bash
acquirium driver start deployments/WATERTAP/scripts/acquirium.toml
```

## Changing the config (`scripts/acquirium.toml`)

The TOML ships with **one `[[drivers]]` block enabled** (seawater-ro parquet
ingestion) and the rest commented out — eight blocks in total covering all three
models (`seawater-ro`, `seawater-ro-fouled`, `simple-pipe`) × {simulation, GUI,
parquet}, except `seawater-ro-fouled` which only has simulation and parquet
variants (no GUI driver). To switch, comment the active block and uncomment
another; enable several at once to run them together.

**NOTE:**
- **We recommend default settings for working with the regulation.ipynb example.**

Per-driver keys live in each `[[drivers]]` entry (merged over the shared
`[driver]` section). Key WaterTAP options:

| Key | Purpose |
|-----|---------|
| `watertap_mapping_path` | model `watertap-mapping.json` (the points to ingest) — **required** |
| `watertap_build_spec` / `watertap_solve_spec` | `file.py:fn` for `build` / `solve` — **required** |
| `watertap_change_inputs_spec` | `file.py:change_inputs` to apply inputs before solving |
| `watertap_generate_spec` | `file.py:generate_new_values` (simulation driver) |
| `watertap_inputs` | static input dict (base `WaterTAPDriver`) |
| `watertap_graph_path` + `watertap_insert_graph` | insert the model's `model.ttl` so point nodes carry domain semantics |
| `source_id` | datasource id under which streams register |

**Path bases differ by setting** (existing Acquirium behavior, noted in the TOML
header):

- `watertap_*_spec`, `watertap_mapping_path`, `watertap_graph_path`,
  `watertap_model_dir` → relative to the **current working directory** (run from
  the repo root).
- driver `spec`, parquet `watch_dir`, GUI `gui_script_path` /
  `watertap_inputs_path` → relative to **the config file's directory**.
