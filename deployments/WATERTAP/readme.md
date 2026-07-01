# WaterTAP × Acquirium deployment

This deployment wires [WaterTAP](https://watertap.readthedocs.io/) process models
into Acquirium as live data sources. Each model is a steady-state flowsheet
(built on IDAES/Pyomo) that is re-solved over time — either against synthetic
operating conditions or live user input — and its mapped variables are ingested
as timeseries streams, semantically described by an `s223`/`nawi` RDF model.

There are two models and a set of generic, model-agnostic tools that drive any
of them.

## Quick Start
1. create and activate a fresh virtual environment: `python -m venv .venv && source .venv/bin/activate` (Linux/macOS) or `.venv\Scripts\activate` (Windows). **Requires Python 3.12**+.
    - Alternatively you can use the [uv package manager](https://docs.astral.sh/uv/) with `uv init --python 3.12`
2. Install Acquirium from PyPI: `pip install acquirium[watertap]`.
    - Alternatively: `uv add acquirium[watertap]`
3. Start the server (plus any drivers listed in the config), from the repo root:
    - `acquirium server --config deployments/WATERTAP/scripts/acquirium.toml`.
    - Alternatively, `uv run acquirium server --config deployments/WATERTAP/scripts/acquirium.toml`.
4. Verify it's up by opening [`http://localhost:8000/docs`](http://localhost:8000/docs) (or whichever host/port your config sets) in a browser.
    - Alternatively: `curl localhost:8000/health` from another terminal
    - Or using Python session or notebook, run:
    ```
    from acquirium import Acquirium 
    acq = Acquirium(server_url="localhost", server_port=8000, use_ssl=False)
    ```
5. We distribute our examples with jupyter notebooks. There're multiple ways to run a jupyter notebook:
    - If you used uv for initial setup: `uv run --with jupyter jupyter lab` will start a jupyterlab in browser (`http://localhost:8888` by default)
    - If you use VS code, when you try to run a notebook it will ask py environments. Provide the one you initiated above (.venv)
    - If neither, then run the following in your .venv (make sure it's active)
        - ` pip install ipykernel `
        - ` pip install notebook `
        - ` python -m ipykernel install --user --name=venv --display-name "Python (venv)" `
        - ` jupyter notebook `

6. Notebooks are in [this folder](../../notebooks/watertap/). We'll add new notebooks when we want to demonstrate new features! We recommend to follow the notebooks in order.


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

### `simple-pipe` — single pump
A minimal one-unit flowsheet (a `Pump` on a seawater stream) — the smallest
end-to-end example. **5 mapped properties.** Varying inputs: inlet
**temperature**, **pressure**, fresh-water and saltwater **mass flows**; the
mapped output of interest is the pump's mechanical work (`Pump1-Work`).

## Layout

```
deployments/WATERTAP/
├── models/
│   ├── seawater-ro/   build-and-solve.py · generate-values.py · model.ttl · watertap-mapping.json
│   └── simple-pipe/   build-and-solve.py · generate-values.py · model.ttl · watertap-mapping.json
└── scripts/
    ├── data-generator.py     batch: write parquet snapshots over time
    ├── simulation_driver.py  live: auto-generate inputs, solve, ingest
    ├── gui_driver.py         live: solve on GUI input changes, ingest
    ├── input_gui.py          generic Streamlit GUI for any model's inputs
    └── acquirium.toml        driver configuration (one active block)
```

## Generating data (batch → parquet)

`scripts/data-generator.py` builds a model once, then walks `--N` timestamps,
re-solving at each, and writes **wide** parquet snapshots (one column per mapped
property) into `models/<name>/data/`:

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
| **Parquet** | `acquirium.BuiltinDrivers.parquet_ingest:ParquetIngestDriver` | Watches `models/<name>/data/` and replays generated parquet snapshots into Acquirium. |

Both WaterTAP drivers inherit `WaterTAPDriver` and so share its
`build → change_inputs → solve → read` pipeline. The GUI is **model-agnostic**:
it introspects whatever model folder it is pointed at (via
`generate_new_values`) to build one field per input variable.

Run the configured drivers (from the repo root, against a running server):

```bash
acquirium run --config deployments/WATERTAP/scripts/acquirium.toml
```

## Changing the config (`scripts/acquirium.toml`)

The TOML ships with **exactly one `[[drivers]]` block enabled** (the seawater-ro
simulation) and the rest commented out — six blocks in total covering both
models × {simulation, GUI, parquet}. To switch, comment the active block and
uncomment another; enable several at once to run them together.

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
| `watertap_source_id` | datasource id under which streams register |

**Path bases differ by setting** (existing Acquirium behavior, noted in the TOML
header):

- `watertap_*_spec`, `watertap_mapping_path`, `watertap_graph_path`,
  `watertap_model_dir` → relative to the **current working directory** (run from
  the repo root).
- driver `spec`, parquet `watch_dir`, GUI `gui_script_path` /
  `watertap_inputs_path` → relative to **the config file's directory**.
