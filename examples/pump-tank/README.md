# Pump schedule experiment

Record a pump schedule optimization with Acquirium. A pump fills a storage
tank, and the tank supplies a water demand that changes through the day.
SciPy's `linprog` chooses the pump's hourly flow so the tank stays between its
minimum and capacity at the lowest electricity cost.

The environment installs this Acquirium checkout, SciPy, marimo, and
matplotlib. The lock file pins their versions.

## Notebook

From this directory:

```bash
uv sync
uv run marimo edit ../../docs/tutorials/first-experiment.py
```

A running server keeps the code it started with. After updating Acquirium,
stop the old server and restart the notebook kernel. A 404 for
`/experiments/metadata/sync` means the notebook connected to an older server.

The notebook declares variables, records a baseline in separate cells, then
runs four more scenarios, one of them infeasible. It reopens the Study,
compares the scenarios one row per run, reads back a saved schedule table and
tank-volume samples, and compares a saved result with site measurements.
The notebook supplies synthetic measurements; at your site, use the stream
collected by your driver.

`pump-tank-model.ttl` describes the pump, the tank, and their properties.

## Script

```bash
uv run python record_experiment.py
```

The script starts a local Acquirium server and records one baseline run:

- the input files;
- the initial tank volume and pump capacity;
- the operating cost;
- the hourly schedule as a table;
- tank-volume samples;
- the solver's status.

It prints the Experiment ID, the cost, the tank-volume stream URI, the first
rows of the saved schedule, and the baseline runs' scalar results, then stops
the server. Data stays in `.data/`. Each execution reuses the Study and
creates a new run.

## Input files

- `inputs/site.json`: the start time, tank capacity and minimum volume, and
  the pump's energy use per cubic metre.
- `inputs/hourly.csv`: two days of hourly water demand and electricity price.

## See also

- [Recording guide](../../docs/how-to/record-experiments.md): recipes for saving and reading results.
- [Compare runs with formulas](../../docs/how-to/compare-experiment-runs.md): one row per run and formulas.
- [API reference](../../docs/reference/experiments.md): methods grouped by task.
- [Experiments explained](../../docs/explanation/experiments.md): Studies, runs, variables, and recordings.
