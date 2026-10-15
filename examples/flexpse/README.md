# FlexPSE experiment

Record a FlexPSE optimization with Acquirium. The model uses the
[`api_freeze` fixture](https://github.com/flex-pse/flex-pse/tree/v0.1.0/src/flexops/tests/fixtures/api_freeze):
a tank, a treatment plant, and a battery on a time-varying tariff. This example
sets treatment demand to 10 m³/h; the upstream fixture leaves it free.

The environment installs this Acquirium checkout, FlexPSE `v0.1.0`, and HiGHS
through `flex-pse[solvers]`. The lock file pins the FlexPSE commit.

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
runs two more scenarios. It reopens the Study, selects saved runs and variables,
reads power samples, and compares costs and trajectories.

It also shows how to find a site stream and compare it with saved tank-volume
results. The notebook supplies synthetic measurements for this example.
At your site, use the stream collected by your driver.

`flexpse-watr-model.ttl` describes the facility, tank, plant, battery, and their
properties.

## Script

```bash
uv run python record_experiment.py
```

The script starts a local Acquirium server and records:

- model, tariff, and demand-response files;
- treatment flow;
- solver completion;
- operating cost;
- electrical-power samples.

It prints the Experiment ID, objective, stream URI, first five samples, and
tables of saved baseline runs and power-output variables, then stops the server.
Data stays in `.data/`. Each execution reuses the Study and creates a new run.

- [Recording guide](../../docs/how-to/record-experiments.md): recipes for saving and reading results.
- [API reference](../../docs/reference/experiments.md): methods grouped by task.
- [Experiments explained](../../docs/explanation/experiments.md): Studies, runs, variables, and recordings.

## Input files

The files under `inputs/` match FlexPSE's `v0.1.0` API-freeze fixtures. They are
redistributed under its
[Apache-2.0 license](https://github.com/flex-pse/flex-pse/blob/v0.1.0/LICENSE).
