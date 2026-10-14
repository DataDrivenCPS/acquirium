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

The notebook records one baseline in separate cells, then two more scenarios
in a loop. It reopens the Study to compare costs and trajectories.

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

It prints the Experiment ID, objective, stream URI, and first five samples,
then stops the server. Data stays in `.data/`. Each execution reuses the Study
and creates a new run.

## Input files

The files under `inputs/` match FlexPSE's `v0.1.0` API-freeze fixtures. They are
redistributed under its
[Apache-2.0 license](https://github.com/flex-pse/flex-pse/blob/v0.1.0/LICENSE).
