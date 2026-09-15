# FlexPSE experiment

This example tracks a FlexPSE optimization with Acquirium's experiment
interface. It is based on FlexPSE's
[`api_freeze` fixture](https://github.com/flex-pse/flex-pse/tree/main/src/flexops/tests/fixtures/api_freeze):
a tank feeds a constant-energy-intensity treatment plant, and a battery can
shift the combined electrical load against a time-varying tariff. The upstream
fixture deliberately leaves operating conditions free; this example adds and
records a constant 10 m³/h treatment demand so the solve has a nonzero load.

The derived `pyproject.toml` installs this Acquirium checkout as an editable
path dependency. FlexPSE documents `flex-pse[solvers]` as the way to install
HiGHS for LP/MILP models. Because that project name is not yet published on
PyPI, the uv source table maps it to FlexPSE's official `v0.1.0` tag; the lock
file pins the tag's commit. This example does not require the separately
installed IPOPT extensions.

From this directory, create the environment:

```bash
uv sync
```

Open the guided notebook:

```bash
uv run marimo edit ../../docs/tutorials/first-experiment.py
```

The notebook loads `flexpse-watr-model.ttl`, which represents the facility,
tank, plant, battery, and their properties in the plant graph. It runs three
scenarios, reopens their Study without redeclaring variables, and compares the
baseline tank trajectory with an explicitly synthetic deployment stream. Both
streams point at the model's `tank-volume` property but retain separate
simulation and deployment provenance.

The synthetic rows demonstrate where data from a real driver would enter. They
are generated only because this example does not ship measurements from an
actual facility.

Run the model and record its Experiment:

```bash
uv run python record_experiment.py
```

The script calls `aq.init(data_dir=".data")`, which starts an exact-only local
Acquirium runtime on an available loopback port. No separate server or config
file is required. It shuts the runtime down after printing the result; normal
interpreter exit also cleans it up after an error.

The script records:

- the model configuration, tariff, and demand-response files;
- the scenario's treatment flow;
- a structured solver-completion event;
- the aggregate operating cost;
- the optimized aggregate electrical-power trajectory.

Section comments in `record_experiment.py` separate the ordinary FlexPSE model
build and solve from the Acquirium calls that declare variables, record inputs
and outputs, and finish the Experiment.

It prints the Experiment ID, objective value, time-series reference URI, and
the first five stored power samples. Run it again to reuse the Study definition
and create a separate Experiment and result stream.

The local runtime keeps its data under `.data/`, so subsequent executions reuse
the Study and stored results.

The files under `inputs/` match FlexPSE's `v0.1.0` API-freeze fixtures and are
redistributed under FlexPSE's
[Apache-2.0 license](https://github.com/flex-pse/flex-pse/blob/v0.1.0/LICENSE).

For a guided walkthrough, see
[`docs/tutorials/first-experiment.py`](../../docs/tutorials/first-experiment.py).
