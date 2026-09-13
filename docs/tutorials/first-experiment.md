---
title: Your first experiment
---

An optimization script usually leaves its result in memory and its context in
the files around it. In this tutorial, you will run the model in
[`examples/flexpse`](../../examples/flexpse/README.md) and use an Acquirium
Experiment to preserve its inputs, operating cost, solver events, and optimized
electrical-power trajectory.

The model is based on FlexPSE's
[`api_freeze` example](https://github.com/flex-pse/flex-pse/tree/main/src/flexops/tests/fixtures/api_freeze):
a tank feeds a constant-energy treatment plant while a battery can shift the
electrical load against a time-varying tariff. Because the upstream API fixture
leaves operating conditions free, this example adds and records a constant
10 m³/h treatment demand.

## 1. Create the example environment

From the Acquirium repository, enter the example and synchronize its derived
project:

```bash
cd examples/flexpse
uv sync
```

Its `pyproject.toml` installs the current Acquirium checkout as an editable
dependency and installs `flex-pse[solvers]` from FlexPSE's official `v0.1.0`
tag. FlexPSE's solver extra includes HiGHS, which is sufficient for this LP. It
does not need the separately installed IPOPT extensions used by nonlinear
models.

The script starts its own exact-only local Acquirium runtime, so there is no
server or configuration file to manage. Its graph and time-series data remain
under `examples/flexpse/.data/`.

## 2. Initialize Acquirium and define the Study

Open `record_experiment.py`. The first executable line initializes Acquirium
with a persistent data directory. The script then defines a reusable Study and
declares what each run will record:

```python
ac = aq.init(data_dir=HERE / ".data")
study = ac.study.define("flexpse-api-freeze")

model_inputs = study.input("model input files").file(
    media_type="application/json"
)
treatment_flow = study.input("treatment flow").scalar(unit="M3-PER-HR")
operating_cost = study.output("aggregate operating cost").scalar(unit="USD")
electrical_power = study.output("aggregate electrical power").timeseries(
    observed="urn:flex-pse:api-freeze:aggregate-electrical-power",
    unit="KiloW",
)
solver_events = study.log("solver events")
```

`aq.init()` starts the embedded services on an available loopback port and
returns the usual Acquirium client. Exact-only mode avoids building the
semantic-resolution indexes this example does not use. See
[Run Acquirium locally](../how-to/local-runtime.md) for other initialization
options.

A Study describes a family of comparable calculations. Its inputs, outputs,
and logs are declared once and reused by each execution. The labels are the
names stored by Acquirium; the Python variables are handles used to record
values during the active run.

The electrical-power output needs an `observed` URI because time-series results
are ordinary Acquirium streams. This standalone example uses a stable URI for
the modeled facility load. When a plant knowledge graph is available, use the
URI of the matching observable property instead.

## 3. Start a run and capture its inputs

The script starts one Experiment for the baseline scenario:

```python
experiment = study.start(
    metadata={"model": "api-freeze", "scenario": "baseline"}
)
try:
    input_paths = (
        INPUTS / "model.json",
        INPUTS / "tariff.json",
        INPUTS / "dr_events.json",
    )
    for path in input_paths:
        model_inputs.record(path)
    treatment_flow.record(TREATMENT_FLOW)

    os.chdir(INPUTS)
    model = build_model(load_model_config(INPUTS / "model.json"))
    for t in model.time_block.time_index:
        model.waterfacility.tank.flow_in[t].fix(TREATMENT_FLOW)
        model.waterfacility.plant.flow_out[t].fix(TREATMENT_FLOW)
    solver_events.record({"event": "model-built"})
```

Metadata describes this execution. The file handle copies the configuration,
tariff, and demand-response inputs into Acquirium, so the record remains stable
if the working files change later. The scalar handle records the scenario's
10 m³/h treatment flow.

The config-driven builder produces the same model as FlexPSE's imperative
`api_freeze.py`. Its configuration refers to the other two inputs by bare
filename, so the script uses their directory while constructing the model. The
two fixed flow variables add the constant treatment condition while preserving
tank inventory.

## 4. Solve and record the results

The next part follows FlexPSE's documented solve sequence, then records the
scalar objective and the time-varying aggregate electrical power:

```python
    pyo.TransformationFactory("network.expand_arcs").apply_to(model)
    results = get_solver(model=model, prefer="highs").solve(model)
    assert_optimal_termination(results)

    objective = float(pyo.value(model.objective))
    operating_cost.record(objective)

    power_rows = [
        (
            timestamp,
            pyo.value(
                pyo.units.convert(
                    model.costing.aggregate_electrical_power[t],
                    to_units=pyo.units.kW,
                )
            ),
        )
        for timestamp, t in zip(
            model.time_block.datetime_index,
            model.time_block.time_index,
            strict=True,
        )
    ]
    electrical_power.record(power_rows)

    solver_events.record(
        {
            "event": "solve-complete",
            "status": str(results.solver.status),
            "termination_condition": str(results.solver.termination_condition),
        }
    )
    experiment.finish()
```

The scalar handle records one objective value. The time-series handle accepts
timestamp/value pairs and stores the month-long trajectory under a source
unique to this run. The example's timezone-naive model timestamps are
interpreted as UTC by the experiment interface.

Calling `record()` on the log again appends an event rather than replacing the
first. `finish()` makes the run terminal with status `succeeded`.

## 5. Preserve failures

The solver and recording operations stay inside a `try` block so failures also
become part of the record:

```python
except Exception as error:
    experiment.fail(error)
    raise
```

`fail(error)` makes the run terminal with status `failed` and records the
exception type and message. Inputs and events written before the failure remain
available when diagnosing how far the run progressed.

## 6. Run the experiment

Execute the example:

```bash
uv run python record_experiment.py
```

The script fetches the stored power samples through their deterministic
reference URI and prints output resembling:

```text
experiment: 5a38...
aggregate operating cost: 465.89 USD
electrical power: urn:acquirium#...
shape: (5, 2)
...
```

Run the script again to reuse the Study declarations while creating a new
Experiment and power stream. That is the parameter-sweep pattern: keep the
declarations fixed, put scenario-specific context in metadata and inputs, and
start one Experiment per solve. The final `aq.shutdown()` stops the local
runtime cleanly; the recorded data remains in `.data/` for the next run.

## Where to go next

- [Record experiments](../how-to/record-experiments.md) — record other value
  types, exploratory outputs, plant-linked time series, and stored input
  references.
- [Experiment reference](../reference/experiments.md) — the complete Python and
  HTTP interface.
- [How experiments preserve context](../explanation/experiments.md) — why
  Studies, runs, artifacts, and streams have separate identities.
