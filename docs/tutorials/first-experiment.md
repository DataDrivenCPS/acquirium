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

## 2. Describe the experiment record

Open `record_experiment.py`. The first executable line initializes Acquirium
with a persistent data directory. The script then defines a reusable Study and
declares what each run will record:

```python
# --- Acquirium: describe what each experiment records -----------------------
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
and logs are declared once and reused by each execution. These declarations do
not save any values. They return typed handles—`model_inputs`,
`treatment_flow`, `operating_cost`, and so on—that know how a later
`record()` call should store a value.

The declaration controls what `record()` does:

| Handle declaration | Effect of `record(value)` |
|---|---|
| `.file(...)` | Copy the file at `value` into Acquirium's artifact store |
| `.scalar(...)` | Store the number, including its declared unit, in the Experiment ledger |
| `.timeseries(...)` | Write timestamp/value rows to a stream owned by this Experiment |
| `study.log(...)` | Append an event to the Experiment ledger |

The labels are the persistent names stored by Acquirium; the Python variables
are only handles for recording values into the currently active Experiment.

The electrical-power output needs an `observed` URI because time-series results
are ordinary Acquirium streams. This standalone example uses a stable URI for
the modeled facility load. When a plant knowledge graph is available, use the
URI of the matching observable property instead.

## 3. Start a run and capture its inputs

The first recording block is entirely Acquirium code. It starts one Experiment
for the baseline scenario, copies the three input files, and records the
treatment flow that this execution will use:

```python
# --- Acquirium: start this experiment and snapshot its inputs ----------------
experiment = study.start(
    metadata={"model": "api-freeze", "scenario": "baseline"}
)
try:
    for path in (
        INPUTS / "model.json",
        INPUTS / "tariff.json",
        INPUTS / "dr_events.json",
    ):
        model_inputs.record(path)
    treatment_flow.record(TREATMENT_FLOW)
```

`study.start()` makes this run active. From that point until `finish()` or
`fail()`, calls on the Study's handles are routed to this Experiment.

`model_inputs.record(path)` sees that `model_inputs` was declared as a file and
copies the file at `path`. The Experiment therefore retains its own immutable
input artifacts if the working files change later.

`treatment_flow.record(TREATMENT_FLOW)` sees a scalar handle and stores the
number with the handle's declared `M3-PER-HR` unit. Metadata describes the run
as a whole; recorded variables are the inputs and outputs you want to compare
across runs.

## 4. Build and solve with FlexPSE

The next block is the model calculation. It uses FlexPSE and Pyomo in the usual
way; the experiment interface does not replace or wrap the model or solver:

```python
    # --- FlexPSE: build and solve the model ----------------------------------
    os.chdir(INPUTS)
    model = build_model(load_model_config(INPUTS / "model.json"))

    for t in model.time_block.time_index:
        model.waterfacility.tank.flow_in[t].fix(TREATMENT_FLOW)
        model.waterfacility.plant.flow_out[t].fix(TREATMENT_FLOW)

    pyo.TransformationFactory("network.expand_arcs").apply_to(model)
    results = get_solver(model=model, prefer="highs").solve(model)
    assert_optimal_termination(results)

    objective = float(pyo.value(model.objective))
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
```

The config-driven builder produces the same model as FlexPSE's imperative
`api_freeze.py`. Its configuration refers to the other two inputs by bare
filename, so the script uses their directory while constructing the model. The
two fixed flow variables are this example's added operating condition; they
give the solve a nonzero load while preserving tank inventory.

## 5. Record the results

Once FlexPSE has produced ordinary Python values, a second Acquirium block
saves them:

```python
    # --- Acquirium: save the results and complete the experiment -------------
    operating_cost.record(objective)
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

`operating_cost.record(objective)` stores one scalar observation.
`electrical_power.record(power_rows)` dispatches to the time-series operation:
it accepts the timestamp/value pairs, creates a stream unique to this run, and
records that stream's URI and time range in the Experiment ledger. The
example's timezone-naive model timestamps are interpreted as UTC.

The same method name deliberately covers each variable kind so recording sites
stay small. Calling `record()` repeatedly adds observations; it does not
replace an earlier value. On a log handle it appends events in order.
`finish()` makes the run terminal with status `succeeded` and prevents further
recording.

## 6. Preserve failures

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

## 7. Read and plot the time-series output

Time-series results use Acquirium's ordinary stream storage. The final block
reconstructs this run's deterministic reference URI and reads the samples back
as a Polars DataFrame:

```python
# --- Acquirium: read back the time-series result -----------------------------
power_ref = ac.reference_uri(
    f"experiment/{experiment.run_id}",
    electrical_power.label,
)
stored_power = ac.client.timeseries_df(str(power_ref))
```

In a notebook, that frame can be plotted directly with the usual dataframe and
plotting tools:

```python
import matplotlib.pyplot as plt

plt.plot(stored_power["ts"], stored_power["value"])
plt.ylabel("Aggregate electrical power (kW)")
plt.show()
```

The current experiment interface does not yet expose a public read API for
listing a Study's completed Experiments or retrieving their scalar, JSON,
text, and log observations. Cross-run tables, comparisons, and operations such
as “which Experiment had the highest operating cost?” therefore cannot yet be
expressed through `study`. That is a missing query surface, not something the
time-series query API solves.

## 8. Run the experiment

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
shape: (5, 3)
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
