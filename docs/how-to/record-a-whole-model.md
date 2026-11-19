---
title: Record a whole model
---

In this article we'll describe model adapters.
A model adapter lists the values of a model that a study should record, with a function that reads each value.
The study then declares all the variables in one call, and records all the values in one call.

This is useful for a model with many values, where declaring and recording them one by one takes a lot of code.
Read the tutorial [Running experiments](../tutorials/advanced/experiments.md) first.

## 1. Build an adapter

The examples use a small model of a pump, written in plain Python.
It stands for your own model:

```python
from datetime import datetime, timedelta, timezone

import acquirium as aq

acq = aq.init()


class PumpModel:
    def __init__(self):
        self.speed = 50.0          # Hz
        self.rated_power = 1100.0  # kW
        self.power = 0.0           # kW
        self.pressure = 0.0        # bar

    def step(self, hour):
        self.power = round(self.rated_power * (self.speed / 50) ** 3, 1)
        self.pressure = round(60 * (self.speed / 50) ** 2 + 0.5 * hour, 2)


model = PumpModel()
```

`aq.adapters.define()` creates an empty adapter.
Each line below adds one value, with a `read` function that returns the current value from the model:

```python
adapter = aq.adapters.define()

adapter.input.scalar("speed", read=lambda: model.speed, unit="Hz")
adapter.input.scalar("rated power", read=lambda: model.rated_power, unit="kW")
adapter.output.scalar("power", read=lambda: model.power, unit="kW")
adapter.output.timeseries("pressure", read=lambda: model.pressure, unit="bar")
```

- `adapter.input` and `adapter.output` set the role of the variable.
- The kinds are `scalar()`, `text()`, `json()`, `table()`, `object()` and `timeseries()`.
- A `timeseries()` value is read once per time step. The other kinds are read once per run.

`frame()` lists what the adapter declares, and `preview()` reads every value from the model.
Both work without a study, so the adapter can be checked before anything is recorded:

```python
print(adapter.frame().select("label", "role", "kind", "unit"))
print(adapter.preview())
```
```text
shape: (4, 4)
┌─────────────┬────────┬────────────┬──────┐
│ label       ┆ role   ┆ kind       ┆ unit │
│ ---         ┆ ---    ┆ ---        ┆ ---  │
│ str         ┆ str    ┆ str        ┆ str  │
╞═════════════╪════════╪════════════╪══════╡
│ speed       ┆ input  ┆ scalar     ┆ Hz   │
│ rated power ┆ input  ┆ scalar     ┆ kW   │
│ power       ┆ output ┆ scalar     ┆ kW   │
│ pressure    ┆ output ┆ timeseries ┆ bar  │
└─────────────┴────────┴────────────┴──────┘
{'speed': 50.0, 'rated power': 1100.0, 'power': 0.0, 'pressure': 0.0}
```

## 2. Declare the variables in a study

`declare_from()` declares every variable of the adapter in the study:

```python
study = acq.study.define("pump-model")
variables = study.declare_from(adapter)
```

`variables` holds the declared variables together with their `read` functions.
One variable is available as `variables["power"]`.

## 3. Record the runs

- `record_values()` reads and records every value that is not a timeseries.
- `sample(at=time)` reads and records one sample of every timeseries, with the given time.

The loop below records three runs.
The inputs are recorded before the model runs, one sample is recorded at each step, and the outputs are recorded at the end:

```python
start = datetime(2025, 7, 1, tzinfo=timezone.utc)

for hz in [40.0, 45.0, 50.0]:
    model.speed = hz
    run = study.start({"operator": "ana"})

    variables.input.record_values()
    for hour in range(3):
        model.step(hour)
        variables.sample(at=start + timedelta(hours=hour))
    variables.output.record_values()

    run.finish()
```

`variables.input` and `variables.output` select the variables of one role.
`variables.record_values()` records both roles at once.

## 4. Read the results

The variables are ordinary variables of the study, so the results are read in the same way as in the tutorial:

```python
print(study.experiments.scalars())
```
```text
shape: (3, 4)
┌─────────────────────────────────┬───────┬─────────────┬────────┐
│ experiment_id                   ┆ speed ┆ rated power ┆ power  │
│ ---                             ┆ ---   ┆ ---         ┆ ---    │
│ str                             ┆ f64   ┆ f64         ┆ f64    │
╞═════════════════════════════════╪═══════╪═════════════╪════════╡
│ c155695b-1db3-4414-a3f1-b093fe… ┆ 40.0  ┆ 1100.0      ┆ 563.2  │
│ 01c100fe-cbae-4cc4-948f-d9c6d6… ┆ 45.0  ┆ 1100.0      ┆ 801.9  │
│ 387a589a-aa84-4a02-a457-91b973… ┆ 50.0  ┆ 1100.0      ┆ 1100.0 │
└─────────────────────────────────┴───────┴─────────────┴────────┘
```

Each `sample()` call is one observation, so `latest()` gives only the last sample of a timeseries.
A query on the `run_id` gives all the samples of the run:

```python
print(acq.query().measurement(run_id=run.run_id).dataframe())
```
```text
shape: (3, 2)
┌─────────────────────────┬────────────────┐
│ time                    ┆ data__pressure │
│ ---                     ┆ ---            │
│ datetime[μs, UTC]       ┆ f64            │
╞═════════════════════════╪════════════════╡
│ 2025-07-01 00:00:00 UTC ┆ 60.0           │
│ 2025-07-01 01:00:00 UTC ┆ 60.5           │
│ 2025-07-01 02:00:00 UTC ┆ 61.0           │
└─────────────────────────┴────────────────┘
```

## The Pyomo adapter

`aq.adapters.pyomo(model)` is an adapter for [Pyomo](https://www.pyomo.org/) models, such as WaterTAP flowsheets.
It needs Pyomo, which is installed with `pip install "acquirium[pyomo]"`.

With this adapter a value is given as a Pyomo component instead of a `read` function.
The unit and the description are taken from the component:

```python
import pyomo.environ as pyo

model = pyo.ConcreteModel()
model.speed = pyo.Var(initialize=50, units=pyo.units.Hz, doc="Speed of the pumps")
model.flow = pyo.Var(["P1", "P2"], initialize=100, units=pyo.units.kg / pyo.units.s, doc="Flow of each pump")
model.power = pyo.Expression(
    expr=1100 * pyo.units.kW * (model.speed / (50 * pyo.units.Hz)) ** 3,
    doc="Power of the pumps",
)

adapter = aq.adapters.pyomo(model)
adapter.input.scalar("speed", model.speed)
adapter.output.scalar("power", model.power)
for var in model.flow.values():
    adapter.output.scalar(var.name, var)

print(adapter.frame().select("label", "role", "unit", "description"))
```
```text
shape: (4, 4)
┌──────────┬────────┬──────┬────────────────────┐
│ label    ┆ role   ┆ unit ┆ description        │
│ ---      ┆ ---    ┆ ---  ┆ ---                │
│ str      ┆ str    ┆ str  ┆ str                │
╞══════════╪════════╪══════╪════════════════════╡
│ speed    ┆ input  ┆ Hz   ┆ Speed of the pumps │
│ power    ┆ output ┆ kW   ┆ Power of the pumps │
│ flow[P1] ┆ output ┆ kg/s ┆ null               │
│ flow[P2] ┆ output ┆ kg/s ┆ null               │
└──────────┴────────┴──────┴────────────────────┘
```

Note that an indexed component such as `model.flow` cannot be given as a whole.
The loop adds one variable for each element of it.
For a large model, a loop over `model.component_data_objects(pyo.Var)` adds every variable of the model in the same way.

The rest is the same as with the generic adapter.
In this example the values are set by hand.
In your own script the solver sets them:

```python
study = acq.study.define("pump-pyomo")
variables = study.declare_from(adapter)

for hz in [40, 45, 50]:
    model.speed.set_value(hz)
    model.flow["P1"].set_value(2 * hz)
    model.flow["P2"].set_value(hz)

    run = study.start()
    variables.record_values()
    run.finish()

print(study.experiments.scalars())
```
```text
shape: (3, 5)
┌─────────────────────────────────┬───────┬────────┬──────────┬──────────┐
│ experiment_id                   ┆ speed ┆ power  ┆ flow[P1] ┆ flow[P2] │
│ ---                             ┆ ---   ┆ ---    ┆ ---      ┆ ---      │
│ str                             ┆ i64   ┆ f64    ┆ i64      ┆ i64      │
╞═════════════════════════════════╪═══════╪════════╪══════════╪══════════╡
│ c7ec4b2d-db0b-4913-bc63-067596… ┆ 40    ┆ 563.2  ┆ 80       ┆ 40       │
│ 40697563-f637-44c9-836b-f9ac22… ┆ 45    ┆ 801.9  ┆ 90       ┆ 45       │
│ cd3cfe69-bb6b-4fcb-8d51-14701f… ┆ 50    ┆ 1100.0 ┆ 100      ┆ 50       │
└─────────────────────────────────┴───────┴────────┴──────────┴──────────┘
```

## Use a new model object

The `read` functions of an adapter point to one model object.
If your script builds a new model object, build a new adapter for it and call `declare_from()` again.
The study keeps its variables, and the new `variables` object reads from the new model.

## Next

- [Model adapters](../reference/experiments/model-adapters.md) lists every method of an adapter.
- [Compare experiment runs](compare-experiment-runs.md) selects runs and calculates with their results.
- [Record experiments](record-experiments.md) covers the kinds of variables.
