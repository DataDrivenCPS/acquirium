---
title: Running experiments
---

In this article we'll describe experiments.
An experiment records what went into a calculation or a test, and what came out, so the runs can be found and compared later.
The records are kept next to the plant data, and can refer to it.

This page is useful if you run simulations, optimisations, pilot tests or what-if studies.

This page is also available as a [Python script](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/advanced_experiments.py) and a [Jupyter notebook](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/advanced_experiments.ipynb).

## The pieces

- A **study** is one question, such as "how does the pump power change with speed?".
- A **variable** is a named input or output of the study, with a unit.
- A **run** is one execution of the study, with its own values.

## 1. Create a study and declare variables

`acq.study.define()` creates a study, or opens it again if the name already exists.
The study below has one input and one output:

```python
import acquirium as aq

acq = aq.init()

study = acq.study.define("pump-speed")

speed = study.input("speed").scalar(unit="Hz")
power = study.output("power").scalar(unit="kW")
```

- `scalar()` is a variable that holds one number per run.
- The unit is matched the same way as for streams (see [3 - Inserting Metadata](../inserting-metadata.md)).

Other kinds of variables hold text, tables, files and timeseries (see the [experiment reference](../../reference/experiments/declaring-variables.md)).

## 2. Record runs

`study.start()` starts a run, and `finish()` ends it.
In between, `record()` saves a value of a variable.

The loop below records three runs.
The dictionary given to `start()` describes the run, with keys of your own:

```python
for hz in [40, 45, 50]:
    run = study.start({"operator": "ana", "pump": "P1"})

    speed.record(hz)
    result = round(1100 * (hz / 50) ** 3, 1)     # your calculation or test goes here
    power.record(result)

    run.finish()
```

Acquirium saves the start time, the end time and the status of each run automatically:

```python
study.experiments.frame().select("experiment_id", "status", "started_at")
```
```text
shape: (3, 3)
┌─────────────────────────────────┬───────────┬────────────────────────────────┐
│ experiment_id                   ┆ status    ┆ started_at                     │
│ ---                             ┆ ---       ┆ ---                            │
│ str                             ┆ str       ┆ datetime[μs, UTC]              │
╞═════════════════════════════════╪═══════════╪════════════════════════════════╡
│ 1251d7a8-25bf-49d0-9df2-054527… ┆ succeeded ┆ 2026-11-18 05:31:43.867577 UTC │
│ c222dd3d-8fef-45f1-9844-ca4f2d… ┆ succeeded ┆ 2026-11-18 05:31:43.934990 UTC │
│ e70de41f-822e-403b-a4b1-82b5ec… ┆ succeeded ┆ 2026-11-18 05:31:43.997784 UTC │
└─────────────────────────────────┴───────────┴────────────────────────────────┘
```

**Note:** If your calculation can fail, call `run.fail(error)` instead of `finish()`. The values recorded so far are kept, and the run is marked as failed.

## 3. Read the results back

`scalars()` gives one row per run and one column per variable.
This is useful for comparing runs:

```python
study.experiments.scalars("speed", "power", metadata=["operator"])
```
```text
shape: (3, 4)
┌─────────────────────────────────┬──────────┬───────┬────────┐
│ experiment_id                   ┆ operator ┆ speed ┆ power  │
│ ---                             ┆ ---      ┆ ---   ┆ ---    │
│ str                             ┆ str      ┆ i64   ┆ f64    │
╞═════════════════════════════════╪══════════╪═══════╪════════╡
│ 1251d7a8-25bf-49d0-9df2-054527… ┆ ana      ┆ 40    ┆ 563.2  │
│ c222dd3d-8fef-45f1-9844-ca4f2d… ┆ ana      ┆ 45    ┆ 801.9  │
│ e70de41f-822e-403b-a4b1-82b5ec… ┆ ana      ┆ 50    ┆ 1100.0 │
└─────────────────────────────────┴──────────┴───────┴────────┘
```

`where()` filters the runs, by status or by the keys given to `start()`:

```python
study.experiments.where(status="succeeded", operator="ana").scalars()
```

One variable can also be read across the runs:

```python
power.observations().frame().select("experiment_id", "value")
```
```text
shape: (3, 2)
┌─────────────────────────────────┬────────┐
│ experiment_id                   ┆ value  │
│ ---                             ┆ ---    │
│ str                             ┆ f64    │
╞═════════════════════════════════╪════════╡
│ 1251d7a8-25bf-49d0-9df2-054527… ┆ 563.2  │
│ c222dd3d-8fef-45f1-9844-ca4f2d… ┆ 801.9  │
│ e70de41f-822e-403b-a4b1-82b5ec… ┆ 1100.0 │
└─────────────────────────────────┴────────┘
```

## 4. Connect an experiment to the plant

This section uses the plant of [5 - A Full Example](../a-full-example.md).
Run that page first, and run this page in the same folder.

### Plant data as an input

`annotate_stream()` links a time range of a stream to a run.
It records which data the run used, and gives that data back as a dataframe:

```python
from datetime import datetime, timezone

p1_power = acq.stream("csv-data", "P1-mechanical-power")

run = study.start({"operator": "ana", "pump": "P1"})

measured = run.annotate_stream(
    p1_power.stream_uri,
    start=datetime(2025, 7, 1, tzinfo=timezone.utc),
    end=datetime(2025, 7, 1, 3, tzinfo=timezone.utc),
)
df = measured.dataframe()
df
```
```text
shape: (4, 2)
┌─────────────────────────┬──────────┐
│ time                    ┆ value    │
│ ---                     ┆ ---      │
│ datetime[μs, UTC]       ┆ f64      │
╞═════════════════════════╪══════════╡
│ 2025-07-01 00:00:00 UTC ┆ 1.0658e6 │
│ 2025-07-01 01:00:00 UTC ┆ 1.0913e6 │
│ 2025-07-01 02:00:00 UTC ┆ 1.0295e6 │
│ 2025-07-01 03:00:00 UTC ┆ 1.0421e6 │
└─────────────────────────┴──────────┘
```

The run can record a result calculated from it:

```python
power.record(df["value"].mean() / 1000)
```

### A timeseries result

A result that changes over time is a `timeseries()` variable.
`observed=` names the property of the plant model that the result describes:

```python
predicted = study.output("predicted power").timeseries(
    observed="urn:swro/P1-mechanical-power",
    unit="W",
)

predicted.record([(t, v * 0.98) for t, v in zip(df["time"], df["value"])])

run.finish()
```

Each run keeps its own series.
It is read back from the run:

```python
run.observations(predicted).latest().dataframe()
```
```text
shape: (4, 2)
┌─────────────────────────┬──────────┐
│ time                    ┆ value    │
│ ---                     ┆ ---      │
│ datetime[μs, UTC]       ┆ f64      │
╞═════════════════════════╪══════════╡
│ 2025-07-01 00:00:00 UTC ┆ 1.0445e6 │
│ 2025-07-01 01:00:00 UTC ┆ 1.0695e6 │
│ 2025-07-01 02:00:00 UTC ┆ 1.0089e6 │
│ 2025-07-01 03:00:00 UTC ┆ 1.0213e6 │
└─────────────────────────┴──────────┘
```

**Note:** Declaring a variable while a run is active gives a warning. Declare the variables of a study before its first run when you can.

## Model adapters

Declaring variables one by one does not scale to a model with hundreds of values, such as a WaterTAP flowsheet.
A model adapter declares and records every variable of a model in one call.
This is described in [Record a whole model](../../how-to/record-a-whole-model.md).

## Next

- The [experiment reference](../../reference/experiments/_index.md) describes every method.
- [Record experiments](../../how-to/record-experiments.md) and [Compare experiment runs](../../how-to/compare-experiment-runs.md) cover more cases.
- [How experiments work](../../explanation/experiments.md) explains what is stored and where.
