---
title: Record experiments
---

In this article we'll describe how to record the other kinds of values in a run, how to mark a run as failed, and how to continue a run later.
The basics of studies, variables and runs are in the tutorial [Running experiments](../tutorials/advanced/experiments.md).
Read that page first.

## Declare variables of other kinds

A scalar variable holds one number.
A study can also hold text, structured values, files, tables, Python objects, timeseries and logs.
The study below declares one variable of each kind:

```python
import json

import acquirium as aq
import polars as pl

acq = aq.init()

study = acq.study.define("ro-cleaning")

note = study.input("note").text()
settings = study.input("settings").json()
config_file = study.input("config file").file()
schedule = study.output("pump schedule").table()
state = study.output("model state").object(codec=aq.PickleCodec)
pump_power = study.output("pump power").timeseries(unit="kW", keys=("pump",))
events = study.log("events")
```

- `text()` holds a string.
- `json()` holds a dictionary, a list or any other JSON value.
- `file()` holds a copy of a file.
- `table()` holds a Polars, pandas or Arrow table.
- `object()` holds a Python object. The `codec` decides how the object is saved. `aq.PickleCodec` works for most Python objects.
- `timeseries()` holds numeric values over time. With `keys`, one variable holds several series in a run.
- `study.log()` holds events. A log is neither an input nor an output.

Inputs and outputs accept the same kinds.
The options of each kind are listed in [Declaring variables](../reference/experiments/declaring-variables.md).

## Record and read the values

`record()` is used for every kind of variable.
The run below records a text, a JSON value, a file, a table and an object:

```python
with open("config.json", "w") as f:
    json.dump({"flux": 18.5}, f)

run = study.start({"operator": "ana"})

note.record("Cleaning after the high pressure alarm")
settings.record({"chemical": "citric acid", "ph": 2.5, "steps": ["flush", "soak", "rinse"]})
config_file.record("config.json")
schedule.record(pl.DataFrame({"hour": [0, 1, 2], "P1": [True, True, False], "P2": [False, True, True]}))
state.record({"iteration": 12, "weights": [0.2, 0.8]})
```

`run.observations(variable).latest()` gives the last recorded value of a variable in the run.
Text and JSON values are read with `.value`:

```python
print(run.observations(note).latest().value)
print(run.observations(settings).latest().value)
```
```text
Cleaning after the high pressure alarm
{'chemical': 'citric acid', 'ph': 2.5, 'steps': ['flush', 'soak', 'rinse']}
```

A file is read with `read_bytes()`, a table with `dataframe()` and an object with `load()`:

```python
print(run.observations(config_file).latest().read_bytes())
print(run.observations(schedule).latest().dataframe())
print(run.observations(state).latest().load())
```
```text
b'{"flux": 18.5}'
shape: (3, 3)
┌──────┬───────┬───────┐
│ hour ┆ P1    ┆ P2    │
│ ---  ┆ ---   ┆ ---   │
│ i64  ┆ bool  ┆ bool  │
╞══════╪═══════╪═══════╡
│ 0    ┆ true  ┆ false │
│ 1    ┆ true  ┆ true  │
│ 2    ┆ false ┆ true  │
└──────┴───────┴───────┘
{'iteration': 12, 'weights': [0.2, 0.8]}
```

**Note:** The file is copied when `record()` is called. Later changes to `config.json` do not change the recorded copy.

**Note:** NumPy numbers, tuples and dates are converted when they are recorded. To record your own classes in a JSON variable, see [`register_value_handler`](../reference/experiments/codecs-and-values.md#register_value_handler).

### Events

Every `record()` call adds one observation, so a log variable can be recorded many times in a run.
`occurred_at` sets the time of the event when it differs from the time of recording:

```python
events.record({"event": "soak started"})
events.record({"event": "soak finished"}, occurred_at="2025-07-01T02:00:00Z")

print(run.observations(events).frame().select("sequence", "occurred_at", "value"))
```
```text
shape: (2, 3)
┌──────────┬─────────────────────────┬────────────────────────────┐
│ sequence ┆ occurred_at             ┆ value                      │
│ ---      ┆ ---                     ┆ ---                        │
│ i64      ┆ datetime[μs, UTC]       ┆ object                     │
╞══════════╪═════════════════════════╪════════════════════════════╡
│ 6        ┆ null                    ┆ {'event': 'soak started'}  │
│ 7        ┆ 2025-07-01 02:00:00 UTC ┆ {'event': 'soak finished'} │
└──────────┴─────────────────────────┴────────────────────────────┘
```

`sequence` is the position of the observation in the run.

### Timeseries with keys

The variable `pump power` was declared with `keys=("pump",)`.
It is recorded with a dictionary, with one list of `(time, value)` rows for each pump:

```python
pump_power.record({
    ("P1",): [("2025-07-01T00:00:00Z", 1066.0), ("2025-07-01T01:00:00Z", 1091.0)],
    ("P2",): [("2025-07-01T00:00:00Z", 262.0), ("2025-07-01T01:00:00Z", 270.0)],
})

for observation in run.observations(pump_power).all():
    print(observation.key)
    print(observation.dataframe())

run.finish()
```
```text
('P1',)
shape: (2, 2)
┌─────────────────────────┬────────┐
│ time                    ┆ value  │
│ ---                     ┆ ---    │
│ datetime[μs, UTC]       ┆ f64    │
╞═════════════════════════╪════════╡
│ 2025-07-01 00:00:00 UTC ┆ 1066.0 │
│ 2025-07-01 01:00:00 UTC ┆ 1091.0 │
└─────────────────────────┴────────┘
('P2',)
shape: (2, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2025-07-01 00:00:00 UTC ┆ 262.0 │
│ 2025-07-01 01:00:00 UTC ┆ 270.0 │
└─────────────────────────┴───────┘
```

Note that a key is a tuple, so one name is written as `("P1",)`.
A timeseries variable without keys is recorded with the list of rows directly, as in the tutorial.

## Mark a run as failed

If the calculation raises an error, call `run.fail(error)` instead of `finish()`.
The values recorded before the error are kept:

```python
run = study.start({"operator": "ana"})
try:
    note.record("Second cleaning")
    result = 1 / 0                # your calculation goes here
    run.finish()
except Exception as error:
    run.fail(error)

print(study.experiments.frame().select("status", "error"))
```
```text
shape: (2, 2)
┌───────────┬─────────────────────────────────┐
│ status    ┆ error                           │
│ ---       ┆ ---                             │
│ str       ┆ object                          │
╞═══════════╪═════════════════════════════════╡
│ succeeded ┆ null                            │
│ failed    ┆ {'message': 'division by zero'… │
└───────────┴─────────────────────────────────┘
```

The saved error holds the type and the message of the exception:

```python
print(study.experiments.get(run.run_id).error)
```
```text
{'message': 'division by zero', 'type': 'ZeroDivisionError'}
```

**Note:** A run that has finished or failed does not accept new values.

## Continue a run later

A run stays open until `finish()` or `fail()` is called.
This is useful for a test that takes days, such as a pilot trial.
Keep the `run_id` of the run:

```python
run = study.start({"operator": "ben"})
note.record("Long soak started")
print(run.run_id)
```
```text
7a1e77e8-0b1b-4cef-924f-be6cb90382b7
```

In a later session, open the study with `acq.study.get()` and continue the run with `resume()`.
The variables are found by their label in `study.variables`:

```python
study = acq.study.get("ro-cleaning")
run = study.resume("7a1e77e8-0b1b-4cef-924f-be6cb90382b7")

study.variables["note"].record("Long soak finished")
run.finish()

print([observation.value for observation in run.observations("note").all()])
```
```text
['Long soak started', 'Long soak finished']
```

- `resume()` only accepts a run that is still running.
- A study object has one active run at a time. Finish the active run before starting or resuming another one.

## Run the script again

Running this script a second time adds new runs to the same study.
To start from an empty study, delete it first with `acq.study.get("ro-cleaning").delete()`.
Deleting single runs is described in [Compare experiment runs](compare-experiment-runs.md).

## Next

- [Record a whole model](record-a-whole-model.md) declares and records many values of a model at once.
- [Compare experiment runs](compare-experiment-runs.md) selects runs and calculates with their results.
- [Recording values](../reference/experiments/recording.md) and [Reading recorded data](../reference/experiments/reading.md) list every method.
- [How experiments work](../explanation/experiments.md) explains what is stored and where.
