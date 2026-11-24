---
title: Compare experiment runs
---

In this article we'll describe how to compare the runs of a study.
The results are read as one table, formulas are calculated on it with Polars, and runs are selected or deleted with `where()`.
Read the tutorial [Running experiments](../tutorials/advanced/experiments.md) first.

## The example study

The study below tests four recovery settings of the RO unit, with a new membrane and with an old one.
One run fails:

```python
import acquirium as aq
import polars as pl

acq = aq.init()

study = acq.study.define("ro-recovery")

recovery = study.input("recovery").scalar(unit="%")
permeate = study.output("permeate").scalar(unit="m3")
energy = study.output("energy").scalar(unit="kWh")

for age, operator in [(1, "ana"), (4, "ben")]:
    for percent in [40, 45, 50, 55]:
        run = study.start({"operator": operator, "membrane": {"age": age}})
        recovery.record(percent)
        try:
            if percent > 50 and age > 2:
                raise ValueError("pressure limit exceeded")
            volume = 10.0 * percent      # your calculation goes here
            permeate.record(volume)
            energy.record(round(volume * (2.2 + 0.02 * (percent - 40) + 0.1 * age), 1))
            run.finish()
        except ValueError as error:
            run.fail(error)
```

## 1. Read one row per run

`scalars()` gives one row per run and one column per scalar variable.
`metadata=` adds keys of the dictionary given to `start()` as columns.
A nested key is written with a dot, such as `"membrane.age"`:

```python
print(study.experiments.scalars(metadata=["operator", "membrane.age"]))
```
```text
shape: (8, 6)
┌─────────────────────────────────┬──────────┬──────────────┬──────────┬──────────┬────────┐
│ experiment_id                   ┆ operator ┆ membrane.age ┆ recovery ┆ permeate ┆ energy │
│ ---                             ┆ ---      ┆ ---          ┆ ---      ┆ ---      ┆ ---    │
│ str                             ┆ str      ┆ i64          ┆ i64      ┆ f64      ┆ f64    │
╞═════════════════════════════════╪══════════╪══════════════╪══════════╪══════════╪════════╡
│ b0395a4d-f0b2-4d19-8ab7-5ce38f… ┆ ana      ┆ 1            ┆ 40       ┆ 400.0    ┆ 920.0  │
│ b96fe828-3142-4b6a-bb59-aca4c4… ┆ ana      ┆ 1            ┆ 45       ┆ 450.0    ┆ 1080.0 │
│ 59bc8220-335d-40a2-b890-9ba493… ┆ ana      ┆ 1            ┆ 50       ┆ 500.0    ┆ 1250.0 │
│ c68f7e22-eb3d-4aa1-9983-50b8ed… ┆ ana      ┆ 1            ┆ 55       ┆ 550.0    ┆ 1430.0 │
│ 194f4336-4321-445b-8be2-5e1094… ┆ ben      ┆ 4            ┆ 40       ┆ 400.0    ┆ 1040.0 │
│ 7e4a8bb9-4dbe-4796-b8a1-97a200… ┆ ben      ┆ 4            ┆ 45       ┆ 450.0    ┆ 1215.0 │
│ 7fdf0521-895c-4edb-b859-e27376… ┆ ben      ┆ 4            ┆ 50       ┆ 500.0    ┆ 1400.0 │
│ ce722067-8697-43fb-84ad-e6d348… ┆ ben      ┆ 4            ┆ 55       ┆ null     ┆ null   │
└─────────────────────────────────┴──────────┴──────────────┴──────────┴──────────┴────────┘
```

- A cell holds the last value the run recorded for the variable.
- A cell is `null` when the run did not record the variable. The last run failed before it recorded its outputs.
- The values are in the unit of the variable. `study.variables["energy"].unit` gives the unit.

## 2. Calculate a formula

The table is a Polars dataframe, so a formula is a new column.
The code below keeps the succeeded runs, selects two variables by their label, and calculates the energy used per cubic meter:

```python
done = study.experiments.where(status="succeeded")

results = done.scalars("permeate", "energy", metadata=["membrane.age"])
results = results.with_columns(
    (pl.col("energy") / pl.col("permeate")).alias("energy per m3")
)
print(results.drop("experiment_id"))
```
```text
shape: (7, 4)
┌──────────────┬──────────┬────────┬───────────────┐
│ membrane.age ┆ permeate ┆ energy ┆ energy per m3 │
│ ---          ┆ ---      ┆ ---    ┆ ---           │
│ i64          ┆ f64      ┆ f64    ┆ f64           │
╞══════════════╪══════════╪════════╪═══════════════╡
│ 1            ┆ 400.0    ┆ 920.0  ┆ 2.3           │
│ 1            ┆ 450.0    ┆ 1080.0 ┆ 2.4           │
│ 1            ┆ 500.0    ┆ 1250.0 ┆ 2.5           │
│ 1            ┆ 550.0    ┆ 1430.0 ┆ 2.6           │
│ 4            ┆ 400.0    ┆ 1040.0 ┆ 2.6           │
│ 4            ┆ 450.0    ┆ 1215.0 ┆ 2.7           │
│ 4            ┆ 500.0    ┆ 1400.0 ┆ 2.8           │
└──────────────┴──────────┴────────┴───────────────┘
```

**Note:** The new column exists only in this dataframe. To keep a calculated value with its run, record it as an output before `finish()`.

The usual Polars methods rank and group the runs.
For instance, the average per membrane age:

```python
print(results.group_by("membrane.age").agg(pl.col("energy per m3").mean()).sort("membrane.age"))
```
```text
shape: (2, 2)
┌──────────────┬───────────────┐
│ membrane.age ┆ energy per m3 │
│ ---          ┆ ---           │
│ i64          ┆ f64           │
╞══════════════╪═══════════════╡
│ 1            ┆ 2.45          │
│ 4            ┆ 2.7           │
└──────────────┴───────────────┘
```

## 3. Select runs

`where()` narrows the runs before they are read.

- `status=` is `"running"`, `"succeeded"` or `"failed"`.
- A key of the dictionary given to `start()` is matched by name, such as `operator="ana"`.
- `acq.attr` builds a comparison, also on a nested key.
- `started_after=` and `started_before=` select by the start time of the run.

The conditions of one `where()` call are combined with AND:

```python
old_membrane = study.experiments.where(acq.attr.membrane.age > 2, status="succeeded")

print(old_membrane.scalars("recovery", "energy"))
```
```text
shape: (3, 3)
┌─────────────────────────────────┬──────────┬────────┐
│ experiment_id                   ┆ recovery ┆ energy │
│ ---                             ┆ ---      ┆ ---    │
│ str                             ┆ i64      ┆ f64    │
╞═════════════════════════════════╪══════════╪════════╡
│ 194f4336-4321-445b-8be2-5e1094… ┆ 40       ┆ 1040.0 │
│ 7e4a8bb9-4dbe-4796-b8a1-97a200… ┆ 45       ┆ 1215.0 │
│ 7fdf0521-895c-4edb-b859-e27376… ┆ 50       ┆ 1400.0 │
└─────────────────────────────────┴──────────┴────────┘
```

Expressions are combined with `&` and `|`, for instance `(acq.attr.operator == "ana") & (acq.attr.membrane.age < 2)`.
The same expressions are used in plant queries (see [filtering](../reference/query/filtering.md)).

## 4. Delete runs

`delete()` removes the selected runs with everything they recorded.
The code below prints the failed runs and deletes them:

```python
failed = study.experiments.where(status="failed")

for run in failed.all():
    print(run.run_id, run.error)

print(failed.delete())
```
```text
ce722067-8697-43fb-84ad-e6d34872a658 {'message': 'pressure limit exceeded', 'type': 'ValueError'}
{'runs': 1, 'observations': 1, 'annotations': 0, 'samples': 0, 'files': 0}
```

- `run.delete()` deletes one run.
- `study.delete()` deletes the study with its variables and all its runs.

Be aware that a deleted run cannot be restored.

**Note:** A run that is still running cannot be deleted. Call `finish()` or `fail()` on it first.

## Other kinds of results

`scalars()` reads scalar variables only.
Text, JSON, file, table and timeseries values are read from the observations of each run (see [Record experiments](record-experiments.md)).

## Next

- [Selecting and comparing runs](../reference/experiments/selecting-runs.md) lists every option of `where()` and `scalars()`.
- [Record a whole model](record-a-whole-model.md) records many values of a model at once.
- [How experiments work](../explanation/experiments.md) explains what is stored and what `delete()` removes.
