# Compare runs with formulas

Read a Study's scalar results as one row per run, then compute formulas from
them with Polars. Formulas are ordinary Python: nothing is saved unless you
record it. See [Record experiments](record-experiments.md) for recording runs.

## Read one row per run

```python
import polars as pl

study = ac.study.get("pump schedule")
succeeded = study.experiments.where(status="succeeded")
results = succeeded.scalars(metadata=["scenario"])
```

`scalars()` returns `experiment_id`, the metadata keys you name, and one
column per scalar variable, named by its label. Each cell is the last value
the run recorded, or null if the run recorded none. Dotted keys such as
`"settings.flow"` read nested metadata.

Pass labels or handles to choose the columns, for example
`succeeded.scalars("operating cost", "treated volume")`. Any selection works,
including attribute filters such as
`study.experiments.where(ac.attr.settings.flow > 10)`.

## Compute a formula

Add a column from a Polars expression:

```python
results = results.with_columns(
    (pl.col("operating cost") / pl.col("treated volume")).alias("cost per m3"),
)
```

Each column holds numbers in its variable's declared unit. Check the units
before combining variables:

```python
study.variables["operating cost"].unit  # "USD"
```

## Use a Python function

Write a formula as a function when that is clearer, and apply it to each row:

```python
def specific_energy(row):
    return row["pump energy"] / row["treated volume"]

results = results.with_columns(
    pl.Series("kWh per m3", [specific_energy(row) for row in results.iter_rows(named=True)]),
)
```

Each row is a dictionary keyed by column name. A run that did not record a
variable passes `None`; filter those rows first, or handle `None` in the
function.

## Compare and rank runs

```python
cheapest = results.filter(pl.col("feasible")).sort("cost per m3").head(3)
by_scenario = results.group_by("scenario").agg(pl.col("cost per m3").mean())
```

## Save a formula's result with each run

A finished run does not accept new recordings. To keep a derived value with
its run, compute it before `finish()` and record it as an output. Use one
function in both places so recorded and recomputed values agree:

```python
def cost_per_volume(cost, volume):
    return cost / volume

cost_per_m3 = study.output("cost per m3").scalar()

cost, volume = 462.0, 1210.0  # from your calculation
experiment = study.start({"scenario": "peak shaving"})
study.output["operating cost"].record(cost)
study.output["treated volume"].record(volume)
cost_per_m3.record(cost_per_volume(cost, volume))
experiment.finish()
```

The recorded value then appears as its own `scalars()` column in later reads.

## Other kinds of results

`scalars()` reads scalar variables only. Read text, JSON, file, and table
results through observations, for example
`succeeded.observations(variable).frame()`, and time-series samples with
`.dataframe()`. See [Record experiments](record-experiments.md#read-saved-scalar-results).
