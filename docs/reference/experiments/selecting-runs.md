---
title: Selecting and comparing runs
---

`study.experiments` is the collection of a study's runs. It filters runs by status, start time and metadata, and builds a table that compares scalar values across runs. A selection can also be deleted or measured. Nothing is read from the server until a method such as `all()` or `frame()` is called.

| Item | Description |
|---|---|
| [`Study.experiments`](#studyexperiments) | The runs of a study |
| [`ExperimentCollection.where`](#experimentcollectionwhere) | Filter runs |
| [`ExperimentCollection.all`](#experimentcollectionall) | Return the selected runs as a list |
| [`ExperimentCollection.frame`](#experimentcollectionframe) | Return the selected runs as a table |
| [`ExperimentCollection.get`](#experimentcollectionget) | Look up one run by ID |
| [`ExperimentCollection.scalars`](#experimentcollectionscalars) | Compare scalar values across runs |
| [`ExperimentCollection.query`](#experimentcollectionquery) | Query run metadata |
| [`ExperimentCollection.usage`](#experimentcollectionusage) | Count and size what the selected runs store |
| [`ExperimentCollection.delete`](#experimentcollectiondelete) | Delete the selected runs |

Reading the values of the selected runs is on the [Reading recorded data](reading.md) page.

The examples continue from this setup:

```python
import acquirium as aq

acq = aq.init()
study = acq.study.define("pump tuning")
speed = study.input("speed").scalar(unit="Hz")
cost = study.output("cost").scalar(unit="USD")

for hz, usd in ((40, 14.0), (45, 12.5), (50, 13.1)):
    run = study.start({"scenario": "sweep", "operator": "ana", "settings": {"flow": hz / 4}})
    speed.record(hz)
    cost.record(usd)
    run.finish()

failed = study.start({"scenario": "stress"})
failed.fail(RuntimeError("solver diverged"))
```

## `Study.experiments`

```python
study.experiments
```

The collection of every run of the study. Printing a collection shows its filters and does not contact the server.

**Example**

```python
print(study.experiments)
print(study.experiments.where(status="succeeded"))
```

```text
<ExperimentCollection study='pump tuning'>
<ExperimentCollection study='pump tuning' status='succeeded'>
```

## `ExperimentCollection.where`

```python
ExperimentCollection.where(*expressions, status=None, started_after=None, started_before=None, metadata=None, **attrs)
```

Returns a narrower collection. Conditions are combined with AND, and the result can be filtered again.

**Parameters**

- `*expressions`: attribute expressions built with `acq.attr`, such as `acq.attr.settings.flow >= 12`. Expressions support comparisons and `&`, `|` and `~`. See [filtering](../query/filtering.md).
- `status` (`str | None`, default `None`): `"running"`, `"succeeded"` or `"failed"`.
- `started_after` (`datetime | str | None`, default `None`): keep runs that started at or after this time.
- `started_before` (`datetime | str | None`, default `None`): keep runs that started before this time.
- `metadata` (`dict | None`, default `None`): keep runs whose saved metadata has exactly these values for these keys.
- `**attrs`: run metadata values to match, such as `scenario="sweep"`.

**Returns**

An `ExperimentCollection`.

**Raises**

- `ValueError`: when a keyword in `**attrs` names an attribute that is not known to the metadata graph.

**Example**

```python
print(study.experiments.where(acq.attr.settings.flow >= 12).all())
print(study.experiments.where(metadata={"scenario": "stress"}).all())
```

```text
[<Experiment 250e59e6-77c5-4b5e-9234-bbab46e6b675 succeeded {'operator': 'ana', 'scenario': 'sweep', 'settings': {'flow': 12.5}}>]
[<Experiment 449fcc87-28db-40f2-aafa-465fbba83bba failed {'scenario': 'stress'}>]
```

## `ExperimentCollection.all`

```python
ExperimentCollection.all()
```

Reads the selected runs.

**Returns**

`list[Experiment]`, ordered by start time. See [`Experiment`](studies-and-runs.md#experiment).

**Example**

```python
for run in study.experiments.where(status="failed").all():
    print(run.run_id, run.error)
```

```text
449fcc87-28db-40f2-aafa-465fbba83bba {'message': 'solver diverged', 'type': 'RuntimeError'}
```

## `ExperimentCollection.frame`

```python
ExperimentCollection.frame()
```

Reads the selected runs as a table with one row per run.

**Returns**

A Polars `DataFrame` with the columns `experiment_id`, `status`, `metadata`, `started_at`, `finished_at` and `error`. The `metadata` and `error` columns hold Python dictionaries. The table has no columns when no run is selected.

**Example**

```python
print(study.experiments.frame().select("experiment_id", "status", "started_at"))
```

```text
shape: (4, 3)
┌─────────────────────────────────┬───────────┬────────────────────────────────┐
│ experiment_id                   ┆ status    ┆ started_at                     │
│ ---                             ┆ ---       ┆ ---                            │
│ str                             ┆ str       ┆ datetime[μs, UTC]              │
╞═════════════════════════════════╪═══════════╪════════════════════════════════╡
│ 85e7e541-1614-46eb-b7f8-59e26f… ┆ succeeded ┆ 2026-11-18 01:04:33.949577 UTC │
│ d0379064-e725-488d-947e-cee102… ┆ succeeded ┆ 2026-11-18 01:04:34.011411 UTC │
│ 250e59e6-77c5-4b5e-9234-bbab46… ┆ succeeded ┆ 2026-11-18 01:04:34.066602 UTC │
│ 449fcc87-28db-40f2-aafa-465fbb… ┆ failed    ┆ 2026-11-18 01:04:34.125392 UTC │
└─────────────────────────────────┴───────────┴────────────────────────────────┘
```

## `ExperimentCollection.get`

```python
ExperimentCollection.get(experiment_id)
```

Looks up one run of the study by ID. The filters of the collection are not applied.

**Parameters**

- `experiment_id` (`str`): the `run_id`.

**Returns**

An [`Experiment`](studies-and-runs.md#experiment).

**Raises**

- `KeyError`: when the run belongs to another study.
- `requests.HTTPError` (404): when no run has this ID.

**Example**

```python
print(study.experiments.get(run.run_id))
```

```text
<Experiment 250e59e6-77c5-4b5e-9234-bbab46e6b675 succeeded {'operator': 'ana', 'scenario': 'sweep', 'settings': {'flow': 12.5}}>
```

## `ExperimentCollection.scalars`

```python
ExperimentCollection.scalars(*variables, metadata=())
```

Builds a table with one row per selected run and one column per scalar variable. Each cell holds the last value the run recorded for that variable, or null when the run did not record it. This is useful for comparing runs of a parameter sweep.

**Parameters**

- `*variables` (`ExperimentVariable | str`): the scalar variables to include, as variable objects or labels. With none given, every scalar variable of the study is included.
- `metadata` (`tuple[str, ...] | list[str]`, default `()`): run metadata keys to add as columns. A dotted name such as `"settings.flow"` reads a nested field.

**Returns**

A Polars `DataFrame`. The columns are `experiment_id`, the requested metadata keys, then the variables by label. A variable no selected run recorded is a `Float64` column of nulls.

**Raises**

- `TypeError`: when a given variable is not a scalar.
- `ValueError`: when a variable belongs to another study, or two columns would have the same name.
- `KeyError`: when a label is not declared.

**Example**

```python
runs = study.experiments.where(scenario="sweep")
print(runs.scalars("speed", "cost", metadata=["operator", "settings.flow"]))
```

```text
shape: (3, 5)
┌─────────────────────────────────┬──────────┬───────────────┬───────┬──────┐
│ experiment_id                   ┆ operator ┆ settings.flow ┆ speed ┆ cost │
│ ---                             ┆ ---      ┆ ---           ┆ ---   ┆ ---  │
│ str                             ┆ str      ┆ f64           ┆ i64   ┆ f64  │
╞═════════════════════════════════╪══════════╪═══════════════╪═══════╪══════╡
│ 85e7e541-1614-46eb-b7f8-59e26f… ┆ ana      ┆ 10.0          ┆ 40    ┆ 14.0 │
│ d0379064-e725-488d-947e-cee102… ┆ ana      ┆ 11.25         ┆ 45    ┆ 12.5 │
│ 250e59e6-77c5-4b5e-9234-bbab46… ┆ ana      ┆ 12.5          ┆ 50    ┆ 13.1 │
└─────────────────────────────────┴──────────┴───────────────┴───────┴──────┘
```

## `ExperimentCollection.query`

```python
ExperimentCollection.query()
```

Returns a metadata query limited to the selected runs of this study. See [building queries](../query/building.md) and [reading query results](../query/reading.md).

**Returns**

A query object.

**Example**

```python
query = study.experiments.where(scenario="sweep").query()
print(query.include("scenario").metadata())
```

```text
shape: (3, 2)
┌─────────────────────────────────┬─────────────────────┐
│ experiment                      ┆ experiment.scenario │
│ ---                             ┆ ---                 │
│ str                             ┆ str                 │
╞═════════════════════════════════╪═════════════════════╡
│ ns1:85e7e541-1614-46eb-b7f8-59… ┆ sweep               │
│ ns1:d0379064-e725-488d-947e-ce… ┆ sweep               │
│ ns1:250e59e6-77c5-4b5e-9234-bb… ┆ sweep               │
└─────────────────────────────────┴─────────────────────┘
```

## `ExperimentCollection.usage`

```python
ExperimentCollection.usage()
```

Counts what the selected runs store and estimates its size.

**Returns**

A `dict` with the keys described under [`acq.study.usage`](studies-and-runs.md#acqstudyusage). `studies` and `variables` are always 0 for a selection of runs.

**Example**

```python
print(study.experiments.where(status="failed").usage())
```

```text
{'studies': 0, 'variables': 0, 'runs': 1, 'observations': 0, 'annotations': 0, 'samples': 0, 'files': 0, 'sample_bytes': 0, 'file_bytes': 0, 'ledger_bytes': 148, 'total_bytes': 148}
```

## `ExperimentCollection.delete`

```python
ExperimentCollection.delete()
```

Deletes the selected runs with their recorded values, stream annotations, stored samples and the files no remaining run uses.

**Returns**

A `dict` with the number of `runs`, `observations`, `annotations`, `samples` and `files` removed. All counts are 0 when no run is selected.

**Raises**

- `requests.HTTPError` (409): when a selected run is still running, or when a run outside the selection recorded a reference to a stream a selected run stored. Nothing is deleted.

**Example**

```python
print(study.experiments.where(status="failed").delete())
```

```text
{'runs': 1, 'observations': 0, 'annotations': 0, 'samples': 0, 'files': 0}
```
