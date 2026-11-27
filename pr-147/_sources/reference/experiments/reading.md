---
title: Reading recorded data
---

Each call to `record()` saves one observation. This page covers selecting observations by run and by variable, and reading what an observation holds: a value, time-series samples, a table, a file or an object.

| Item | Description |
|---|---|
| [`Experiment.observations`](#experimentobservations) | The observations of one run |
| [`ExperimentVariable.observations`](#experimentvariableobservations) | The observations of one variable across runs |
| [`ExperimentCollection.observations`](#experimentcollectionobservations) | The observations of a selection of runs |
| [`ObservationCollection.all`](#observationcollectionall) | Return the observations as a list |
| [`ObservationCollection.frame`](#observationcollectionframe) | Return the observations as a table |
| [`ObservationCollection.latest`](#observationcollectionlatest) | Return the last observation |
| [`Observation`](#observation) | Attributes of an observation |
| [`Observation.dataframe`](#observationdataframe) | Read time-series samples or a table |
| [`Observation.read_bytes`](#observationread_bytes) | Read the bytes of a file, table or object |
| [`Observation.load`](#observationload) | Rebuild a recorded object |
| [`Observation.as_dict`](#observationas_dict) | Return the observation as a dictionary |

To compare one scalar value per run in a single table, see [`ExperimentCollection.scalars`](selecting-runs.md#experimentcollectionscalars).

The examples continue from this setup:

```python
import acquirium as aq
import polars as pl

acq = aq.init()
study = acq.study.define("pump tuning")
speed = study.input("speed").scalar(unit="Hz")
report = study.input("report").file(media_type="text/plain")
cost = study.output("cost").scalar(unit="USD")
power = study.output("power").timeseries(unit="kW")
schedule = study.output("schedule").table()
model = study.output("model").object(codec=aq.PickleCodec)

run = study.start({"scenario": "baseline"})
speed.record(45)
cost.record(12.5)
cost.record(12.1)
power.record([("2026-01-01T00:00:00Z", 5.0), ("2026-01-01T00:01:00Z", 5.5)])
schedule.record(pl.DataFrame({"hour": [0, 1], "on": [True, False]}))
model.record({"weights": [1, 2, 3]})
report.record("report.txt")
run.finish()
```

## `Experiment.observations`

```python
Experiment.observations(variable=None)
```

Selects the observations of this run. Nothing is read until you call `all()`, `frame()` or `latest()` on the result.

**Parameters**

- `variable` (`ExperimentVariable | str | None`, default `None`): keep only this variable, given as a variable object, a label or a `variable_id`. With `None`, every variable is included.

**Returns**

An `ObservationCollection`.

**Raises**

- `KeyError`: when a label is not declared in the study.
- `ValueError`: when the variable belongs to another study.

**Example**

```python
print(run.observations("cost").all())
```

```text
[<Observation 'cost' = 12.5 (run 1fb3c862-ee1f-4de0-8093-caf6127b5a71, #2)>, <Observation 'cost' = 12.1 (run 1fb3c862-ee1f-4de0-8093-caf6127b5a71, #3)>]
```

## `ExperimentVariable.observations`

```python
ExperimentVariable.observations(experiments=None)
```

Selects the observations of this variable across runs.

**Parameters**

- `experiments` (`ExperimentCollection | None`, default `None`): the runs to read from, such as `study.experiments.where(status="succeeded")`. With `None`, every run of the study is included.

**Returns**

An `ObservationCollection`.

**Raises**

- `ValueError`: when `experiments` belongs to another study.

**Example**

```python
sweep = study.experiments.where(scenario="sweep")
print(cost.observations(sweep).frame().select("experiment_id", "value", "sequence"))
```

```text
shape: (3, 3)
┌─────────────────────────────────┬───────┬──────────┐
│ experiment_id                   ┆ value ┆ sequence │
│ ---                             ┆ ---   ┆ ---      │
│ str                             ┆ f64   ┆ i64      │
╞═════════════════════════════════╪═══════╪══════════╡
│ 85e7e541-1614-46eb-b7f8-59e26f… ┆ 14.0  ┆ 2        │
│ d0379064-e725-488d-947e-cee102… ┆ 12.5  ┆ 2        │
│ 250e59e6-77c5-4b5e-9234-bbab46… ┆ 13.1  ┆ 2        │
└─────────────────────────────────┴───────┴──────────┘
```

This output comes from the three `"sweep"` runs of the [Selecting and comparing runs](selecting-runs.md) page.

## `ExperimentCollection.observations`

```python
ExperimentCollection.observations(variable=None)
```

Selects the observations of the runs in this collection.

**Parameters**

- `variable` (`ExperimentVariable | str | None`, default `None`): keep only this variable. With `None`, every variable is included.

**Returns**

An `ObservationCollection`.

**Raises**

- `KeyError`: when a label is not declared in the study.
- `ValueError`: when the variable belongs to another study.

**Example**

```python
sweep = study.experiments.where(scenario="sweep")
print(sweep.observations(cost).latest())
```

```text
<Observation 'cost' = 13.1 (run 250e59e6-77c5-4b5e-9234-bbab46e6b675, #2)>
```

## `ObservationCollection.all`

```python
ObservationCollection.all()
```

Reads the selected observations.

**Returns**

`list[Observation]`. Observations are ordered by the start time of their run, then by their sequence number in the run.

**Example**

```python
for observation in run.observations(cost).all():
    print(observation.sequence, observation.value)
```

```text
2 12.5
3 12.1
```

## `ObservationCollection.frame`

```python
ObservationCollection.frame()
```

Reads the selected observations as a table with one row per observation.

**Returns**

A Polars `DataFrame` with the columns `experiment_id`, `variable_id`, `variable`, `role`, `kind`, `variable_metadata`, `status`, `experiment_metadata`, `sequence`, `recorded_at`, `occurred_at`, `started_at`, `finished_at`, `value`, `stream_uri`, `range_start`, `range_end`, `artifact_digest`, `filename`, `media_type`, `byte_length` and `key`. When the values are of mixed types, the `value` column holds Python objects. The table has no columns when nothing is selected.

**Example**

```python
print(run.observations().frame().select("variable", "kind", "value", "filename", "sequence"))
```

```text
shape: (7, 5)
┌──────────┬────────────┬───────┬──────────────────┬──────────┐
│ variable ┆ kind       ┆ value ┆ filename         ┆ sequence │
│ ---      ┆ ---        ┆ ---   ┆ ---              ┆ ---      │
│ str      ┆ str        ┆ f64   ┆ str              ┆ i64      │
╞══════════╪════════════╪═══════╪══════════════════╪══════════╡
│ speed    ┆ scalar     ┆ 45.0  ┆ null             ┆ 1        │
│ cost     ┆ scalar     ┆ 12.5  ┆ null             ┆ 2        │
│ cost     ┆ scalar     ┆ 12.1  ┆ null             ┆ 3        │
│ power    ┆ timeseries ┆ null  ┆ null             ┆ 4        │
│ schedule ┆ table      ┆ null  ┆ schedule.parquet ┆ 5        │
│ model    ┆ object     ┆ null  ┆ model.pkl        ┆ 6        │
│ report   ┆ file       ┆ null  ┆ report.txt       ┆ 7        │
└──────────┴────────────┴───────┴──────────────────┴──────────┘
```

## `ObservationCollection.latest`

```python
ObservationCollection.latest()
```

Reads the last observation of the selection. Note that for a time series recorded in several calls, this is the observation of the last call only. It covers the rows of that call, not every row of the run.

**Returns**

An [`Observation`](#observation).

**Raises**

- `LookupError`: when the selection is empty.

**Example**

```python
print(run.observations(cost).latest().value)
```

```text
12.1
```

## `Observation`

```python
observation = run.observations(variable).latest()
```

One recorded value. An observation cannot be changed.

| Attribute | Type | Meaning |
|---|---|---|
| `experiment_id` | `str` | The `run_id` of the run |
| `variable` | `ExperimentVariable` | The variable that was recorded |
| `sequence` | `int` | The position of the observation in its run, starting at 1 |
| `recorded_at` | `datetime` | When the server received the value, in UTC |
| `occurred_at` | `datetime \| None` | The `occurred_at` given to `record()`, if any |
| `value` | any | The value of a scalar, text, JSON or log observation. `None` for other kinds |
| `status` | `str` | The status of the run |
| `experiment_metadata` | `dict` | The metadata of the run |
| `started_at`, `finished_at` | `datetime` | The start and end time of the run |
| `stream_uri` | `str \| None` | For a time series, the URI of the stream |
| `range_start`, `range_end` | `datetime \| None` | For a time series, the first and last sample time the observation covers |
| `key` | `tuple \| None` | For a keyed time series, the key of the stream |
| `stream` | `Stream \| None` | For a time series, a handle for the stream. See [streams](../client/streams.md) |
| `interval` | `StreamInterval \| None` | For a time series, the stream and its time range. It can be recorded in another run with `record()` |
| `artifact_digest` | `str \| None` | For a file, table or object, the SHA-256 of the stored bytes |
| `filename`, `media_type`, `byte_length` | | For a file, table or object, the stored name, content type and size |

A table is stored as `<label>.parquet` with the content type `application/vnd.apache.parquet`. An object is stored as the label plus the suffix of its codec.

The deprecated attribute `ref_uri` returns `stream_uri` and emits a `FutureWarning`.

**Example**

```python
print(run.observations(power).latest())
print(run.observations(schedule).latest())
```

```text
<Observation 'power' = stream urn:acquirium#2881ad4a-7440-5031-80bf-6e2e2af59d55 [2026-01-01T00:00:00+00:00, 2026-01-01T00:01:00+00:00] (run 1fb3c862-ee1f-4de0-8093-caf6127b5a71, #4)>
<Observation 'schedule' = table 'schedule.parquet' (744 bytes) (run 1fb3c862-ee1f-4de0-8093-caf6127b5a71, #5)>
```

## `Observation.dataframe`

```python
Observation.dataframe(**kwargs)
```

Reads the data of a time-series or table observation. For a time series, it reads the stream's samples between `range_start` and `range_end`, both included. For a table, it reads the stored Parquet file.

**Parameters**

- `**kwargs`: for a time series, options of the stream read, such as `start`, `end`, `limit` and `order`. `start` and `end` default to the range of the observation. See [`Stream.dataframe`](../client/streams.md). A table takes no options.

**Returns**

A Polars `DataFrame`. A time series has the columns `time` and `value`. A table has the columns that were recorded.

**Raises**

- `TypeError`: when the observation is not a time series or a table, or when options are given for a table.

**Example**

```python
print(run.observations(power).latest().dataframe())
print(run.observations(schedule).latest().dataframe())
```

```text
shape: (2, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ 5.0   │
│ 2026-01-01 00:01:00 UTC ┆ 5.5   │
└─────────────────────────┴───────┘
shape: (2, 2)
┌──────┬───────┐
│ hour ┆ on    │
│ ---  ┆ ---   │
│ i64  ┆ bool  │
╞══════╪═══════╡
│ 0    ┆ true  │
│ 1    ┆ false │
└──────┴───────┘
```

## `Observation.read_bytes`

```python
Observation.read_bytes()
```

Downloads the stored bytes of a file, table or object observation.

**Returns**

`bytes`.

**Raises**

- `TypeError`: when the observation is not a file, table or object.

**Example**

```python
print(run.observations(report).latest().read_bytes())
```

```text
b'hello'
```

## `Observation.load`

```python
Observation.load(*, codec=None)
```

Rebuilds the object an object variable recorded, using the codec of its declaration. The codec runs in your Python process, not on the server. See [Codecs and values](codecs-and-values.md).

**Parameters**

- `codec` (`type | None`, default `None`): a codec class to use in place of the declared one. This is useful when the declared codec cannot be imported, or to decode the bytes of a file or table observation.

**Returns**

The object returned by the codec's `loads()`.

**Raises**

- `TypeError`: when the variable is not an object variable and no `codec` is given.
- `ImportError`: when the declared codec cannot be imported.

**Example**

```python
print(run.observations(model).latest().load())
print(run.observations(report).latest().load(codec=aq.BytesCodec))
```

```text
{'weights': [1, 2, 3]}
b'hello'
```

## `Observation.as_dict`

```python
Observation.as_dict()
```

Returns the observation as a dictionary. The keys are the columns of [`ObservationCollection.frame`](#observationcollectionframe).

**Returns**

A `dict`.

**Example**

```python
print(run.observations(cost).latest().as_dict())
```

```text
{'experiment_id': '1fb3c862-ee1f-4de0-8093-caf6127b5a71', 'variable_id': 'urn:acquirium#accf8b9e-f829-504c-8459-79da1bb9a0f8', 'variable': 'cost', 'role': 'output', 'kind': 'scalar', 'variable_metadata': {}, 'status': 'succeeded', 'experiment_metadata': {'scenario': 'baseline'}, 'sequence': 3, 'recorded_at': datetime.datetime(2026, 11, 18, 1, 4, 34, 833211, tzinfo=datetime.timezone.utc), 'occurred_at': None, 'started_at': datetime.datetime(2026, 11, 18, 1, 4, 34, 777204, tzinfo=datetime.timezone.utc), 'finished_at': datetime.datetime(2026, 11, 18, 1, 4, 35, 46259, tzinfo=datetime.timezone.utc), 'value': 12.1, 'stream_uri': None, 'range_start': None, 'range_end': None, 'artifact_digest': None, 'filename': None, 'media_type': None, 'byte_length': None, 'key': None}
```
