---
title: Recording values
---

Values are recorded through the variables of a study and go to the study's active run. Each call adds one observation to the run. Earlier observations are never changed. This page covers `record()`, the kind-specific methods behind it, and assignment through `run.output`.

| Item | Description |
|---|---|
| [`ExperimentVariable.record`](#experimentvariablerecord) | Record a value of any kind |
| [`ExperimentVariable.set`](#experimentvariableset) | Record a scalar, text, JSON or log value |
| [`ExperimentVariable.append`](#experimentvariableappend) | Record a log event |
| [`ExperimentVariable.attach`](#experimentvariableattach) | Record a file |
| [`ExperimentVariable.add`](#experimentvariableadd) | Record time-series rows |
| [`Experiment.output`](#experimentoutput) | Record outputs by label on a run |

The examples continue from this setup:

```python
import acquirium as aq
import polars as pl

acq = aq.init()
study = acq.study.define("pump tuning")
speed = study.input("speed").scalar(unit="Hz")
configuration = study.input("configuration").json()
report = study.input("report").file(media_type="text/plain")
cost = study.output("cost").scalar(unit="USD")
power = study.output("power").timeseries(unit="kW")
flow = study.output("flow").timeseries(keys=("pump",))
schedule = study.output("schedule").table()
model = study.output("model").object(codec=aq.PickleCodec)
events = study.log("solver events")

run = study.start({"scenario": "baseline"})
```

## `ExperimentVariable.record`

```python
ExperimentVariable.record(value, *, occurred_at=None)
```

Records one value in the active run. The kind of the variable decides what `value` must be and what is returned.

| Kind | `value` | Returns |
|---|---|---|
| `scalar` | A finite number or a boolean | A receipt `dict` |
| `text` | A string | A receipt `dict` |
| `json`, `log` | Any JSON value | A receipt `dict` |
| `file` | A path to a file | A file `dict` |
| `table` | A Polars, pandas or Arrow table | A file `dict` |
| `object` | Any object the variable's codec can save | A file `dict` |
| `timeseries` | Rows of time and value | The run's [`Stream`](../client/streams.md), or `None` when there are no rows |
| `timeseries` with keys | A `dict` from key tuples to rows | A `dict` from key tuples to `Stream` |
| `timeseries` | A stream interval from `stream.between(start, end)` | A receipt `dict` |

A receipt has `sequence` (the position of the observation in the run) and `recorded_at` (the server time, as an ISO 8601 string). A file `dict` has `digest` (the SHA-256 of the bytes), `filename` and `byte_length`.

Time-series rows are `(time, value)` pairs, or a Polars or Arrow table with a `time` (or `ts`) column and a `value` (or `value_numeric`) column. A table with a time column and one other column also works. Times are `datetime` objects or ISO 8601 strings. A time without a timezone is read as UTC.

Recording a stream interval stores only the stream and the time range, not the samples. Later reads return the samples the stream holds at that moment. To store a fixed copy, record `interval.dataframe()`.

**Parameters**

- `value`: the value to record, as in the table above.
- `occurred_at` (`datetime | str | None`, default `None`): when the value occurred, for scalar, text, JSON and log variables. A string must be ISO 8601. It is saved next to the server's `recorded_at` time.

**Returns**

See the table above.

**Raises**

- `RuntimeError`: when the study has no active run.
- `TypeError`: when the value does not fit the kind, when `occurred_at` is given for a file, table, object or time-series variable, when a stream is recorded without a time range, or when a stream is recorded to a variable that is not a time series.
- `ValueError`: when a number is NaN or infinite, when a time-series row has a value that is not a number, or when a table of rows has no time or value column.
- `FileNotFoundError`: when the path of a file variable does not exist.

**Example**

```python
print(speed.record(45))
print(configuration.record({"mode": "auto", "limits": [1, 2]}))
print(events.record({"iteration": 1}, occurred_at="2026-01-01T00:00:00"))
```

```text
{'sequence': 1, 'recorded_at': '2026-11-18T00:54:37.849563+00:00'}
{'sequence': 3, 'recorded_at': '2026-11-18T00:54:37.895799+00:00'}
{'sequence': 5, 'recorded_at': '2026-11-18T00:54:37.951673+00:00'}
```

Files, tables and objects:

```python
print(report.record("report.txt"))
print(schedule.record(pl.DataFrame({"hour": [0, 1], "on": [True, False]})))
print(model.record({"weights": [1, 2, 3]}))
```

```text
{'digest': '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824', 'filename': 'report.txt', 'byte_length': 5}
{'digest': 'f1d93070eadc81006496e7d823878823b483035e70332c2d085167626f4bd6d7', 'filename': 'schedule.parquet', 'byte_length': 744}
{'digest': 'b2327a1a3eafcb64769f0401dc13c846c1eb769fbde8640484e4d76f9f29d3a7', 'filename': 'model.pkl', 'byte_length': 35}
```

Time series:

```python
rows = [("2026-01-01T00:00:00Z", 5.0), ("2026-01-01T00:01:00Z", 5.5)]

stream = power.record(rows)
print(stream.dataframe())

print(flow.record({("p1",): rows, ("p2",): rows}))

# record a reference to an existing stream range, without copying samples
print(power.record(stream.between("2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z")))
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
{('p1',): <Stream urn:acquirium#5c642073-dc63-573c-af77-d8b5641ee69a>, ('p2',): <Stream urn:acquirium#ec8859da-76b4-5a79-b2a0-6ef36297c877>}
{'sequence': 12, 'recorded_at': '2026-11-18T00:54:39.877042+00:00'}
```

A value of the wrong kind is rejected before anything is sent:

```python
cost.record("high")
```

```text
TypeError: 'cost' is a scalar and records a finite number or boolean, not 'high'
```

## `ExperimentVariable.set`

```python
ExperimentVariable.set(value, *, occurred_at=None)
```

Records a value for a scalar, text, JSON or log variable. `record()` calls this method for those kinds.

**Parameters**

- `value`: a finite number or boolean for a scalar, a string for text, any JSON value for JSON and log variables. Values are converted to JSON first. See [value conversion](codecs-and-values.md#value-conversion).
- `occurred_at` (`datetime | str | None`, default `None`): when the value occurred.

**Returns**

A receipt `dict` with `sequence` and `recorded_at`.

**Raises**

- `RuntimeError`: when the study has no active run.
- `TypeError`: when the value does not fit the kind, or the variable is a file, table, object or time series.
- `ValueError`: when a number is NaN or infinite.

**Example**

```python
print(cost.set(12.5))
```

```text
{'sequence': 2, 'recorded_at': '2026-11-18T00:54:37.872968+00:00'}
```

## `ExperimentVariable.append`

```python
ExperimentVariable.append(value, *, occurred_at=None)
```

Records one event. It does the same as [`set`](#experimentvariableset) and reads better for log variables.

**Parameters**

- `value`: the event, as any JSON value.
- `occurred_at` (`datetime | str | None`, default `None`): when the event occurred.

**Returns**

A receipt `dict` with `sequence` and `recorded_at`.

**Raises**

The same errors as [`set`](#experimentvariableset).

**Example**

```python
print(events.append("started"))
```

```text
{'sequence': 2, 'recorded_at': '2026-11-18T00:58:35.600242+00:00'}
```

## `ExperimentVariable.attach`

```python
ExperimentVariable.attach(path)
```

Copies a file into the run. Note that `attach()` does not send the `media_type` declared on the variable. Use `record(path)` to save the declared type with the file.

**Parameters**

- `path` (`str | Path`): the file to copy.

**Returns**

A `dict` with `digest`, `filename` and `byte_length`. Files with identical bytes are stored once and keep the name they first arrived with.

**Raises**

- `RuntimeError`: when the study has no active run.
- `FileNotFoundError`: when the path does not exist.
- `requests.HTTPError` (400): when the variable is not a file, table or object variable.

**Example**

```python
print(report.attach("r.txt"))
```

```text
{'digest': '8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4', 'filename': 'r.txt', 'byte_length': 2}
```

## `ExperimentVariable.add`

```python
ExperimentVariable.add(rows)
```

Writes time-series rows to the run's stream for this variable. `record()` calls this method for rows. Calling it again in the same run adds to the same stream and records another observation that covers the new rows.

**Parameters**

- `rows`: `(time, value)` pairs, or a Polars or Arrow table with time and value columns. For a keyed variable, a `dict` from key tuples to rows. Every value must be a number.

**Returns**

The run's [`Stream`](../client/streams.md) for this variable, or `None` when `rows` is empty. For a keyed variable, a `dict` from key tuples to `Stream`.

**Raises**

- `RuntimeError`: when the study has no active run.
- `TypeError`: when the variable is not a time series, when `rows` is a stream interval (use `record()` for that), or when a keyed variable is not given a `dict`.
- `ValueError`: when a value is not a number, when a table has no time or value column, when the rows come from several streams, or when a key does not have one entry per key name.

**Example**

```python
from datetime import datetime

stream = power.add([(datetime(2026, 1, 1, 0, minute), float(minute)) for minute in range(5)])
print(repr(stream))
```

```text
<Stream urn:acquirium#6132b366-843f-5142-9337-2c3da2674983>
```

## `Experiment.output`

```python
run.output[label] = value
```

Records an output on this run by label. If no output has this label yet, a variable is declared from the value: a string declares a text variable, a number or boolean declares a scalar without a unit, and any other JSON value declares a JSON variable. Declare files, tables, objects and time series yourself before assigning to them.

Reading `run.output[label]` returns the variable, not the value recorded in the run. The mapping iterates over the output labels of the study.

**Parameters**

- `label` (`str`): the output label.
- `value`: the value to record, as for [`record`](#experimentvariablerecord).

**Returns**

Assignment returns nothing. Reading returns an [`ExperimentVariable`](declaring-variables.md#experimentvariable).

**Raises**

- `RuntimeError`: when this run is not the active run of its study.
- `TypeError`: when the label is new and the value cannot be converted to JSON.
- `KeyError`: when reading a label that is not declared.

A `UserWarning` is emitted when the assignment declares a new variable.

**Example**

```python
run.output["cost"] = 13.0
run.output["verdict"] = "ok"
run.output["flags"] = {"converged": True}
print(run.output["verdict"], run.output["flags"])
```

```text
<ExperimentVariable output 'verdict' text> <ExperimentVariable output 'flags' json>
```
