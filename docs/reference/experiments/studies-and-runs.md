---
title: Studies and runs
---

A study holds variable declarations under a name. A run is one execution of a study, and every recorded value belongs to a run. This page covers creating and reopening studies, starting and ending runs, deleting them and measuring what they store.

| Item | Description |
|---|---|
| [`acq.study.define`](#acqstudydefine) | Create a study, or reopen the one with that name |
| [`acq.study.get`](#acqstudyget) | Reopen a saved study by name or ID |
| [`acq.study.all`](#acqstudyall) | List every study |
| [`acq.study.usage`](#acqstudyusage) | Count and size what all studies store |
| [`acq.study.clear`](#acqstudyclear) | Delete every study and run |
| [`Study`](#study) | Attributes of a study |
| [`Study.start`](#studystart) | Start a run |
| [`Study.resume`](#studyresume) | Continue a run that is still running |
| [`Study.usage`](#studyusage) | Count and size what one study stores |
| [`Study.delete`](#studydelete) | Delete a study with its declarations and runs |
| [`Experiment`](#experiment) | Attributes of a run |
| [`Experiment.finish`](#experimentfinish) | Mark a run as succeeded |
| [`Experiment.fail`](#experimentfail) | Mark a run as failed and save the error |
| [`Experiment.as_dict`](#experimentas_dict) | Return the run as a dictionary |
| [`Experiment.delete`](#experimentdelete) | Delete a finished run and what it recorded |

The examples on this page continue from this setup:

```python
import acquirium as aq

acq = aq.init()
study = acq.study.define("pump tuning")
```

## `acq.study.define`

```python
acq.study.define(name)
```

Creates a study. If a study with this name already exists, it is reopened with its saved declarations. Note that calling `define()` at the top of a script is safe to repeat.

**Parameters**

- `name` (`str`): the study name. It must contain at least one character that is not whitespace.

**Returns**

A [`Study`](#study).

**Raises**

- `requests.HTTPError` (400): when `name` is empty or only whitespace.

**Example**

```python
study = acq.study.define("pump tuning")
print(study)
```

```text
<Study 'pump tuning': 0 inputs, 0 outputs, 0 logs>
```

## `acq.study.get`

```python
acq.study.get(name_or_id)
```

Reopens a saved study. Unlike `define()`, it does not create one.

**Parameters**

- `name_or_id` (`str`): the study name or its `study_id`.

**Returns**

A [`Study`](#study).

**Raises**

- `requests.HTTPError` (404): when no study has this name or ID.

**Example**

```python
same = acq.study.get("pump tuning")
print(same.study_id == study.study_id)
```

```text
True
```

## `acq.study.all`

```python
acq.study.all()
```

Lists every study on the server, oldest first.

**Returns**

`list[Study]`.

**Example**

```python
acq.study.define("b")
print(acq.study.all())
```

```text
[<Study 'pump tuning': 1 input, 2 outputs, 0 logs>, <Study 'b': 0 inputs, 0 outputs, 0 logs>]
```

## `acq.study.usage`

```python
acq.study.usage()
```

Counts what all studies store and estimates its size. A file used by several runs is counted once.

**Returns**

A `dict` with these keys:

| Key | Meaning |
|---|---|
| `studies`, `variables`, `runs` | Number of studies, declared variables and runs |
| `observations` | Number of recorded values |
| `annotations` | Number of stream annotations |
| `samples` | Number of time-series samples stored by runs |
| `files` | Number of stored files, tables and objects |
| `sample_bytes` | Estimated size of the samples |
| `file_bytes` | Exact size of the stored files |
| `ledger_bytes` | Estimated size of the study, run and observation records |
| `total_bytes` | Sum of the three sizes |

Note that the estimates describe the stored values, not the compressed size of the database.

**Example**

```python
print(acq.study.usage())
```

```text
{'studies': 1, 'variables': 3, 'runs': 3, 'observations': 9, 'annotations': 0, 'samples': 6, 'files': 0, 'sample_bytes': 96, 'file_bytes': 0, 'ledger_bytes': 1983, 'total_bytes': 2079}
```

## `acq.study.clear`

```python
acq.study.clear(*, confirm=False)
```

Deletes every study, declaration and run, with the samples and files the runs stored. Other streams, their metadata and the plant model are not touched.

**Parameters**

- `confirm` (`bool`, default `False`): must be `True`. This guards against deleting everything by accident.

**Returns**

A `dict` with the number of `studies`, `variables`, `runs`, `observations`, `annotations`, `samples` and `files` removed.

**Raises**

- `ValueError`: when `confirm` is not `True`.
- `requests.HTTPError` (409): when any run is still running. Nothing is deleted.

**Example**

```python
print(acq.study.clear(confirm=True))
```

```text
{'studies': 1, 'variables': 0, 'runs': 0, 'observations': 0, 'annotations': 0, 'samples': 0, 'files': 0}
```

## `Study`

```python
study = acq.study.define(name)
```

A study object. Create it with [`acq.study.define`](#acqstudydefine) or [`acq.study.get`](#acqstudyget), not directly. One study object has at most one active run at a time.

| Attribute | Type | Meaning |
|---|---|---|
| `name` | `str` | The study name |
| `study_id` | `str` | The study ID |
| `uri` | `str` | The URI of the study in the metadata graph |
| `created_at` | `datetime` | When the study was created, in UTC |
| `input`, `output`, `log` | collection | Declare and look up variables by role. See [Declaring variables](declaring-variables.md) |
| `variables` | catalog | Every declared variable. See [`Study.variables`](declaring-variables.md#studyvariables) |
| `experiments` | collection | The runs of the study. See [Selecting and comparing runs](selecting-runs.md) |

Printing a study shows the number of declared variables and the active run. It does not contact the server.

**Example**

```python
print(study.name, study.uri)
```

```text
pump tuning urn:acquirium:experiment:study/3a924e5c-db1f-4891-ab94-c7dcf36f946b
```

## `Study.start`

```python
Study.start(metadata=None)
```

Starts a run and makes it the active run of this study object. Values recorded through the study's variables go to the active run.

**Parameters**

- `metadata` (`dict | None`, default `None`): fields that describe this run, such as a scenario name or settings. Values must be convertible to JSON (see [value conversion](codecs-and-values.md#value-conversion)). Keys use letters, digits, underscores and hyphens, and start with a letter or underscore. A top-level value of `None`, `[]` or `{}` is dropped.

**Returns**

An [`Experiment`](#experiment) with status `"running"`.

**Raises**

- `RuntimeError`: when this study object already has an active run.
- `requests.HTTPError` (400): when a metadata key is not valid, or is one of the reserved keys `study_id`, `run_id`, `variable_id`, `annotation_id`, `role`, `kind`, `status`, `created_at`, `started_at`, `finished_at`, `range_start`, `range_end`, `stream_uri`, `label`. Names with a fixed meaning in the plant model, such as `unit` and `entity`, are also rejected. Put such fields inside a nested object, for example `{"context": {"unit": "kW"}}`.

**Example**

```python
run = study.start({"scenario": "baseline", "settings": {"flow": 12}})
print(run)
```

```text
<Experiment d0f5b988-53a7-4062-9417-88a623318346 running {'scenario': 'baseline', 'settings': {'flow': 12}}>
```

## `Study.resume`

```python
Study.resume(run_id)
```

Makes a saved run that is still running the active run of this study object. This is useful for continuing a run from another script or session.

**Parameters**

- `run_id` (`str`): the ID of a running run of this study.

**Returns**

An [`Experiment`](#experiment).

**Raises**

- `RuntimeError`: when this study object already has an active run.
- `ValueError`: when the run belongs to another study, or has already finished or failed.
- `requests.HTTPError` (404): when no run has this ID.

**Example**

```python
run_id = study.start({"scenario": "baseline"}).run_id

# later, in another session
study = acq.study.get("pump tuning")
run = study.resume(run_id)
print(run)
```

```text
<Experiment 9229d5f6-6c7b-4f64-b51d-148a1b89e320 running {'scenario': 'baseline'}>
```

## `Study.usage`

```python
Study.usage()
```

Counts what this study stores and estimates its size.

**Returns**

A `dict` with the keys described under [`acq.study.usage`](#acqstudyusage).

**Example**

```python
print(study.usage())
```

```text
{'studies': 1, 'variables': 3, 'runs': 3, 'observations': 9, 'annotations': 0, 'samples': 6, 'files': 0, 'sample_bytes': 96, 'file_bytes': 0, 'ledger_bytes': 1983, 'total_bytes': 2079}
```

## `Study.delete`

```python
Study.delete()
```

Deletes the study, its declarations and every run, with the samples and files the runs stored.

**Returns**

A `dict` with the number of `studies`, `variables`, `runs`, `observations`, `annotations`, `samples` and `files` removed.

**Raises**

- `requests.HTTPError` (409): when a run of the study is still running. Nothing is deleted.

**Example**

```python
print(study.delete())
```

```text
{'studies': 1, 'variables': 3, 'runs': 3, 'observations': 6, 'annotations': 0, 'samples': 4, 'files': 0}
```

## `Experiment`

```python
run = study.start(metadata)
```

One run of a study. Get it from [`Study.start`](#studystart), [`Study.resume`](#studyresume) or [`study.experiments`](selecting-runs.md).

| Attribute | Type | Meaning |
|---|---|---|
| `run_id` | `str` | The run ID |
| `experiment_id` | `str` | The same value as `run_id`. Result tables use this name |
| `uri` | `str` | The URI of the run in the metadata graph |
| `study` | `Study` | The study the run belongs to |
| `status` | `str` | `"running"`, `"succeeded"` or `"failed"` |
| `metadata` | `dict` | The metadata given to `start()` |
| `started_at` | `datetime` | When the run started, in UTC |
| `finished_at` | `datetime \| None` | When the run ended, or `None` while it is running |
| `error` | `dict \| None` | The saved error of a failed run, with keys `type` and `message` |
| `output` | mapping | Record outputs by label. See [`Experiment.output`](recording.md#experimentoutput) |

A run also reads its recorded values with [`Experiment.observations`](reading.md#experimentobservations) and marks stream intervals with [`Experiment.annotate_stream`](annotations.md#experimentannotate_stream).

**Example**

```python
print(run.status, run.metadata, run.finished_at)
```

```text
running {'scenario': 'baseline', 'settings': {'flow': 12}} None
```

## `Experiment.finish`

```python
Experiment.finish()
```

Marks the run as succeeded and clears the active run of its study. A finished run accepts no more recorded values. Stream annotations can still be added to it.

**Returns**

A `dict` with `run_id`, `status` and `finished_at` (an ISO 8601 string). The `status` and `finished_at` attributes of the run are updated.

**Raises**

- `requests.HTTPError` (400): when the run has already finished or failed.

**Example**

```python
print(run.finish())
```

```text
{'run_id': 'd0f5b988-53a7-4062-9417-88a623318346', 'status': 'succeeded', 'finished_at': '2026-11-18T00:54:40.712606+00:00'}
```

## `Experiment.fail`

```python
Experiment.fail(error)
```

Marks the run as failed, saves the error and clears the active run of its study.

**Parameters**

- `error` (`BaseException` or any value): the reason. Its type name and `str(error)` are saved.

**Returns**

A `dict` with `run_id`, `status` and `finished_at`. Note that the `error` attribute of this object is not updated. Read the saved error from a fresh handle, as in the example.

**Raises**

- `requests.HTTPError` (400): when the run has already finished or failed.

**Example**

```python
run = study.start()
try:
    raise ValueError("bad input")
except ValueError as error:
    run.fail(error)

print(study.experiments.get(run.run_id).error)
```

```text
{'message': 'bad input', 'type': 'ValueError'}
```

## `Experiment.as_dict`

```python
Experiment.as_dict()
```

Returns the run as a dictionary.

**Returns**

A `dict` with the keys `experiment_id`, `status`, `metadata`, `started_at`, `finished_at` and `error`.

**Example**

```python
print(run.as_dict())
```

```text
{'experiment_id': '1409a1c3-faec-4f57-9fd0-06ffae570005', 'status': 'running', 'metadata': {'scenario': 'baseline'}, 'started_at': datetime.datetime(2026, 11, 18, 0, 58, 35, 547670, tzinfo=datetime.timezone.utc), 'finished_at': None, 'error': None}
```

## `Experiment.delete`

```python
Experiment.delete()
```

Deletes a finished or failed run. This removes its recorded values, its stream annotations, the time-series samples it stored and the files no other run uses.

**Returns**

A `dict` with the number of `runs`, `observations`, `annotations`, `samples` and `files` removed.

**Raises**

- `requests.HTTPError` (409): when the run is still running, or when another run recorded a reference to a stream this run stored.
- `requests.HTTPError` (404): when the run no longer exists.

**Example**

```python
print(run.delete())
```

```text
{'runs': 1, 'observations': 3, 'annotations': 0, 'samples': 2, 'files': 0}
```
