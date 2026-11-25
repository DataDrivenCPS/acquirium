---
title: Stream annotations
---

A stream annotation links a time interval of an existing stream to a run. It copies no samples. This is useful for marking which part of live plant data belongs to a trial. This page covers creating annotations, finding them and reading their samples.

| Item | Description |
|---|---|
| [`Experiment.annotate_stream`](#experimentannotate_stream) | Link a stream interval to a run |
| [`Experiment.stream_annotations`](#experimentstream_annotations) | Find the annotations of one run |
| [`Study.stream_annotations`](#studystream_annotations) | Find the annotations of one study |
| [`acq.study.stream_annotations`](#acqstudystream_annotations) | Find annotations across all studies |
| [`StreamAnnotation`](#streamannotation) | Attributes of an annotation |
| [`StreamAnnotation.close`](#streamannotationclose) | Set the end of an open annotation |
| [`StreamAnnotation.dataframe`](#streamannotationdataframe) | Read the samples in the interval |

The examples continue from this setup:

```python
import acquirium as aq

acq = aq.init()
study = acq.study.define("pump tuning")
power = study.output("power").timeseries(unit="kW")
run = study.start({"scenario": "baseline"})
stream = power.record([("2026-01-01T00:00:00Z", 5.0), ("2026-01-01T00:01:00Z", 5.5)])
```

Any stream works, including one written by a driver. Get its URI from `acq.stream(source_id, stream_name).stream_uri`. See [streams](../client/streams.md).

## `Experiment.annotate_stream`

```python
Experiment.annotate_stream(stream_uri, *, start, end=None, metadata=None)
```

Links an interval of a stream to this run. The run can be running, finished or failed. Intervals may overlap.

**Parameters**

- `stream_uri` (`str`): the URI of the stream.
- `start` (`datetime | str`): the start of the interval. A string must be ISO 8601. A time without a timezone is read as UTC.
- `end` (`datetime | str | None`, default `None`): the end of the interval. Leave it out to keep the interval open and set it later with [`StreamAnnotation.close`](#streamannotationclose).
- `metadata` (`dict | None`, default `None`): fields that describe the interval. The key rules are the same as for run metadata. See [`Study.start`](studies-and-runs.md#studystart).

**Returns**

A [`StreamAnnotation`](#streamannotation).

**Raises**

- `requests.HTTPError` (400): when `stream_uri` is empty, `end` is before `start`, or a metadata key is not valid.
- `requests.HTTPError` (404): when the run does not exist.

The deprecated keyword `ref_uri` is accepted in place of `stream_uri` and emits a `FutureWarning`.

**Example**

```python
annotation = run.annotate_stream(
    stream.stream_uri, start="2026-01-01T00:00:00Z", metadata={"phase": "warmup"}
)
print(annotation.range_start, annotation.range_end, annotation.metadata)
```

```text
2026-01-01 00:00:00+00:00 None {'phase': 'warmup'}
```

## `Experiment.stream_annotations`

```python
Experiment.stream_annotations(*, stream_uri=None, start=None, end=None)
```

Finds the annotations of this run. With no arguments it returns all of them.

**Parameters**

- `stream_uri` (`str | None`, default `None`): keep only annotations of this stream.
- `start` (`datetime | str | None`, default `None`): keep annotations that end at or after this time. Open annotations always match.
- `end` (`datetime | str | None`, default `None`): keep annotations that start at or before this time.

**Returns**

`list[StreamAnnotation]`, ordered by interval start.

**Raises**

- `requests.HTTPError` (400): when `end` is before `start`.

**Example**

```python
print(len(run.stream_annotations()))
```

```text
1
```

## `Study.stream_annotations`

```python
Study.stream_annotations(*, run_id=None, stream_uri=None, start=None, end=None)
```

Finds annotations in the runs of this study.

**Parameters**

- `run_id` (`str | None`, default `None`): keep only annotations of this run.
- `stream_uri` (`str | None`, default `None`): keep only annotations of this stream.
- `start` (`datetime | str | None`, default `None`): keep annotations that end at or after this time, or are open.
- `end` (`datetime | str | None`, default `None`): keep annotations that start at or before this time.

**Returns**

`list[StreamAnnotation]`, ordered by interval start. The list is empty when nothing matches.

**Raises**

- `requests.HTTPError` (400): when `end` is before `start`.

**Example**

```python
found = study.stream_annotations(start="2025-12-31T00:00:00Z")
print([item.metadata for item in found])
```

```text
[{'phase': 'warmup'}]
```

## `acq.study.stream_annotations`

```python
acq.study.stream_annotations(*, stream_uri=None, start=None, end=None)
```

Finds annotations across all studies. This is useful for asking which runs used a given stream.

**Parameters**

- `stream_uri` (`str | None`, default `None`): keep only annotations of this stream.
- `start` (`datetime | str | None`, default `None`): keep annotations that end at or after this time, or are open.
- `end` (`datetime | str | None`, default `None`): keep annotations that start at or before this time.

**Returns**

`list[StreamAnnotation]`, ordered by interval start.

**Raises**

- `requests.HTTPError` (400): when `end` is before `start`.

**Example**

```python
found = acq.study.stream_annotations(stream_uri=stream.stream_uri)
print([item.run_id == run.run_id for item in found])
```

```text
[True]
```

## `StreamAnnotation`

```python
annotation = run.annotate_stream(stream_uri, start=...)
```

One annotation. Get it from [`Experiment.annotate_stream`](#experimentannotate_stream) or one of the `stream_annotations()` methods.

| Attribute | Type | Meaning |
|---|---|---|
| `annotation_id` | `str` | The annotation ID |
| `run_id` | `str` | The run the interval belongs to |
| `template_id` | `str` | The `study_id` of that run's study |
| `stream_uri` | `str` | The URI of the stream |
| `stream` | `Stream` | A handle for the stream. See [streams](../client/streams.md) |
| `range_start` | `datetime` | The start of the interval, in UTC |
| `range_end` | `datetime \| None` | The end of the interval, or `None` while it is open |
| `metadata` | `dict` | The fields given at creation |
| `created_at` | `datetime` | When the annotation was saved, in UTC |

The deprecated attribute `ref_uri` returns `stream_uri` and emits a `FutureWarning`.

**Example**

```python
print(annotation.stream)
```

```text
urn:acquirium#749f4255-20cf-52a5-8762-fa67675cfb6b
```

## `StreamAnnotation.close`

```python
StreamAnnotation.close(end)
```

Sets the end of an open annotation. An annotation can be closed once. This also works after its run has finished.

**Parameters**

- `end` (`datetime | str`): the end of the interval. It must not be before the start.

**Returns**

`None`. The `range_end` attribute is updated.

**Raises**

- `requests.HTTPError` (400): when `end` is before the start, or the annotation is already closed.
- `requests.HTTPError` (404): when the annotation does not exist.

**Example**

```python
annotation.close("2026-01-01T00:01:00Z")
print(annotation.range_end)
```

```text
2026-01-01 00:01:00+00:00
```

## `StreamAnnotation.dataframe`

```python
StreamAnnotation.dataframe(**kwargs)
```

Reads the samples the stream holds now within the interval. Both ends are included. An open annotation reads from its start onward.

**Parameters**

- `**kwargs`: options of the stream read, such as `start`, `end`, `limit` and `order`. `start` and `end` default to the interval. See [`Stream.dataframe`](../client/streams.md).

**Returns**

A Polars `DataFrame` with the columns `time` and `value`.

**Example**

```python
print(annotation.dataframe())
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
```
