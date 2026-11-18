---
title: 2 - Querying Data
---

In this article we'll describe the simplest way to read data back from Acquirium.
The examples use the two streams inserted in [1 - Inserting Data](inserting-data.md).

This page is also available as a [Python script](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/2_querying_data.py) and a [Jupyter notebook](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/2_querying_data.ipynb).

## Building a query

A query describes which streams you are looking for.
It starts with `acq.query()`, and `measurement()` selects streams.
Printing a query shows a summary of it and the first five streams it selects:

```python
q = acq.query().measurement()     # every registered stream
print(q)
```
```text
Query
  data: measurement
2 matches:
┌─────────────────────────────────┬──────────────┐
│ data                            ┆ data.label   │
│ ---                             ┆ ---          │
│ str                             ┆ str          │
╞═════════════════════════════════╪══════════════╡
│ acq:b67d0b22-3ac0-5060-9618-d9… ┆ effluent-tds │
│ acq:56d39005-89a9-50a3-9b51-f2… ┆ effluent-ph  │
└─────────────────────────────────┴──────────────┘
```

## Reading the data

`dataframe()` returns the data of the selected streams as one table in **wide** format by default.
In this format, there's one column per stream, and the rows are aligned on `time`.
A stream with no reading at a timestamp has `null` there.
Column names are `data__<label>`.
The label is the `stream_name` unless a `label` was explicitly given at registration.

```python
q.dataframe()
```
```text
shape: (3, 3)
┌─────────────────────────┬───────────────────┬────────────────────┐
│ time                    ┆ data__effluent-ph ┆ data__effluent-tds │
│ ---                     ┆ ---               ┆ ---                │
│ datetime[μs, UTC]       ┆ f64               ┆ f64                │
╞═════════════════════════╪═══════════════════╪════════════════════╡
│ 2026-08-08 10:00:00 UTC ┆ null              ┆ 412.0              │
│ 2026-08-08 10:05:00 UTC ┆ null              ┆ 415.3              │
│ 2026-08-08 10:10:00 UTC ┆ 7.2               ┆ 418.1              │
└─────────────────────────┴───────────────────┴────────────────────┘
```

The result is a polars dataframe.
If you use pandas, convert it with `q.dataframe().to_pandas()`.
To open it in Excel, save it as a CSV file with `q.dataframe().write_csv("readings.csv")`.

The other format is **narrow**, which gives one row per reading.
This is useful when the streams do not share timestamps, or for text values.

```python
q.dataframe(shape="narrow")
```
```text
shape: (4, 5)
┌────────────────────┬────────────────────────┬───────────────────────┬───────────────┬────────────┐
│ data_alias         ┆ property_id            ┆ time                  ┆ value_numeric ┆ value_text │
│ ---                ┆ ---                    ┆ ---                   ┆ ---           ┆ ---        │
│ str                ┆ str                    ┆ datetime[μs, UTC]     ┆ f64           ┆ str        │
╞════════════════════╪════════════════════════╪═══════════════════════╪═══════════════╪════════════╡
│ data__effluent-tds ┆ acq:b67d0b22-3ac0-5060 ┆ 2026-08-08 10:00:00   ┆ 412.0         ┆ null       │
│                    ┆ -9618-d9…              ┆ UTC                   ┆               ┆            │
│ data__effluent-tds ┆ acq:b67d0b22-3ac0-5060 ┆ 2026-08-08 10:05:00   ┆ 415.3         ┆ null       │
│                    ┆ -9618-d9…              ┆ UTC                   ┆               ┆            │
│ data__effluent-tds ┆ acq:b67d0b22-3ac0-5060 ┆ 2026-08-08 10:10:00   ┆ 418.1         ┆ null       │
│                    ┆ -9618-d9…              ┆ UTC                   ┆               ┆            │
│ data__effluent-ph  ┆ acq:56d39005-89a9-50a3 ┆ 2026-08-08 10:10:00   ┆ 7.2           ┆ null       │
│                    ┆ -9b51-f2…              ┆ UTC                   ┆               ┆            │
└────────────────────┴────────────────────────┴───────────────────────┴───────────────┴────────────┘
```

## Selecting streams

`where()` filters the selected streams.
Only the streams matching all the given filters are kept.

By label:

```python
acq.query().measurement().where(label="effluent-tds").dataframe()
```
```text
shape: (3, 2)
┌─────────────────────────┬────────────────────┐
│ time                    ┆ data__effluent-tds │
│ ---                     ┆ ---                │
│ datetime[μs, UTC]       ┆ f64                │
╞═════════════════════════╪════════════════════╡
│ 2026-08-08 10:00:00 UTC ┆ 412.0              │
│ 2026-08-08 10:05:00 UTC ┆ 415.3              │
│ 2026-08-08 10:10:00 UTC ┆ 418.1              │
└─────────────────────────┴────────────────────┘
```

By `source_id` and `stream_name`:

```python
tds = acq.stream("lab-import", "effluent-tds")
acq.query().measurement(stream=tds).dataframe()
```

`stream=` also takes a list, to select several streams.

## Selecting a time range

`start` and `end` limit the rows by time.
Both ends are included:

```python
from datetime import datetime, timezone

q.dataframe(
    start=datetime(2026, 8, 8, 10, 5, tzinfo=timezone.utc),
    end=datetime(2026, 8, 8, 10, 10, tzinfo=timezone.utc),
)
```
```text
shape: (2, 3)
┌─────────────────────────┬───────────────────┬────────────────────┐
│ time                    ┆ data__effluent-ph ┆ data__effluent-tds │
│ ---                     ┆ ---               ┆ ---                │
│ datetime[μs, UTC]       ┆ f64               ┆ f64                │
╞═════════════════════════╪═══════════════════╪════════════════════╡
│ 2026-08-08 10:05:00 UTC ┆ null              ┆ 415.3              │
│ 2026-08-08 10:10:00 UTC ┆ 7.2               ┆ 418.1              │
└─────────────────────────┴───────────────────┴────────────────────┘
```

`limit` and `order` give the first or the last rows.
For instance, the latest reading:

```python
q.dataframe(limit=1, order="desc")
```
```text
shape: (1, 3)
┌─────────────────────────┬───────────────────┬────────────────────┐
│ time                    ┆ data__effluent-ph ┆ data__effluent-tds │
│ ---                     ┆ ---               ┆ ---                │
│ datetime[μs, UTC]       ┆ f64               ┆ f64                │
╞═════════════════════════╪═══════════════════╪════════════════════╡
│ 2026-08-08 10:10:00 UTC ┆ 7.2               ┆ 418.1              │
└─────────────────────────┴───────────────────┴────────────────────┘
```

## Checking what a query selects

`metadata()` lists the selected streams without reading their data.
This is useful before a large read.

```python
q.metadata()
```
```text
shape: (2, 2)
┌─────────────────────────────────┬──────────────┐
│ data                            ┆ data.label   │
│ ---                             ┆ ---          │
│ str                             ┆ str          │
╞═════════════════════════════════╪══════════════╡
│ acq:b67d0b22-3ac0-5060-9618-d9… ┆ effluent-tds │
│ acq:56d39005-89a9-50a3-9b51-f2… ┆ effluent-ph  │
└─────────────────────────────────┴──────────────┘
```

## Reading one stream

When you already know the stream, you can skip the query:

```python
acq.stream("lab-import", "effluent-tds").dataframe()
```
```text
shape: (3, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-08-08 10:00:00 UTC ┆ 412.0 │
│ 2026-08-08 10:05:00 UTC ┆ 415.3 │
│ 2026-08-08 10:10:00 UTC ┆ 418.1 │
└─────────────────────────┴───────┘
```

## Moving on

So far the streams only have a name.
The next page shows how to annotate streams with metadata, such as units and quantity kinds, and how to use this metadata in queries.

```{button-ref} inserting-metadata
:ref-type: doc
:color: primary
:outline:
:align: right

Next: Inserting Metadata →
```
