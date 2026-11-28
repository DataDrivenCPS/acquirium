---
title: 1 - Inserting Data
---

In this article we'll describe the simplest way to insert data into Acquirium.
This method is useful for inserting static datasets, one-off imports, running examples and tests.
You can also use it for building your own data ingestion pipeline.

This page is also available as a [Python script](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/1_inserting_data.py) and a [Jupyter notebook](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/1_inserting_data.ipynb).

## Running the examples

The examples of pages 1 to 4 are parts of a script, or cells of a notebook.
Acquirium has to be started before them, and stopped at the end:

```python
import acquirium as aq

acq = aq.init()

# the examples go here

aq.shutdown()
```

- The pages follow each other: page 2 reads the data inserted in page 1, and so on. Run them in the same folder, in number order.
- The script in [Getting started](../getting-started.md) left a `temperature` stream behind. Run `acquirium clear` in the command line before you start, so your results match the ones shown here. It erases the data Acquirium keeps in the current folder.
- In a notebook, the result of the last line of a cell is displayed. In a script, wrap that line in `print()`.

## Stream registration

Inserting data starts with registering the streams in Acquirium.
A stream is identified by a `source_id` (who writes it) and a `stream_name` (which series).
You can consider a stream as a "value column" of your dataset, where `stream_name` is the column header and `source_id` describes the file it comes from.

Register the streams before the first row:

```python
acq.register_streams([
    {
        "source_id": "lab-import",
        "stream_name": "effluent-tds",
        "value_kind": "numeric",    # optional
    },
    {
        "source_id": "lab-import",
        "stream_name": "effluent-ph",
        "value_kind": "numeric",    # optional
    },
])
```

- `value_kind` can be "text" or "numeric". It is inferred automatically if not given.

**Note:** Registration is required before the first insert. Inserting into an unregistered stream raises an error:

```text
HTTPError: 400 Client Error: Bad Request for url: http://localhost:8000/insert_timeseries;
response body: stream urn:acquirium#a98759dd-... is not registered
```

## Insert timeseries data

Then insert rows as `(timestamp, value)` pairs:

```python
rows = [("2026-08-08 10:00", 412.0),
        ("2026-08-08 10:05", 415.3)]

acq.insert_timeseries("lab-import", "effluent-tds", rows)["rows_inserted"]
```
```text
2
```

A timestamp is a text such as "2026-08-08 10:00", or a Python `datetime`.
Timestamps without a timezone are taken as UTC.

To insert into several streams at once, use `insert_timeseries_batch()` with a dictionary of `stream_name` to rows:

```python
acq.insert_timeseries_batch("lab-import", {
    "effluent-tds": [("2026-08-08 10:10", 418.1)],
    "effluent-ph":  [("2026-08-08 10:10", 7.2)],
})["rows_inserted"]
```
```text
2
```

Large inputs are split into several requests automatically, so you can pass the whole dataset in one call.

To insert a CSV file, read it with a dataframe library such as Polars or pandas, and turn its columns into this dictionary.
This is shown in [5 - A Full Example](a-full-example.md#inserting-the-data-of-the-file).

For large inputs from dataframes or database connections, you can insert Arrow tables directly with `insert_timeseries_arrow()` for higher performance (see the [client reference](../reference/client/timeseries.md)).

## Inserting the same data twice

Rows are identified by their stream and timestamp.
Inserting a row with a timestamp that already exists overwrites the old value, so re-running an import does not duplicate anything.

To make a stream contain exactly the rows you supply, use `replace=True`.
Rows with other timestamps are removed:

```python
acq.insert_timeseries("lab-import", "effluent-tds", rows, replace=True)
```

Be aware that an empty list with `replace=True` clears the stream:

```python
acq.insert_timeseries("lab-import", "effluent-tds", [], replace=True)
```

**Note:** These two calls remove rows of the example. If you run them, run the two inserts above again before moving on to the next page.

```{button-ref} querying-data
:ref-type: doc
:color: primary
:outline:
:align: right

Next: Querying Data →
```
