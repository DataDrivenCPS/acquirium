---
title: 1 - Inserting Data
---

In this article we'll describe the simplest way to insert data into Acquirium.
This method is useful for inserting static datasets, one-off imports, running examples and tests.
You can also use it for building your own data ingestion pipeline.

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
from datetime import datetime, timezone

rows = [(datetime(2026, 8, 8, 10, 0, tzinfo=timezone.utc), 412.0),
        (datetime(2026, 8, 8, 10, 5, tzinfo=timezone.utc), 415.3)]

acq.insert_timeseries("lab-import", "effluent-tds", rows)
```
```text
{'ok': True, 'rows_inserted': 2}
```

To insert into several streams at once, use `insert_timeseries_batch()` with a dictionary of `stream_name` to rows:

```python
acq.insert_timeseries_batch("lab-import", {
    "effluent-tds": [(datetime(2026, 8, 8, 10, 10, tzinfo=timezone.utc), 418.1)],
    "effluent-ph":  [(datetime(2026, 8, 8, 10, 10, tzinfo=timezone.utc), 7.2)],
})
```
```text
{'ok': True, 'rows_inserted': 2, 'batches': 1}
```

Large inputs are split into several requests automatically, so you can pass the whole dataset in one call.

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

```{button-ref} querying-data
:ref-type: doc
:color: primary
:outline:
:align: right

Next: Querying Data →
```
