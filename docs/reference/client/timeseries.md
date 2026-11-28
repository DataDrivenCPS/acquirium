---
title: Time series
---

These methods insert values into streams. A stream must be registered with [`register_streams`](streams.md#acquiriumregister_streams) before values are inserted into it. To read values back, use [`Stream.dataframe`](streams.md#streamdataframe) for one stream or a [query](../query/_index.md) for several.

| Item | Purpose |
|---|---|
| [`Acquirium.insert_timeseries`](#acquiriuminsert_timeseries) | Insert values into one stream |
| [`Acquirium.insert_timeseries_batch`](#acquiriuminsert_timeseries_batch) | Insert values into several streams |
| [`Acquirium.insert_timeseries_arrow`](#acquiriuminsert_timeseries_arrow) | Insert an Arrow table of values |

The three methods share these rules:

- A stream holds one value per timestamp. Inserting a value at a timestamp that already has one overwrites it, so sending the same rows twice is safe.
- A timestamp without a time zone is taken as UTC.
- A value is a number, a string or `None`. Numeric-looking strings become numbers in a numeric stream. Other text is preserved and available through `value_mode="text"` or `"coalesce"`. A text stream stores numbers as text.
- A stream registered without a `value_kind` becomes numeric when it receives its first number. Booleans count as numbers and travel as `1.0` or `0.0` on numeric streams, and as `"True"` or `"False"` on text streams.
- A response includes `rows_inserted` and one item in `receipts` per request. By default the client waits up to 30 seconds for publication. Pass `wait=0` for durable acceptance, then call `acq.wait(receipt)` to wait for publication.
- Pass a stable `request_id` to retry the same payload. Escapement deduplicates requests within its retry window. The client retries requests refused with HTTP 503 for up to `retry_for` seconds (60 by default).

The examples on this page use the streams registered on the [Streams](streams.md) page and these two timestamps:

```python
from datetime import datetime, timezone

t0 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
t1 = datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)
```

## `Acquirium.insert_timeseries`

```python
Acquirium.insert_timeseries(source_id, stream_name, rows, *, property_uri=None,
                            replace=False, request_id=None, wait=30, timeout=60, retry_for=60)
```

Inserts values into one stream.

**Parameters**

- `source_id` (`str`): name of the source the stream belongs to.
- `stream_name` (`str`): name of the stream within the source.
- `rows` (`list` of `(datetime, value)` tuples): the values to insert. The timestamp can also be an ISO 8601 string.
- `property_uri` (`str`, optional): kept for older code. It has no effect. Connect a stream to a property with [`register_streams`](streams.md#acquiriumregister_streams).
- `replace` (`bool`, default `False`): when `True`, the stream holds exactly `rows` afterwards. Values at other timestamps are removed, and an empty `rows` clears the stream.

**Returns**

A dict with `ok`, `rows_inserted`, and `receipts`.

**Raises**

- `requests.exceptions.HTTPError`: when the stream is not registered.
- `requests.exceptions.InvalidJSONError`: when a value is `nan` or infinite. Use `None` for a missing value.

**Example**

```python
flow = acq.stream("lab", "flow")
print(acq.insert_timeseries("lab", "flow", [(t0, 1.5), (t1, 2.5)])["rows_inserted"])
print(acq.insert_timeseries("lab", "flow", [(t0, 9.0)], replace=True)["rows_inserted"])
print(flow.dataframe())
```

```text
2
1
shape: (1, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[ns, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ 9.0   │
└─────────────────────────┴───────┘
```

A stream that was not registered:

```python
acq.insert_timeseries("lab", "pressure", [(t0, 1.5)])
```

```text
requests.exceptions.HTTPError: 400 Client Error: Bad Request for url: http://127.0.0.1:50737/insert_timeseries; response body: stream urn:acquirium#... is not registered
```

## `Acquirium.insert_timeseries_batch`

```python
Acquirium.insert_timeseries_batch(source_id, streams, *, request_id=None,
                                  replace=(), wait=30, timeout=60, retry_for=60)
```

Inserts values into several streams of one source. Large inputs are split into several requests of at most 10,000 rows each. Set `insert_batch_rows` on the client to impose a smaller limit.

**Parameters**

- `source_id` (`str`): name of the source the streams belong to.
- `streams` (`dict`): maps each `stream_name` to a list of `(datetime, value)` tuples.

**Returns**

A dict with `ok`, `rows_inserted`, `receipts`, and the number of requests in `batches`.

**Raises**

- `requests.exceptions.HTTPError`: when one of the streams is not registered. Note that requests sent before the failing one are already stored.
- `requests.exceptions.InvalidJSONError`: when a value is `nan` or infinite.

**Example**

```python
print(acq.insert_timeseries_batch("lab", {
    "flow": [(t0, 1.0), (t1, 2.0)],
    "temp": [(t0, 20.5)],
    "state": [(t0, "on"), (t1, "off")],
})["rows_inserted"])
print(acq.stream("lab", "state").dataframe())
```

```text
5
shape: (2, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[ns, UTC]       ┆ str   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ on    │
│ 2026-01-01 01:00:00 UTC ┆ off   │
└─────────────────────────┴───────┘
```

## `Acquirium.insert_timeseries_arrow`

```python
Acquirium.insert_timeseries_arrow(source_id, table, *, request_id=None,
                                  wait=30, timeout=60, retry_for=60)
```

Inserts a PyArrow table that holds values of one or more streams of one source. The table is sent in batches of up to 10,000 rows using Arrow IPC.

**Parameters**

- `source_id` (`str`): name of the source the streams belong to.
- `table` (`pyarrow.Table`): a table with three columns:
  - `ts`: the timestamp.
  - `stream_name`: the name of the stream the row belongs to.
  - `value`: the value. The column has one type, so numbers and strings go in separate tables.
- `request_id` (`str`, optional): a retry identity for the input. Each chunk receives its own derived identity.

**Returns**

A dict with `ok`, `rows_inserted`, and `receipts`.

**Raises**

- `requests.exceptions.HTTPError`: when a stream is not registered or a column is missing.

**Example**

```python
import pyarrow as pa

table = pa.table({
    "ts": pa.array([datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)] * 2,
                   type=pa.timestamp("us", tz="UTC")),
    "stream_name": ["flow", "temp"],
    "value": [3.0, 21.0],
})
print(acq.insert_timeseries_arrow("lab", table)["rows_inserted"])
print(flow.dataframe())
```

```text
2
shape: (3, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[ns, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ 1.0   │
│ 2026-01-01 01:00:00 UTC ┆ 2.0   │
│ 2026-01-01 02:00:00 UTC ┆ 3.0   │
└─────────────────────────┴───────┘
```
