---
title: Inserting data
---

Most data enters acquirium through drivers, covered in the [drivers guide](./drivers.md).
Drivers can pull data from any source, and once registered, acquirium manages running them for continuous data ingestion.
Use this only if you want to build your own data ingestion pipeline.
This might be useful for backfills, one-off imports and tests.


## Writing data

The client can write timeseries data directly.

A stream is identified by a `source_id` (who writes it) and a `ref_name`
(which series).
Register both before the first row:

```python
acq.register_datasource("lab-import")

acq.register_streams([{
    "source_id": "lab-import",
    "ref_name": "effluent-tds",
    "value_kind": "numeric",
    "point_uri": "urn:swro/effluent-tds",
    "label": "Effluent TDS (lab)",
    "unit": "mg/L",
    "quantity_kind": "mass concentration",
}])
```

`point_uri` ties the stream to a point in the semantic model.
This link makes the rows reachable by queries.
A stream registered without it gets a placeholder point, `<ref_uri>__point`,
labelled `source_id__ref_name` unless you pass `label`.
`measurement()` on an empty query finds it, but no equipment refers to it, so
topology queries do not reach it.
`unit`, `quantity_kind`, `medium` and `substance` accept free text or URIs,
like everywhere else.

When `point_uri` names a point the graph already has, the metadata you pass
is checked against it.
A field the point lacks is added, a field that conflicts raises `ValueError`
before anything is inserted, and a unit that differs from the point's is
accepted only when the two are convertible.
In that case it is recorded on the reference node as the storage unit, and
reads convert to the point's unit automatically; see
[Automatic conversion](../explanation/units.md#automatic-conversion).

Registration is required before the first insert.
Inserting to an unregistered stream raises an error:

```text
HTTPError: 400 Client Error: Bad Request for url: http://localhost:8000/insert_timeseries;
response body: stream urn:acquirium#a98759dd-... is not registered
```

Then insert rows as `(timestamp, value)` pairs:

```python
from datetime import datetime, timezone

rows = [(datetime(2026, 8, 8, 10, 0, tzinfo=timezone.utc), 412.0),
        (datetime(2026, 8, 8, 10, 5, tzinfo=timezone.utc), 415.3)]

acq.insert_timeseries("lab-import", "effluent-tds", rows)
```

For several streams at once, `insert_timeseries_batch(source_id, {ref_name:
rows, ...})` chunks the upload automatically.
For high volume, `insert_timeseries_arrow(source_id, table)` takes a
`(ts, ref_name, value)` arrow table; this is the path the drivers use.

Writes are idempotent on the (stream, timestamp) pair.
Re-inserting the same timestamps overwrites those rows, so re-running an
import is safe and will not duplicate anything.
`replace=True` on `insert_timeseries` clears the stream first.

### The logbook

Separate from the timeseries, acquirium keeps a plant logbook for human notes.

```python
acq.insert_log("backwash started early, foam in tank 2")
acq.read_logs()
```

`read_logs()` returns the notes as a frame with `message`, `log_time` and the
observation period a note covers.
Both calls filter by `log_time_start=`/`log_time_end=` and
`observation_start=`/`observation_end=`.
`delete_logs()` clears the logbook.
