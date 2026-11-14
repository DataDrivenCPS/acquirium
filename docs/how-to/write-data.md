---
title: Inserting data
---
In this article we'll describe the simplest way to insert data into Acquirium.
This method is useful for inserting static datasets, one-off imports, running examples and tests.
You can also use it for building your own data ingestion pipeline.

For continuous data ingestion from specific sources we recommend writing your own [driver](../tutorials/first-driver.md) instead.

## Stream registration

Inserting data starts with registering the streams in Acquirium.
A stream is identified by a `source_id` (who writes it) and a `ref_name` (which series).
You can consider a stream as a "value column" of your dataset, where `ref_name` is the column header and `source_id` describes the file it comes from.

Register both before the first row:

```python
acq.register_datasource("lab-import")

acq.register_streams([{
    "source_id": "lab-import",
    "ref_name": "effluent-tds",
    "value_kind": "numeric",
    "point_uri": "urn:swro/effluent-tds",    # optional
    "label": "Effluent TDS (lab)",           # optional
    "unit": "mg/L",                          # optional
    "quantity_kind": "mass concentration",   # optional
}])
```
- `value_kind` can be "text" or "numeric".
- `point_uri` ties the stream to a point in the semantic model (see below).
- `label` defaults to the `ref_name` when it is not given.
- To attach metadata, you can use the keys `unit`, `quantity_kind`, `medium` and `substance`, whose values accept free text or URIs.
- You can also insert other metadata with your own keys using `metadata = {key : value}`. See [Attach metadata to a node](attach-metadata.md) for more detail.

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

To insert into several streams at once, use `insert_timeseries_batch()` with a dictionary of `ref_name` to rows.
Every stream in the dictionary has to be registered first:

```python
acq.register_streams([{
    "source_id": "lab-import",
    "ref_name": "effluent-ph",
    "value_kind": "numeric",
}])

acq.insert_timeseries_batch("lab-import", {
    "effluent-tds": [(datetime(2026, 8, 8, 10, 10, tzinfo=timezone.utc), 418.1)],
    "effluent-ph":  [(datetime(2026, 8, 8, 10, 10, tzinfo=timezone.utc), 7.2)],
})
```
```text
{'ok': True, 'rows_inserted': 2, 'batches': 1}
```

Large inputs are split into several requests automatically, so you can pass the whole dataset in one call.

If your data is already in a table, use `insert_timeseries_arrow()`.
It takes an Arrow table with the columns `ts`, `ref_name` and `value`.
This is useful for large imports, and it is the method the drivers use:

```python
import pyarrow as pa

table = pa.table({
    "ts":       [datetime(2026, 8, 8, 10, 15, tzinfo=timezone.utc),
                 datetime(2026, 8, 8, 10, 15, tzinfo=timezone.utc)],
    "ref_name": ["effluent-tds", "effluent-ph"],
    "value":    [420.4, 7.1],
})

acq.insert_timeseries_arrow("lab-import", table)
```

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

## Relating streams to the metadata model

If you have a metadata model of the plant, you can associate the registered streams with the points in that model.
For instance, the model below describes the effluent TDS point:

```python
acq.insert_graph("""
@prefix s223: <http://data.ashrae.org/standard223#> .
@prefix qudt: <http://qudt.org/schema/qudt/> .
@prefix quantitykind: <http://qudt.org/vocab/quantitykind/> .
@prefix unit: <http://qudt.org/vocab/unit/> .

<urn:swro/effluent-tds> a s223:QuantifiableObservableProperty ;
    qudt:hasQuantityKind quantitykind:MassConcentration ;
    qudt:hasUnit unit:MilliGM-PER-L .
""", source_id="plant")
```

Insert the model first, then pass the URI of the point as `point_uri` when registering the stream:

```python
acq.register_streams([{
    "source_id": "lab-import",
    "ref_name": "effluent-tds",
    "value_kind": "numeric",
    "point_uri": "urn:swro/effluent-tds",
}])
```

The stream is now attached to that point, and its data is reachable through the model:

```python
acq.query().measurement(quantity_kind="mass concentration").dataframe()
```
```text
shape: (4, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ data  │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-08-08 10:00:00 UTC ┆ 412.0 │
│ 2026-08-08 10:05:00 UTC ┆ 415.3 │
│ 2026-08-08 10:10:00 UTC ┆ 418.1 │
│ 2026-08-08 10:15:00 UTC ┆ 420.4 │
└─────────────────────────┴───────┘
```

The point already has its unit and quantity kind in the model, so you don't need to repeat them.
If you do pass them, they are checked against the point:

- A field the point does not have is added to it.
- A field that conflicts with the point raises a `ValueError`, and nothing is inserted.
- A different unit is accepted only when it is convertible to the point's unit. See [Units](../explanation/units.md#automatic-conversion) for more detail.

```text
ValueError: quantity_kind mismatch for point <urn:swro/effluent-tds>: the point has
<http://qudt.org/vocab/quantitykind/MassConcentration>, the stream provided
<http://qudt.org/vocab/quantitykind/Temperature>
```

**Note:** If the `point_uri` does not exist in the model, a new point is created with that URI.
See [Load a plant model](load-a-plant-model.md) for loading a full model from a file.
