---
title: 3 - Inserting Metadata
---

In this article we'll describe how to annotate streams with metadata, and how to use that metadata in queries.
The examples continue with the two streams from [1 - Inserting Data](inserting-data.md).

This page is also available as a [Python script](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/3_inserting_metadata.py) and a [Jupyter notebook](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/3_inserting_metadata.ipynb).

If you have a plant model, some of this metadata might already be in the graph.
The querying section below works the same way, and you can still add more metadata.
Plant models are discussed in the next tutorial.

## Annotating streams

As described in [0 - Fundamentals](fundamentals.md), the metadata of a stream is attached to its property.
Without a plant model, Acquirium creates this property when the stream is registered.

### Built-in keys

`register_streams()` takes four built-in keys: `unit`, `quantity_kind`, `medium` and `substance`.
The values can be plain text, such as "mg/L" or "mass concentration".
Acquirium matches the text to a standard unit or quantity kind.

Registering a stream again adds the new keys and replaces the ones that changed.
The data of the stream is not touched.

```python
acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-tds",
    "unit": "mg/L",
    "quantity_kind": "mass concentration",
}])
```

If a text does not match anything, the key is skipped with a warning:

```python
acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-ph",
    "unit": "pH units",
}])
```
```text
UserWarning: Could not resolve 'unit' value 'pH units' to a QUDT URI; skipping
```

**Note:** By default only exact names and symbols are matched.
Start Acquirium with `aq.init(exact_only=False)` to also match text that is close.

The symbol of the unit is "pH", so this one is matched:

```python
acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-ph",
    "unit": "pH",
}])
```

### Your own keys

After a stream is registered, `insert_metadata()` adds metadata to it.
It takes the stream and a dictionary of keys, which can be keys of your own:

```python
ph = acq.stream("lab-import", "effluent-ph")

acq.insert_metadata(ph, {"sampled_by": "day shift"})
```
```text
{'ok': True, 'message': 'update applied', 'changed': True, 'nodes': 1}
```

`acq.stream()` selects the stream, as in the previous page.

The values can be text, numbers, booleans, dates, lists and nested dictionaries (see [`insert_metadata`](../reference/client/metadata.md)).
Writing a key of your own again replaces its value.
Note that a key describes the stream as a whole, not single readings.

Your own keys can also be given at registration, with the `metadata` key:

```python
acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-tds",
    "metadata": {"sampled_by": "night shift"},
}])
```


## Querying with metadata

### Seeing the metadata

`include()` adds metadata columns to the result of `metadata()`:

```python
q = acq.query().measurement()
q.include("unit", "quantity_kind", "sampled_by").metadata()
```
```text
shape: (2, 5)
┌─────────────────────────────────┬──────────────┬────────────────────┬────────────────────────────────┬─────────────────┐
│ data                            ┆ data.label   ┆ data.unit          ┆ data.quantity_kind             ┆ data.sampled_by │
│ ---                             ┆ ---          ┆ ---                ┆ ---                            ┆ ---             │
│ str                             ┆ str          ┆ str                ┆ str                            ┆ str             │
╞═════════════════════════════════╪══════════════╪════════════════════╪════════════════════════════════╪═════════════════╡
│ acq:b67d0b22-3ac0-5060-9618-d9… ┆ effluent-tds ┆ unit:MilliGM-PER-L ┆ quantitykind:MassConcentration ┆ night shift     │
│ acq:56d39005-89a9-50a3-9b51-f2… ┆ effluent-ph  ┆ unit:PH            ┆ null                           ┆ day shift       │
└─────────────────────────────────┴──────────────┴────────────────────┴────────────────────────────────┴─────────────────┘
```

`unit:MilliGM-PER-L` is the standard unit that "mg/L" was matched to.
`effluent-ph` has no quantity kind, because none was given.

### Selecting streams by metadata

`where()` works with metadata the same way it works with `label` in the previous page.

By a built-in key:

```python
q.where(quantity_kind="mass concentration").dataframe()
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

By your own key:

```python
q.where(sampled_by="day shift").dataframe()
```
```text
shape: (1, 2)
┌─────────────────────────┬───────────────────┐
│ time                    ┆ data__effluent-ph │
│ ---                     ┆ ---               │
│ datetime[μs, UTC]       ┆ f64               │
╞═════════════════════════╪═══════════════════╡
│ 2026-08-08 10:10:00 UTC ┆ 7.2               │
└─────────────────────────┴───────────────────┘
```

Several filters in one `where()` are combined, and only the streams matching all of them are kept:

```python
q.where(unit="mg/L", sampled_by="night shift").metadata()
```
```text
shape: (1, 2)
┌─────────────────────────────────┬──────────────┐
│ data                            ┆ data.label   │
│ ---                             ┆ ---          │
│ str                             ┆ str          │
╞═════════════════════════════════╪══════════════╡
│ acq:b67d0b22-3ac0-5060-9618-d9… ┆ effluent-tds │
└─────────────────────────────────┴──────────────┘
```

`options()` lists the values a key has.
This is useful to see what you can filter on:

```python
q.options("sampled_by")
```
```text
shape: (2, 2)
┌─────────────┬───────┐
│ sampled_by  ┆ count │
│ ---         ┆ ---   │
│ str         ┆ i64   │
╞═════════════╪═══════╡
│ day shift   ┆ 1     │
│ night shift ┆ 1     │
└─────────────┴───────┘
```

### Converting units

A stream with a unit can be read in another unit:

```python
q.where(unit="mg/L").data().convert_to("g/L").dataframe()
```
```text
shape: (3, 2)
┌─────────────────────────┬────────────────────┐
│ time                    ┆ data__effluent-tds │
│ ---                     ┆ ---                │
│ datetime[μs, UTC]       ┆ f64                │
╞═════════════════════════╪════════════════════╡
│ 2026-08-08 10:00:00 UTC ┆ 0.412              │
│ 2026-08-08 10:05:00 UTC ┆ 0.4153             │
│ 2026-08-08 10:10:00 UTC ┆ 0.4181             │
└─────────────────────────┴────────────────────┘
```

## Moving on

The next page introduces how to insert a graph, and how to connect streams to a plant model, so queries can also select streams by equipment.

```{button-ref} inserting-plant-model
:ref-type: doc
:color: primary
:outline:
:align: right

Next: Inserting a Plant Model →
```
