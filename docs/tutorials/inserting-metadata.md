---
title: 3 - Inserting Metadata
---

--> Intro: in this article we'll describe how to annotate streams with metadata, and how to use that metadata in queries.
--> The examples continue with the two streams from [1 - Inserting Data](inserting-data.md).

## Annotating streams

--> Remind from Fundamentals: metadata is attached to the property of a stream. Without a plant model, Acquirium creates the property.

### Built-in keys

--> `register_streams()` takes `unit`, `quantity_kind`, `medium` and `substance`.
--> Values can be plain text ("mg/L", "mass concentration"). Acquirium matches the text to a standard unit or quantity kind.
--> Registering a stream again adds to what is already there. The data of the stream is not touched.

```python
acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-tds",
    "unit": "mg/L",
    "quantity_kind": "mass concentration",
}])
```

--> What happens when a text does not match anything (warning, the key is skipped).
--> What happens when a value conflicts with an earlier one (error).

### Your own keys

--> The `metadata` key takes any keys of your own.

```python
acq.register_streams([
    {
        "source_id": "lab-import",
        "stream_name": "effluent-tds",
        "metadata": {"sampled_by": "night shift"},
    },
    {
        "source_id": "lab-import",
        "stream_name": "effluent-ph",
        "metadata": {"sampled_by": "day shift"},
    },
])
```

--> Values can be text, numbers, booleans, dates, lists and nested dictionaries (one line, link to the how-to for detail).
--> Mention `insert_metadata()` for adding metadata later, without registering again (link to [Attach metadata to a node](../how-to/attach-metadata.md)).

## Querying with metadata

### Seeing the metadata

--> `include()` adds metadata columns to the result of `metadata()`.

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
│ acq:56d39005-89a9-50a3-9b51-f2… ┆ effluent-ph  ┆ null               ┆ null                           ┆ day shift       │
└─────────────────────────────────┴──────────────┴────────────────────┴────────────────────────────────┴─────────────────┘
```

--> Explain `unit:MilliGM-PER-L`: the standard unit that "mg/L" was matched to.

### Selecting streams by metadata

--> `where()` works with metadata the same way it works with `label` in the previous page.

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

--> Several filters in one `where()` are combined with AND.
--> `options("sampled_by")` lists the values a key has, useful to see what you can filter on.

### Converting units

--> A stream with a unit can be read in another unit.

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

--> Closing: the next page connects streams to a plant model, so queries can also select streams by equipment (placeholder).
