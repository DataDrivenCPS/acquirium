---
title: Units
---

In this article we'll describe how Acquirium handles units: where a unit is kept, what happens when a stream and its property have different units, and how values are converted.
Units are the standard units of [QUDT](https://qudt.org/).
Wherever a unit is accepted, it can be written as plain text such as "psi" or "mg/L" (see [Text matcher](text-resolution.md)).

## Where a unit is kept

A unit can be kept in two places:

- The **unit of the property** says in which unit the property is read. A query uses this one: `where(unit=...)` filters on it, `include("unit")` shows it and `units()` reports it.
- The **unit of the stream** says in which unit the values of the stream are stored.

Most of the time the two are the same, and only the first one matters.
They differ when a plant model gives a property in one unit and the source of the data reports another one.
For instance, the model has a pressure in pascals and the gauge reports psi.

## How a unit is recorded

A unit is recorded when a stream is registered, with the `unit` key of `register_streams()` or with `unit=` of `self.declare()` in a driver.
A plant model can also give the unit of a property, as `qudt:hasUnit`.

When the stream is registered on a property of the plant model, the unit you pass is compared with the unit of the property:

| the property has | you pass | result |
|---|---|---|
| no unit | a unit | the unit is written on the property |
| a unit | the same unit | the unit is also recorded as the unit of the stream |
| a unit | a different unit that can be converted to it | the unit is recorded as the unit of the stream, and the property keeps its own |
| a unit | a unit that cannot be converted to it | `ValueError`, and nothing is written |

For instance, a temperature unit on a pressure property:

```python
acq.register_streams([{
    "source_id": "scada",
    "stream_name": "PT-101",
    "property_uri": "wbs:P1-out-pressure-pa",
    "unit": "degC",
}])
```
```text
ValueError: unit mismatch for point <urn:swro/P1-out-pressure-pa>: the point has <http://qudt.org/vocab/unit/PA>, the stream provided <http://qudt.org/vocab/unit/DEG_C> and they are not convertible
```

Note that the unit is the only key that accepts a different value.
A different `quantity_kind`, `medium` or `substance` always raises a `ValueError`, because these cannot be converted.

A stream registered without a `property_uri` gets a property of its own, and the unit is written there.

## What convertible means

Two units can be converted into each other when they measure the same physical dimension.
QUDT gives every unit a dimension vector, and the server compares the two vectors.
For instance, pascal and psi are both pressures, so they are convertible.
Pascal and degree Celsius are not.

When a unit has no dimension vector, the server compares the quantity kinds of the two units instead.

## Automatic conversion

When the unit of the stream differs from the unit of the property, the values are converted while they are read.
You always get the values in the unit of the property.

The examples use the small model of [4 - Inserting a Plant Model](../tutorials/inserting-plant-model.md), where the property `wbs:P1-out-pressure-pa` is in pascals.
The stream is registered in psi:

```python
from datetime import datetime, timezone

def at(minute):
    return datetime(2026, 8, 8, 10, minute, tzinfo=timezone.utc)

acq.register_streams([{
    "source_id": "scada",
    "stream_name": "PT-101",
    "property_uri": "wbs:P1-out-pressure-pa",
    "unit": "psi",
}])
acq.insert_timeseries("scada", "PT-101", [(at(0), 14.5), (at(5), 15.0), (at(10), 100.0)])

d = acq.query().measurement(quantity_kind="pressure").data()
d.units()
```
```text
{'data': ['http://qudt.org/vocab/unit/PA']}
```

```python
d.dataframe()
```
```text
shape: (3, 2)
┌─────────────────────────┬──────────────────────────┐
│ time                    ┆ data__P1 out pressure pa │
│ ---                     ┆ ---                      │
│ datetime[μs, UTC]       ┆ f64                      │
╞═════════════════════════╪══════════════════════════╡
│ 2026-08-08 10:00:00 UTC ┆ 99973.980751             │
│ 2026-08-08 10:05:00 UTC ┆ 103421.359398            │
│ 2026-08-08 10:10:00 UTC ┆ 689475.729317            │
└─────────────────────────┴──────────────────────────┘
```

The stored values are not changed.
`acq.stream("scada", "PT-101").dataframe()` reads the stream without its property, and returns 14.5, 15.0 and 100.0.

### When the units do not convert

Registration refuses a unit that cannot be converted, so this only happens when the unit of the property is changed in the plant model afterwards.
In that case the values are returned as they are stored, with a warning:

```text
UserWarning: point <urn:swro/P1-out-pressure-pa> is annotated http://qudt.org/vocab/unit/DEG_C but its stream is stored in http://qudt.org/vocab/unit/PSI, which does not convert; the values are returned in http://qudt.org/vocab/unit/PSI
```

Be aware that `units()` still reports the unit of the property here, although the values are in the unit of the stream.
`convert_to()` raises a `ValueError` for such a property until the model is corrected.
Passing `from_unit=` states the unit of the values, for instance `convert_to("bar", from_unit="psi")`.

## Converting on request

`convert_to()` converts the values to a unit you choose.
It returns a new object and leaves the original unchanged.

```python
bar = d.convert_to("bar")
bar.units()
```
```text
{'data': ['http://qudt.org/vocab/unit/BAR']}
```

```python
bar.dataframe()
```
```text
shape: (3, 2)
┌─────────────────────────┬──────────────────────────┐
│ time                    ┆ data__P1 out pressure pa │
│ ---                     ┆ ---                      │
│ datetime[μs, UTC]       ┆ f64                      │
╞═════════════════════════╪══════════════════════════╡
│ 2026-08-08 10:00:00 UTC ┆ 0.99974                  │
│ 2026-08-08 10:05:00 UTC ┆ 1.034214                 │
│ 2026-08-08 10:10:00 UTC ┆ 6.894757                 │
└─────────────────────────┴──────────────────────────┘
```

Each property is converted from its own unit.
So a query that matches properties in different units ends up in one unit.

A property with no unit cannot be converted this way:

```text
ValueError: No unit annotation found for point <urn:swro/P1-speed> (alias 'data'). Provide from_unit explicitly.
```

Pass `from_unit=` to say which unit the values are in.

The target unit is matched as text, like any other unit.
When the text matches several units, the server picks one that can be converted from the unit of the values.
When none can be converted, `convert_to()` raises a `ValueError`.

## The arithmetic

QUDT describes every unit with a multiplier and an offset, relative to the base unit of its dimension.
A value is converted through that base unit:

```text
converted = (value + from_offset) * from_multiplier / to_multiplier - to_offset
```

For instance, psi has the multiplier 6894.757 and pascal has the multiplier 1, so 14.5 psi is 14.5 * 6894.757 = 99973.98 Pa.
Only numbers are converted.

## Further reading

- [`register_streams`](../reference/client/streams.md#acquiriumregister_streams): the keys of a stream and the checks at registration.
- [`DataObject.units`](../reference/query/data-object.md#dataobjectunits) and [`DataObject.convert_to`](../reference/query/data-object.md#dataobjectconvert_to): the full description of both methods.
- [Text matcher](text-resolution.md): how the text of a unit is matched.
