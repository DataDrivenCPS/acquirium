---
title: Units
---

This is an explanation of how acquirium handles units: where a unit is
recorded, what happens when the unit of a stream and the unit of its point
disagree, and how values are converted, automatically and on request.
Units are QUDT units throughout; free text is resolved to a QUDT URI wherever
a unit is accepted.

## Where a unit lives

A unit can be recorded in two places, and they mean different things.

The **point's unit** is the unit the plant model declares for a measurement,
as `qudt:hasUnit` on the property node.
<!-- This is the unit a query sees: `where(unit=...)` filters on it,
`include("unit")` shows it, and `units()` reports it. -->

The **storage unit** is the unit the raw rows of one stream are written in,
recorded as `qudt:hasUnit` on the stream's reference node.
It exists because the same point can be fed by a stream whose source reports
in a different unit, for instance a driver ingesting a stream in psi under a
point annotated in pascals.

Reads work in the point's unit.
When a point has no unit of its own, the storage unit is adopted as its
effective unit, so `units()` still reports one.
A stream with neither reports `None`, and `convert_to()` needs `from_unit=`
for it.

`where(unit=...)` filter and `include("unit")` only works if the point node has an annotated unit.

## How a unit gets recorded

A unit reaches the graph through stream registration: `self.declare(...,
unit=...)` in a driver, or `register_streams()` directly.
Free text is resolved against QUDT at that point, jointly with the
`quantity_kind` when both are given, so a quantity kind can disambiguate a
unit and vice versa (see [text resolution](text-resolution.md)).

TODO: This has a risk of registering arbitrary units due to mismatch in text resolver. We need a mechanism to verify the resolved units from the users.

When the `point_uri` already exists in the graph, the unit you pass is checked
against the point's:

| the point has | you pass | result |
|---|---|---|
| no unit | a unit | the unit is written on the point |
| the same unit | the same unit | recorded on the reference node as well |
| a different, convertible unit | psi for a pascal point | recorded on the reference node as the storage unit; reads convert |
| a different, non-convertible unit | celsius for a pascal point | `ValueError`, nothing is inserted |

Note that this is the one metadata field that tolerates a mismatch.
Every other field (`quantity_kind`, `medium`, `substance`, `data_source`)
raises on a conflict, because there is no equivalent of a conversion for them.
See the [lifecycle guide](stream-lifecycle.md#what-registration-writes-to-the-graph).

## What convertible means

Two units are compatible when their QUDT dimension vectors are equal.
This is the reliable check: it accepts pairs that share a physical dimension
under different quantity-kind labels, such as `L` (LiquidVolume) and `MilliL`
(Volume).
When a unit has no dimension vector, the check falls back to an overlap of
quantity kinds, then to a single quantity kind, and when nothing can be
determined the pair is allowed and the conversion may fail at the arithmetic.

`acq.client.get_conversion_factors(from_uri, to_uri)` returns the verdict
(`compatible`) together with the factors.

## Automatic conversion

A point can carry one unit in the graph while its stream stores another.
When the two disagree, the values are converted for you as they are fetched,
to the point's unit, which is the one `units()` reports.
The conversion happens per stream, client side, when a `DataObject` fetches
its rows; the factors are fetched from the server once per unit pair.

For instance, a stream registered in psi under a point annotated in pascals
is stored in psi and read in pascals:

```python
acq.register_streams([{
    "source_id": "gauge", "ref_name": "p1", "value_kind": "numeric",
    "point_uri": "urn:demo/P1-pressure", "unit": "http://qudt.org/vocab/unit/PSI",
}])
acq.insert_timeseries("gauge", "p1", [(t0, 14.5), (t1, 15.0), (t2, 100.0)])

d = acq.query().entity("Pump").measurement(alias="p").data()
d.units()
d.dataframe()
```
```text
{'p': ['http://qudt.org/vocab/unit/PA']}

shape: (3, 2)
┌─────────────────────────┬───────────────┐
│ time                    ┆ p             │
╞═════════════════════════╪═══════════════╡
│ 2026-01-01 00:00:00 UTC ┆ 99973.980751  │
│ 2026-01-01 00:01:00 UTC ┆ 103421.359398 │
│ 2026-01-01 00:02:00 UTC ┆ 689475.729317 │
└─────────────────────────┴───────────────┘
```

If the two are not convertible, the readings come through untouched, in the
stream's unit, and a `UserWarning` names the point and both units.
A mismatch should not break a query that did not ask for a conversion; the
warning says that the model needs a fix.
Since registration rejects a non-convertible unit, this only happens when the
graph was edited after the stream was registered.

```text
UserWarning: point <urn:demo/P1-pressure> is annotated http://qudt.org/vocab/unit/DEG_C
but its stream is stored in http://qudt.org/vocab/unit/PSI, which does not convert;
the values are returned in http://qudt.org/vocab/unit/PSI
```

Be aware that `units()` still reports the point's unit in this case, although
the values are in the stream's unit.
`convert_to()` raises `ValueError` for such a point, because the unit it would
convert from is not the unit of the values.
Passing `from_unit=` states the unit of the values and makes the conversion
possible until the model is fixed, for instance
`convert_to("bar", from_unit="psi")`.

A conversion you asked for with `convert_to()` is different: if it cannot be
applied when the values are fetched, the fetch raises `ValueError` rather
than returning values in the wrong unit.

## Converting on request

`convert_to(to_unit)` returns a new `DataObject` with the values converted and
`units()` updated; the original is not modified.
The source unit is the effective unit of each alias, so after an automatic
conversion the source is the point's unit, not the storage unit.
`alias=` converts one alias and leaves the others alone.
`from_unit=` supplies the source for a point with no unit, and overrides it
otherwise.

The target may be a URI, label, symbol or UCUM code.
It is resolved together with the source: among the top candidates for the
text, the server picks the one that is actually convertible from the source
unit, so a near-match that is not convertible never shadows one that is.
When no candidate is convertible, `convert_to()` raises with both candidate
lists in the message.
The [data tutorial](../tutorials/data.md#units) shows the calls.

## The arithmetic

QUDT expresses every unit as a multiplier and an offset against the base unit
of its dimension.
A value is converted by moving through that base unit:

```text
converted = (value + from_offset) * from_multiplier / to_multiplier - to_offset
```

Only `numeric_value` is converted; text readings pass through unchanged.
