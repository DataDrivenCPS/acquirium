---
title: Debugging queries for an unexpected result
---

<!-- TODO: intro -->

## When a query returns nothing or more/less than you expect

An empty or unexpected result is usually one of five things.

### 1. The text resolved to the wrong concept

In this case, the query runs without errors, but the result might look unexpected.
The text matcher finds the closest match it can, for classes, predicates and attribute values alike.

**DEBUG:** Check with `acq.client.resolve(text, kind, top_k=3)`.

```python
acq.client.resolve("ro membrane", "class", top_k=3, min_score=0.3)
```
```text
[{'uri': 'urn:nawi-water-ontology#ReverseOsmosisMembrane',        'score': 0.839, ...},
 {'uri': 'urn:nawi-water-ontology#ElectricallyConductingMembrane', 'score': 0.788, ...},
 {'uri': 'urn:nawi-water-ontology#MolybdenumSulfideBasedMembrane', 'score': 0.786, ...}]
```

Here the top match is the right one, but the runners-up are close.
A slightly different phrasing can land on another membrane class, and the query would still run.
If the top candidate is not what you meant, rephrase the text or pass the URI directly.

### 2. The focus is on the wrong node

`related()` and `measurement()` without `frm=`, and `where()` without `target=`, all act on the node the pointer is on.
After a `related()` the pointer is the new entity, so

```python
acq.query().entity("pump").related("tank").related("valve")
```

finds the shape `Pump -> Tank -> Valve`.
If you are looking for

```text
Pump -> Tank
    \-> Valve
```

you should refocus:

```python
acq.query().entity("pump").related("tank").refocus("pump").related("valve")
```

Further, after a `measurement()` the pointer moves to the measurement, not the equipment.
You might encounter this when running `where()` after a `measurement()`.
Attributes that do not apply to the node raise rather than return nothing:

```text
ValueError: attribute 'process' does not apply to data node 'pump_data'
ValueError: include: attribute 'unit' does not apply to entity node 'pump'
```

### 3. Nothing was within reach

`related()` stops at `max_depth` (3 by default, 1 for predicate lists).
Raise the `max_depth` value, or pass `max_depth=0` for an unbounded walk.

```python
q = acq.query().entity(uri="wbs:intake", alias="intake")

q.related("reverse osmosis membrane").metadata()                # empty: the RO is more than 3 hops from the intake
q.related("reverse osmosis membrane", max_depth=0).metadata()   # unbounded: finds wbs:RO
```
<!-- pending live capture on seawater-ro -->

### 4. `required=True` removed the rows

An attribute that is missing from the data drops every row that lacks it.
Take `required=` off and look for nulls in the column.
The RO pressures below carry no unit, so `required=True` silently drops them:

```python
q = acq.query().entity(uri="wbs:RO", alias="ro").measurement(alias="m")

q.include("unit", required=True).metadata()    # no pressure rows
q.include("unit").metadata()                   # the pressures are back, with null units
```
```text
shape: (6, 3)                                    # include("unit", required=True)
┌────────┬─────────────────────────────────┬─────────────────────┐
│ ro     ┆ m                               ┆ m.unit              │
╞════════╪═════════════════════════════════╪═════════════════════╡
│ wbs:RO ┆ wbs:RO-membrane-area            ┆ unit:M2             │
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-flow-mass-tds         ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-flow-mass-water       ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-flow-mass-water      ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-flow-mass-tds        ┆ unit:KiloGM-PER-SEC │
└────────┴─────────────────────────────────┴─────────────────────┘

shape: (6, 3)                                    # include("unit")
┌────────┬─────────────────────────────────┬─────────────────────┐
│ ro     ┆ m                               ┆ m.unit              │
╞════════╪═════════════════════════════════╪═════════════════════╡
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-pressure              ┆ null                │
│ wbs:RO ┆ wbs:RO-membrane-area            ┆ unit:M2             │
│ wbs:RO ┆ wbs:RO-out-retentate-pressure   ┆ null                │
│ wbs:RO ┆ wbs:RO-in-temperature           ┆ null                │
│ wbs:RO ┆ wbs:RO-out-flow-mass-water      ┆ unit:KiloGM-PER-SEC │
└────────┴─────────────────────────────────┴─────────────────────┘
```

### 5. The points are on connection points you excluded

`include_connection_points=False` keeps only directly-owned points, which for
most equipment is very few.
For `wbs:RO` it excludes every pressure reading, because they all live on the
inlet and outlet:

```python
acq.query().entity(uri="wbs:RO").measurement(include_connection_points=False).metadata()
```

When in doubt, run `metadata()` after each step and see where the rows
disappear.
`facets()` on the last surviving node shows what values actually exist there.
