---
title: Debug an empty or unexpected query result
---

In this article we'll describe what to check when a query selects nothing, or fewer streams than expected.
The examples use the seawater desalination plant of [5 - A Full Example](../tutorials/a-full-example.md).

```python
import acquirium as aq

acq = aq.init()
```

## Finding the step that loses the matches

Build the query one step at a time and print it after each step.
`print()` shows how the query was understood and how many matches it has:

```python
q = acq.query().entity("pump")
print(q)
```
```text
Query
  pump: entity, Pump
3 matches:
┌────────────┐
│ pump       │
│ ---        │
│ str        │
╞════════════╡
│ wbs:intake │
│ wbs:P2     │
│ wbs:P1     │
└────────────┘
```

```python
q = q.measurement(alias="m", quantity_kind="power")
print(q)
```
```text
Query
  pump: entity, Pump
  m: measurement, of pump, quantity_kind=Power
2 matches:
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ m                       ┆ m.label             │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power │
└────────┴─────────────────────────┴─────────────────────┘
```

```python
q = q.where(unit="kW")
print(q)
```
```text
Query
  pump: entity, Pump
  m: measurement, of pump, quantity_kind=Power, unit=KiloW
0 matches
```

The matches are lost at the last step.
The sections below list the common causes, each with a way to check it.

## The text was matched to another term

A filter value such as `"kW"` is text, and the text matcher turns it into a term of the ontology.
The printed query shows the term that was used.
In the example above it is `unit=KiloW`, but the power streams of the pumps are stored in watts.
A filter compares terms and does not convert between units.

`options()` on the step before the filter lists the values that exist:

```python
acq.query().entity("pump").measurement(alias="m", quantity_kind="power").options("unit")
```
```text
shape: (1, 2)
┌────────┬───────┐
│ unit   ┆ count │
│ ---    ┆ ---   │
│ str    ┆ i64   │
╞════════╪═══════╡
│ unit:W ┆ 2     │
└────────┴───────┘
```

To read these streams in kilowatts, filter on the quantity kind and convert the values with `data().convert_to("kW")`.

The same can happen with a class.
`"valve"` is matched to the class `Valve`, which this plant model does not use:

```python
print(acq.query().entity("valve"))
```
```text
Query
  valve: entity, Valve
0 matches
```

To see the classes the model uses, count them with `options("type")`:

```python
acq.query().entity("equipment").options("type")
```
```text
shape: (18, 2)
┌─────────────────────────────┬───────┐
│ type                        ┆ count │
│ ---                         ┆ ---   │
│ str                         ┆ i64   │
╞═════════════════════════════╪═══════╡
│ watr:FlowSensor             ┆ 12    │
│ watr:PressureSensor         ┆ 6     │
│ watr:Pump                   ┆ 4     │
│ watr:Reactor                ┆ 4     │
│ watr:Tank                   ┆ 4     │
│ …                           ┆ …     │
│ watr:MediaFiltrationUnit    ┆ 1     │
│ watr:PressureExchanger      ┆ 1     │
│ watr:ReverseOsmosisMembrane ┆ 1     │
│ watr:StaticMixer            ┆ 1     │
│ watr:UltravioletLightUnit   ┆ 1     │
└─────────────────────────────┴───────┘
```

A text that matches no term raises an error:

```python
acq.query().entity("cartridge filter")
```
```text
ValueError: Could not resolve 'cartridge filter' as class
```

**Note:** These outputs are from the default setup, which matches exact names only.
With `aq.init(exact_only=False)` close matches are used as well, so a text can be matched to a term you did not mean.
The printed query shows the matched term in both cases.

## The filter is on another node

`where()` filters the current node, which is the node added last.
After `measurement()` the current node is the measurement, so a pump label finds nothing here:

```python
q = acq.query().entity("pump").measurement(alias="m", quantity_kind="pressure")
q.where(label="High-pressure Pump P1").metadata()
```
```text
shape: (0, 2)
┌──────┬─────┐
│ pump ┆ m   │
│ ---  ┆ --- │
│ str  ┆ str │
╞══════╪═════╡
└──────┴─────┘
```

Give the alias of the node to filter as the first argument:

```python
q.where("pump", label="High-pressure Pump P1").metadata()
```
```text
shape: (1, 3)
┌────────┬────────────────────────┬────────────────────┐
│ pump   ┆ m                      ┆ m.label            │
│ ---    ┆ ---                    ┆ ---                │
│ str    ┆ str                    ┆ str                │
╞════════╪════════════════════════╪════════════════════╡
│ wbs:P1 ┆ wbs:P1-out-pressure-pa ┆ P1 out pressure pa │
└────────┴────────────────────────┴────────────────────┘
```

`related()` and `measurement()` also start from the current node.
The query below looks for a reactor connected to the filter, not to the pump:

```python
acq.query().entity("pump").related("filter").related("reactor").metadata()
```
```text
shape: (3, 3)
┌────────┬──────────────────────────┬───────────────────────────┐
│ pump   ┆ filter                   ┆ reactor                   │
│ ---    ┆ ---                      ┆ ---                       │
│ str    ┆ str                      ┆ str                       │
╞════════╪══════════════════════════╪═══════════════════════════╡
│ wbs:P1 ┆ wbs:RO                   ┆ wbs:uv-aop                │
│ wbs:P2 ┆ wbs:RO                   ┆ wbs:uv-aop                │
│ wbs:P1 ┆ wbs:cartridge-filtration ┆ wbs:anti-scalant-addition │
└────────┴──────────────────────────┴───────────────────────────┘
```

`frm=` names the node to start from:

```python
acq.query().entity("pump").related("filter").related("reactor", frm="pump").metadata()
```
```text
shape: (2, 3)
┌────────┬──────────────────────────┬───────────────────────────┐
│ pump   ┆ filter                   ┆ reactor                   │
│ ---    ┆ ---                      ┆ ---                       │
│ str    ┆ str                      ┆ str                       │
╞════════╪══════════════════════════╪═══════════════════════════╡
│ wbs:P1 ┆ wbs:cartridge-filtration ┆ wbs:anti-scalant-addition │
│ wbs:P1 ┆ wbs:RO                   ┆ wbs:anti-scalant-addition │
└────────┴──────────────────────────┴───────────────────────────┘
```

Be aware that an attribute that does not apply to a measurement raises an error instead of returning nothing:

```python
acq.query().entity("pump").measurement().where(process="filtration")
```
```text
ValueError: attribute 'process' does not apply to data node 'pump_data'
```

## The equipment is too far away

`related()` looks at most three steps away by default.
The reverse osmosis membrane is further than that from the intake:

```python
intake = acq.query().entity(uri="wbs:intake", alias="intake")
intake.related("reverse osmosis membrane", direction="downstream").metadata()
```
```text
shape: (0, 2)
┌────────┬──────────────────────────┐
│ intake ┆ reverse osmosis membrane │
│ ---    ┆ ---                      │
│ str    ┆ str                      │
╞════════╪══════════════════════════╡
└────────┴──────────────────────────┘
```

`max_depth=0` removes the limit:

```python
intake.related("reverse osmosis membrane", direction="downstream", max_depth=0).metadata()
```
```text
shape: (1, 2)
┌────────────┬──────────────────────────┐
│ intake     ┆ reverse osmosis membrane │
│ ---        ┆ ---                      │
│ str        ┆ str                      │
╞════════════╪══════════════════════════╡
│ wbs:intake ┆ wbs:RO                   │
└────────────┴──────────────────────────┘
```

`measurement(direction=...)` has the same `max_depth` parameter.

## `required=True` removed the rows

`include()` keeps the rows that have no value for the column and shows `null`.
With `required=True` these rows are removed.
Only two of the eight pump streams have a substance:

```python
m = acq.query().entity("pump").measurement(alias="m")
m.include("substance", required=True).metadata()
```
```text
shape: (2, 4)
┌────────────┬───────────────────────────────┬─────────────────────┬───────────────────────────────┐
│ pump       ┆ m                             ┆ m.label             ┆ m.substance                   │
│ ---        ┆ ---                           ┆ ---                 ┆ ---                           │
│ str        ┆ str                           ┆ str                 ┆ str                           │
╞════════════╪═══════════════════════════════╪═════════════════════╪═══════════════════════════════╡
│ wbs:intake ┆ wbs:intake-in-tss-concentrati ┆ intake in tss       ┆ watr:Constituent-SuspendedSol │
│            ┆ o…                            ┆ concentration kg…   ┆ i…                            │
│ wbs:intake ┆ wbs:intake-in-tds-concentrati ┆ intake in tds       ┆ watr:Constituent-DissolvedSol │
│            ┆ o…                            ┆ concentration       ┆ i…                            │
└────────────┴───────────────────────────────┴─────────────────────┴───────────────────────────────┘
```

Without `required=True` all eight streams are listed.

## The streams are on the connection points

Most streams of a piece of equipment are on its inlets and outlets.
`measurement()` includes them by default.
With `include_connection_points=False` only the streams on the equipment itself are left:

```python
ro = acq.query().entity(uri="wbs:RO", alias="ro")
ro.measurement(alias="m", include_connection_points=False).metadata()
```
```text
shape: (1, 3)
┌────────┬──────────────────────┬──────────────────┐
│ ro     ┆ m                    ┆ m.label          │
│ ---    ┆ ---                  ┆ ---              │
│ str    ┆ str                  ┆ str              │
╞════════╪══════════════════════╪══════════════════╡
│ wbs:RO ┆ wbs:RO-membrane-area ┆ RO membrane area │
└────────┴──────────────────────┴──────────────────┘
```

The default returns 11 streams for `wbs:RO`.

## The URI is not in the model

A URI that is not in the model matches nothing.
Here the URI ends with a zero instead of the letter O:

```python
print(acq.query().entity(uri="wbs:R0", alias="ro"))
```
```text
Query
  ro: entity, uri=R0
0 matches
```

To find the right URI, select the equipment by its class or its label first, as in the sections above.

## The property has no stream

`measurement()` selects only the properties that have a stream.
`property(has_stream=False)` lists the properties of the model that are not connected to a stream yet.
[5 - A Full Example](../tutorials/a-full-example.md) shows how to connect them.

## The query selects streams but the table is empty

When `metadata()` lists the streams and `dataframe()` is empty, the time range has no readings.
`time_range` gives the first and the last timestamp of the selected streams:

```python
q = acq.query().entity("pump").measurement(quantity_kind="power")
start, end = q.data().time_range
print(start, end)
```
```text
2025-01-01 00:00:00+00:00 2025-12-31 23:00:00+00:00
```

```python
q.dataframe(start="2026-01-01").is_empty()
```
```text
True
```

## Moving on

- [Explore a plant model](explore-a-model.md): list the values a model uses before writing a filter.
- [Filtering](../reference/query/filtering.md): `where()` and the attributes a filter can use.
- [Building a query](../reference/query/building.md): all parameters of `entity()`, `related()` and `measurement()`.
