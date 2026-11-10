---
title: Querying
---

A plant model is a graph. Pumps, tanks and membranes are nodes; pipes, inlets
and the sensors attached to them are edges and more nodes. That structure lets
you ask questions in the plant's own terms, such as *which pressure readings
are on the RO?*, instead of hunting for tag names. But nobody wants to write
graph queries by hand. Acquirium's query builder lets you describe what you
are looking for one step at a time, and the server works out how to find it.

This tutorial uses the WaterTAP seawater-RO plant. Start its server as the
[getting started](getting-started.md) page describes, and connect:

```python
from acquirium import Acquirium

acq = Acquirium(server_url="127.0.0.1", server_port=8000)
```

## Find the equipment

Start by asking for every pump:

```python
acq.query().entity("pump").metadata()
```
```text
shape: (3, 1)
┌────────────┐
│ pump       │
╞════════════╡
│ wbs:intake │
│ wbs:P2     │
│ wbs:P1     │
└────────────┘
```

You wrote `"pump"`, and the server resolved it to the class `s223:Pump` and
returned its instances. You could also pass the class's URI. Nothing is sent
to the server until a terminal method such as `metadata()` runs the query.
`metadata()` returns a polars frame with one column per node in the pattern.
The `wbs:` prefix is the model's shorthand for `urn:swro/`.

Keyword arguments narrow the match. Every piece of equipment that performs
reverse osmosis:

```python
acq.query().entity("Equipment", process="reverse osmosis").metadata()
```
```text
shape: (1, 1)
┌───────────┐
│ Equipment │
╞═══════════╡
│ wbs:RO    │
└───────────┘
```

When you already know the node, name it with `uri=`. Give it an alias at the
same time; the alias becomes its column name, and you will use it to refer back
to the node:

```python
ro = acq.query().entity(uri="wbs:RO", alias="ro")
```

## Follow the connections

Equipment is rarely interesting alone. `related()` steps from the current node
to its neighbours. Here is the nearest tank to each pump:

```python
acq.query().entity("pump").related("tank").metadata()
```
```text
shape: (4, 2)
┌────────────┬──────────────────────────────┐
│ pump       ┆ tank                         │
╞════════════╪══════════════════════════════╡
│ wbs:P1     ┆ wbs:storage-tank-2           │
│ wbs:P2     ┆ wbs:storage-tank-2           │
│ wbs:intake ┆ wbs:ferric-chloride-addition │
│ wbs:P1     ┆ wbs:anti-scalant-addition    │
└────────────┴──────────────────────────────┘
```

`related()` walks any relationship in the graph and keeps the nearest matches.
P1 has two tanks at the same distance, so it appears twice. Pass
`nearest=False` to keep every tank within reach.

"Related" is a loose notion. In a treatment plant, the question you usually
mean is about flow: what feeds the RO? `direction="upstream"` follows the
piping instead of every kind of edge:

```python
ro.related("pump", direction="upstream").metadata()
```
```text
shape: (2, 2)
┌────────┬────────┐
│ ro     ┆ pump   │
╞════════╪════════╡
│ wbs:RO ┆ wbs:P2 │
│ wbs:RO ┆ wbs:P1 │
└────────┴────────┘
```

Each step moves a pointer to the node it added, and the next step starts from
there. After `.related("tank")`, a further `.related("valve")` would look for
valves near the tanks. To branch from an earlier node instead, name it with
`frm=`:

```python
(acq.query().entity("pump").related("tank")
 .related("Pressure Exchanger", frm="pump")
 .metadata())
```

`refocus("pump")` does the same for every step that follows, not just one.

## Reach the measurements

Sensors are what connect the model to data. `measurement()` adds the
measurement points of the current node. Here are the pressures on the RO:

```python
ro.measurement(alias="p", quantity_kind="pressure").metadata()
```
```text
shape: (3, 2)
┌────────┬───────────────────────────────┐
│ ro     ┆ p                             │
╞════════╪═══════════════════════════════╡
│ wbs:RO ┆ wbs:RO-out-pressure           │
│ wbs:RO ┆ wbs:RO-out-retentate-pressure │
│ wbs:RO ┆ wbs:RO-in-pressure            │
└────────┴───────────────────────────────┘
```

None of these points belong to the membrane itself. They sit on its inlet and
outlets, which is where pressure is measured, and `measurement()` looks there
as well by default.

On an empty query, `measurement()` matches every stream in the plant, whatever
equipment it belongs to. `acq.query().measurement(quantity_kind="pressure")`
finds all six pressure readings. From there, `context()` goes the other way,
from each point to the equipment it is about:

```python
(acq.query().measurement(quantity_kind="pressure")
 .context(process="reverse osmosis")
 .metadata())
```

## Filter and choose columns

The keyword arguments you have been passing are filters, and `where()` applies
the same filters as a separate step. `Not()` excludes a value. Here are the
RO's mass flows, leaving out the brine:

```python
from acquirium.Client.explore import Not

(acq.query().entity(uri="wbs:RO", alias="ro")
 .measurement(alias="flow")
 .where(quantity_kind="mass flow rate", medium=Not("brine"))
 .metadata())
```

Comparisons, and `or` across attributes, use expressions built on `acq.attr`,
such as `acq.attr.product_info.year >= 2015`. The
[client reference](../reference/client-api.md#attribute-expressions) lists
them, and the attributes you can filter on.

The frame holds one column per node. `include()` adds an attribute as a column
next to its node:

```python
ro.measurement(alias="m").include("unit").metadata()
```

A point without a unit still appears, with `null` in the `m.unit` column. In
this model, the three pressures and the temperature have none. Pass
`required=True` to keep only the rows that have one, or use `drop()` to hide a
node that is there only to constrain the match.

## Get the values

So far every answer has been about the model. Change the terminal method and
the same pattern returns the readings:

```python
ro.measurement(alias="p", quantity_kind="pressure").dataframe().tail(3)
```
```text
shape: (3, 4)
┌────────────────────────────────┬───────────────────────┬────────────────────────┬──────────────────────────────────┐
│ time                           ┆ p__wbs:RO-in-pressure ┆ p__wbs:RO-out-pressure ┆ p__wbs:RO-out-retentate-pressure │
╞════════════════════════════════╪═══════════════════════╪════════════════════════╪══════════════════════════════════╡
│ 2026-08-07 22:53:38.669786 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8043e6                         │
│ 2026-08-07 22:53:54.023514 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8001e6                         │
│ 2026-08-07 22:54:10.019299 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8056e6                         │
└────────────────────────────────┴───────────────────────┴────────────────────────┴──────────────────────────────────┘
```

You never named a stream or a tag. You described the readings by what they
measure and where, and that description keeps working when someone adds a
fourth pressure sensor to the RO.

[Working with data](data.md) picks up from here: layouts, units, and splitting
a result by equipment. The [query cookbook](query-cookbook.md) answers common
plant questions with these steps, and [the query model](../explanation/query-model.md)
explains how a pattern becomes a graph query.
