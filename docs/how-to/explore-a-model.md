---
title: Explore a plant model
---

In this article we'll describe how to explore a plant model you do not know yet.
The steps go from the whole plant to one stream: list what is measured, find the equipment, filter, and read the data.

The examples use the seawater desalination plant of [5 - A Full Example](../tutorials/a-full-example.md).
Run that example first, in the same folder.

```python
import acquirium as aq

acq = aq.init()
```

## Listing what is measured

`options()` lists the values of one attribute on the nodes a query selects, with a count for each value.
On `measurement()` with no filter, it counts over every stream of the plant:

```python
acq.query().measurement().options("quantity_kind")
```
```text
shape: (8, 2)
┌────────────────────────────────┬───────┐
│ quantity_kind                  ┆ count │
│ ---                            ┆ ---   │
│ str                            ┆ i64   │
╞════════════════════════════════╪═══════╡
│ quantitykind:MassFlowRate      ┆ 10    │
│ quantitykind:MassConcentration ┆ 6     │
│ quantitykind:Pressure          ┆ 6     │
│ quantitykind:Efficiency        ┆ 3     │
│ quantitykind:Power             ┆ 2     │
│ quantitykind:Temperature       ┆ 2     │
│ quantitykind:VolumeFlowRate    ┆ 2     │
│ quantitykind:Area              ┆ 1     │
└────────────────────────────────┴───────┘
```

The same works for the other attributes of a stream, such as `unit`, `medium` and `substance`.
The full list is in [built-in attributes](../reference/query/filtering.md#built-in-attributes).

## Finding where the streams are

`context()` adds the part of the plant each stream belongs to.
`options("type")` then counts the classes of these parts:

```python
acq.query().measurement().context().options("type")
```
```text
shape: (11, 2)
┌─────────────────────────────┬───────┐
│ type                        ┆ count │
│ ---                         ┆ ---   │
│ str                         ┆ i64   │
╞═════════════════════════════╪═══════╡
│ s223:OutletConnectionPoint  ┆ 5     │
│ s223:Pump                   ┆ 3     │
│ watr:Pump                   ┆ 3     │
│ watr:Reactor                ┆ 3     │
│ s223:InletConnectionPoint   ┆ 2     │
│ …                           ┆ …     │
│ s223:Filter                 ┆ 1     │
│ s223:Pipe                   ┆ 1     │
│ watr:PressureExchanger      ┆ 1     │
│ watr:ReverseOsmosisMembrane ┆ 1     │
│ watr:Tank                   ┆ 1     │
└─────────────────────────────┴───────┘
```

The connection points are the inlets and outlets of the equipment.

## Listing the equipment of one class

`entity()` selects the parts of the plant by their class.
The class is written as text, and the text matcher finds the term of the ontology.
`include("label")` adds the label of each match as a column:

```python
pumps = acq.query().entity("pump")
print(pumps.include("label"))
```
```text
Query
  pump: entity, Pump
3 matches:
┌────────────┬─────────────────────────┐
│ pump       ┆ pump.label              │
│ ---        ┆ ---                     │
│ str        ┆ str                     │
╞════════════╪═════════════════════════╡
│ wbs:intake ┆ Seawater Onshore Intake │
│ wbs:P2     ┆ Booster Pump P2         │
│ wbs:P1     ┆ High-pressure Pump P1   │
└────────────┴─────────────────────────┘
```

The first lines of the output show how the query was understood.
Here the text `pump` was matched to the class `Pump`.

**Note:** Every method returns a new query and leaves the original unchanged.
`pumps` can be kept in a variable and extended in several ways, as in the next steps.

## Selecting by label or URI

The name shown in `pump.label` is the equipment's label. Pass it as `label=`
to `entity()`. You can also select a measurement by its label within that
equipment.

```python
p1 = acq.query().entity(label="High-pressure Pump P1", alias="pump")
power_on_p1 = p1.measurement(label="P1 mechanical power", alias="power")
power_on_p1.metadata()
```

The result has one row, with `wbs:P1` in `pump` and `wbs:P1-mechanical-power`
in `power`. Labels match exactly and can be shared by several nodes.

To select a specific node, pass its identifier as `uri=`. You can copy the
short form from the result or get the full URI with `resolved_nodes()`.

```python
acq.query().entity(uri="wbs:P1").measurement(uri="wbs:P1-mechanical-power")
power_on_p1.resolved_nodes(alias="power")  # ['urn:swro/P1-mechanical-power']
```

## Seeing the values you can filter on

`facets()` does what `options()` does, for every attribute of a node in one call.
This is useful for choosing a filter:

```python
f = pumps.measurement(alias="m").facets()
f
```
```text
FacetSummary('m')
  type [matched]: acq:VirtualPoint (8), s223:QuantifiableObservableProperty (8)
  medium [matched]: watr:Water-Seawater (2)
  substance [matched]: watr:Constituent-DissolvedSolids (1), watr:Constituent-SuspendedSolids (1)
  quantity_kind [matched]: quantitykind:Efficiency (2), quantitykind:MassConcentration (2), quantitykind:Power (2), quantitykind:Pressure (1), quantitykind:VolumeFlowRate (1)
  unit [matched]: unit:KiloGM-PER-M3 (2), unit:UNITLESS (2), unit:W (2), unit:M3-PER-SEC (1), unit:PA (1)
  enumeration_kind: (no values)
  data_source: (no values)
  app: (no values)
  is_disconnected: (no values)
  label [matched]: P1 efficiency (1), P1 mechanical power (1), P1 out pressure pa (1), P2 efficiency (1), P2 mechanical power (1), … +3
  generation: (no values)
```

Each line shows an attribute and its first five values, with counts.
`[matched]` means the values are counted on the nodes the query selects.
The full table of one attribute is read by its name:

```python
f["unit"]
```
```text
shape: (5, 2)
┌────────────────────┬───────┐
│ unit               ┆ count │
│ ---                ┆ ---   │
│ str                ┆ i64   │
╞════════════════════╪═══════╡
│ unit:KiloGM-PER-M3 ┆ 2     │
│ unit:UNITLESS      ┆ 2     │
│ unit:W             ┆ 2     │
│ unit:M3-PER-SEC    ┆ 1     │
│ unit:PA            ┆ 1     │
└────────────────────┴───────┘
```

## Filtering and checking the result

A value from these lists can be used as a filter.
`metadata()` shows what the query selects without reading any data:

```python
power = pumps.measurement(alias="m", quantity_kind="power")
power.include("unit").metadata()
```
```text
shape: (2, 4)
┌────────┬─────────────────────────┬─────────────────────┬────────┐
│ pump   ┆ m                       ┆ m.label             ┆ m.unit │
│ ---    ┆ ---                     ┆ ---                 ┆ ---    │
│ str    ┆ str                     ┆ str                 ┆ str    │
╞════════╪═════════════════════════╪═════════════════════╪════════╡
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power ┆ unit:W │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power ┆ unit:W │
└────────┴─────────────────────────┴─────────────────────┴────────┘
```

The filter can also be added later with `where()`.
`pumps.measurement(alias="m").where(quantity_kind="power")` is the same query.

## Following the connections

`related()` adds the equipment connected to the current node.
With `direction`, it follows the flow of the plant `"upstream"` or `"downstream"`:

```python
pumps.related("reverse osmosis membrane", direction="downstream").include("label").metadata()
```
```text
shape: (2, 3)
┌────────┬──────────────────────────┬────────────────────────────────┐
│ pump   ┆ reverse osmosis membrane ┆ reverse osmosis membrane.label │
│ ---    ┆ ---                      ┆ ---                            │
│ str    ┆ str                      ┆ str                            │
╞════════╪══════════════════════════╪════════════════════════════════╡
│ wbs:P1 ┆ wbs:RO                   ┆ Reverse Osmosis Stage          │
│ wbs:P2 ┆ wbs:RO                   ┆ Reverse Osmosis Stage          │
└────────┴──────────────────────────┴────────────────────────────────┘
```

Note that the intake pump is not in the table.
`related()` looks at most three steps away by default, and the membrane is further from the intake.
`max_depth=0` removes the limit:

```python
pumps.related("reverse osmosis membrane", direction="downstream", max_depth=0).include("label").metadata()
```
```text
shape: (3, 3)
┌────────────┬──────────────────────────┬────────────────────────────────┐
│ pump       ┆ reverse osmosis membrane ┆ reverse osmosis membrane.label │
│ ---        ┆ ---                      ┆ ---                            │
│ str        ┆ str                      ┆ str                            │
╞════════════╪══════════════════════════╪════════════════════════════════╡
│ wbs:P1     ┆ wbs:RO                   ┆ Reverse Osmosis Stage          │
│ wbs:P2     ┆ wbs:RO                   ┆ Reverse Osmosis Stage          │
│ wbs:intake ┆ wbs:RO                   ┆ Reverse Osmosis Stage          │
└────────────┴──────────────────────────┴────────────────────────────────┘
```

When several pieces of equipment match, `nearest=True` keeps only the closest one for each pump.

`measurement()` takes `direction` as well, to find the nearest streams along the flow.
For instance, the pressure streams upstream of the reverse osmosis membrane:

```python
ro = acq.query().entity(uri="wbs:RO", alias="ro")
ro.measurement(direction="upstream", quantity_kind="pressure").metadata()
```
```text
shape: (1, 4)
┌────────┬────────────────────┬───────────────────────┬────────────────────────┐
│ ro     ┆ ro_upstream_entity ┆ ro_upstream_data      ┆ ro_upstream_data.label │
│ ---    ┆ ---                ┆ ---                   ┆ ---                    │
│ str    ┆ str                ┆ str                   ┆ str                    │
╞════════╪════════════════════╪═══════════════════════╪════════════════════════╡
│ wbs:RO ┆ wbs:RO-in          ┆ wbs:RO-in-pressure-pa ┆ RO in pressure pa      │
└────────┴────────────────────┴───────────────────────┴────────────────────────┘
```

## Reading the data

`data()` returns an object that describes the data of the selected streams.
It reads the values only when asked.
This is useful for checking the size and the time range before a large read:

```python
d = power.data()
print(d.total_rows)
start, end = d.time_range
print(start, end)
```
```text
17520
2025-01-01 00:00:00+00:00 2025-12-31 23:00:00+00:00
```

`dataframe()` reads the values.
`limit` keeps the first rows of each stream:

```python
power.dataframe(limit=3)
```
```text
shape: (3, 3)
┌─────────────────────────┬────────────────────────┬────────────────────────┐
│ time                    ┆ m__P1 mechanical power ┆ m__P2 mechanical power │
│ ---                     ┆ ---                    ┆ ---                    │
│ datetime[μs, UTC]       ┆ f64                    ┆ f64                    │
╞═════════════════════════╪════════════════════════╪════════════════════════╡
│ 2025-01-01 00:00:00 UTC ┆ 1.1874e6               ┆ 106089.85853           │
│ 2025-01-01 01:00:00 UTC ┆ 1.1816e6               ┆ 101473.943758          │
│ 2025-01-01 02:00:00 UTC ┆ 1.1653e6               ┆ 97750.261488           │
└─────────────────────────┴────────────────────────┴────────────────────────┘
```

## Moving on

- [Query cookbook](query-cookbook.md): queries for common questions about a plant.
- [Debug an empty or unexpected query result](debug-an-empty-query.md): what to check when a query selects nothing.
- [Query reference](../reference/query/_index.md): every method, with all parameters.
