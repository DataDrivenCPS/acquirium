---
title: Explore a model
---

<!-- TODO: intro -->

## Building a query step by step

A `Query` is immutable and lazy, so you can build it the way you build a
dataframe pipeline: keep a partial query in a variable, look at what it
matches, and extend it.
Every step below is a complete query you can run, and `.metadata()` shows what
it matches without fetching any data.

Start wide and check what is there:

```python
pumps = acq.query().entity("pump")
pumps.metadata()
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

Extend it and check again.
`pumps` is untouched, so you can branch off it as many times as you like:

```python
with_tanks = pumps.related("tank")
with_tanks.metadata()
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

Attach the measurements, and before filtering them, ask what kinds there are:

```python
readings = pumps.measurement(alias="m")
readings.options("quantity_kind")
```
<!-- pending live capture on seawater-ro -->

Narrow with `where()`, using one of the values `options()` reported, and check
that the right points are left:

```python
power = readings.where(quantity_kind="power")
power.metadata()
```
<!-- pending live capture on seawater-ro -->

Only now fetch the data.
`.data()` gives a lazy `DataObject` that reports the row count and time range
before pulling anything; `.dataframe()` pulls it:

```python
d = power.data()
d                          # DataObject(lazy, ~... rows, range=..., aliases=['m'], entities=['pump'])
d.dataframe().tail(3)
```
<!-- pending live capture on seawater-ro -->

The pattern is the same at every size: extend, `metadata()`, `options()` when
you are not sure what to filter on, `where()`, and `data()` last.
Note that repeating `metadata()` on a partial query costs one graph query and
no data transfer, so checking often is cheap.

## Exploring what is there

TODO: More exploration/visulaization methods and __repr__ and __str__ for visualizing query object will be implemented

In a new plant, the hard part is knowing what to ask for.
`options()` and `facets()` answer that: they report the attribute values that
actually occur in your current matches, so you can narrow down step by step.
Both run immediately and return values rather than a new query.

### options()

`options(attr)` lists the distinct values of one attribute across the matched
nodes, with counts.
It reads the node the pointer is on; `of=` reads another one by alias.

```python
acq.query().measurement().options("quantity_kind")
```
```text
shape: (9, 2)
┌──────────────────────────┬───────┐
│ quantity_kind            ┆ count │
╞══════════════════════════╪═══════╡
│ qudtqk:MassFlowRate      ┆ 10    │
│ qudtqk:Pressure          ┆ 6     │
│ qudtqk:MassConcentration ┆ 5     │
│ qudtqk:Efficiency        ┆ 3     │
│ qudtqk:Power             ┆ 2     │
│ qudtqk:Temperature       ┆ 2     │
│ qudtqk:VolumeFlowRate    ┆ 2     │
│ qudtqk:Area              ┆ 1     │
│ qudtqk:Density           ┆ 1     │
└──────────────────────────┴───────┘
```

The count is the number of matched nodes carrying that value, so it also tells
you how much a filter would leave behind.
Add a filter and ask again; the options narrow with the pattern:

```python
acq.query().measurement(quantity_kind="mass flow rate").options("unit")
```
```text
shape: (1, 2)
┌─────────────────────┬───────┐
│ unit                ┆ count │
╞═════════════════════╪═══════╡
│ unit:KiloGM-PER-SEC ┆ 10    │
└─────────────────────┴───────┘
```

### facets()

`facets()` does the same for every attribute that applies to a node, in one
call.
It prints compactly and indexes like a dict, so you can pull the full frame
for any attribute.

```python
f = acq.query().entity(uri="wbs:RO", alias="RO").measurement().facets()
f
```
```text
FacetSummary('RO_data')
  type [matched]: s223:QuantifiableObservableProperty (11), ns1:VirtualPoint (11)
  medium [matched]: s223:Fluid-Water (3), nawi:Water-Seawater (2), nawi:Water-Brine (1)
  substance [matched]: nawi:Constituent-Salt (3)
  quantity_kind [matched]: qudtqk:MassFlowRate (6), qudtqk:Pressure (3), qudtqk:Area (1), qudtqk:Temperature (1)
  unit [matched]: unit:KiloGM-PER-SEC (6), unit:M2 (1)
  enumeration_kind: (no values)
  data_source: (no values)
```

```python
f["quantity_kind"]
```
```text
shape: (4, 2)
┌─────────────────────┬───────┐
│ quantity_kind       ┆ count │
╞═════════════════════╪═══════╡
│ qudtqk:MassFlowRate ┆ 6     │
│ qudtqk:Pressure     ┆ 3     │
│ qudtqk:Area         ┆ 1     │
│ qudtqk:Temperature  ┆ 1     │
└─────────────────────┴───────┘
```

The `[matched]` tag is the scope of those counts.
When the pattern matches nothing for an attribute, `facets()` falls back:
first to how the attribute is used model-wide (`[model]`), then to the
ontology's own vocabulary (`[vocabulary]`, values only, no counts).
So an empty query still tells you which filters exist to try.
