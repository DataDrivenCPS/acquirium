---
title: A driver against an existing plant model
---

[Your first driver](first-driver.md) ingested the RO skid's CSV files with no
plant model, so each column landed on a placeholder point of its own. Now
suppose the plant already has a model that describes the skid: its inlets and
outlets, and the sensors on them. Connecting the driver to that model is a
small change in code, and it changes what you can ask of the data.

This tutorial uses the seawater-RO model, loaded as
[loading a plant model](../how-to/load-a-plant-model.md) describes.

## Find the points the model already has

The model's points carry URIs, and the driver will refer to them. Before writing
any code, ask the server which points the RO has and what it already says about
them:

```python
(acq.query().entity(uri="wbs:RO", alias="ro")
 .measurement(alias="m")
 .include("m.quantity_kind", "m.unit")
 .metadata())
```
```text
shape: (11, 4)
┌────────┬─────────────────────────────────┬─────────────────────┬─────────────────────┐
│ ro     ┆ m                               ┆ m.quantity_kind     ┆ m.unit              │
╞════════╪═════════════════════════════════╪═════════════════════╪═════════════════════╡
│ wbs:RO ┆ wbs:RO-membrane-area            ┆ qudtqk:Area         ┆ unit:M2             │
│ wbs:RO ┆ wbs:RO-in-temperature           ┆ qudtqk:Temperature  ┆ null                │
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… ┆ qudtqk:MassFlowRate ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-flow-mass-tds         ┆ qudtqk:MassFlowRate ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-flow-mass-water      ┆ qudtqk:MassFlowRate ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-pressure              ┆ qudtqk:Pressure     ┆ null                │
│ wbs:RO ┆ wbs:RO-out-retentate-pressure   ┆ qudtqk:Pressure     ┆ null                │
│ wbs:RO ┆ wbs:RO-out-flow-mass-tds        ┆ qudtqk:MassFlowRate ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-flow-mass-water       ┆ qudtqk:MassFlowRate ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-pressure             ┆ qudtqk:Pressure     ┆ null                │
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… ┆ qudtqk:MassFlowRate ┆ unit:KiloGM-PER-SEC │
└────────┴─────────────────────────────────┴─────────────────────┴─────────────────────┘
```

Three of the skid's columns have a match: feed flow, permeate flow and feed
pressure. The fourth, `Permeate TDS`, does not, because the model has no TDS
concentration point on the RO. That is normal. Models are rarely complete, and
the driver has to cope.

## Declare against those points

The driver is the same class as before. `declare()` now gets a `point_uri`
naming a point in the model. The `wbs:` prefix stands for `urn:swro/`:

```python
from acquirium import CSVIngestDriver

POINTS = {
    "Feed Flow":     "urn:swro/RO-in-flow-mass-water",
    "Permeate Flow": "urn:swro/RO-out-flow-mass-water",
    "Feed Pressure": "urn:swro/RO-in-pressure",
}

class ROSkidDriver(CSVIngestDriver):
    def declare_stream(self, ref_name):
        if ref_name not in POINTS:
            self.declare(ref_name)          # no point in the model: ingest it anyway
            return
        self.declare(ref_name, value_kind="numeric", point_uri=POINTS[ref_name])
```

The unit and quantity kind have gone, because the model's points already carry
them. `Permeate TDS` is still ingested onto a placeholder point. It just
cannot be reached from the RO until someone adds the point to the model. The
config does not change.

Copy URIs exactly. A typo does not fail; it creates a new point that nothing in
the model refers to.

## The model checks what you declare

When the point already exists, registering a stream compares what you declare
with what the model says:

- if the point says nothing about a field, your value is written onto it;
- if it already holds the same value, nothing changes;
- if it holds a different value, say `qudtqk:Pressure` against your
  `quantity_kind="temperature"`, registration raises `ValueError` and stores
  nothing.

Compare that with the first tutorial, where nothing could contradict you. A
placeholder point has no prior opinion, so declaring a pressure column as a
temperature produced a confidently wrong point and no complaint. Here the
mistake fails before a single row is stored.

Units are the exception, and in one specific way. The skid reports feed pressure
in psi. Suppose the model recorded that point in pascals. Declaring
`unit="psi"` is not a contradiction, since the two convert. The raw rows are
stored in psi, and reads convert them to the model's pascals, so nobody
querying the point needs to know the skid's preference. A unit that does not
convert, such as celsius on a pressure point, raises. [Units](../explanation/units.md#how-a-unit-gets-recorded)
has the rules. The model as shipped records no unit for this point at all, so
declaring psi simply writes it onto the point.

## Check that the data arrived

The test of a binding is whether a query that starts from equipment reaches the
data:

```python
(acq.query().entity(uri="wbs:RO", alias="ro")
 .measurement(alias="p", quantity_kind="pressure")
 .dataframe().tail(3))
```

If that returns rows, the driver's readings now have a place in the plant. They
can be found by topology, by what they measure, and by everything else in the
[querying tutorial](querying.md), like every other point on the skid.

If the data shows up only under `acq.query().measurement()`, the `point_uri`
matched nothing and a placeholder was created instead. Its label is the column
name and its URI ends in `__point`, so it is easy to spot in that query's
output. Fix the URI and restart the driver. Rows already written stay on the
placeholder; re-declaring does not move them.

## Moving from one version to the other

If you already ran the first driver, switching to this one strands nothing. A
stream's identity comes from its source and column name, `ro-skid` and
`Feed Pressure`, not from the point it is attached to. Declaring a `point_uri`
links the same stream to the model's point, and its history comes with it.

All that is left behind is the old placeholder point, still in the graph. It is
stale metadata, not a second copy of the data. Once the binding works, delete
it, taking its URI from the query above:

```python
acq.sparql_update(
    "DELETE WHERE { <urn:acquirium#...__point> ?p ?o }",
    source_id="ro-skid",
)
```

Bind to a model whenever there is one. It is the only way two sources can feed
the same point, and the only version in which the model can tell you that a
column was described wrongly.
