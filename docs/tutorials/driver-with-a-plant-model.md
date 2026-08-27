---
title: A driver against an existing plant model
---

[Your first driver](first-driver.md) ingested a CSV export with no plant model
at all. Every column got a placeholder point, described by its unit and
quantity kind, and the readings were queryable by what they measure — but not
by where they were measured, because nothing said where that was.

This tutorial is the same driver, the same CSV file, and the same four
columns, in the situation you are actually in most of the time: **the plant
model already exists**. Somebody has loaded it with
[load a plant model](../how-to/load-a-plant-model.md), the RO skid is already
described in it, and the points your columns measure are already there with
their units and quantity kinds.

What changes is one argument. `declare()` gains a `point_uri` naming a point
already in the graph, and the stream attaches to it instead of to a
placeholder. In exchange the driver gets shorter — the model already owns the
units — and the graph gets a consistency check it did not have before.

## The situation

The same file lands in the folder every day:

```csv
Date,Time,Feed Flow,Permeate Flow,Feed Pressure,Permeate TDS
8/1/2026,12:00:00 AM,120.5,55.2,801,210
8/1/2026,12:15:00 AM,120.1,55.0,803,208
8/1/2026,12:30:00 AM,NaN,54.9,802,211
```

And the plant model already says what the skid is. These are the seawater-ro
model's own triples:

```turtle
wbs:RO  a nawi:ReverseOsmosisMembrane ;
    rdfs:label "Reverse Osmosis Stage" ;
    s223:cnx wbs:RO-in, wbs:RO-out, wbs:RO-out-retentate .

wbs:RO-in-flow-mass-water  a s223:QuantifiableObservableProperty ;
    s223:ofMedium        nawi:Water-Seawater ;
    qudt:hasQuantityKind qudtqk:MassFlowRate ;
    qudt:hasUnit         unit:KiloGM-PER-SEC .

wbs:RO-in-pressure  a s223:QuantifiableObservableProperty ;
    qudt:hasQuantityKind qudtqk:Pressure .
```

Note the asymmetry, because it drives everything below: the flow point carries
a unit, the pressure point does not. Real models are like this.

## Step 1: find the points, do not invent them

Before writing any code, ask the server which points exist and what they
already say. This is the step that replaces choosing URIs:

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

Every URI in the `m` column is a binding target. Copy them exactly; a typo
mints a placeholder point instead of failing, which is the one mistake in this
tutorial that stays silent.

Note also what is *not* there: the model has no TDS concentration point on the
RO, so the `Permeate TDS` column has nothing to bind to. That is the normal
state of affairs, and step 2 handles it.

## Step 2: declare against those points

The driver is the same class as before, with one difference in
`declare_stream()`: `point_uri` names a point that already exists.

```python
from acquirium import CSVIngestDriver

POINTS = {
    "Feed Flow":     "urn:swro/RO-in-flow-mass-water",
    "Permeate Flow": "urn:swro/RO-out-flow-mass-water",
    "Feed Pressure": "urn:swro/RO-in-pressure",
}


class ROSkidDriver(CSVIngestDriver):
    def declare_stream(self, ref_name: str) -> None:
        point_uri = POINTS.get(ref_name)
        if point_uri is None:
            self.declare(ref_name)          # unmapped column: ingest it, no point
            return
        self.declare(ref_name, value_kind="numeric", point_uri=point_uri)
```

The `unit` and `quantity_kind` arguments are gone. The point already carries
both, and they are the model's business, not the driver's. Passing them again
is allowed but only creates a chance to contradict the model.

`Permeate TDS` falls through to the bare `self.declare(ref_name)`. It still
gets ingested and still gets a placeholder point, exactly as in the first
tutorial — the rows are kept, they are just not reachable through `wbs:RO`
until somebody adds the point to the model. Keeping the column is the right
call: dropping data because the model is incomplete is a worse trade than
storing it somewhere findable.

The config from the first tutorial is unchanged:

```toml
[[drivers]]
spec        = "ro_skid_driver.py:ROSkidDriver"
source_id   = "ro-skid"
watch_dir   = "./data/ro-skid"
glob        = "*.csv"
format      = "wide"
date_col    = "Date"
clock_col   = "Time"
day_first   = false
null_values = ["NaN"]
interval    = 60.0
```

The driver still contributes no RDF of its own beyond its stream
registrations. The difference is where those registrations land: on points the
plant model already owns, rather than on placeholders minted for them.

## Step 3: what the check buys you

Because the point exists, registration now compares what you declared against
what the graph says, and refuses to write a contradiction:

| the point has | you declare | result |
|---|---|---|
| nothing for that field | a value | the value is written onto the point |
| the same value | the same value | no change |
| `qudtqk:Pressure` | `quantity_kind="temperature"` | `ValueError`, nothing inserted |

That check is the whole reason to bind. In the first tutorial nothing could
contradict you: a placeholder point has no prior opinion, so declaring
`quantity_kind="temperature"` on a pressure column produced a confidently
wrong point and no complaint. Here the model already holds the opinion, and
the mistake fails at declaration time, before a single row is stored.

Units are the one field that tolerates a difference, and only in one specific
way.

## Step 4: units the source reports differently

The skid reports feed pressure in psi. Suppose the model had annotated
`wbs:RO-in-pressure` in pascals. Declaring psi does not conflict and does not
overwrite:

```python
self.declare("Feed Pressure", value_kind="numeric",
             point_uri="urn:swro/RO-in-pressure", unit="psi")
```

The two are convertible, so psi is recorded on the *reference node* as the
storage unit: the raw rows stay in psi, and reads convert to the point's
pascals automatically. `units()` reports pascals, and nobody querying the
point has to know the skid's preference.

A non-convertible unit (celsius on a pressure point) raises instead. The rules
are in [units](../explanation/units.md#how-a-unit-gets-recorded).

For `wbs:RO-in-pressure` as the model actually ships it — no unit at all —
declaring `unit="psi"` writes psi onto the point, because a field the point
lacks is added rather than compared. That is a real improvement to the model,
and it is the one case where a driver should pass a unit for a point it does
not own.

## Step 5: check the binding

Drop a file in, wait one interval, and confirm the rows arrived through the
plant model rather than beside it. The test is that a query starting from
*equipment* reaches them:

```python
(acq.query().entity(uri="wbs:RO", alias="ro")
 .measurement(alias="m", quantity_kind="pressure")
 .dataframe().tail(3))
```

If that has rows, the binding worked: the driver's data is now reachable by
topology, by quantity kind, and by everything else in the
[querying tutorial](querying.md), exactly like every other point on the skid.

If instead the data only shows up under `measurement()` on an empty query, the
`point_uri` did not match anything and a placeholder point was minted. Listing
every point in the plant shows them:

```python
acq.query().measurement(alias="m").metadata()
```

`metadata()` adds an `m.label` column whenever the matched points carry an
`rdfs:label`, and a placeholder's label is `ro-skid__Feed Flow` — the
`source_id` and the `ref_name` joined by a double underscore. A row like that
next to a URI ending in `__point` is a binding that missed.

Fix the URI and re-declare. The misdirected rows stay under the placeholder,
so clear them or leave them orphaned deliberately; re-declaring does not move
data that was already written.

## Which version to write

Bind to an existing model whenever there is one. It is the normal case for a
real deployment, the only way two sources can feed the same point, and the
only version where the graph can tell you that you got a column wrong.

Ingest without one, as in [your first driver](first-driver.md), when no model
describes the source yet. The data is not stranded by that choice: it is stored,
described, and queryable by what it measures from the first tick.

Going from one to the other is a small change, and it does not strand the data
you already collected. Load the model, point `declare()` at the real URIs, and
restart the driver.

Rows already written come along. A stream's `ref_uri` is computed from
`(source_id, ref_name)`, so it does not change when you add a `point_uri`; the
new point is linked to the same reference node, and reads follow that link.
The history was never stored per point.

What you are left with is the old placeholder point, still linked to the same
reference node and still turning up in `measurement()` on an empty query. It
is stale metadata, not a second copy of the data. Drop it once the binding is
confirmed:

```python
acq.sparql_update(
    "DELETE WHERE { <urn:acquirium#...__point> ?p ?o }",
    source_id="ro-skid",
)
```

Take the placeholder's URI from the `m` column of the query above; it is the
one ending in `__point`.

Every option, base class and hook is in the
[driver reference](../reference/drivers.md).
