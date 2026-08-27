---
title: Query cookbook
---

<!-- TODO: intro -->

Each entry starts from a question a plant engineer would ask, works out how to
phrase it for the query interface, and ends with the query and its result.
Every result below was captured on the WaterTAP seawater-ro model.

```python
import polars as pl
from acquirium import Acquirium
from acquirium.Client.explore import Not

acq = Acquirium(server_url="localhost", server_port=8000)
```

## How salty is the brine we discharge, compared to the seawater we take in?

**How to phrase it.**
The question is about two measurements, not two pieces of equipment: a salt
concentration in brine and a salt concentration in seawater.
So start from `measurement()` on an empty query, which matches every point in
the plant, and narrow it by what is measured: the substance (`salt`) and the
quantity kind (`mass concentration`).
Which stream is which comes from the `medium` attribute, so include it as a
column.

```python
salt = (acq.query()
        .measurement(alias="salt", substance="constituent salt", quantity_kind="mass concentration")
        .include("medium"))
salt.metadata()
```
```text
shape: (3, 2)
┌──────────────────────────────────────────┬─────────────────────┐
│ salt                                     ┆ salt.medium         │
╞══════════════════════════════════════════╪═════════════════════╡
│ wbs:PXR-brine-out-tds-concentration      ┆ nawi:Water-Brine    │
│ wbs:storage-tank-3-out-tds-concentration ┆ s223:Fluid-Water    │
│ wbs:intake-in-tds-concentration          ┆ nawi:Water-Seawater │
└──────────────────────────────────────────┴─────────────────────┘
```

Three points: the brine leaving the pressure exchanger, the seawater at the
intake, and the product water.
The wide frame has one column per point, so the ratio is one polars
expression:

```python
df = salt.dataframe()
df.with_columns(
    ratio=pl.col("salt__wbs:PXR-brine-out-tds-concentration")
        / pl.col("salt__wbs:intake-in-tds-concentration")
).select("time", "ratio").tail(3)
```
```text
┌────────────────────────────────┬──────────┐
│ time                           ┆ ratio    │
╞════════════════════════════════╪══════════╡
│ 2026-08-15 06:24:59.610773 UTC ┆ 1.731381 │
│ 2026-08-15 06:26:45.916347 UTC ┆ 1.709988 │
│ 2026-08-15 06:27:00.574609 UTC ┆ 1.742583 │
└────────────────────────────────┴──────────┘
```

The brine is about 1.7 times as salty as the intake.

## What is the pressure drop across the RO?

**How to phrase it.**
This time the equipment is the anchor: the pressures we want are the ones
measured *on* the RO, at its inlet and its outlets.
So pin the RO with `entity(uri=...)`, attach its measurements, and keep the
pressures.
`measurement()` includes the points on the connection points (inlet and
outlets), which is where the pressures live.

```python
p = (acq.query().entity(uri="wbs:RO", alias="ro")
     .measurement(alias="p", quantity_kind="pressure"))
p.metadata()
```
```text
shape: (3, 2)
┌────────┬───────────────────────────────┐
│ ro     ┆ p                             │
╞════════╪═══════════════════════════════╡
│ wbs:RO ┆ wbs:RO-in-pressure            │
│ wbs:RO ┆ wbs:RO-out-pressure           │
│ wbs:RO ┆ wbs:RO-out-retentate-pressure │
└────────┴───────────────────────────────┘
```

The drop across the membrane is inlet minus retentate:

```python
p.dataframe().with_columns(
    drop=pl.col("p__wbs:RO-in-pressure") - pl.col("p__wbs:RO-out-retentate-pressure")
).select("time", "drop").tail(3)
```
```text
┌────────────────────────────────┬───────────────┐
│ time                           ┆ drop          │
╞════════════════════════════════╪═══════════════╡
│ 2026-08-15 06:24:59.610773 UTC ┆ 198691.953202 │
│ 2026-08-15 06:26:45.916347 UTC ┆ 207048.129224 │
│ 2026-08-15 06:27:00.574609 UTC ┆ 192383.477345 │
└────────────────────────────────┴───────────────┘
```

About 2 bar.
Note that `p.data().units()` returns `{'p': None}`: these pressure points
carry no unit in the model (the values are pascals), which is the last
question in this cookbook.

## What is the RO recovery?

Recovery is permeate flow over feed flow.

**How to phrase it.**
Both are mass flows of water on the RO, so the filters are the same as
before, plus `substance=Not("constituent salt")` to leave out the salt mass
flows that sit next to them.
What separates feed from permeate is *where* the flow is measured: on the
inlet connection point (medium seawater) or on the permeate outlet (medium
fluid water).
So walk from the RO to its connection points, filter each by medium, and
attach one measurement node to each.
`hasConnectionPoint` is a structural predicate that generic traversal skips,
so it is named in `via=`.

```python
S223 = "http://data.ashrae.org/standard223#"
CP = [S223 + "hasConnectionPoint"]

rec = (acq.query().entity(uri="wbs:RO", alias="ro")
       .related("connection point", alias="feed_cp", via=CP).where(medium="seawater")
       .measurement(alias="feed", quantity_kind="mass flow rate", substance=Not("constituent salt"))
       .related("connection point", alias="permeate_cp", frm="ro", via=CP).where(medium="fluid water")
       .measurement(alias="permeate", quantity_kind="mass flow rate", substance=Not("constituent salt")))
rec.metadata()
```
```text
shape: (2, 5)
┌────────┬───────────┬───────────────────────────┬─────────────┬────────────────────────────┐
│ ro     ┆ feed_cp   ┆ feed                      ┆ permeate_cp ┆ permeate                   │
╞════════╪═══════════╪═══════════════════════════╪═════════════╪════════════════════════════╡
│ wbs:RO ┆ wbs:RO-in ┆ null                      ┆ wbs:RO-out  ┆ wbs:RO-out-flow-mass-water │
│ wbs:RO ┆ wbs:RO-in ┆ wbs:RO-in-flow-mass-water ┆ wbs:RO-out  ┆ null                       │
└────────┴───────────┴───────────────────────────┴─────────────┴────────────────────────────┘
```

Each alias matched exactly one point, so the wide columns are simply `feed`
and `permeate`:

```python
rec.dataframe().with_columns(recovery=pl.col("permeate") / pl.col("feed")).tail(3)
```
```text
┌────────────────────────────────┬────────────┬────────────┬──────────┐
│ time                           ┆ feed       ┆ permeate   ┆ recovery │
╞════════════════════════════════╪════════════╪════════════╪══════════╡
│ 2026-08-15 06:24:59.610773 UTC ┆ 290.491228 ┆ 124.645078 ┆ 0.429084 │
│ 2026-08-15 06:26:45.916347 UTC ┆ 298.795226 ┆ 126.066582 ┆ 0.421916 │
│ 2026-08-15 06:27:00.574609 UTC ┆ 286.678595 ┆ 124.154369 ┆ 0.433079 │
└────────────────────────────────┴────────────┴────────────┴──────────┘
```

Recovery is around 43 %.

## What is downstream of the cartridge filter, and what is measured there?

**How to phrase it.**
First find the filter.
`entity("cartridge filter")` returns nothing here: the text resolves to
`nawi:CartridgeFiltrationUnit`, but this plant types its filters as plain
`nawi:Filter`.
`options("type")` on the equipment shows what classes the model actually
uses, and `entity("filter")` finds three of them, one of which is the RO
itself.
So pin the one we mean by URI.

```python
acq.query().entity("filter").metadata()
```
```text
┌──────────────────────────┐
│ filter                   │
╞══════════════════════════╡
│ wbs:media-filtration     │
│ wbs:cartridge-filtration │
│ wbs:RO                   │
└──────────────────────────┘
```

"Downstream" is the piping topology, so `direction="downstream"`.
For the equipment, `related()`; for what is measured there,
`measurement(direction=..., nearest=True)`, which stops at the first points
found along the flow.

```python
cf = acq.query().entity(uri="wbs:cartridge-filtration", alias="cf")

cf.related("equipment", direction="downstream").metadata()
```
```text
┌──────────────────────────┬───────────┐
│ cf                       ┆ equipment │
╞══════════════════════════╪═══════════╡
│ wbs:cartridge-filtration ┆ wbs:P2    │
│ wbs:cartridge-filtration ┆ wbs:PXR   │
│ wbs:cartridge-filtration ┆ wbs:P1    │
└──────────────────────────┴───────────┘
```

```python
(cf.measurement(alias="after", direction="downstream", nearest=True)
   .include("quantity_kind")
   .metadata())
```
```text
shape: (5, 3)
┌──────────────────────────┬─────────────────────────────────────────────────────┬──────────────────────────┐
│ cf                       ┆ after                                               ┆ after.quantity_kind      │
╞══════════════════════════╪═════════════════════════════════════════════════════╪══════════════════════════╡
│ wbs:cartridge-filtration ┆ wbs:cartridge-filtration-out-toc-concentration      ┆ qudtqk:MassConcentration │
│ wbs:cartridge-filtration ┆ wbs:conn-cartridge-filtration-to-S1-temperature     ┆ qudtqk:Temperature       │
│ wbs:cartridge-filtration ┆ wbs:conn-cartridge-filtration-to-S1-flow-mass-tds   ┆ qudtqk:MassFlowRate      │
│ wbs:cartridge-filtration ┆ wbs:conn-cartridge-filtration-to-S1-flow-mass-wate… ┆ qudtqk:MassFlowRate      │
│ wbs:cartridge-filtration ┆ wbs:conn-cartridge-filtration-to-S1-pressure        ┆ qudtqk:Pressure          │
└──────────────────────────┴─────────────────────────────────────────────────────┴──────────────────────────┘
```

The filter feeds the two high-pressure pumps and the pressure exchanger, and
the first thing measured after it is the connection to the splitter: TOC,
temperature, mass flows and pressure.

## Which measurement points carry no unit?

**How to phrase it.**
This is a question about the model rather than the plant.
`unit` is an attribute, and attributes are optional: `include("unit")` keeps
a point with no unit and shows `null`.
So ask for every point with its unit, and keep the nulls in polars.

```python
m = acq.query().measurement(alias="m").include("unit", "quantity_kind")
m.metadata().filter(pl.col("m.unit").is_null()).select("m", "m.quantity_kind")
```
```text
shape: (10, 2)
┌───────────────────────────────────────────┬───────────────────────┐
│ m                                         ┆ m.quantity_kind       │
╞═══════════════════════════════════════════╪═══════════════════════╡
│ wbs:P1-out-pressure                       ┆ qudtqk:Pressure       │
│ wbs:intake-in-flow-rate                   ┆ qudtqk:VolumeFlowRate │
│ wbs:RO-out-pressure                       ┆ qudtqk:Pressure       │
│ wbs:RO-in-pressure                        ┆ qudtqk:Pressure       │
│ wbs:conn-cartridge-filtration-to-S1-pres… ┆ qudtqk:Pressure       │
│ wbs:storage-tank-3-out-flow-rate          ┆ qudtqk:VolumeFlowRate │
│ wbs:conn-cartridge-filtration-to-S1-temp… ┆ qudtqk:Temperature    │
│ wbs:PXR-brine-out-pressure                ┆ qudtqk:Pressure       │
│ wbs:RO-out-retentate-pressure             ┆ qudtqk:Pressure       │
│ wbs:RO-in-temperature                     ┆ qudtqk:Temperature    │
└───────────────────────────────────────────┴───────────────────────┘
```

Ten of the 32 points have no unit: every pressure and temperature, and the
two volume flow rates.
Their quantity kinds are set, so a unit is the only thing missing;
`convert_to()` on any of them needs `from_unit=`.
