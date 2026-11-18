---
title: Query cookbook
---

In this article we'll list queries for common questions about a plant.
Each question has one query and its output.

The examples use the seawater desalination plant of [5 - A Full Example](../tutorials/a-full-example.md).
Run that example first, in the same folder.

```python
import polars as pl

import acquirium as aq
from acquirium.Client.explore import Not

acq = aq.init()
```

## Which pumps does the plant have?

```python
acq.query().entity("pump").include("label").metadata()
```
```text
shape: (3, 2)
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

## What is measured on the high-pressure pump?

`entity(uri=...)` selects one piece of equipment, and `measurement()` adds its streams.

```python
p1 = acq.query().entity(uri="wbs:P1", alias="p1")
p1.measurement(alias="m").include("quantity_kind", "unit").metadata()
```
```text
shape: (3, 5)
┌────────┬─────────────────────────┬─────────────────────┬─────────────────────────┬───────────────┐
│ p1     ┆ m                       ┆ m.label             ┆ m.quantity_kind         ┆ m.unit        │
│ ---    ┆ ---                     ┆ ---                 ┆ ---                     ┆ ---           │
│ str    ┆ str                     ┆ str                 ┆ str                     ┆ str           │
╞════════╪═════════════════════════╪═════════════════════╪═════════════════════════╪═══════════════╡
│ wbs:P1 ┆ wbs:P1-efficiency       ┆ P1 efficiency       ┆ quantitykind:Efficiency ┆ unit:UNITLESS │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power ┆ quantitykind:Power      ┆ unit:W        │
│ wbs:P1 ┆ wbs:P1-out-pressure-pa  ┆ P1 out pressure pa  ┆ quantitykind:Pressure   ┆ unit:PA       │
└────────┴─────────────────────────┴─────────────────────┴─────────────────────────┴───────────────┘
```

## How much power did the pumps draw on the first hours of July, in kilowatts?

The streams are stored in watts.
`convert_to()` converts the values when they are read.

```python
power = acq.query().entity("pump").measurement(alias="power", quantity_kind="power")
power.data().convert_to("kW").dataframe(start="2025-07-01", end="2025-07-01T03:00:00")
```
```text
shape: (4, 3)
┌─────────────────────────┬────────────────────────────┬────────────────────────────┐
│ time                    ┆ power__P1 mechanical power ┆ power__P2 mechanical power │
│ ---                     ┆ ---                        ┆ ---                        │
│ datetime[μs, UTC]       ┆ f64                        ┆ f64                        │
╞═════════════════════════╪════════════════════════════╪════════════════════════════╡
│ 2025-07-01 00:00:00 UTC ┆ 1065.809783                ┆ 108.456097                 │
│ 2025-07-01 01:00:00 UTC ┆ 1091.32405                 ┆ 116.73533                  │
│ 2025-07-01 02:00:00 UTC ┆ 1029.535151                ┆ 96.421121                  │
│ 2025-07-01 03:00:00 UTC ┆ 1042.124925                ┆ 103.118663                 │
└─────────────────────────┴────────────────────────────┴────────────────────────────┘
```

## What was the average pump power on each day of a week?

The result of `dataframe()` is a polars dataframe, so the daily average is one polars step.

```python
df = power.data().convert_to("kW").dataframe(start="2025-07-01", end="2025-07-07T23:00:00")
df.group_by_dynamic("time", every="1d").agg(pl.all().exclude("time").mean())
```
```text
shape: (7, 3)
┌─────────────────────────┬────────────────────────────┬────────────────────────────┐
│ time                    ┆ power__P1 mechanical power ┆ power__P2 mechanical power │
│ ---                     ┆ ---                        ┆ ---                        │
│ datetime[μs, UTC]       ┆ f64                        ┆ f64                        │
╞═════════════════════════╪════════════════════════════╪════════════════════════════╡
│ 2025-07-01 00:00:00 UTC ┆ 1091.299082                ┆ 124.34938                  │
│ 2025-07-02 00:00:00 UTC ┆ 1096.988257                ┆ 124.845055                 │
│ 2025-07-03 00:00:00 UTC ┆ 1098.484659                ┆ 125.641056                 │
│ 2025-07-04 00:00:00 UTC ┆ 1091.334641                ┆ 124.788923                 │
│ 2025-07-05 00:00:00 UTC ┆ 1094.654196                ┆ 123.808746                 │
│ 2025-07-06 00:00:00 UTC ┆ 1092.088617                ┆ 123.005782                 │
│ 2025-07-07 00:00:00 UTC ┆ 1096.54999                 ┆ 126.38584                  │
└─────────────────────────┴────────────────────────────┴────────────────────────────┘
```

## What are the latest pressures at the reverse osmosis membrane, in bar?

`latest()` returns the newest reading of each stream.

```python
ro = acq.query().entity(uri="wbs:RO", alias="ro")
ro.measurement(alias="p", quantity_kind="pressure").data().convert_to("bar").latest()
```
```text
shape: (3, 4)
┌────────────┬─────────────────────────────────┬─────────────────────────┬───────────┐
│ data_alias ┆ property_uri                    ┆ time                    ┆ value     │
│ ---        ┆ ---                             ┆ ---                     ┆ ---       │
│ str        ┆ str                             ┆ datetime[μs, UTC]       ┆ f64       │
╞════════════╪═════════════════════════════════╪═════════════════════════╪═══════════╡
│ p          ┆ urn:swro/RO-in-pressure-pa      ┆ 2025-12-31 23:00:00 UTC ┆ 70.0      │
│ p          ┆ urn:swro/RO-out-pressure-pa     ┆ 2025-12-31 23:00:00 UTC ┆ 1.01325   │
│ p          ┆ urn:swro/RO-out-retentate-pres… ┆ 2025-12-31 23:00:00 UTC ┆ 67.871345 │
└────────────┴─────────────────────────────────┴─────────────────────────┴───────────┘
```

## What is the pressure drop across the reverse osmosis membrane?

The drop is the inlet pressure minus the retentate pressure.
Both streams are in the same query, so they are two columns of one table.

```python
df = ro.measurement(alias="p", quantity_kind="pressure").data().convert_to("bar").dataframe(limit=3)
df.select("time", drop=pl.col("p__RO in pressure pa") - pl.col("p__RO out retentate pressure pa"))
```
```text
shape: (3, 2)
┌─────────────────────────┬──────────┐
│ time                    ┆ drop     │
│ ---                     ┆ ---      │
│ datetime[μs, UTC]       ┆ f64      │
╞═════════════════════════╪══════════╡
│ 2025-01-01 00:00:00 UTC ┆ 2.169838 │
│ 2025-01-01 01:00:00 UTC ┆ 2.103855 │
│ 2025-01-01 02:00:00 UTC ┆ 2.026713 │
└─────────────────────────┴──────────┘
```

## What is the recovery of the reverse osmosis membrane?

Recovery is the permeate flow divided by the feed flow.
The membrane has mass flow streams for water and for dissolved solids.
`Not` leaves out the streams of dissolved solids.
The stream `RO out flow mass water` is the permeate, and `RO in flow mass water` is the feed.

```python
flow = ro.measurement(alias="flow", quantity_kind="mass flow rate", substance=Not("dissolved solids"))
df = flow.dataframe(limit=3)
df.select("time", recovery=pl.col("flow__RO out flow mass water") / pl.col("flow__RO in flow mass water"))
```
```text
shape: (3, 2)
┌─────────────────────────┬──────────┐
│ time                    ┆ recovery │
│ ---                     ┆ ---      │
│ datetime[μs, UTC]       ┆ f64      │
╞═════════════════════════╪══════════╡
│ 2025-01-01 00:00:00 UTC ┆ 0.476323 │
│ 2025-01-01 01:00:00 UTC ┆ 0.48358  │
│ 2025-01-01 02:00:00 UTC ┆ 0.486428 │
└─────────────────────────┴──────────┘
```

## How salty is the product water compared to the seawater?

This question is about streams, not about one piece of equipment, so the query starts from `measurement()`.
`include("medium")` shows which stream is the seawater and which is the product water.

```python
tds = acq.query().measurement(alias="tds", substance="dissolved solids", quantity_kind="mass concentration")
tds.include("medium").metadata().select("tds", "tds.medium")
```
```text
shape: (2, 2)
┌─────────────────────────────────┬───────────────────────┐
│ tds                             ┆ tds.medium            │
│ ---                             ┆ ---                   │
│ str                             ┆ str                   │
╞═════════════════════════════════╪═══════════════════════╡
│ wbs:storage-tank-3-out-tds-con… ┆ watr:Water-Freshwater │
│ wbs:intake-in-tds-concentratio… ┆ watr:Water-Seawater   │
└─────────────────────────────────┴───────────────────────┘
```

```python
tds.dataframe(limit=3)
```
```text
shape: (3, 3)
┌─────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
│ time                    ┆ tds__intake in tds concentrati… ┆ tds__storage tank 3 out tds co… │
│ ---                     ┆ ---                             ┆ ---                             │
│ datetime[μs, UTC]       ┆ f64                             ┆ f64                             │
╞═════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
│ 2025-01-01 00:00:00 UTC ┆ 33.428505                       ┆ 0.217005                        │
│ 2025-01-01 01:00:00 UTC ┆ 33.303636                       ┆ 0.218916                        │
│ 2025-01-01 02:00:00 UTC ┆ 33.484903                       ┆ 0.223305                        │
└─────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘
```

## What was the feed temperature of the reverse osmosis membrane, in Celsius?

```python
ro.measurement(alias="t", quantity_kind="temperature").data().convert_to("degC").dataframe(start="2025-07-01", limit=3)
```
```text
shape: (3, 2)
┌─────────────────────────┬────────────────────────┐
│ time                    ┆ t__RO in temperature k │
│ ---                     ┆ ---                    │
│ datetime[μs, UTC]       ┆ f64                    │
╞═════════════════════════╪════════════════════════╡
│ 2025-07-01 00:00:00 UTC ┆ 30.691354              │
│ 2025-07-01 01:00:00 UTC ┆ 30.703025              │
│ 2025-07-01 02:00:00 UTC ┆ 30.959595              │
└─────────────────────────┴────────────────────────┘
```

## Which chemicals are dosed, and where?

```python
dose = acq.query().entity("reactor").measurement(alias="dose").include("substance", "unit")
dose.metadata().select("reactor", "dose.substance", "dose.unit")
```
```text
shape: (3, 3)
┌──────────────────────────────┬─────────────────────────────────┬────────────────────┐
│ reactor                      ┆ dose.substance                  ┆ dose.unit          │
│ ---                          ┆ ---                             ┆ ---                │
│ str                          ┆ str                             ┆ str                │
╞══════════════════════════════╪═════════════════════════════════╪════════════════════╡
│ wbs:lime-addition            ┆ watr:pHAdjuster-Lime            ┆ unit:MilliGM-PER-L │
│ wbs:anti-scalant-addition    ┆ watr:WastewaterTreatmentChemic… ┆ unit:MilliGM-PER-L │
│ wbs:ferric-chloride-addition ┆ watr:Coagulant-FerricChloride   ┆ unit:MilliGM-PER-L │
└──────────────────────────────┴─────────────────────────────────┴────────────────────┘
```

## Which pumps feed the reverse osmosis membrane?

`related()` with `direction="upstream"` follows the connections of the plant against the flow.

```python
ro.related("pump", direction="upstream").include("label").metadata()
```
```text
shape: (2, 3)
┌────────┬────────┬───────────────────────┐
│ ro     ┆ pump   ┆ pump.label            │
│ ---    ┆ ---    ┆ ---                   │
│ str    ┆ str    ┆ str                   │
╞════════╪════════╪═══════════════════════╡
│ wbs:RO ┆ wbs:P2 ┆ Booster Pump P2       │
│ wbs:RO ┆ wbs:P1 ┆ High-pressure Pump P1 │
└────────┴────────┴───────────────────────┘
```

**Note:** `related()` looks at most three steps away by default.
`max_depth=0` removes the limit, and then the intake pump is listed as well.

## Which tank comes after each pump?

```python
pumps = acq.query().entity("pump")
pumps.related("tank", direction="downstream", max_depth=0, nearest=True).include("label").metadata()
```
```text
shape: (3, 3)
┌────────────┬────────────────────┬────────────────┐
│ pump       ┆ tank               ┆ tank.label     │
│ ---        ┆ ---                ┆ ---            │
│ str        ┆ str                ┆ str            │
╞════════════╪════════════════════╪════════════════╡
│ wbs:P1     ┆ wbs:storage-tank-2 ┆ Storage Tank 2 │
│ wbs:P2     ┆ wbs:storage-tank-2 ┆ Storage Tank 2 │
│ wbs:intake ┆ wbs:storage-tank-1 ┆ Storage Tank 1 │
└────────────┴────────────────────┴────────────────┘
```

## What is measured right after the cartridge filter?

`measurement()` with `direction="downstream"` selects the first streams along the flow.
The column `cf_downstream_entity` is the place where they were found, here the pipe that leaves the filter.

```python
cf = acq.query().entity(uri="wbs:cartridge-filtration", alias="cf")
after = cf.measurement(alias="after", direction="downstream").include("quantity_kind")
after.metadata().select("cf_downstream_entity", "after.label", "after.quantity_kind")
```
```text
shape: (4, 3)
┌─────────────────────────────────┬─────────────────────────────────┬───────────────────────────┐
│ cf_downstream_entity            ┆ after.label                     ┆ after.quantity_kind       │
│ ---                             ┆ ---                             ┆ ---                       │
│ str                             ┆ str                             ┆ str                       │
╞═════════════════════════════════╪═════════════════════════════════╪═══════════════════════════╡
│ wbs:conn-cartridge-filtration-… ┆ conn cartridge filtration to S… ┆ quantitykind:Pressure     │
│ wbs:conn-cartridge-filtration-… ┆ conn cartridge filtration to S… ┆ quantitykind:Temperature  │
│ wbs:conn-cartridge-filtration-… ┆ conn cartridge filtration to S… ┆ quantitykind:MassFlowRate │
│ wbs:conn-cartridge-filtration-… ┆ conn cartridge filtration to S… ┆ quantitykind:MassFlowRate │
└─────────────────────────────────┴─────────────────────────────────┴───────────────────────────┘
```

## Which pressure streams are on a pump?

`context()` adds the equipment a stream belongs to.
With a class, only the streams on equipment of that class are kept.

```python
pressure = acq.query().measurement(alias="p", quantity_kind="pressure")
pressure.context("pump", alias="pump").metadata()
```
```text
shape: (1, 3)
┌────────────────────────┬────────────────────┬────────┐
│ p                      ┆ p.label            ┆ pump   │
│ ---                    ┆ ---                ┆ ---    │
│ str                    ┆ str                ┆ str    │
╞════════════════════════╪════════════════════╪════════╡
│ wbs:P1-out-pressure-pa ┆ P1 out pressure pa ┆ wbs:P1 │
└────────────────────────┴────────────────────┴────────┘
```

## Where does the data of a stream come from?

`stream_info()` lists the `source_id` and the `stream_name` of each selected stream.

```python
power.data().stream_info().select("property_uri", "source_id", "stream_name")
```
```text
shape: (2, 3)
┌──────────────────────────────┬───────────┬─────────────────────┐
│ property_uri                 ┆ source_id ┆ stream_name         │
│ ---                          ┆ ---       ┆ ---                 │
│ str                          ┆ str       ┆ str                 │
╞══════════════════════════════╪═══════════╪═════════════════════╡
│ urn:swro/P1-mechanical-power ┆ csv-data  ┆ P1-mechanical-power │
│ urn:swro/P2-mechanical-power ┆ csv-data  ┆ P2-mechanical-power │
└──────────────────────────────┴───────────┴─────────────────────┘
```

## Moving on

- [Explore a plant model](explore-a-model.md): find the classes and values to use in a query.
- [Debug an empty or unexpected query result](debug-an-empty-query.md): what to check when a query selects nothing.
- [Query reference](../reference/query/_index.md): every method, with all parameters.
