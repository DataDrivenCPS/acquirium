---
title: Querying
---

<!-- TODO: intro -->

In this tutorial, we show how querying metadata (what equipment exists, how things
connect, and what measurements hang off them) and data (the actual timeseries)
is done with Acquirium.

Every example in this doc runs on the public WaterTAP seawater-ro model, so you
can follow along.
Getting one running is the [WaterTAP deployment guide](https://github.com/DataDrivenCPS/acquirium/blob/main/deployments/WATERTAP/readme.md):
clone the repo, install the `watertap` extra, and start the server against
`deployments/WATERTAP/models/seawater-ro/acquirium.toml`.


**Connecting to a running acquirium server using Acquirium Client:**

Every query starts with connecting to a running acquirium server. The client object `acq` exposes all the required methods to interact with the server.

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)
```

## entity()

`entity(cls)` adds a node for every instance of a class.
The class can be a URI, CURIE or free text (see [Glossary](../reference/glossary.md) for terminology).
Free text is matched to an actual class by the server (e.g. `"pump"` finds `s223:Pump`).

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

Here, `metadata()` actually executes the query.
It returns the matched items as a polars frame, one column per node, and every
example in this doc ends with it.
Timeseries values are requested separately, covered under
[Getting the values](#getting-the-values).

`uri=` pins one specific instance instead (CURIEs work).
Keyword arguments filter by attributes inline, using the same attribute names
as `where()` (covered later):

```python
q = acq.query().entity(uri="wbs:RO")                            # exactly this item
q = acq.query().entity("Equipment", process="reverse osmosis")  # filter by attribute

q.metadata()
```
```text
shape: (1, 1)
┌───────────┐
│ Equipment │
╞═══════════╡
│ wbs:RO    │
└───────────┘
```

### Aliases

Every node has an alias, and aliases are the column names of every result.
The default is the text you typed (`pump`, `Equipment` above).
To provide an alias: either use the `alias=` keyword or the `.alias()` method.

Note that it is recommended to assign an alias to a node built from `uri=`; otherwise the column is named by the node's internal numeric id.


```python
q = acq.query().entity(uri="wbs:RO", alias="ro")     # same thing as:
q = acq.query().entity(uri="wbs:RO").alias("ro")
q.metadata()
```
```text
shape: (1, 1)
┌────────┐
│ ro     │
╞════════╡
│ wbs:RO │
└────────┘
```

Aliases are unique per query.
If two nodes derive the same name, the second default alias becomes `pump_2`.
Explicitly reusing an alias raises an error.

## related()

`related(cls)` adds an entity connected to an existing node.
By default it starts from the node the pointer is on (see next chapter) and returns the *nearest*
matches within 3 hops of any visible edge (equal-distance ties all
survive).

```python
pumps = acq.query().entity("pump")     # every pump in the plant
with_tanks = pumps.related("tank")     # ...and the nearest tank to each

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

`nearest=False` keeps every reachable match, not just the closest:

```python
acq.query().entity("pump").related("tank", nearest=False).metadata()
```
```text
shape: (6, 2)
┌────────────┬──────────────────────────────┐
│ pump       ┆ tank                         │
╞════════════╪══════════════════════════════╡
│ wbs:intake ┆ wbs:ferric-chloride-addition │
│ wbs:P2     ┆ wbs:storage-tank-2           │
│ wbs:P1     ┆ wbs:storage-tank-2           │
│ wbs:intake ┆ wbs:chlorination             │
│ wbs:P1     ┆ wbs:anti-scalant-addition    │
│ wbs:intake ┆ wbs:static-mixer             │
└────────────┴──────────────────────────────┘
```

### The pointer, refocus() and frm=

The query keeps a pointer at the node you added last.
For instance, in the query above the focus starts at the pumps and then moves
on to the tanks.
This means a new step like `.related("valve")` would find the valves related to
the tanks, not the pumps.
If you want to branch off the chain (say, go back to the pumps and find their
related valves), move the pointer back with `refocus()`, or redirect a single
step with `frm=`:

```python
q = acq.query().entity("pump").related("tank")

q.refocus("pump").related("Pressure Exchanger").metadata()   # pointer moves back to the pumps
q.related("Pressure Exchanger", frm="pump").metadata()       # same result, one step only
```
```text
shape: (3, 3)
┌────────┬───────────────────────────┬────────────────────┐
│ pump   ┆ tank                      ┆ Pressure Exchanger │
╞════════╪═══════════════════════════╪════════════════════╡
│ wbs:P1 ┆ wbs:storage-tank-2        ┆ wbs:PXR            │
│ wbs:P2 ┆ wbs:storage-tank-2        ┆ wbs:PXR            │
│ wbs:P1 ┆ wbs:anti-scalant-addition ┆ wbs:PXR            │
└────────┴───────────────────────────┴────────────────────┘
```

Both forms produce the same result.
`refocus("pump")` moves the pointer, so everything after it starts from the
pumps; `frm="pump"` redirects just that one step.
Both take the node's alias.

### via=

`via=` controls which predicates the traversal may use.
A list input to `via=` means "any of these".
The default `"any"` walks every predicate except the hidden metadata ones.
A single predicate (free text or URI) restricts the walk to it, repeated up to
`max_depth`; prefix with `"^"` to follow it in reverse.

```python
(acq.query().entity("System")
 .related("Equipment", via="has member")
 .metadata())
```
```text
shape: (36, 2)
┌──────────────────────────┬──────────────────────────────┐
│ System                   ┆ Equipment                    │
╞══════════════════════════╪══════════════════════════════╡
│ wbs:desalination-system  ┆ wbs:P1                       │
│ wbs:pretreatment-system  ┆ wbs:anti-scalant-addition    │
│ wbs:seawater-ro-plant    ┆ wbs:ferric-chloride-addition │
│ …                        ┆ …                            │
│ wbs:seawater-ro-plant    ┆ wbs:storage-tank-1           │
│ wbs:posttreatment-system ┆ wbs:storage-tank-3           │
└──────────────────────────┴──────────────────────────────┘
```

Note that `nearest` follows `via`.
Plain `via="any"` with no direction returns the nearest matches.
An explicit predicate, a predicate list or a `direction=` returns all matches
within `max_depth`.
Set `nearest=` to override either way.

### direction=

`direction="upstream"` or `"downstream"` follows the piping topology instead
of generic relatedness:

```python
(acq.query().entity(uri="wbs:RO", alias="ro")
 .related("pump", direction="upstream")
 .metadata())
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

The exact step patterns each direction expands to are inspectable constants in
`acquirium.Client.explore.directions`, and you can pass them to `via=`
directly (e.g. `via=UPSTREAM_EQUIPMENT`) when you want *nearest along the
flow* rather than nearest in the graph.

### max_depth=

`max_depth` bounds the number of hops.
The default is 3 (1 for predicate lists).
`max_depth=0` means unbounded.
Be aware that unbounded walks over a large plant can be slow.


## measurement()

`measurement()` adds the data-bearing points of the node the pointer is on.
Equipment owns some points directly, but most hang off its connection points
(inlets and outlets), so those are included by default
(`include_connection_points=False` limits to directly-owned points; for
`wbs:RO` that excludes every pressure reading, because they all live on the
inlet and outlet).
Keyword arguments filter inline, and the new node's alias defaults to
`<source>_data`.

```python
(acq.query().entity(uri="wbs:RO", alias="ro")
 .measurement(quantity_kind="pressure")
 .metadata())
```
```text
shape: (3, 2)
┌────────┬───────────────────────────────┐
│ ro     ┆ ro_data                       │
╞════════╪═══════════════════════════════╡
│ wbs:RO ┆ wbs:RO-out-pressure           │
│ wbs:RO ┆ wbs:RO-out-retentate-pressure │
│ wbs:RO ┆ wbs:RO-in-pressure            │
└────────┴───────────────────────────────┘
```

Called on an empty query, `measurement()` matches every registered stream in
the plant.

```python
acq.query().measurement(quantity_kind="pressure").metadata()
```
```text
shape: (6, 1)
┌─────────────────────────────────┐
│ data                            │
╞═════════════════════════════════╡
│ wbs:P1-out-pressure             │
│ wbs:RO-in-pressure              │
│ wbs:PXR-brine-out-pressure      │
│ wbs:conn-cartridge-filtration-… │
│ wbs:RO-out-pressure             │
│ wbs:RO-out-retentate-pressure   │
└─────────────────────────────────┘
```

### frm="*"

`frm=` picks the source node by alias, same as in `related()`.
`frm="*"` attaches a measurement node to *every* entity in the pattern.
The result has one row per point (M+N rows): each row carries one
node's point and null for the others.

```python
(acq.query().entity("pump").related("Pressure Exchanger")
 .measurement(frm="*")
 .metadata().head(8))
```
```text
shape: (8, 4)
┌────────┬────────────────────┬─────────────────────┬─────────────────────────────────┐
│ pump   ┆ Pressure Exchanger ┆ pump_data           ┆ Pressure Exchanger_data         │
╞════════╪════════════════════╪═════════════════════╪═════════════════════════════════╡
│ wbs:P2 ┆ wbs:PXR            ┆ null                ┆ wbs:PXR-brine-out-flow-mass-wa… │
│ wbs:P2 ┆ wbs:PXR            ┆ null                ┆ wbs:PXR-brine-out-pressure      │
│ wbs:P1 ┆ wbs:PXR            ┆ null                ┆ wbs:PXR-brine-out-flow-mass-wa… │
│ wbs:P1 ┆ wbs:PXR            ┆ null                ┆ wbs:PXR-brine-out-flow-mass-td… │
│ wbs:P1 ┆ wbs:PXR            ┆ wbs:P1-efficiency   ┆ null                            │
│ wbs:P2 ┆ wbs:PXR            ┆ null                ┆ wbs:PXR-efficiency              │
│ wbs:P1 ┆ wbs:PXR            ┆ null                ┆ wbs:PXR-efficiency              │
│ wbs:P1 ┆ wbs:PXR            ┆ wbs:P1-out-pressure ┆ null                            │
└────────┴────────────────────┴─────────────────────┴─────────────────────────────────┘
```
TODO: organize the metadata table to group nulls together.

<!-- TODO: `metadata()` returns rows in no guaranteed order, so this
     `.head(8)` capture shows a different eight rows on every run. Sort the
     frame in the example, or show the full result. -->
<!-- TODO: two other captures in this file are `.head()`/`.tail()` of an
     unordered result for the same reason; decide on one convention. -->

### direction= and nearest=

Both work here too.
`direction=` with `nearest=True` finds the closest matching measurement up or
downstream of the source:

```python
(acq.query().entity(uri="wbs:P1", alias="p1")
 .measurement(direction="downstream", nearest=True, quantity_kind="pressure")
 .metadata())
```
```text
shape: (1, 2)
┌────────┬─────────────────────┐
│ p1     ┆ p1_downstream_data  │
╞════════╪═════════════════════╡
│ wbs:P1 ┆ wbs:P1-out-pressure │
└────────┴─────────────────────┘
```

## Getting the values

`dataframe()` returns the timeseries in one frame.
`shape="wide"` puts `time` first and one column per point.
This shape is useful for plotting or joining:

```python
q = (acq.query().entity(uri="wbs:RO", alias="ro")
     .measurement(alias="m", quantity_kind="pressure"))

q.dataframe(shape="wide").tail(3)
```
```text
shape: (3, 4)
┌────────────────────────────────┬───────────────────────┬────────────────────────┬──────────────────────────────────┐
│ time                           ┆ m__wbs:RO-in-pressure ┆ m__wbs:RO-out-pressure ┆ m__wbs:RO-out-retentate-pressure │
╞════════════════════════════════╪═══════════════════════╪════════════════════════╪══════════════════════════════════╡
│ 2026-08-07 22:53:38.669786 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8043e6                         │
│ 2026-08-07 22:53:54.023514 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8001e6                         │
│ 2026-08-07 22:54:10.019299 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8056e6                         │
└────────────────────────────────┴───────────────────────┴────────────────────────┴──────────────────────────────────┘
```

`shape="narrow"` gives one row per reading instead, with the point it came
from in its own column.
Either way you can bound what you pull with `start=`, `end=` and `limit=`.
Note that a point with an `rdfs:label` is shown by that label: `metadata()`
adds a `<alias>.label` column next to the node, and wide columns use it in
place of the CURIE.
The seawater-ro points carry no labels, so none appear here.

`data()` returns a `DataObject`, a lazy reference to the same data.
It reports the row count and time range before fetching anything, and lets
you take one alias at a time:

```python
d = q.data()
d
```
```text
DataObject(lazy, ~11817 rows, range=2026-08-05T16:28:39.137381+00:00 to 2026-08-07T22:53:38.669786+00:00, aliases=['m'], entities=['ro'])
```

```python
d["m"].tail(3)
```
```text
shape: (3, 3)
┌────────────────────────────────┬──────────┬─────────────────────────────────┐
│ time                           ┆ value    ┆ point_uri                       │
╞════════════════════════════════╪══════════╪═════════════════════════════════╡
│ 2026-08-07 22:53:38.669786 UTC ┆ 6.8043e6 ┆ urn:swro/RO-out-retentate-pres… │
│ 2026-08-07 22:53:38.669786 UTC ┆ 101325.0 ┆ urn:swro/RO-out-pressure        │
│ 2026-08-07 22:53:38.669786 UTC ┆ 7e6      ┆ urn:swro/RO-in-pressure         │
└────────────────────────────────┴──────────┴─────────────────────────────────┘
```

Unit conversion, grouping by entity and the rest of the `DataObject` API are
in the [data guide](data.md).

## where()

`where()` filters a node by its attributes.
It applies to the node the pointer is on; `target=` filters any node by alias,
and `target="*"` filters every measurement node at once.
The keyword arguments you saw on `entity()` and `measurement()` are exactly
this, inline.

The attribute vocabulary is one shared registry:

| attribute | usable on | meaning |
|---|---|---|
| `type` | both | class of the node, subclass-closed (`"tank"` matches all tank kinds) |
| `process` | entities | treatment process the entity performs (`"ozonation"`, `"reverse osmosis"`) |
| `cp_type` | entities | class of one of the entity's connection points (`"outlet connection point"`) |
| `medium` | both | carried medium (`"fluid water"`, `"air"`, `"brine"`) |
| `substance` | measurements | measured substance/constituent (`"chlorine"`, `"organics"`) |
| `quantity_kind` | measurements | QUDT quantity kind (`"volume flow rate"`, `"turbidity"`) |
| `unit` | measurements | QUDT unit (`"mg/l"`, `"PSI"`, `"NTU"`) |
| `enumeration_kind` | measurements | enumeration kind of a state/enum property (`"on off"`) |
| `data_source` | measurements | origin tag literal, matched verbatim (`"Lab"`, `"SCADA"`) |

The same table is generated into the docstring of every attribute-taking
method, so `help(q.where)` has it too.
Values may be URIs (used as-is) or free text (resolved by the server against
the right vocabulary for that attribute).
A list means "any of these".
`Not()` excludes.

```python
from acquirium.Client.explore import Not

(acq.query().entity(uri="wbs:RO", alias="ro")
 .measurement(alias="feed")
 .where(quantity_kind="mass flow rate", medium=Not("brine"))
 .metadata())
```
```text
shape: (5, 2)
┌────────┬─────────────────────────────────┐
│ ro     ┆ feed                            │
╞════════╪═════════════════════════════════╡
│ wbs:RO ┆ wbs:RO-in-flow-mass-tds         │
│ wbs:RO ┆ wbs:RO-out-flow-mass-water      │
│ wbs:RO ┆ wbs:RO-out-flow-mass-tds        │
│ wbs:RO ┆ wbs:RO-in-flow-mass-water       │
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… │
└────────┴─────────────────────────────────┘
```

```python
acq.query().measurement(quantity_kind=["temperature", "efficiency"]).metadata()
```
```text
shape: (5, 1)
┌─────────────────────────────────┐
│ data                            │
╞═════════════════════════════════╡
│ wbs:PXR-efficiency              │
│ wbs:conn-cartridge-filtration-… │
│ wbs:P1-efficiency               │
│ wbs:P2-efficiency               │
│ wbs:RO-in-temperature           │
└─────────────────────────────────┘
```

`target=` reaches back to an earlier node without moving the pointer.
Here the pointer is on `m`, but the filter applies to the equipment.

```python
q = acq.query().entity("Equipment").measurement(alias="m", quantity_kind="pressure")

q.where(target="Equipment", process="reverse osmosis").metadata()
```
```text
shape: (3, 2)
┌───────────┬───────────────────────────────┐
│ Equipment ┆ m                             │
╞═══════════╪═══════════════════════════════╡
│ wbs:RO    ┆ wbs:RO-out-retentate-pressure │
│ wbs:RO    ┆ wbs:RO-out-pressure           │
│ wbs:RO    ┆ wbs:RO-in-pressure            │
└───────────┴───────────────────────────────┘
```

## Choosing columns

By default the result has one column per node.
`include()` adds attribute columns, `drop()` removes columns, and
`with_columns()` does both in one call.

### include()

`include()` adds an attribute of a node as its own column, named
`alias.attribute` and placed right after that node's column.
It applies to the node the pointer is on, unless you say otherwise with `of=`
or the dotted `"alias.attribute"` form.

```python
(acq.query().entity(uri="wbs:RO", alias="ro").include("process")
 .measurement(alias="m", quantity_kind="pressure")
 .include("m.unit")
 .metadata())
```
```text
shape: (3, 4)
┌────────┬─────────────────────────────┬───────────────────────────────┬────────┐
│ ro     ┆ ro.process                  ┆ m                             ┆ m.unit │
╞════════╪═════════════════════════════╪═══════════════════════════════╪════════╡
│ wbs:RO ┆ nawi:Process-ReverseOsmosis ┆ wbs:RO-in-pressure            ┆ null   │
│ wbs:RO ┆ nawi:Process-ReverseOsmosis ┆ wbs:RO-out-pressure           ┆ null   │
│ wbs:RO ┆ nawi:Process-ReverseOsmosis ┆ wbs:RO-out-retentate-pressure ┆ null   │
└────────┴─────────────────────────────┴───────────────────────────────┴────────┘
```

Attributes are optional by default: a point without the attribute still shows
up, with `null` in that column (these RO pressures carry no unit).
`required=True` keeps only the rows that have it.

```python
q = acq.query().entity(uri="wbs:RO", alias="ro").measurement(alias="m")

q.include("unit").metadata()                   # every point, null where there is no unit
q.include("unit", required=True).metadata()    # only points that have a unit
```
```text
shape: (11, 3)                                   # include("unit")
┌────────┬─────────────────────────────────┬─────────────────────┐
│ ro     ┆ m                               ┆ m.unit              │
╞════════╪═════════════════════════════════╪═════════════════════╡
│ wbs:RO ┆ wbs:RO-in-pressure              ┆ null                │
│ wbs:RO ┆ wbs:RO-out-pressure             ┆ null                │
│ wbs:RO ┆ wbs:RO-in-flow-mass-water       ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-flow-mass-tds        ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-membrane-area            ┆ unit:M2             │
│ wbs:RO ┆ wbs:RO-out-retentate-pressure   ┆ null                │
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-temperature           ┆ null                │
│ wbs:RO ┆ wbs:RO-out-flow-mass-water      ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-flow-mass-tds         ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… ┆ unit:KiloGM-PER-SEC │
└────────┴─────────────────────────────────┴─────────────────────┘

shape: (7, 3)                                    # include("unit", required=True)
┌────────┬─────────────────────────────────┬─────────────────────┐
│ ro     ┆ m                               ┆ m.unit              │
╞════════╪═════════════════════════════════╪═════════════════════╡
│ wbs:RO ┆ wbs:RO-in-flow-mass-water       ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-flow-mass-tds        ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-membrane-area            ┆ unit:M2             │
│ wbs:RO ┆ wbs:RO-out-flow-mass-water      ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-out-retentate-flow-mass… ┆ unit:KiloGM-PER-SEC │
│ wbs:RO ┆ wbs:RO-in-flow-mass-tds         ┆ unit:KiloGM-PER-SEC │
└────────┴─────────────────────────────────┴─────────────────────┘
```

<!-- TODO: two RO-out-retentate-flow-mass-* points truncate to the same
     display string; widen the capture or pick a shorter example so the
     required=True table does not look like it has a duplicate row. -->
Note that the four points dropped by `required=True` are the three pressures
and the temperature, which is what [Which measurement points carry no
unit?](query-cookbook.md#which-measurement-points-carry-no-unit) lists
plant-wide.

### drop()

`drop()` hides a node's column while keeping it in the pattern.
The node still constrains the match; it just stops being part of the output.
Rows that differed only in the dropped node collapse into one.

```python
q = (acq.query().entity("System")
     .related("Equipment", via="has member")
     .where(target="Equipment", process="reverse osmosis"))

q.metadata()                  # the RO shows up once per system that contains it
q.drop("System").metadata()   # ...but you only wanted the equipment
```
```text
shape: (2, 2)                          shape: (1, 1)
┌─────────────────────────┬───────────┐    ┌───────────┐
│ System                  ┆ Equipment │    │ Equipment │
╞═════════════════════════╪═══════════╡    ╞═══════════╡
│ wbs:desalination-system ┆ wbs:RO    │    │ wbs:RO    │
│ wbs:seawater-ro-plant   ┆ wbs:RO    │    └───────────┘
└─────────────────────────┴───────────┘
```

With no arguments it drops the node the pointer is on, which is useful when a
node exists only to constrain the pattern:

```python
(acq.query().entity(uri="wbs:pretreatment-system").drop()
 .related("equipment").measurement(alias="sensor"))
```

`include()` and `drop()` are inverses.
Each accepts the other's vocabulary, so any column decision can be reversed
later in the chain: `include()` un-drops a node, `drop()` un-includes an
attribute.

```python
q.drop("System").include("System")      # show it again
q.include("unit").drop("unit")          # remove the attribute again
```

### with_columns()

`with_columns()` merges the two.
Plain names are included, `-` prefixed names are dropped.

```python
(acq.query().entity(uri="wbs:RO", alias="ro")
 .measurement(alias="m", quantity_kind="pressure")
 .with_columns("m.quantity_kind", "-ro")
 .metadata())
```
```text
shape: (3, 2)
┌───────────────────────────────┬─────────────────┐
│ m                             ┆ m.quantity_kind │
╞═══════════════════════════════╪═════════════════╡
│ wbs:RO-out-pressure           ┆ qudtqk:Pressure │
│ wbs:RO-in-pressure            ┆ qudtqk:Pressure │
│ wbs:RO-out-retentate-pressure ┆ qudtqk:Pressure │
└───────────────────────────────┴─────────────────┘
```

All three take the same column names: an attribute of the current node
(`"unit"`), an attribute of a named node (`"m.unit"`), or a node alias
(`"System"`).
When an alias collides with an attribute name, the attribute wins; use the
dotted form to be explicit.
