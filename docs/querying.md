# Querying

This is a guide to querying metadata (what equipment exists, how things
connect, and what measurements hang off them) and data (the actual timeseries)
with Acquirium.

Every example in this doc runs on the public WaterTAP seawater-ro model, so you
can follow along.
<!-- FT1 placeholder: link the seawater-ro run guide here once it exists.
     Until then: deployments/WATERTAP/readme.md in the repo. -->

## The mental model

A `Query` is a description of what you are looking for.
You chain verbs to describe a pattern (a pump, a tank near it, the measurements on either one, etc.), and the server finds every place in the plant model where that
pattern matches.

`acq.query()` starts an empty `Query`.
Every verb returns a new immutable `Query`, so you can hold on to a partial
query and extend it in different directions.

The `Query` interface is an opinionated view over the underlying plant knowledge graph.
It is different from querying a raw RDF graph.
This view treats certain edges as attributes of the source nodes.
For instance a measurement's unit is a separate triple in RDF: `:source qudt:hasUnit :unit`, but here, it is just the measurement's `unit` attribute.
For the same reason, edges whose meaning is not plant topology (class hierarchies, external references etc.) are hidden from generic traversal.

This interface classifies nodes into two main categories.
Every concept, system, equipment and connection is treated as an `Entity` node,
while every measurement (i.e. `s223:Property`) is treated as a `Data` node.

The `Query` interface is lazy. 
The actual query execution will happen after the user executes certain functions like `.metadata()`, `.dataframe()` or `.facets()`.

## entity()

`entity(cls)` adds a node for every instance of a class.
The class can be a URI, CURIE or free text.
Free text is matched to an actual class by the server (e.g. `"pump"` finds `s223:Pump`).

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)

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

`metadata()` actually executes the query.
It returns the matched items as a polars frame, one column per node, and every
example in this doc ends with it.
Timeseries values are requested separately, covered under
[Getting the values](#getting-the-values).

`uri=` pins one specific instance instead (CURIEs work).
Keyword arguments filter by attributes inline, using the same attribute names
as `where()` (covered later):

```python
acq.query().entity(uri="wbs:RO")                          # exactly this item
acq.query().entity("Equipment", process="reverse osmosis")  # filter by attribute
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
acq.query().entity(uri="wbs:RO", alias="ro")     # same thing as:
acq.query().entity(uri="wbs:RO").alias("ro")
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

q.refocus("pump").related("Pressure Exchanger")   # pointer moves back to the pumps
q.related("Pressure Exchanger", frm="pump")       # same result, one step only
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

The default `shape="narrow"` gives one row per reading instead, with the point
it came from in its own column.
Either way you can bound what you pull with `start=`, `end=` and `limit=`.

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

q.include("unit")                   # every point, null where there is no unit
q.include("unit", required=True)    # only points that have a unit
```
```text
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
```

### drop()

`drop()` hides a node's column while keeping it in the pattern.
The node still constrains the match; it just stops being part of the output.
Rows that differed only in the dropped node collapse into one.

```python
q = (acq.query().entity("System")
     .related("Equipment", via="has member")
     .where(target="Equipment", process="reverse osmosis"))

q.metadata()             # the RO shows up once per system that contains it
q.drop("System")         # ...but you only wanted the equipment
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

## Free text and what it resolves to

Every place that takes a class, a predicate or an attribute value also takes
free text.
The server matches it against the ontologies (exact match on a label first,
then semantic similarity) and picks the best hit.
`"ozone unit"` becomes `nawi:OzonationUnit`, `"mg/L"` becomes `unit:MilliGM-PER-L`.
Each slot is matched against its own vocabulary, so `unit="PSI"` searches
units, not classes.

Be aware that free text queries might result in incorrect results.
The match is a best guess, and a wrong guess would still generate a working query but with
the wrong answer.
In such case, try to change the text you enter, use the exact URI of the equipment you're searching or check the text matching results:

```python
acq.client.resolve("ro membrane", "class", top_k=3, min_score=0.3)
```
```text
[{'uri': 'urn:nawi-water-ontology#ReverseOsmosisMembrane',        'score': 0.839, ...},
 {'uri': 'urn:nawi-water-ontology#ElectricallyConductingMembrane', 'score': 0.788, ...},
 {'uri': 'urn:nawi-water-ontology#MolybdenumSulfideBasedMembrane', 'score': 0.786, ...}]
```

Note that only the ontologies are searchable this way, not your plant's own
instance labels.
`entity("pump")` resolves the *class* `Pump`; it will not find the item named "pump-1" in the plant by
its label.

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

## When a query returns nothing

Empty results are usually one of five things.

**The text resolved to the wrong concept.**
This is the most common cause, and the query still runs, so there is no
error to see.
It applies to predicates too: `via="flurb blah"` does not fail, it resolves to
the closest predicate it can find and walks that instead.
Check with `acq.client.resolve(text, kind, top_k=3)`.

**The focus is on the wrong node.**

`related()` and `measurement()` without `frm=`, and `where()` without
`target=`, all act on the node the pointer is on.
After a `related()` that is the new entity; after a `measurement()` it is the
measurement, not the equipment.

Attributes that do not apply to that node raise rather than return nothing:

```text
ValueError: attribute 'process' does not apply to data node 'pump_data'
ValueError: include: attribute 'quantity_kind' does not apply to entity node 'pump'
```

**Nothing was within reach.**
`related()` stops at `max_depth` (3 by default, 1 for predicate lists).
Raise the `max_depth` value.

**`required=True` removed the rows.**
An attribute that is missing from the data drops every row that lacks it.
Take `required=` off and look for nulls in the column.

**The points are on connection points you excluded.**
`include_connection_points=False` keeps only directly-owned points, which for
most equipment is very few.

When in doubt, run `metadata()` after each step and see where the rows
disappear.
`facets()` on the last surviving node shows what values actually exist there.

---

## Notes

Depth and internals. Nothing here is needed for everyday use.

### Seeing and running the raw query

`to_sparql()` returns the SPARQL the query compiles to, without running it.
It is the fastest way to see what a chain actually asks for, and to check
that a filter applies to the node you intended.

`execute()` runs that query and returns the raw bindings as
`{"columns": [...], "rows": [[...]]}`.
`metadata()` is this plus the alias-to-column naming, so you rarely need it
directly.

### Which predicates are hidden

`via="any"` walks every predicate except a default hidden set: the ones backing
the attributes (`rdf:type`, `hasUnit`, `hasQuantityKind`, `ofMedium`,
`hasMedium`, `ofSubstance`, `hasEnumerationKind`, `hasProcess`, `dataSource`),
plus `rdfs:subClassOf`, `s223:hasProperty`, `s223:hasConnectionPoint`,
`s223:cnx` and `ref:hasExternalReference`.
These predicates describe a node rather than connect the plant, and walking
them returns ontology terms instead of nearby equipment.

```python
from acquirium.Client.explore import hidden_predicates, hide, unhide

sorted(hidden_predicates())
```

The set is global and adjustable: `hide(uri)` adds to it, `unhide(uri)` takes
one out, and a bare `unhide()` resets to the defaults.
Naming a predicate in `via=` always overrides the set, so hiding never blocks
an explicit request.
Measurement edges are exempt too, which is why `measurement()` still reaches
points through `hasProperty` and `hasConnectionPoint`.

### The step patterns behind direction=

`direction="upstream"` and `"downstream"` are shorthand for four fixed step
patterns each, exported as constants:

```python
from acquirium.Client.explore import (
    UPSTREAM_EQUIPMENT, DOWNSTREAM_EQUIPMENT,
    UPSTREAM_PROPERTY, DOWNSTREAM_PROPERTY,
)
```

The `EQUIPMENT` pair walks equipment to equipment through connection points and
connections; the `PROPERTY` pair ends on the measurements attached along the
way.
`related()` and `measurement()` pick the right one for you.
Each constant is a readable tuple of chains.
Passing one to `via=` runs the same traversal as a client-side walk, so
`nearest=True` follows the process flow instead of raw graph hops:

```python
q.related("pump", via=UPSTREAM_EQUIPMENT, nearest=True)
```

The module docstring of `acquirium.Client.explore.directions` documents
every step.

### How traversal runs

SPARQL handles multi-hop wildcard traversal poorly: every hop multiplies
joins, and an unbounded path can stall the store.
Acquirium therefore resolves these steps client side.
When a step uses `via="any"` or a repeatable predicate, the client resolves it
at execute time with a breadth-first walk: fetch one layer of neighbors, step,
repeat, up to `max_depth`.
The walk also makes `nearest` exact: distance is counted per source, and
ties survive.
The final SPARQL only ever receives the concrete pairs the walk found.
Predicate lists and `direction=` compile to SPARQL directly, without the
walk.
Layer results are cached until the graph changes, so repeating a query is
cheap.
You can always inspect what will run with `.to_sparql()`.

### Method fine points

`measurement(frm=...)` also accepts a list of aliases, attaching one
measurement node per named entity.
This is the middle ground between a single `frm=` and `frm="*"`:

```python
(acq.query().entity(uri="wbs:P1", alias="p1").related("Pressure Exchanger", alias="px")
 .measurement(frm=["p1", "px"], quantity_kind="pressure"))
# columns: p1, px, p1_data, px_data
```

`alias()` renames the current node, but the previous alias keeps working as
an alternative handle in `target=`, `frm=` and `of=`; display uses the latest
name.

**TODO:** This might be a bad idea. Remove old aliases

The terminals take `use_union=` (default `True`).
`True` queries the union of the model and the ontology closure, which is what
makes subclass matching work.
`False` restricts the query to the model graph alone; it is faster and
correct only for queries that need no ontology terms.

---

## Coming from the old interface

Optional. Skip this unless you have queries written against the previous
`find_*` methods.

Those methods still exist, reachable through `acq.find_entity()` and
`acq.find_all_data()`, but they are deprecated and will not gain features.
The rewrite is usually mechanical: one verb per old method, one `where()` in
place of the `filter_by_*` family.

| old | new |
|---|---|
| `find_entity(_class=, uri=, alias=)` | `entity(cls, uri=, alias=)` |
| `find_entity(process=...)` | `entity(cls, process=...)` |
| `find_related(_class=, _from=, hops=, predicates=)` | `related(cls, frm=, max_depth=, via=)` |
| `find_related(direction=)` | `related(direction=)` |
| `find_data(_from=, alias=, filters_dict=)` | `measurement(frm=, alias=, **attrs)` |
| `find_all_data()` | `measurement(frm="*")`, or `measurement()` on an empty query |
| `find_related_data(...)` | `related(...).measurement(...)` |
| `filter_by_unit(v)` | `where(unit=v)` |
| `filter_by_medium(v)` | `where(medium=v)` |
| `filter_by_substance(v)` | `where(substance=v)` |
| `filter_by_quantity_kind(v)` | `where(quantity_kind=v)` |
| `filter_by_enumeration_kind(v)` | `where(enumeration_kind=v)` |
| `filter_data_nodes(predicate=, value=, _from=)` | `where(attr=value, target=)` |
| `filter_by_*(..., exclude=True)` | `where(attr=Not(value))` |
| `show_query_graph()` | `to_sparql()` |

Keyword names changed with them: `_class` is `cls`, `_from` is `frm`, `hops`
is `max_depth`, and `predicates` is `via`.
`metadata()`, `data()`, `dataframe()`, `execute()`, `to_sparql()`,
`insert_log()` and `read_logs()` kept their names and behave the same.

Two behavior differences matter when porting.

Defaults changed.
`related()` returns nearest matches where `find_related` returned everything
reachable, so add `nearest=False` if you relied on the old behavior.

Filtering is one vocabulary now.
Anywhere you passed a predicate URI to `filter_data_nodes`, you now pass a
named attribute to `where()`, and the same names work as inline keyword
arguments on `entity()`, `related()` and `measurement()`.
