---
title: Data retrieval and formats
---

The [querying tutorial](querying.md) ended with a frame of readings. This one
looks more closely at what comes back: how much data a query will fetch, how
the frame is laid out, what units the numbers are in, and how to split a
result by equipment. It uses the same WaterTAP seawater-RO plant.

```python
from acquirium import Acquirium

acq = Acquirium(server_url="127.0.0.1", server_port=8000)
```

## Lazy data retrieval

A query can match dozens of points, each with months of readings. Fetching all
of that by accident is easy and slow, so `data()` returns a lazy object that
fetches nothing yet:

```python
flows = (acq.query().entity("Equipment")
         .measurement(alias="m", quantity_kind="mass flow rate")
         .data())
flows.total_rows     # asks the server for the count, fetches no readings
```
```text
55980
```

```python
flows
```
```text
DataObject(lazy, ~55980 rows, range=2026-08-05T16:28:39.137381+00:00 to 2026-08-08T05:30:48.864274+00:00, aliases=['m'], entities=['Equipment'])
```

`total_rows` and `time_range` ask the server for the row count and time range
of the matched streams without sending any readings. Printing the object does
not contact the server: until one of them is read, it shows the number of
matched streams in place of the count and range. If 56,000 rows is more than you want, bound the query with `start=`,
`end=` or `limit=` before fetching. `dataframe()` fetches the readings and
keeps them on the object, so later calls reuse them.

`Query.dataframe()`, which the querying tutorial used, is the same thing in
one step: `q.dataframe()` returns what `q.data().dataframe()` does.

## Wide and narrow dataframe formats

By default a frame is **wide**: a `time` column, then one column per point,
with readings lined up by timestamp. That is the shape you want for plotting
or for comparing two signals:

```python
pressures = (acq.query().entity(uri="wbs:RO", alias="ro")
             .measurement(alias="m", quantity_kind="pressure").data())
pressures.dataframe().tail(3)
```

Each column is named after its alias. One alias here covers three points, so
each name adds the point: `m__wbs:RO-in-pressure`. A point with a label shows
the label instead. These points have none.

Points that are not sampled at the same moments line up badly, leaving a wide
frame full of nulls. The **narrow** shape avoids that by giving each reading
its own row:

```python
pressures.dataframe(shape="narrow").head(4)
```

Each row says which point it came from, in a `point_id` column, and carries
its value in `value_numeric`, or in `value_text` for text readings such as a
fault code. [Values](../explanation/values.md) explains when a reading is a
number and when it is text.

## Units

Numbers are only useful once you know their unit. `units()` lists the units
behind each alias:

```python
flow = (acq.query().entity(uri="wbs:RO", alias="ro")
        .measurement(alias="flow", quantity_kind="mass flow rate").data())
flow.units()
```
```text
{'flow': ['http://qudt.org/vocab/unit/KiloGM-PER-SEC']}
```

`convert_to()` returns a new object with every value converted, so
10.67 kg/s becomes 640.35 kg/min:

```python
flow.convert_to("kg/min")
```

Conversion needs to know the starting unit. Some points carry none; in this
model, the RO pressures. Converting them raises an error rather than guessing.
If you know the numbers are pascals, say so with
`convert_to("psi", from_unit="Pa")`. A conversion between incompatible units,
such as a flow to a temperature, also raises.
[Units](../explanation/units.md) describes where units are recorded and which
pairs convert.

## Result partitioning

A query that matched several pumps returns one object for all of them. Often
you want to work through them one at a time, perhaps to fit a model per pump.
`by()` splits the object by an entity alias:

```python
power = acq.query().entity("pump").measurement(alias="m", quantity_kind="power").data()

for pump, readings in power.by("pump"):
    print(pump, readings.dataframe().shape)
```
```text
urn:swro/P1 (5892, 2)
urn:swro/P2 (5892, 2)
```

Each piece is a smaller data object, so `convert_to()` and `dataframe()` still
work on it.

Two other ways to take a result apart are `power["m"]`, which returns one
alias as a frame, and `latest()`, which returns each point's newest reading.
The [client reference](../reference/client-api.md#data) lists the rest of the
data object's methods.
