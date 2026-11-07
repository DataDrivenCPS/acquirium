---
title: Working with data
---

<!-- TODO: intro -->

This is a guide to querying data (timeseries) with Acquirium.

This tutorial uses data from the WaterTAP seawater-RO model, 
so start the server against `deployments/WATERTAP/models/seawater-ro/acquirium.toml`
and connect to acquirium:

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)
```

## Lazy by default

A query can match a number of measurement points, and each of those can hold months of readings.
To improve the performance of querying, acquirium does not fetch anything until `.dataframe()` is called.
For further operations on the data query, acquirium lets you create a data object with `.data()`.
This object is a lazy reference to the underlying data.


```python
from acquirium import Acquirium

acq = Acquirium(server_url="127.0.0.1", server_port=8000)

d = (acq.query().entity("Equipment")
     .measurement(alias="m", quantity_kind="mass flow rate")
     .data())
d
```
```text
DataObject(lazy, ~55980 rows, range=2026-08-05T16:28:39.137381+00:00 to 2026-08-08T05:30:48.864274+00:00, aliases=['m'], entities=['Equipment'])
```

**TODO:** measure the overhead and scalability of this feature

The data object lets you check what the query would retrieve before fetching it.
For instance, the `~55980` figure above is a real count from the server.

Values are fetched (and cached) only when you call `.dataframe()`.

```python
d.dataframe(shape="wide")    # fetches here
d
```
```text
DataObject(5598 rows, aliases=['m'], entities=['Equipment'])
```

### Query.dataframe()

Note that a `dataframe()` method also exists on the query object.
This is a shortcut for `Query.data().dataframe()`, and the two take the same
parameters with the same defaults (`shape`, `start`, `end`, `limit`, `order`,
`include_ref`, `compact`), so `q.dataframe(...)` returns the same frame as
`q.data(...).dataframe(...)`.
The difference is where the window applies: on the query it bounds the fetch,
on a data object it filters the rows already fetched (`limit` keeps that many
rows per stream, in `order` direction).
`include_dependencies`, `cast_value` and `value_mode` act at fetch time, so
they are set on `data()` (or on `Query.dataframe()`) and have no data-object
counterpart.

**TODO:** dataframe funcitons are not consistent with each other. make them consistent, or just remove one

## Shapes

`dataframe()` returns one of two layouts.

`shape="wide"` puts `time` first and one column per point, aligned on the timestamp.
This shape is useful for plotting, correlating or exporting.

```python
d = (acq.query().entity(uri="wbs:RO", alias="ro")
     .measurement(alias="m", quantity_kind="pressure").data())

d.dataframe(shape="wide").tail(3)
```
```text
shape: (3, 4)
┌────────────────────────────────┬───────────────────────┬────────────────────────┬──────────────────────────────────┐
│ time                           ┆ m__wbs:RO-in-pressure ┆ m__wbs:RO-out-pressure ┆ m__wbs:RO-out-retentate-pressure │
╞════════════════════════════════╪═══════════════════════╪════════════════════════╪══════════════════════════════════╡
│ 2026-08-08 05:51:47.691340 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8036e6                         │
│ 2026-08-08 05:52:02.286696 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8062e6                         │
│ 2026-08-08 05:52:16.983763 UTC ┆ 7e6                   ┆ 101325.0               ┆ 6.8105e6                         │
└────────────────────────────────┴───────────────────────┴────────────────────────┴──────────────────────────────────┘
```

`shape="narrow"` gives one row per reading, with the point it came from in its own column.
This is useful when points do not share timestamps.

Wide is the default on both `Query.dataframe()` and `DataObject.dataframe()`.

```python
d.dataframe("narrow").head(4)
```
```text
shape: (4, 5)
┌──────────────────────────────────┬───────────────────────────────┬────────────────────────────────┬───────────────┬────────────┐
│ data_alias                       ┆ point_id                      ┆ time                           ┆ value_numeric ┆ value_text │
╞══════════════════════════════════╪═══════════════════════════════╪════════════════════════════════╪═══════════════╪════════════╡
│ m__wbs:RO-out-pressure           ┆ wbs:RO-out-pressure           ┆ 2026-08-15 06:24:14.709710 UTC ┆ 101325.0      ┆ null       │
│ m__wbs:RO-in-pressure            ┆ wbs:RO-in-pressure            ┆ 2026-08-15 06:24:14.709710 UTC ┆ 7e6           ┆ null       │
│ m__wbs:RO-out-retentate-pressure ┆ wbs:RO-out-retentate-pressure ┆ 2026-08-15 06:24:14.709710 UTC ┆ 6.7932e6      ┆ null       │
│ m__wbs:RO-in-pressure            ┆ wbs:RO-in-pressure            ┆ 2026-08-15 06:24:29.376388 UTC ┆ 7e6           ┆ null       │
└──────────────────────────────────┴───────────────────────────────┴────────────────────────────────┴───────────────┴────────────┘
```

`data_alias` carries the same name the wide column would have, and `point_id`
is the point as a CURIE.

Numeric and text readings live in separate columns, `value_numeric` and
`value_text`.


### Column names in wide shape

A wide column is named after its alias.
When an alias covers several points the name would be ambiguous, so the point
is appended: `m__wbs:RO-in-pressure`.
An alias with a single point keeps its plain name.

The appended part is the point's `rdfs:label` when it has one, and its CURIE
otherwise.
The seawater-ro points carry no labels, which is why the examples here show
CURIEs.
A node you did not alias is named by the label alone, or by the CURIE.
Note that a label shared by several points is not used, since two columns
would collapse into one; those points fall back to their CURIEs.

```python
(acq.query().entity(uri="wbs:P1", alias="p1")
 .measurement(alias="power", quantity_kind="power")
 .data().dataframe(shape="wide").tail(2))
```
```text
shape: (2, 2)
┌────────────────────────────────┬──────────┐
│ time                           ┆ power    │
╞════════════════════════════════╪══════════╡
│ 2026-08-08 05:52:02.286696 UTC ┆ 1.0685e6 │
│ 2026-08-08 05:52:16.983763 UTC ┆ 1.0612e6 │
└────────────────────────────────┴──────────┘
```

Several measurement nodes in one query keep their own aliases, so you can mix
them in a single frame and still tell them apart.
After `time`, columns are sorted alphabetically.

```python
(acq.query().entity(uri="wbs:RO", alias="ro")
 .measurement(alias="press", quantity_kind="pressure")
 .measurement(frm="ro", alias="temp", quantity_kind="temperature")
 .data().dataframe(shape="wide").tail(2))
```
```text
shape: (2, 5)
┌────────────────────────────────┬───────────────────────────┬────────────────────────────┬──────────────────────────────────────┬────────────┐
│ time                           ┆ press__wbs:RO-in-pressure ┆ press__wbs:RO-out-pressure ┆ press__wbs:RO-out-retentate-pressure ┆ temp       │
╞════════════════════════════════╪═══════════════════════════╪════════════════════════════╪══════════════════════════════════════╪════════════╡
│ 2026-08-08 05:52:02.286696 UTC ┆ 7e6                       ┆ 101325.0                   ┆ 6.8062e6                             ┆ 303.966991 │
│ 2026-08-08 05:52:16.983763 UTC ┆ 7e6                       ┆ 101325.0                   ┆ 6.8105e6                             ┆ 304.016483 │
└────────────────────────────────┴───────────────────────────┴────────────────────────────┴──────────────────────────────────────┴────────────┘
```
**TODO:** Find a better way to create the schema from aliases

Both `dataframe()` methods shorten the identifiers by default
(`compact=True`): URIs are CURIEs and the narrow shape identifies a point by a
`point_id` column.
`compact=False` returns the raw layout, with full `point_uri` and `ref_uri`
columns and one `entity__<alias>` column per entity node in narrow shape.
Pass `include_ref=True` to add the reference URI to the compact layout.
This is only needed if you need to distinguish data coming from different sources for the same measurement point.

## Units

`units()` lists the distinct units of each alias's points.
The `unit` column of `metadata()` gives the unit per point.

```python
d = (acq.query().entity(uri="wbs:RO", alias="ro")
     .measurement(alias="flow", quantity_kind="mass flow rate").data())

d.units()
```
```text
{'flow': ['http://qudt.org/vocab/unit/KiloGM-PER-SEC']}
```
`convert_to()` returns a new data object by converting the values into a target unit.

**TODO**: this method and its params should be renamed (something like: `.convert()`)

```python
kg_per_min = d.convert_to("kg/min")
kg_per_min.units()
```
```text
{'flow': ['http://qudt.org/vocab/unit/KiloGM-PER-MIN']}
```

Values change with it: `10.67 kg/s` becomes `640.35 kg/min`.

When the object holds several aliases, pass `alias=` to convert only one:

```python
d2 = (acq.query().entity(uri="wbs:RO", alias="ro")
      .measurement(alias="flow", quantity_kind="mass flow rate")
      .measurement(frm="ro", alias="area", quantity_kind="area").data())

d2.convert_to("g/s", alias="flow")     # area stays in m²
```

### When conversion fails

- A point with no unit in the graph will fail:

```text
ValueError: No unit annotation found for alias 'p'. Provide from_unit explicitly.
```

Pass `from_unit=` when you know what the numbers are:

```python
pressures.convert_to("psi", from_unit="Pa")
```

- An incompatible conversion will fail:

```text
ValueError: convert_to: no convertible unit pair for
'.../KiloGM-PER-SEC' -> 'celsius'
```

## Resampling and aligning

Streams rarely share timestamps. `resample()` and `align()` put the values of
a data object on a time grid; both are lazy and return a new data object.

`resample(every, agg)` reduces each stream to one value per `every`-wide
bucket. A bucket starts at `origin + k * every` (the UNIX epoch by default),
so two streams bucketed the same way share their bucket times. `agg` is one
of `mean`, `min`, `max`, `sum`, `count`, `first`, `last` or `median`.

```python
d = (acq.query().entity(uri="wbs:RO", alias="ro")
     .measurement(alias="flow", quantity_kind="mass flow rate")
     .measurement(frm="ro", alias="pressure", quantity_kind="pressure").data())

hourly = d.resample("1h", agg="mean")
hourly
```
```text
DataObject(lazy, 2 streams, aliases=['flow', 'pressure'], entities=['ro'], resampled every=1h agg=mean)
```

Nothing has been fetched yet. On the first call that needs values the server
buckets and reduces the rows in the database (`time_bucket` plus the
aggregate, on both backends), and only the buckets come back.

```python
hourly.dataframe()
```
```text
┌─────────────────────────┬───────────┬──────────┐
│ time                    ┆ flow      ┆ pressure │
╞═════════════════════════╪═══════════╪══════════╡
│ 2026-08-05 16:00:00 UTC ┆ 10.671334 ┆ null     │
│ 2026-08-05 17:00:00 UTC ┆ 10.670891 ┆ 55.02    │
│ 2026-08-05 18:00:00 UTC ┆ 10.669907 ┆ 55.07    │
└─────────────────────────┴───────────┴──────────┘
```

Only numeric values are bucketed, and a bucket with no reading is left out,
so a stream sparser than `every` keeps its gaps (the `null` above). The
`start`/`end` given to `data()` bound the readings, so a bucket straddling a
bound is reduced from the readings inside it; `limit` keeps that many
buckets per stream. On a data object that already holds its rows, the same
reduction runs in polars with the same result.

`every` is a `timedelta`, a number of seconds, or a string such as `"500ms"`,
`"10s"`, `"5m"`, `"1h"`, `"1d"` or `"1w"`. Months and years are not accepted,
since their length varies.

A bucket is named by its start, as in SQL and pandas: `[t, t + every)` is
labelled `t`. Pass `label="center"` to name `[t - every/2, t + every/2)` by
`t` instead, so that each value estimates the signal at its own time. Pass
`origin=` to anchor the grid somewhere other than the epoch.

`align(every, agg, fill)` does the same reduction and then gives every stream
every bucket from the earliest to the latest bucket of the object. `fill`
says what an empty bucket gets: `None` (default) leaves it null, `"ffill"`
carries the stream's previous value forward, `"interpolate"` interpolates
linearly in time between the stream's neighbouring values, and a number fills
with that constant. The grid and the fill are computed on the reduced rows,
which are few.

```python
grid = d.align("10s", fill="interpolate")
grid.dataframe()
```
```text
┌─────────────────────────┬───────────┬──────────┐
│ time                    ┆ flow      ┆ pressure │
╞═════════════════════════╪═══════════╪══════════╡
│ 2026-08-05 16:28:30 UTC ┆ 10.671334 ┆ 55.01    │
│ 2026-08-05 16:28:40 UTC ┆ 10.671290 ┆ 55.015   │
│ 2026-08-05 16:28:50 UTC ┆ 10.671102 ┆ 55.02    │
└─────────────────────────┴───────────┴──────────┘
```

`grid["flow"]` and `grid["pressure"]` now have the same `time` column, so the
frames can be subtracted or correlated directly. Note that interpolation does
not extrapolate: buckets before a stream's first value or after its last stay
null.

Without `every`, `align()` infers the step from the stream statistics: the
average spacing of the stream with the closest readings, rounded to the
nearest conventional step (1, 2, 5, 10, 15, 30 seconds, minutes, hours, days
or a week). This costs one statistics request and fetches no rows.

```python
d.align().resampling
```
```text
Resampling(every=datetime.timedelta(seconds=10), agg='mean', origin=..., label='left', align=True, fill=None)
```

Be aware that:

- `convert_to()` composes with both in either order; the conversion applies
  to the reduced values. `count` has no unit and is never converted. `sum`
  converts exactly only for scale conversions (no offset).
- A resampled object cannot be resampled again. Call `resample()` or
  `align()` on the object `data()` returned, a second pass would reduce the
  reduced values.
- `latest()` on a resampled object returns the newest bucket.
- `by()` sub-objects keep the bucketing of their parent.

## Taking the result apart

**TODO:** This is quite ugly, think of a better way:

A query that matched several points and several entities comes back as one
object. These four methods split it up.

`d["alias"]` returns one alias as a frame.
When the alias covers more than one point, a `point_uri` column is added to
tell the rows apart.

```python
d = acq.query().entity("pump").measurement(alias="m", quantity_kind="power").data()

d["m"].tail(3)
```
```text
shape: (3, 3)
┌────────────────────────────────┬───────────────┬──────────────────────────────┐
│ time                           ┆ value         ┆ point_uri                    │
╞════════════════════════════════╪═══════════════╪══════════════════════════════╡
│ 2026-08-08 06:42:54.828898 UTC ┆ 109158.500253 ┆ urn:swro/P2-mechanical-power │
│ 2026-08-08 06:43:10.129750 UTC ┆ 1.0610e6      ┆ urn:swro/P1-mechanical-power │
│ 2026-08-08 06:43:10.129750 UTC ┆ 111093.849382 ┆ urn:swro/P2-mechanical-power │
└────────────────────────────────┴───────────────┴──────────────────────────────┘
```

To work on the points one at a time, split that frame on `point_uri`.

```python
for (point_uri,), df in d["m"].partition_by("point_uri", as_dict=True).items():
    print(point_uri, df.shape)
```
```text
urn:swro/P1-mechanical-power (5892, 3)
urn:swro/P2-mechanical-power (5892, 3)
```

`by(entity_alias)` groups by the entity instead of the point, and returns a
smaller data object per entity.
Everything still works on it, including `convert_to()` and `dataframe()`.

```python
for pump, sub in d.by("pump"):
    print(pump, sub.dataframe(shape="wide").shape)
```
```text
urn:swro/P1 (5892, 2)
urn:swro/P2 (5892, 2)
```

This is useful for per-equipment work, like fitting a model per pump.

`latest(alias)` returns the newest reading.

**TODO:** I think latest also can be improved or removed

```python
d.latest("m")
```
```text
shape: (1, 2)
┌────────────────────────────────┬───────────────┐
│ time                           ┆ value         │
╞════════════════════════════════╪═══════════════╡
│ 2026-08-08 06:43:10.129750 UTC ┆ 111093.849382 │
└────────────────────────────────┴───────────────┘
```

Note that `latest()` fetches the whole window and takes the last row.
For a cheap latest value, query with `limit=1, order="desc"` instead.
