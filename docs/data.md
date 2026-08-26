# Working with data

This is a guide to querying data (timeseries) with Acquirium.
Finding the points in the first place is covered in the
[querying guide](tutorials/querying.md).

Every example here runs on the public WaterTAP seawater-ro model.
<!-- FT1 placeholder: link the seawater-ro run guide here once it exists.
     Until then: deployments/WATERTAP/readme.md in the repo. -->

## Lazy by default

A query can match a number of measurement points, and each of those can hold months of readings.
To improve the performance of querying, acquirium does not fetch anything until `.dataframe()` is called.
For further operations on the data query, acquirium lets you create a data object with `.data()`.
This object is a lazy reference to the underlying data.


```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)

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
<!-- pending live re-capture on seawater-ro: derived from the earlier capture
     with the compact layout applied -->
```text
shape: (4, 5)
┌───────────────────────────────────┬───────────────────────────────┬─────────────────────┬───────────────┬────────────┐
│ data_alias                        ┆ point_id                      ┆ time                ┆ value_numeric ┆ value_text │
╞═══════════════════════════════════╪═══════════════════════════════╪═════════════════════╪═══════════════╪════════════╡
│ m__wbs:RO-out-retentate-pressure  ┆ wbs:RO-out-retentate-pressure ┆ 2026-08-05 16:28:…  ┆ 6.7442e6      ┆ null       │
│ m__wbs:RO-out-pressure            ┆ wbs:RO-out-pressure           ┆ 2026-08-05 16:28:…  ┆ 101325.0      ┆ null       │
│ m__wbs:RO-in-pressure             ┆ wbs:RO-in-pressure            ┆ 2026-08-05 16:28:…  ┆ 7e6           ┆ null       │
│ m__wbs:RO-out-retentate-pressure  ┆ wbs:RO-out-retentate-pressure ┆ 2026-08-05 16:28:…  ┆ 6.7626e6      ┆ null       │
└───────────────────────────────────┴───────────────────────────────┴─────────────────────┴───────────────┴────────────┘
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

## Numbers and text

A stream is declared numeric or text when it is registered, and readings are
stored in the matching column: numbers in `numeric_value`, everything else in
`text_value`.
That declaration is called `value_kind`, and it is set by whoever writes the
data, not by the reader.
A reading that cannot be parsed as a number falls back to the text column
even on a numeric stream, so a bad row does not affect the type of the
others.

When reading, `value_mode` decides which of those columns you get.

| value_mode | you get |
|---|---|
| `"default"` | whatever the stream was registered as (numeric if unset and any numbers exist) |
| `"numeric"` | numeric readings only, text rows filtered out |
| `"text"` | text readings only, numeric rows filtered out |
| `"coalesce"` | both, as strings, text winning where a row has both |

**TODO:** I'm not sure how reliable this is, I need to check.


```python
d = q.data(value_mode="numeric")
```

### cast_value

`cast_value` is the last step, applied client side after the values arrive.
`"float"` casts the value column to `Float64` and `"int"` casts to `Int64`.
Anything else, including `None`, leaves the column exactly as the server sent
it, which for a numeric stream is already `Float64`.

Note that the defaults differ: `Query.data()` casts to `"float"`, while
`Query.dataframe()` passes `"str"`, which is one of the values that does
nothing.


## Units

`units()` reports the unit per alias.

```python
d = (acq.query().entity(uri="wbs:RO", alias="ro")
     .measurement(alias="flow", quantity_kind="mass flow rate").data())

d.units()
```
```text
{'flow': 'http://qudt.org/vocab/unit/KiloGM-PER-SEC'}
```
**TODO**: I need a better visual for `.units()` and potentially, show all the units for each point

`convert_to()` returns a new data object by converting the values into a target unit.

**TODO**: this method and its params should be renamed (something like: `.convert()`)

```python
kg_per_min = d.convert_to("kg/min")
kg_per_min.units()
```
```text
{'flow': 'http://qudt.org/vocab/unit/KiloGM-PER-MIN'}
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

### Automatic conversion

A point can carry one unit in the graph while its stream stores another, for
example a driver ingesting a stream in psi under a point annotated in pascals.
Registration records the stream's unit on the reference node as the storage
unit (see [Writing data](#writing-data)).
When the two disagree, the values are converted for you as they are fetched,
to the point's unit, which is the one `units()` reports.
If the two are not convertible, the readings come through untouched and a
warning is logged rather than an exception raised.
A mismatch should not break a query that did not ask for a conversion; the
warning indicates that the model needs a fix.

**TODO:** We need to test and even demonstrate this feature

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

`iter(alias)` walks those points one at a time.
This is useful when each point is its own series.

```python
for point_uri, df in d.iter("m"):
    print(point_uri, df.shape)
```
```text
urn:swro/P1-mechanical-power (5892, 2)
urn:swro/P2-mechanical-power (5892, 2)
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


## Writing data

Most data enters acquirium through drivers, covered in the drivers guide.
The client can also write directly.
This is useful for backfills, one-off imports and tests.

A stream is identified by a `source_id` (who writes it) and a `ref_name`
(which series).
Register both before the first row:

```python
acq.register_datasource("lab-import")

acq.register_streams([{
    "source_id": "lab-import",
    "ref_name": "effluent-tds",
    "value_kind": "numeric",
    "point_uri": "urn:swro/effluent-tds",
    "label": "Effluent TDS (lab)",
    "unit": "mg/L",
    "quantity_kind": "mass concentration",
}])
```

`point_uri` ties the stream to a point in the semantic model.
This link makes the rows reachable by the queries in this guide.
A stream registered without it gets a placeholder point, `<ref_uri>__point`,
labelled `source_id__ref_name` unless you pass `label`.
`measurement()` on an empty query finds it, but no equipment refers to it, so
topology queries do not reach it.
`unit`, `quantity_kind`, `medium` and `substance` accept free text or URIs,
like everywhere else.

When `point_uri` names a point the graph already has, the metadata you pass
is checked against it.
A field the point lacks is added, a field that conflicts raises `ValueError`
before anything is inserted, and a unit that differs from the point's is
accepted only when the two are convertible.
In that case it is recorded on the reference node as the storage unit, and
reads convert to the point's unit automatically; see
[Automatic conversion](#automatic-conversion).

Registration is required before the first insert.
Inserting to an unregistered stream raises an error:

```text
HTTPError: 400 Client Error: Bad Request for url: http://localhost:8000/insert_timeseries;
response body: stream urn:acquirium#a98759dd-... is not registered
```

Then insert rows as `(timestamp, value)` pairs:

```python
from datetime import datetime, timezone

rows = [(datetime(2026, 8, 8, 10, 0, tzinfo=timezone.utc), 412.0),
        (datetime(2026, 8, 8, 10, 5, tzinfo=timezone.utc), 415.3)]

acq.insert_timeseries("lab-import", "effluent-tds", rows)
```

For several streams at once, `insert_timeseries_batch(source_id, {ref_name:
rows, ...})` chunks the upload automatically.
For high volume, `insert_timeseries_arrow(source_id, table)` takes a
`(ts, ref_name, value)` arrow table; this is the path the drivers use.

Writes are idempotent on the (stream, timestamp) pair.
Re-inserting the same timestamps overwrites those rows, so re-running an
import is safe and will not duplicate anything.
`replace=True` on `insert_timeseries` clears the stream first.

### The logbook

Separate from the timeseries, acquirium keeps a plant logbook for human notes.

```python
acq.insert_log("backwash started early, foam in tank 2")
acq.read_logs()
```

`read_logs()` returns the notes as a frame with `message`, `log_time` and the
observation period a note covers.
Both calls filter by `log_time_start=`/`log_time_end=` and
`observation_start=`/`observation_end=`.
`delete_logs()` clears the logbook.
