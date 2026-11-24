---
title: Soft sensor inputs
---

We have two tanks, each with two temperature probes. We want to calculate the
mean and peak temperature for each tank.

```text
selected streams     group by tank          function             output streams

probe A1 ─┐
          ├────────► tank A ──────────► summarize(frame, context)
probe A2 ─┘                                                      │
                                                          ┌─────┴─────┐
                                                          ▼           ▼
                                                       A mean       A peak

probe B1 ─┐
          ├────────► tank B ──────────► summarize(frame, context)
probe B2 ─┘                                                      │
                                                          ┌─────┴─────┐
                                                          ▼           ▼
                                                       B mean       B peak
```

First we define this calculation. Then we insert a reading and trace its
effect on the results.

## Input stream selection

Each probe writes readings in degrees Celsius to its own stream.
A reading's sample timestamp is the time that reading represents.

```text
                  tanks and temperature probes

       ┌──────────────────┐           ┌──────────────────┐
       │      Tank A      │           │      Tank B      │
       │                  │           │                  │
       │  probe A1   A2   │           │  probe B1   B2   │
       └─────┬────────┬───┘           └─────┬────────┬───┘
             │        │                     │        │
             ▼        ▼                     ▼        ▼
          stream   stream                stream   stream
            A1       A2                    B1       B2
```

This query finds the tanks and their raw temperature streams:

```python
selection_query = """
PREFIX watr: <https://watermetadata.org/ontology/watr#>
PREFIX s223: <http://data.ashrae.org/standard223#>
PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?stream ?tank WHERE {
    ?tank a watr:Tank ; s223:hasProperty ?point .
    ?point rdfs:label "Raw temperature" ; ref:hasExternalReference ?stream .
}
"""
```

The query returns these stream/tank pairs, with URIs shortened for display:

| stream | tank |
| --- | --- |
| A1 | A |
| A2 | A |
| B1 | B |
| B2 | B |

A group is a set of streams processed together. A time window selects readings
by their sample timestamps.

Each row pairs a probe's stream with its tank. With `group="tank"`, Acquirium
groups A1 and A2 under tank A. It groups B1 and B2 under tank B.

### Python function

`frame` is a Polars dataframe containing the readings for one group and time
window. `context` is a dictionary describing that group and window, including
the timestamp for the result. `emit(context, value)` returns a dataframe
containing one result at that timestamp.

The function computes the mean and peak of `frame["value"]`.
Save it as `temperature_functions.py`:

```python
from acquirium.functions import emit


def summarize(frame, context):
    # Ignore missing values.
    values = frame["value"].drop_nulls()
    return {
        "mean": emit(context, values.mean()),
        "peak": emit(context, values.max()),
    }
```

### App declaration

Register the function as an app. Use a connected `client` with the probes and
tank metadata registered; the [worked examples](../how-to/soft-sensor-examples.md)
provide the setup steps.

```python
import acquirium as aq

MINUTE = 60 * 1_000_000_000
summary_function = aq.PythonFunction.from_file("temperature_functions.py", "summarize")

by_tank = aq.App(
    name="tank-temperature-summary",
    inputs={"temperature": aq.Selection(selection_query, group="tank")},
    # Declare the two outputs that summarize() returns.
    outputs={"mean": aq.Output(), "peak": aq.Output()},
    function=summary_function,
    window=aq.FixedBucket(MINUTE),  # One non-overlapping minute per result.
)
client.deploy_app(by_tank)
```

The app declares its outputs in `outputs`. Here it declares `mean` and `peak`;
these names are its output ports. The function must return a dataframe for
each declared name. Acquirium creates
a `mean` stream and a `peak` stream for each tank.

`group="tank"` puts A1 and A2 together, and B1 and B2 together.

`FixedBucket(MINUTE)` divides each group's data into non-overlapping minutes:

```text
                         10:00                  10:01                  10:02
tank A (A1 and A2)        [ one bucket            )[ next bucket         )
tank B (B1 and B2)        [ a separate bucket     )[ next bucket         )
```

Each bucket includes its start and excludes its end. A reading exactly at
10:01 belongs to the second bucket. Acquirium stamps each result at its
bucket's start.

On deployment, Acquirium calculates the results for stored readings. After
each write, it recalculates the affected buckets.

## Reading insertion and recalculation

Suppose these readings have already been stored and processed. All times here
are January 1, 2026, in UTC. A dash means no reading is stored at that time.

| Probe | Tank | At 10:00:10 | At 10:00:40 | At 10:01:10 |
| --- | --- | ---: | ---: | ---: |
| A1 | A | 20 | 22 | 28 |
| A2 | A | 24 | — | — |
| B1 | B | 30 | 32 | — |
| B2 | B | 34 | 36 | — |

Tank A's 10:00 bucket currently contains `[20, 22, 24]`, so its mean is 22
and its peak is 24. The A1 reading at 10:01:10 is in a different bucket.

At 10:05, a delayed reading arrives from A2: **26 °C, sampled at 10:00:40**.
The probes are registered under source `"input-demo"` with local names
`"a1"`, `"a2"`, `"b1"`, and `"b2"`. Insert the reading:

```python
# Supply the time of the sample, even though it reaches the server later.
sample_time = aq.to_timestamp("2026-01-01T10:00:40 UTC")
client.insert_timeseries("input-demo", "a2", [(sample_time, 26.0)])
```

`aq.to_timestamp` returns a timezone-aware UTC datetime. You can also write
`"2026-01-01T10:00:40Z"` or `"2026-01-01T03:00:40-07:00"`; both represent
the same instant. See [timestamp conversion](../reference/client/_index.md#timestamp-conversion)
for accepted formats and timezone rules.

### Group and window selection

```text
incoming: A2, 10:00:40, 26 °C
    │
    ├─ stream A2 belongs to tank A
    │
    └─ sample time 10:00:40 falls in [10:00:00, 10:01:00)
                                        │
                                        ▼
                           recalculate tank A's 10:00 bucket
```

The sample timestamp, 10:00:40, selects the 10:00 bucket.

### Input dataframe

To recalculate tank A's 10:00 result, Acquirium selects readings that satisfy
both conditions:

- Their stream belongs to tank A: A1 or A2.
- Their sample timestamp is at or after 10:00:00 and before 10:01:00.

Three stored readings and the new reading match. The mean and peak describe
the whole bucket, so Acquirium passes all four readings to the function:

```text
already stored                         newly arrived
A1  10:00:10  20                        A2  10:00:40  26
A1  10:00:40  22                              │
A2  10:00:10  24                              │
       └──────────────────┬───────────────────┘
                          ▼
                  one four-row frame
                          │
                          ▼
                 summarize(frame, context)

not included: tank B's readings, or A1's reading at 10:01:10
```

The input frame has six columns. Four are shown below. `time` contains
each reading's sample timestamp as a UTC datetime with microsecond precision.
Stream URIs are abbreviated. Sort by time if the calculation requires ordered readings.

| input | stream | time | value |
| --- | --- | --- | ---: |
| temperature | A1 | 2026-01-01 10:00:10 UTC | 20.0 |
| temperature | A1 | 2026-01-01 10:00:40 UTC | 22.0 |
| temperature | A2 | 2026-01-01 10:00:10 UTC | 24.0 |
| temperature | A2 | 2026-01-01 10:00:40 UTC | 26.0 |

The last row is the new reading; the other three were already stored.

`stream` identifies the probe's stream. `input` contains the input's name,
`"temperature"`, the key it has in the app's `inputs`. Filter these columns to
select particular inputs.

The remaining columns are `text` and `context`.
Here, `text` is null. Each row's
`context` contains JSON metadata from the selection, including its tank.

### Function context

Acquirium passes `context` as the function's second argument. It tells the
function which window it is processing and provides the group's parameters.
The function can use these details to decide how to process the readings.

This call is for **tank A**, whose group ID is `urn:input-demo:tank-a`.
Its window is 10:00:00 through 10:01:00. The timestamp values in `context`
are timezone-aware Python datetimes. Written with `aq.to_timestamp`, they are:

```python
{
    "output_time": aq.to_timestamp("2026-01-01T10:00:00 UTC"),
    "window_start": aq.to_timestamp("2026-01-01T10:00:00 UTC"),
    "window_end": aq.to_timestamp("2026-01-01T10:01:00 UTC"),
    "parameters": {
        "group": ("urn:input-demo:tank-a",),  # The group key: tank A.
    },
}
```

Acquirium adds the group key to `parameters` automatically. It is always a
tuple. Here it contains the tank URI from the selection's `tank` column:

```python
context["parameters"]["group"]  # ("urn:input-demo:tank-a",)
```

The trailing comma makes this a tuple with one item. To read the tank URI:

```python
tank, = context["parameters"]["group"]
```

Any settings configured for this group also appear in `parameters`.

The one-minute bucket includes 10:00:00 and excludes 10:01:00. A sample exactly
at 10:01:00 belongs to the next bucket.

### Output replacement

Acquirium calls `summarize(frame, context)`. The function calculates a mean
of 23 and a peak of 26 from `[20, 22, 24, 26]`.

`emit` creates one output row for each result. Its timestamp comes from
`context["output_time"]`, which is 10:00:00 for this bucket.

```text
inputs for tank A                  results for tank A

10:00:10   20, 24  ─┐              mean stream: 10:00:00 → 23
                   ├─ summarize ─►
10:00:40   22, 26  ─┘              peak stream: 10:00:00 → 26
```

Acquirium replaces the old results at the **same output timestamp**:

| Group | Output timestamp | Before the write: mean / peak | After the write: mean / peak |
| --- | --- | --- | --- |
| Tank A | 10:00:00 | 22 / 24 | **23 / 26** |
| Tank A | 10:01:00 | 28 / 28 | 28 / 28, unchanged |
| Tank B | 10:00:00 | 33 / 36 | 33 / 36, unchanged |

The incoming reading changed tank A's 10:00 bucket. Acquirium leaves tank B
and tank A's next bucket unchanged. If another app reads the changed mean or
peak, Acquirium recalculates its affected results next.

A correction follows the same path. If we write a different value for A2 at
10:00:40, Acquirium replaces that sample. It rebuilds the four-row frame and
calls the function with the corrected value.

## Timestamp fields

A sample timestamp is the time represented by an input reading. An output
timestamp is the time represented by a result. Function dataframe timestamps
use Polars `Datetime("us", "UTC")`. Context timestamps are timezone-aware
Python datetimes. Both have microsecond precision.

| Field | Meaning |
| --- | --- |
| Input dataframe `time` | Sample timestamp supplied when the reading was inserted. |
| `context["output_time"]` | Output timestamp used by `emit(context, value)`. |
| `context["window_start"]` | Start of the time window used to select input readings. |
| `context["window_end"]` | Exclusive end of that time window. |
| Query dataframe `time` | Stored timestamp of each reading: the sample timestamp for raw data or the output timestamp for derived data. |

Arrival time is when Acquirium receives a reading. Execution time is when it
runs the function. Acquirium selects windows using the sample timestamp.

## Window types

The window type determines which readings enter a call and the timestamp of
its results. For a rolling window, `lookback_ns` sets how far back to read
from the output timestamp. `step_ns` sets the interval between output
timestamps. These examples use tank A's four readings and a one-minute step.

| Window | Readings in a call | Mean output |
| --- | --- | --- |
| `aq.Pointwise()` | At 10:00:10: 20 and 24. At 10:00:40: 22 and 26. | 22 at 10:00:10; 24 at 10:00:40. |
| `aq.FixedBucket(MINUTE)` | All four readings in the 10:00 minute. | 23 at 10:00:00, the bucket start. |
| `aq.Rolling(lookback_ns=MINUTE, step_ns=MINUTE)` | For the output at 10:01: all four readings from 10:00 through 10:01. | 23 at 10:01:00. |

```text
same tank A readings:       10:00:10 ●             10:00:40 ●

Pointwise:                          [call]                  [call]
Fixed bucket:          [──────────── one call ────────────────────)
                       10:00                                 10:01
                       ▲ result
Rolling at 10:01:       [──────────── one call ────────────────────]
                                                             ▲ result
```

A pointwise call can contain several rows if several probes have readings at
that exact timestamp. Readings at different timestamps are not automatically
aligned or filled forward.

A reading can affect several rolling windows. With a two-minute lookback and
a one-minute step, Acquirium recalculates both of these when the A2 reading at
10:00:40 arrives:

```text
output at 10:01:00: pull tank A's readings from 09:59:00 through 10:01:00
output at 10:02:00: pull tank A's readings from 10:00:00 through 10:02:00
                                                ▲
                       both ranges include the sample at 10:00:40
```

Acquirium builds an input frame for each window. The second frame also
includes A1's reading at 10:01:10, because it falls within that window.

A rolling window includes both endpoints. Adding `lookahead_ns=MINUTE` lets
the call also read samples up to one minute **after** its output timestamp.
This is useful for interpolation; a later sample can fill an earlier gap.
As later data arrives, that earlier output may change.

The step sets the interval between output timestamps. Acquirium calculates
from available readings and updates results when the inputs change. It can
publish a bucket's result before the bucket ends.

## Stream grouping and outputs

The following definitions change the data flow's grouping or output ports.
The numbers show the 10:00 bucket after the A2 reading has arrived.
Here, “one” and “many” count **streams**, not dataframe rows or function calls.

| Pattern | Grouping | Output ports | Result for these four probes |
| --- | --- | --- | --- |
| One-to-one | Each input stream separately | `mean` | Four mean streams. |
| One-to-many | Each input stream separately | `mean`, `peak` | Eight streams: two per probe. |
| Many-to-one | All streams together | `mean` | One mean stream. |
| Many-to-many, grouped | Streams grouped by tank | `mean`, `peak` | Four streams: two per tank. |

### One-to-one: mean per probe

```text
A1 ─► its call ─► A1 mean = 21
A2 ─► its call ─► A2 mean = 25
B1 ─► its call ─► B1 mean = 31
B2 ─► its call ─► B2 mean = 35
```

```python
per_probe = aq.App(
    name="probe-means",
    inputs={"temperature": aq.Selection(selection_query, group="stream")},
    outputs={"mean": aq.Output()},
    function=aq.Native.mean("mean"),
    window=aq.FixedBucket(MINUTE),
)
```

The group ID is the input stream itself. A1's frame has just its two rows,
`[20, 22]`; the other probes' readings go to separate calls. This example uses
a built-in mean, which computes the same result without calling Python.

### One-to-many: mean and peak per probe

```text
A1 ─► summarize([20, 22]) ─┬─► A1 mean = 21
                          └─► A1 peak = 22

Repeat independently for A2, B1, and B2.
```

```python
per_probe_summary = aq.App(
    name="probe-summaries",
    inputs={"temperature": aq.Selection(selection_query, group="stream")},
    outputs={"mean": aq.Output(), "peak": aq.Output()},
    function=summary_function,
    window=aq.FixedBucket(MINUTE),
)
```

The input dataframe is unchanged from one-to-one. A1 still gets one call with
two rows. That call now returns two named results, creating two streams per
probe. Additional ports do not cause additional Python calls.

### Many-to-one: overall mean

```text
A1 ─┐
A2 ─┤
    ├─► one frame with eight readings ─► overall mean = 28
B1 ─┤
B2 ─┘
```

```python
overall = aq.App(
    name="overall-temperature",
    inputs={"temperature": aq.Selection(selection_query, group=None)},
    outputs={"mean": aq.Output()},
    function=aq.Native.mean("mean"),
    window=aq.FixedBucket(MINUTE),
)
```

Here `group=None` on `aq.Selection` puts every selected stream in one
app-wide group. Its frame contains all eight readings, so the overall mean is
28. Every observation has equal weight; a probe reporting more often would
contribute more rows.

### Many-to-many: mean and peak per tank

This is our original `by_tank` definition:

```text
A1 ─┐                              ┌─► A mean = 23
    ├─► tank A: four rows ─► call ──┤
A2 ─┘                              └─► A peak = 26

B1 ─┐                              ┌─► B mean = 33
    ├─► tank B: four rows ─► call ──┤
B2 ─┘                              └─► B peak = 36
```

Four input streams become two groups, each with two outputs. More generally,
each distinct group gets one stream per output port. The group controls which
streams meet; the ports control what each group publishes. A stream selected
into two groups contributes to both groups' calculations.

Grouping by tank and declaring just `mean` would instead produce two streams,
one per tank. The function itself does not need a Polars `group_by("tank")`:
Acquirium has already separated the calls by tank.

## Query API selections

The same grouping choice is available through `Selection.from_query`:

```python
temperatures = client.query().entity("https://watermetadata.org/ontology/watr#Tank", alias="tank").measurement(
    alias="temperature", label="Raw temperature"
)
# Each matched stream gets its own calculation.
separate = aq.Selection.from_query(temperatures, measurement="temperature")
# All matched probes belonging to the same tank enter the same calculation.
together = aq.Selection.from_query(temperatures, measurement="temperature", group="tank")
```

There is one naming detail to watch: omitting `group` from `from_query`
keeps streams separate. Passing `group=None` to the lower-level `Selection`
constructor combines them. The examples above spell out the grouping choice.

## Function execution modes

Everything above uses the ordinary interface: one call for one group's time
window. Two optional modes change how those calls are delivered:

| Mode | What the function receives | When it helps |
| --- | --- | --- |
| Default | One group's readings for one window. | Start here for most calculations. |
| `batch=True` | Several affected windows of one group together. An added `output_time` column assigns each row to a window. | Processing a large backfill with Polars. |
| `sequential=True` | One window at a time in timestamp order, with earlier results in `context["previous"]`. | A running total or filter that uses its previous result. |

Neither mode changes which streams belong to a group. In batch mode, a sample
used by overlapping windows appears once for each window it contributes to.
The extra `output_time` column tells you which result that copy is for; its
`time` still records the sample's own time. For a sequential function,
earlier output timestamps remain distinct from the current output timestamp.

For runnable calculations, continue with the
[soft sensor examples](../how-to/soft-sensor-examples.md). For every frame
column and option, see the [app reference](../reference/apps/_index.md#python-functions).
[Derived stream updates](apps.md) explains what happens when an old
reading, a group membership, or a function changes.
