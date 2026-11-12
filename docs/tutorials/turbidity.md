---
title: Turbidity alarm pipeline
---

A filter's effluent turbidity rises above its limit. One high reading might be
a bubble past the sensor. Fifteen minutes of high readings means the filter
needs attention. So the alarm we want is not a simple threshold: it has to look
at a stretch of time, cope with gaps in the data, and change its mind when
better data arrives.

This tutorial builds that alarm from a chain of small apps, each doing one
thing. It continues from [Soft sensor app](first-app.md). The apps come from the
[bulk-ingest example](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/bulk_ingest/chain.py),
and a short script feeds them twenty-five minutes of readings so that we can
follow every result:

```sh
uv run --locked python -m examples.turbidity.run
```

The script starts a temporary server, loads the data, prints what the alarm
does, and shuts down. It takes seconds, not twenty-five minutes, because every
reading carries the time it was taken.

## Plant and sample readings

The plant has four sampling locations, among them Filter 3 and the combined
effluent. Each has a turbidity point in the
[plant model](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/bulk_ingest/model.ttl),
and the script registers one stream per point. The analysers report twice a
minute. Filter 3 runs at 0.8 NTU from 00:02 to 00:20, above its 0.5 NTU limit,
and its readings for minute 00:10 are missing.

## Minute averages

Two readings a minute, at whatever second the analyser chose, are awkward to
compare. The first app averages them onto a one-minute grid:

```python
snap = aq.App(
    name="bulk-turbidity-snap",
    inputs=[aq.Selection(RAW_TURBIDITY, role="turbidity")],
    outputs={"turbidity": location_turbidity},
    function=aq.PythonFunction.from_file(CALCULATIONS, "snap_to_minute"),
    window=aq.FixedBucket(MINUTE),
)
```

`RAW_TURBIDITY` is a query over the plant model that finds every turbidity
point and the location it belongs to. Each location becomes a separate group,
so the app runs once per location and never mixes Filter 3's readings with
another location's. `FixedBucket(MINUTE)` hands the function one minute of
readings at a time, and the function returns their mean:

```python
def snap_to_minute(frame, context):
    if frame.is_empty():
        return {"turbidity": empty()}
    return {"turbidity": emit(context, frame["value"].mean())}
```

The result carries the bucket's start time: readings at 00:02:00 and 00:02:30
make one value at 00:02.

## Gap interpolation

Filter 3 has no average for 00:10, and a missing minute in the middle of a
high stretch would break the alarm. The next app fills short gaps by drawing a
straight line between the values on either side:

```python
fill = aq.App(
    name="bulk-turbidity-fill",
    inputs=[aq.Selection.outputs_of(snap, "turbidity", role="snapped")],
    outputs={"turbidity": location_turbidity},
    function=aq.PythonFunction.from_file(CALCULATIONS, "fill_gaps"),
    window=aq.Rolling(lookback_ns=MAX_GAP, step_ns=MINUTE, lookahead_ns=MAX_GAP),
)
```

Two things are new. The input is another app's output: `outputs_of(snap,
"turbidity")` refers to the averaging app's port by name, so there is no stream
ID to copy, and each location's results stay in their group. And the window
looks both ways. To fill 00:10, the function needs a value before the gap and
one after it, so `Rolling` gives each minute two hours of context in each
direction. The function interpolates when it finds values on both sides, and
leaves gaps of two hours or more empty.

Here both neighbours are 0.80 NTU, so the filled value at 00:10 is 0.80. Note
what looking ahead implies: an estimate can change when later data arrives.

## Sustained turbidity threshold

Two more apps turn turbidity into an alarm. The first compares each minute with
the location's limit and writes 1 for high or 0 for normal. The second looks
for a long enough run of ones:

```python
SUSTAINED_MINUTES = {
    "urn:bulk/loc-0": 15,
    "urn:bulk/loc-1": 15,
    "urn:bulk/loc-2": 15,   # Filter 3
    "urn:bulk/loc-3": 5,    # combined effluent
}

sustained = aq.App(
    name="bulk-turbidity-sustained",
    inputs=[aq.Selection.outputs_of(high, "alarm", role="high", parameters={
        location: {"sustained_minutes": minutes}
        for location, minutes in SUSTAINED_MINUTES.items()
    })],
    outputs={"alarm": location_alarm},
    function=aq.PythonFunction.from_file(CALCULATIONS, "sustained_high_turbidity"),
    window=aq.Rolling(lookback_ns=15 * MINUTE, step_ns=MINUTE),
)
```

The duration differs by location, so it is a parameter of each group, which
the function reads from `context["parameters"]`. One function serves every
location:

```python
from datetime import timedelta

def sustained_high_turbidity(frame, context):
    t = context["output_time"]
    if frame.filter(pl.col("time") == t).is_empty():
        return {"alarm": empty()}
    minutes = context["parameters"]["sustained_minutes"]
    span = frame.filter(pl.col("time") >= t - timedelta(minutes=minutes))
    every_minute_high = span.height == minutes + 1 and span["value"].min() == 1
    return {"alarm": emit(context, 1 if every_minute_high else 0)}
```

Fifteen minutes on a one-minute grid is sixteen samples, counting both ends.
Filter 3 has been high since 00:02, so its alarm comes on at 00:17 and stays on
until the excursion ends at 00:20.

## Plant-wide alarm count

The last app is the first to combine locations. `combine=True` puts every
location's alarm into a single calculation, and since each alarm is 0 or 1,
their sum is the number of locations in sustained alarm:

```python
plant_alarms = aq.App(
    name="bulk-plant-turbidity-alarms",
    inputs=[aq.Selection.outputs_of(sustained, "alarm", role="sustained", combine=True)],
    outputs={"locations": plant_count},
    function=aq.Native.sum("locations"),
)
```

A sum needs no Python, so `Native.sum` runs inside the engine. Only Filter 3
goes high in this run, so the count is 1 from 00:17 to 00:20.

The script reads all of these results the way you would read any stream, by
asking for the location and the app that produced the output:

```python
filter3 = client.query().entity(uri="urn:bulk/loc-2", alias="filter3")
alarm = filter3.measurement(alias="alarm", app="bulk-turbidity-sustained")
alarm.dataframe()
```

## Late readings and alarm recalculation

Now the readings for 00:10 turn up late, as data from the field often does.
They are 0.15 and 0.25 NTU: Filter 3 briefly dipped below its limit.

The script inserts those two readings and nothing else, and every app
downstream updates on its own. The average for 00:10 is now 0.20, a measured
value instead of an estimate, so the gap fill steps aside. The high-turbidity
app sees a normal minute at 00:10. That splits the excursion into two stretches
of eight and ten minutes, neither of which lasts fifteen, so the sustained
alarm disappears from the whole run and the plant count drops to zero.

That is the right answer, but it raises a question: the alarm *did* fire, and
someone may have acted on it. Acquirium keeps every published version. The
script saved the snapshot number, the epoch, when it first read the alarm, and
reading at that epoch still shows the alarm on for four minutes:

```python
stream = alarm.data().stream_info()["ref_uri"][0]
client.stream(stream).dataframe(epoch=first)
```

So you can always tell what the data says now from what it said when a decision
was made.

The alarm in this tutorial is a stored signal of zeros and ones. To send
notifications from it, [consume the committed changes](../how-to/consume-streaming-changes.md),
where you also decide what to do when an alarm is later revised.
