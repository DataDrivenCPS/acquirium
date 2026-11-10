---
title: Follow a turbidity alarm
---

A filter's turbidity rises above its limit. One high reading might be noise;
fifteen minutes of high readings deserves attention. We want an alarm that
follows that rule and changes when better data arrives.

This example builds on [your first app](first-app.md). It uses the turbidity
calculations from the [bulk-ingest example](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/bulk_ingest),
with a small, fixed set of readings so we can follow the results.

## Run the example

From the repository root, with the [development environment](getting-started.md)
installed:

```sh
uv run --locked python -m examples.turbidity.run
```

The script starts a temporary server, runs twenty-five minutes of data through
the apps, checks the results, and shuts down. It does not wait twenty-five
minutes: timestamps describe when readings happened.

## Make one stream per location

There are four sampling locations. Each has a turbidity point in
[model.ttl](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/bulk_ingest/model.ttl). The script loads that model
and registers a stream for each point:

```python
client.register_streams([{
    "source_id": SOURCE,
    "ref_name": "turbidity-2",
    "point_uri": "urn:bulk/loc-2-turbidity",
    "unit": NTU,
    "quantity_kind": TURBIDITY,
    "value_kind": "numeric",
}])
```

The first app selects those streams from the graph and groups them by location.
Each location gets its own calculation. The later apps keep those groups until
we explicitly combine them for a plant total.

## Average a minute at a time

The analyser reports twice per minute. A one-minute average gives us an even
grid to work with:

```python
window=aq.FixedBucket(MINUTE)
```

The function takes the mean of the readings in that bucket. Its result carries
the bucket's start time: readings at 00:02:00 and 00:02:30 produce one value at
00:02. A reading that arrives late revises the same bucket.

## Fill a short gap

Filter 3 has no readings during minute 00:10. Its averages on either side are
0.80 NTU. The next app interpolates between them, producing 0.80 NTU for the
missing minute.

It reads the averaging app's output directly:

```python
inputs=[aq.Selection.outputs_of(snap, "turbidity", role="snapped")]
```

There is no stream ID to copy between apps. The name `"turbidity"` identifies
an output port, and each location's results stay together.

Interpolation needs a reading on both sides of the gap. This app uses
`Rolling` to look backward and forward, and leaves gaps of two hours or more
empty. Estimates can therefore appear or change when later data arrives.

## Wait for a sustained excursion

Filter 3's one-minute values exceed 0.5 NTU from 00:02 through 00:20. A
threshold app first turns each value into `1` for high or `0` for normal.

The sustained-alarm app then looks for fifteen uninterrupted minutes of high
values. On a one-minute grid, that means sixteen high samples, including both
endpoints. Its first `1` is at 00:17. It remains on through 00:20.

The duration is a parameter for each location. Filter 3 uses fifteen minutes;
the combined effluent uses five. The same function handles both. You can read
the declarations in [chain.py](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/bulk_ingest/chain.py) and the
calculation in [transforms.py](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/bulk_ingest/transforms.py).

## Count alarms across the plant

Now we combine the locations:

```python
inputs=[aq.Selection.outputs_of(sustained, "alarm", role="sustained", combine=True)]
function=aq.Native.sum("locations")
```

The inputs are zeros and ones, so their sum is the number of locations in
sustained alarm at each minute. Only Filter 3 is high in this run; the count
is one from 00:17 through 00:20.

## Replace an estimate with readings

Before making a correction, save the current snapshot:

```python
original = client.engine_snapshot()["epoch"]
```

The missing readings finally arrive: 0.15 and 0.25 NTU. Their average is 0.20,
below the filter's limit. The filled value at 00:10 changes from 0.80 to 0.20
because it now comes from observations.

That one minute breaks the high interval. Neither remaining stretch lasts
fifteen minutes, so the sustained alarms disappear and the plant count becomes
zero throughout. We only insert the missing source readings; the dependent
apps update automatically.

Read the alarm at the saved snapshot:

```python
from acquirium.apps import output_reference

reference = output_reference(sustained.name, "urn:bulk/loc-2", "alarm")
client.stream(reference).dataframe(epoch=original)
```

It still shows the original alarm. This lets us distinguish what the data says
now from what it said when we first raised the alarm.

The alarm here is a stored zero-or-one signal. To deliver notifications, use a
[consumer of committed changes](../how-to/consume-streaming-changes.md), where
you can decide how to handle an alarm that is later revised.
