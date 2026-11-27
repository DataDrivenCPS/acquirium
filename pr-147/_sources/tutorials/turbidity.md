---
title: Turbidity alarm pipeline
---

Turbidity measures how cloudy water is, in nephelometric turbidity units (NTU).
This example cleans analyser readings and raises an alarm when the cleaned
values stay above a limit. Each arrow shows which stream the next app reads.

```text
Analyser readings → Clean turbidity → Sustained alarm
```

From the repository root, run the example on a temporary server.

```sh
uv run --locked python -m examples.turbidity.run
```

The script loads a small plant model with three filters and a combined outlet.
Each location sends two readings per minute for 25 minutes. Filter 3 has a high
period, a fault code, a missing minute, and a spike. [Plot of the sample readings](../images/turbidity-readings.svg)

## Declare the data flow

The query finds turbidity measurements and their locations. Each app uses
`group="location"` to run one calculation per location with its own parameters.
Both `Rolling` windows produce one result per minute.

The cleaner uses `ReplacementOutput()` to make its result the default reading
of the analyser's measurement. The alarm uses the same measurement query, so
it receives the cleaned values. `Output(on_group=True)` attaches the alarm to
the location so it can be found by a query for that location.

```{literalinclude} ../../examples/turbidity/apps.py
:language: python
```

Deploy both apps after registering the plant model and streams.

```python
clean_app, alarm_app = declarations(client)
client.deploy_app(clean_app)
client.deploy_app(alarm_app)
```

## Cleaning steps

For each output minute, the cleaner sees five minutes of analyser readings on
either side. The `pd.DataFrame` annotation asks Acquirium for a pandas frame.
The `value` column holds numbers, and text fault codes have no numeric value.
The function drops faults and spikes, averages by minute, and interpolates
gaps between valid minutes. The app publishes the result for its output minute.

```{literalinclude} ../../examples/turbidity/calculations.py
:language: python
:pyobject: clean
```

## Sustained turbidity threshold

The alarm receives a Polars dataframe (`pl.DataFrame`) of cleaned readings.
`context["output_time"]` is the minute being checked. For Filter 3 the alarm
requires 15 consecutive one-minute values above 0.5 NTU. `emit` stamps the
result at the output time. A `1` means the alarm is on and a `0` means it is off.

```{literalinclude} ../../examples/turbidity/calculations.py
:language: python
:pyobject: alarm
```

## Read the results

Once the sample readings publish, a normal read of the Filter 3 analyser
returns the clean values. Use `view="raw"` to inspect the original readings
on the same stream.

```python
analyser = client.stream(source="turbidity-analysers", name="filter-3")
clean = analyser.dataframe()
raw = analyser.dataframe(view="raw", value_mode="coalesce")
```

The script finds the alarm by location and app name. `stream_info()` identifies
the output stream so the script can also read it at an earlier version of the
data.

```{literalinclude} ../../examples/turbidity/run.py
:language: python
:start-after: Start alarm read
:end-before: End alarm read
:dedent: 4
```

Filter 3 stays above the limit from 00:02 through 00:20. The cleaner fills the
gap at 00:10, so the alarm is on for five minutes, from 00:16 through 00:20.

## Late readings and alarm recalculation

The missing 00:10 readings arrive late and show a dip below the limit. The
clean value changes from 0.80 to 0.20 NTU, splitting the high period into two
shorter runs. Acquirium recalculates the affected minutes and the alarm clears.

An **epoch** identifies a published version of the data. The script saved
`first_epoch` from the original insert. Reading at that epoch still shows
the five alarm minutes, while the current alarm shows zero.

```{literalinclude} ../../examples/turbidity/run.py
:language: python
:start-after: Start correction
:end-before: End correction
:dedent: 4
```

For an alarm that checks on a wall-clock schedule, see the [standalone timer
actor](../explanation/soft-sensor-actors.md#standalone-turbidity-alarm). To
send notifications from stored alarm changes, see the [change feed guide](../how-to/consume-streaming-changes.md).
