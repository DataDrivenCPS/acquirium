---
title: Build a turbidity alarm pipeline
---

This pipeline cleans turbidity readings and raises an alarm when they stay
above a limit. Turbidity measures how cloudy water is, in nephelometric
turbidity units (NTU).

```text
Analyser readings → Clean turbidity → Sustained alarm
```

For a single calculation with a few sample readings, start with
[Build your first soft sensor](advanced/first-app.md).

From the repository root, run the example on a temporary server.

```sh
uv run --locked python -m examples.turbidity.run
```

The script loads a small plant model with three filters and a combined outlet.
Each location sends two readings per minute for 25 minutes. Filter 3 has a high
period, a fault code, a missing minute, and a spike. [Plot of the sample readings](../images/turbidity-readings.svg)

## Deploy the cleaner

After registering the plant, the script selects turbidity measurements and
groups them by location. Each location has a maximum distance from the local
median for rejecting spikes. The rolling window steps once per minute.
`ReplacementOutput()` makes the cleaned values the default for each analyser.

```{literalinclude} ../../examples/turbidity/run.py
:language: python
:start-after: Start cleaner deployment
:end-before: End cleaner deployment
:dedent: 4
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

## Deploy the alarm

The alarm uses the same measurement query, which now selects the cleaned
values. Its settings give each location a limit and duration.
`Output(on_group=True)` attaches the result to the location so you can query
for a filter's alarm.

```{literalinclude} ../../examples/turbidity/run.py
:language: python
:start-after: Start alarm deployment
:end-before: End alarm deployment
:dedent: 4
```

## Sustained turbidity threshold

The alarm receives a Polars dataframe (`pl.DataFrame`) of cleaned readings.
`context["output_time"]` is the minute being checked. For Filter 3 the alarm
requires 15 consecutive one-minute values above 0.5 NTU. Acquirium stamps the
returned number at the output time. A `1` means the alarm is on and a `0` means it is off.

```{literalinclude} ../../examples/turbidity/calculations.py
:language: python
:pyobject: alarm
```

## Read the results

Once both apps are deployed, the script inserts the sample readings.
Filter 3 stays above the limit from 00:02 through 00:20. The cleaner fills the
gap at 00:10, so the alarm is on for five minutes, from 00:16 through 00:20.
To inspect the original analyser samples alongside the cleaned values, see
[reading replacement outputs](../reference/apps/outputs.md#replacement-outputs).

## Late readings and alarm recalculation

The script inserts two late readings for the gap at 00:10.

```{literalinclude} ../../examples/turbidity/run.py
:language: python
:start-after: Start late insert
:end-before: End late insert
:dedent: 4
```

The clean value changes from 0.80 to 0.20 NTU, splitting the high period into
two runs that are too short to trigger the alarm. An **epoch** identifies a
published version of the data. Reading the alarm with `epoch=first_epoch`
shows its state before the late readings.

The demo prints:

```text
Alarm minutes before the late readings: 5
Alarm minutes after the late readings: 0
Alarm minutes in the original snapshot: 5
```

For an alarm that checks on a wall-clock schedule, see the [standalone timer
actor](../explanation/soft-sensor-actors.md#standalone-turbidity-alarm). To
send notifications from stored alarm changes, see the [change feed guide](../how-to/consume-streaming-changes.md).
To choose windows, outputs, and settings for your own pipeline, see
[Write a soft sensor](../how-to/write-soft-sensors.md).
