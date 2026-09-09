# Apps

An app selects streams with a semantic query and keeps derived streams up to
date. It runs over the latest available readings. Late readings and corrections
update its results, including removing results that no longer apply.

## Convert each sensor

Put the class in an importable module, such as `plant_apps.py`:

```python
import acquirium as aq
import polars as pl

class Celsius(aq.App):
    name = "celsius"
    backfill = True
    outputs = {
        "temperature": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
            quantity_kind="http://qudt.org/vocab/quantitykind/Temperature",
        )
    }

    def build_query(self, plant):
        return plant.query().measurement(
            alias="temperature", data_source="raw-temperature"
        )

    def transform(self, inputs, output, context):
        frame = inputs["temperature"].in_unit(
            "http://qudt.org/vocab/unit/DEG_C"
        ).df()
        output["temperature"] = frame.select("time", "value")
```

The default `grouping = "per_match"` calls the app once for each query match.
A match can contain one sensor or a related pair of sensors.
`inputs["temperature"].stream` describes the sensor for this call.
The output gets a stable identity derived from the app, port, and input streams.

Select inputs specifically enough to exclude the app's own derived streams.
Downstream apps can select these results using `measurement(app="celsius")`.

## Understand the three arguments

`inputs` maps query aliases to stream sets. Each set offers:

| Accessor | Result |
|---|---|
| `.df()` | Polars dataframe with `ref_uri`, `time`, and `value` |
| `.df("pandas")` | The same rows as pandas |
| `.collect()` | An Arrow table |
| `.batches()` | Chunks of the already loaded Arrow table |
| `.stream` | The one stream for this alias; raises if there are several |
| `.streams` | All stream descriptors |
| `.changes` | Live rows updated within this revision range |
| `.in_unit(uri)` | A stream set with converted numeric values and units |

`output` collects declared output tables. Assign exactly `time` and `value`
columns, using Arrow, Polars, or pandas. Times must be unique, non-null,
timezone-aware timestamps. Values must be non-null and match the declared kind.

`context` describes the query match and the calculation:

| Field | Meaning |
|---|---|
| `.row` | The individual match; unavailable for a multi-match aggregate |
| `.result` | All distinct query matches as a Polars dataframe |
| `.changed_window` | The timestamps that caused this invocation |
| `.output_window` | The interval this invocation replaces |
| `.read_window` | The input interval, including calculation context |

An assigned output replaces its output window. An assigned empty table removes
old results in that interval. A port left unassigned is unchanged.
The runtime clips returned rows to the output window, so extra context cannot
overwrite historical results.

## Average into complete minutes

Declare the bucket size once. `aq.align` uses that declaration:

```python
class MinuteTemperature(Celsius):
    name = "minute-temperature"
    every = "1m"

    def transform(self, inputs, output, context):
        converted = {
            "temperature": inputs["temperature"].in_unit(
                "http://qudt.org/vocab/unit/DEG_C"
            )
        }
        output["temperature"] = aq.align(converted).rename(
            {"temperature": "value"}
        )
```

Each affected minute is read completely, even if only one new reading arrived.
A late reading revises the minute's result. There is no wait for a minute to
become final or for every sensor to report.

## Calculate a rolling result

Declare how much preceding input each output depends on:

```python
class RollingTemperature(Celsius):
    name = "rolling-temperature"
    lookback = "10m"

    def transform(self, inputs, output, context):
        frame = inputs["temperature"].in_unit(
            "http://qudt.org/vocab/unit/DEG_C"
        ).df().sort("time")
        output["temperature"] = frame.select(
            "time",
            pl.col("value").rolling_mean_by("time", window_size="10m"),
        )
```

A correction at noon can affect outputs through 12:10. Those outputs also need
the readings preceding noon. The runtime computes the affected output interval
and reads additional context, then publishes only that output interval.

Use `lookahead` for calculations depending on later inputs. Use
`lookback = "all"` only when the calculation needs the complete retained
history on every invocation; that history must fit in memory.

## Combine several sensors

Grouping belongs to the app. Output naming is a separate decision:

```python
class PlantAverageFlow(aq.App):
    name = "plant-average-flow"
    grouping = "all_matches"
    every = "1m"
    backfill = True
    outputs = {
        "average": aq.output.named("plant-average-flow", value_kind="numeric")
    }

    def build_query(self, plant):
        return plant.query().measurement(
            alias="flow", data_source="raw-flow"
        )

    def transform(self, inputs, output, context):
        frame = aq.align(inputs)
        output["average"] = frame.select(
            "time", pl.mean_horizontal(pl.exclude("time")).alias("value")
        ).drop_nulls()
```

This produces a minute average from the sensors that have readings in that
minute. Later readings revise it. Normalize mixed units before combining them.
A named output has one owner, so it cannot be shared by multiple per-match calls.

## Emit alarms that follow corrections

```python
class HighTemperature(Celsius):
    name = "high-temperature"
    outputs = {"alarm": aq.output.stream(value_kind="text")}

    def transform(self, inputs, output, context):
        frame = inputs["temperature"].in_unit(
            "http://qudt.org/vocab/unit/DEG_C"
        ).df()
        output["alarm"] = frame.filter(pl.col("value") > 40).select(
            "time", pl.lit("temperature above 40 C").alias("value")
        )
```

If a reading is corrected below 40 C, its alarm disappears. Downstream apps
observe the removal. This stream describes alarms justified by the current
readings; it is not an immutable record of notifications previously sent.

## Check, deploy, and repair

A check executes the app against retained data without publishing results:

```bash
acquirium app check plant_apps:Celsius --local
```

Use `--local` to run in your terminal with breakpoints and tracebacks. Without
it, the server executes the check. Checks load retained input history and can
consume more memory than normal partitioned execution.

Deploy an imported class with `client.deploy_app(Celsius)`, where `client` is
your configured Acquirium instance. The server must be able to import the same
module; deployment sends its entrypoint, parameters, and source digest.
Use `parameters={...}` for constructor arguments.

Code updates preserve stream identities and consumed progress. To apply changed
code to retained history, use an explicit output interval:

```python
from datetime import datetime, timezone

client.reprocess_app(
    "celsius",
    start=datetime(2026, 1, 1, tzinfo=timezone.utc),
    end=datetime(2026, 1, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
)
```

Reprocessing survives a restart and does not reset incremental progress.
Bucketed apps expand the requested interval to complete buckets.
A second request for a binding already processing a durable interval is rejected;
wait for that work to finish.

`client.remove_app("celsius")` stops the app and forgets its progress and pending
work. Existing derived history remains stored.

## Control execution frequency

- `batch_delay = "2s"`: wait two seconds from the first pending change to collect
  a burst. Subsequent changes do not restart the timer.
- `min_interval = "1m"`: limit invocation frequency.
- `backfill = True`: process retained history on initial activation.

These settings affect when computation happens. `every`, `lookback`, and
`lookahead` determine its time semantics. They are not timers or watermarks.

See the [app reference](reference/apps.md) for the complete contract,
and [operations](materialization-implementation.md) for storage and server settings.
