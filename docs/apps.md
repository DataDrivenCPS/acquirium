# Apps

An app uses a semantic query to select input streams, calculates new values
from their readings, and publishes the results as derived streams. Acquirium
keeps those results up to date as readings arrive or are corrected. This guide
starts with a unit conversion, then extends it to averages and alarms.

## Convert each sensor

Put the class in an importable module, such as `plant_apps.py`:

```python
import acquirium as aq
import polars as pl

class Celsius(aq.App):
    name = "celsius"
    grouping = "per_match"
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

Here, `grouping = "per_match"` gives each matched sensor a separate call to
`transform`. The call receives that sensor's readings through
`inputs["temperature"]`; its `.stream` attribute provides the sensor's metadata.
`output.stream` gives each result a stable identity based on the app, output
port, and input stream.

Every app must explicitly declare `grouping` as either `"per_match"` or
`"all_matches"`. A missing or invalid value is rejected on instantiation.
A query match can also contain related sensors, such as flow and pressure
measurements on the same pump; per-match grouping keeps those inputs together.

The query selects only `raw-temperature` inputs so that the app does not read
its own output. A downstream app can select the converted streams with
`measurement(app="celsius")`.

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

Assigning a table to an output port replaces the stored results within
`context.output_window`. Assign an empty table when previous results in that
interval should be removed, or leave the port unassigned to keep them unchanged.
The runtime discards returned rows outside the output window, allowing the
transform to read extra input context without overwriting adjacent results.

## Average into complete minutes

To calculate a value for each minute, set `every = "1m"`. The runtime uses this
setting to read complete minute buckets, and `aq.align` uses it to resample the
readings:

```python
class MinuteTemperature(Celsius):
    name = "minute-temperature"
    grouping = "per_match"
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

If a new reading falls within a minute that already has a result, the runtime
reads all available readings in that minute and calculates it again. The result
reflects the data currently stored; later arrivals can revise it. The runtime
does not wait for every sensor to report or for the minute to become final.

## Calculate a rolling result

Declare how much preceding input each output depends on:

```python
class RollingTemperature(Celsius):
    name = "rolling-temperature"
    grouping = "per_match"
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

A plant-wide average needs readings from several sensors in the same call.
Set `grouping = "all_matches"` to receive them together, and use `output.named`
to give the aggregate a name that remains stable as the sensor set changes:

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
    grouping = "per_match"
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

## Advanced execution controls

- `batch_delay = "2s"`: wait two seconds from the first pending change before
  running. This lets a rapidly updating stream collect several readings into
  one invocation and can reduce the overhead of expensive computations.
  Subsequent changes do not restart the timer.
- `min_interval = "1m"`: wait at least one minute after a successful invocation
  before running the binding again, even when additional changes arrive. Use
  this to cap the successful execution rate of an expensive computation.
- `backfill = True`: process retained history on initial activation.

Most apps can leave `batch_delay` and `min_interval` at their defaults. These
advanced settings control how often the runtime performs a calculation, while
`every`, `lookback`, and `lookahead` describe which readings the calculation
needs. A ten-minute rolling average, for example, may be cheap enough to update
on every arrival or expensive enough to run less often. Its lookback alone
does not determine an appropriate delay.

Both controls measure elapsed wall-clock time, and their timing state resets
on server restart. They do not delay failure retries; a failed transform can
retry at the materialization polling cadence.

See the [app reference](reference/apps.md) for the complete contract,
and [operations](materialization-implementation.md) for storage and server settings.
