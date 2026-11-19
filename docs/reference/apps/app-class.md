---
title: The App class
---

An app is a subclass of `acquirium.App`. The class attributes say when and on what the app runs. Two methods do the work: `build_query` selects the input streams and `transform` computes the output tables.

| Item | Purpose |
|---|---|
| [`App`](#app) | Base class and its settings |
| [`App.build_query`](#appbuild_query) | Select the input streams |
| [`App.transform`](#apptransform) | Compute the outputs |
| [`App.output_window`](#appoutput_window) | Optional: choose which output times a change affects |
| [Durations](#durations) | How `every`, `lookback` and the other time settings are written |
| [Time windows of a call](#time-windows-of-a-call) | Which input rows a call reads and which output rows it replaces |

## `App`

```python
App()
```

Base class of every app. Subclass it, set the class attributes below, and implement [`build_query`](#appbuild_query) and [`transform`](#apptransform). The server creates the instance.

**Class attributes**

| Attribute | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` | the class name | Name of the deployed app. The output streams get the `source_id` `derived:<name>` |
| `grouping` | `str` | required | `"per_match"`: one call for each match of the query. `"all_matches"`: one call that receives every match |
| `outputs` | `dict` | required | Output streams, as a mapping of a port name to [`aq.output.stream(...)`](outputs.md#outputstream) or [`aq.output.named(...)`](outputs.md#outputnamed). At least one |
| `every` | duration | `None` | Size of the time buckets the app works in, for example `"1m"`. A call then always covers whole buckets. Required by [`align`](inputs.md#align) |
| `lookback` | duration or `"all"` | `"0s"` | How far before an output time the app reads its inputs. `"all"` reads everything that is stored |
| `lookahead` | duration | `"0s"` | How far after an output time the app reads its inputs |
| `backfill` | `bool` | `False` | `True`: the first run also processes values stored before the app was deployed. `False`: only values that arrive afterwards |
| `batch_delay` | duration | `"0s"` | Wait this long after a change before running, so that changes arriving close together are handled in one call |
| `min_interval` | duration | `None` | Shortest time between two successful calls for the same inputs |

**Constructor parameters**

An app may define `__init__` with parameters of its own. The `parameters` given at deployment are passed as keyword arguments. They must be JSON values.

```python
class Scale(aq.App):
    grouping = "per_match"
    outputs = {"scaled": aq.output.stream(value_kind="numeric")}

    def __init__(self, factor=2.0):
        self.factor = factor
```

Note that one instance serves every call of the app, and calls may run at the same time. Do not store results of a call on `self`.

Note that the server may run `transform` again for the same data. Keep it free of side effects such as writing files or sending messages.

**Raises**

- `ValueError`: when an instance is created and `grouping` is not `"per_match"` or `"all_matches"`.

### Grouping

| | `"per_match"` | `"all_matches"` |
|---|---|---|
| Calls | One for each distinct match of the query | One for all matches |
| `inputs[alias]` | Holds one stream | Holds every matched stream of that alias |
| `context.row` | The match of this call | Raises, unless the query has exactly one match |
| Output streams | One set for each match | One set |

A [named output](outputs.md#outputnamed) is one stream, so it cannot be used with `"per_match"` when the query has more than one match. Deploying such an app fails.

## `App.build_query`

```python
App.build_query(plant)
```

Returns the query that selects the input streams. Write this method in the subclass. The server calls it when the app is deployed and again whenever the graph changes, so new matching streams are picked up.

**Parameters**

- `plant`: an object with `plant.query()`, which starts a query, and `plant.attr`. They work like `acq.query()` and `acq.attr` of the client. See [Building queries](../query/building.md).

**Returns**

A `Query`. Every alias of the query that has a stream becomes a key of `inputs` in `transform`.

**Raises**

Deployment fails when the method returns something that is not a `Query`.

**Example**

```python
def build_query(self, plant):
    return plant.query().measurement(alias="temp", quantity_kind="temperature")
```

Note that output streams of apps are streams too, so a broad query can match them. To read the output of one app on purpose, select it with `measurement(app="<name>")`.

## `App.transform`

```python
App.transform(inputs, output, context)
```

Computes the output tables for one call. Write this method in the subclass.

**Parameters**

- `inputs` (`dict[str, StreamSet]`): the input data, keyed by query alias. See [`StreamSet`](inputs.md#streamset).
- `output` ([`OutputBuilder`](outputs.md#outputbuilder)): assign one table to each port with `output["port"] = table`.
- `context` ([`InputBatch`](inputs.md#inputbatch)): what this call is about: the matched row and the time windows.

**Returns**

`None`. The results are the tables assigned to `output`.

**Raises**

An exception stops this call. The app's status for these inputs becomes `failed` and the error text is shown by [`inspect_app`](deploying.md#acquiriuminspect_app).

**Example**

```python
def transform(self, inputs, output, context):
    df = inputs["temp"].df()
    output["scaled"] = df.select("time", (df["value"] * self.factor).alias("value"))
```

## `App.output_window`

```python
App.output_window(changed)
```

Optional. Returns the span of output times that a change of the inputs affects. Override it only when `every`, `lookback` and `lookahead` cannot describe the app. When it is not overridden, the span is computed from those settings, see [Time windows of a call](#time-windows-of-a-call).

**Parameters**

- `changed` ([`TimeWindow`](inputs.md#timewindow)): the span of input times that changed.

**Returns**

[`TimeWindow`](inputs.md#timewindow). An override must return one for every call and must return the same result for the same input.

**Raises**

- `TypeError`: raised by the server when an override returns something that is not a `TimeWindow`.

**Example**

An app that writes one value per day at midnight.

```python
from datetime import timedelta
from acquirium.Materialization import TimeWindow

def output_window(self, changed):
    start = changed.start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = changed.end.replace(hour=0, minute=0, second=0, microsecond=0)
    return TimeWindow(start, end + timedelta(days=1) - timedelta(microseconds=1))
```

## Durations

`every`, `lookback`, `lookahead`, `batch_delay` and `min_interval` take a `datetime.timedelta` or a string. A string is a number followed by a unit.

| Unit | Meaning | Example |
|---|---|---|
| `ms` | milliseconds | `"500ms"` |
| `s` | seconds | `"30s"` |
| `m` | minutes | `"1m"` |
| `h` | hours | `"1.5h"` |
| `d` | days | `"7d"` |

A negative duration raises `ValueError`. `every` must be greater than zero. `lookback` also accepts `"all"`.

## Time windows of a call

A call is started by a change: new, corrected or removed input values. Three spans of time describe it. All three include both of their ends.

| Window | In `context` | Computed as |
|---|---|---|
| Changed | `context.changed_window` | From the earliest to the latest input time that changed |
| Output | `context.output_window` | From `changed.start - lookahead` to `changed.end + lookback`. With `every`, widened to whole buckets. Buckets are counted from 1970-01-01 00:00 UTC |
| Read | `context.read_window` | From `output.start - lookback` to `output.end + lookahead` |

The inputs passed to `transform` hold every stored value in the read window. A table assigned to an output replaces the stored values of that output in the output window. Rows outside the output window are dropped.

With `lookback = "all"` the read window is everything that is stored for the inputs and outputs, and the output window is the same span.

**Example**

An app with `every = "1m"` whose inputs changed between 00:00:00 and 00:01:30 was called with these windows:

```text
changed_window  2026-01-01 00:00:00 .. 2026-01-01 00:01:30
output_window   2026-01-01 00:00:00 .. 2026-01-01 00:01:59.999999
read_window     2026-01-01 00:00:00 .. 2026-01-01 00:01:59.999999
```
