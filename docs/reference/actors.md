---
title: Actors
---

An actor is a handler class that Acquirium calls on a timer, and when the
streams matched by one of its query inputs change. Each call returns a
`Transition`: the state to save and the readings to publish. For a guided
introduction, see [Soft sensor actors](../explanation/soft-sensor-actors.md).

| Item | Kind | What it is |
|---|---|---|
| [`aq.Actor`](#aqactor) | class | the declaration: handler, inputs, outputs, timers |
| [`[[actors]]`](#actors-configuration) | config | loads a declaration when the server starts |
| [Handler methods](#handler-methods) | hooks | `initial_state`, `on_timer`, `on_graph_change` |
| [`aq.TimerFired`](#aqtimerfired) | message | passed to `on_timer` |
| [`aq.GraphChange`](#aqgraphchange) | message | passed to `on_graph_change` |
| [`InputSnapshot`](#inputsnapshot) | class | reads the actor's inputs for one call |
| [`aq.Transition`](#aqtransition) | class | what a handler returns |

## `aq.Actor`

```python
aq.Actor(name, handler, inputs, outputs, timers)
```

**Parameters**

- `name` (`str`): letters, digits, `_`, `.` and `-` only. Outputs are
  published under the source `actor:<name>`, and the name must be unique
  on the server.
- `handler` (`type`): the handler class, not an instance. It needs
  `initial_state()` and `on_timer(tick)` methods and a constructor that
  takes no arguments. `on_graph_change(change)` is optional.
- `inputs` (`dict[str, Stream | Query]`): names the handler reads from.
  A `Stream` always reads one stream. A `Query` is resolved against the
  graph before each call and must select at least one measurement.
- `outputs` (`dict[str, "numeric" | "text"]`): the streams the actor
  publishes, and the kind of value each one holds.
- `timers` (`dict[str, timedelta]`): at least one timer, each with a
  positive interval in whole microseconds. `pd.Timedelta("1h")` works here.

**Raises**

`ValueError` when any of the rules above is broken.

**Example**

```python
import pandas as pd
import acquirium as aq

alarm = aq.Actor(
    name="turbidity-alarm",
    handler=TurbidityAlarm,
    inputs={"turbidity": aq.Stream.from_source("filter", "turbidity")},
    outputs={"alarm": "numeric"},
    timers={"check": pd.Timedelta("15m")},
)
```

## Actors configuration

```toml
[[actors]]
spec = "turbidity_alarm.py:alarm"
```

`spec` is `file_or_module:name`. Relative file paths resolve against the
directory of `acquirium.toml`. `name` is usually an `aq.Actor` variable.
It can also be a class with a `configure(client, **parameters)`
classmethod, or a function `factory(client, parameters)`, either of which
returns an `aq.Actor`. Every key other than `spec` is passed to them as a
parameter.

At startup the server registers each actor's output streams, then starts
the actor. An entry that fails to load is logged and skipped, and the
other actors still start.

## Handler methods

Acquirium creates one handler instance per actor and calls it from a single
thread, one message at a time. Before each call it sets `self.state` to the
last saved state.

| Method | Called | Returns |
|---|---|---|
| `initial_state()` | once, when the actor has no saved state | the first state, any JSON value |
| `on_timer(tick: TimerFired)` | for each due tick | `Transition` |
| `on_graph_change(change: GraphChange)` | when a query input's matches change | `Transition` |

**Timers.** Ticks fall on multiples of the interval counted from the Unix
epoch. An hourly timer fires on the hour, and a daily timer fires at
midnight UTC. The first tick is the first one after the actor starts. If
the actor falls behind, it runs only the latest due tick for each timer.
Ticks missed while the server was down are not replayed.

**Graph changes.** `on_graph_change` is called only when the actor has at
least one `Query` input. It is called when the streams a query matches, or
the metadata it selects, differ from what the actor last saw. Resolving
the inputs at startup does not call it. When a graph change and a tick are
both due, `on_graph_change` runs first, and the tick uses the new matches.

**Errors.** When a handler raises, or its transition is rejected (for
example, a non-finite number for a numeric output), the error is logged.
The saved state and published outputs stay as they were. A tick whose
handler failed is not retried, and the actor goes on to the next message.

## `aq.TimerFired`

| Attribute | Type | Meaning |
|---|---|---|
| `name` | `str` | the timer's key in `Actor.timers` |
| `scheduled_at` | `datetime` | the UTC time the tick was due, not the time it ran |
| `interval` | `timedelta` | the timer's interval |
| `snapshot` | `InputSnapshot` | the inputs, as published when the call started |

## `aq.GraphChange`

| Attribute | Type | Meaning |
|---|---|---|
| `old_revision`, `new_revision` | `int` | graph revisions before and after the change |
| `inputs` | `Mapping[str, InputChange]` | only the query inputs that changed |
| `snapshot` | `InputSnapshot` | the inputs at `new_revision` |

Each `InputChange` has `added_streams` and `removed_streams` (tuples of
stream URIs), and `old_metadata` and `new_metadata` (Polars DataFrames, in
the same form as `InputSnapshot.metadata()`).

## `InputSnapshot`

One snapshot holds one published epoch of readings and one assignment of
query inputs to streams. Every read from it during a call sees the same
data, even when new readings arrive or the graph changes during the call.

Time ranges include `start` and exclude `end`. Either can be omitted. Both
accept a `datetime` (a `pd.Timestamp` is one), UTC nanoseconds as an `int`,
or an ISO 8601 string. A `datetime` without a time zone is read as UTC.

### `InputSnapshot.read`

```python
snapshot.read(name, *, start=None, end=None)
```

**Returns**

A Polars DataFrame with columns `time` (UTC datetime), `value` (`Float64`,
null for text readings) and `text`. A `Query` input also has a
`stream_uri` column. A stream with no readings yet gives no rows.

**Raises**

`KeyError` when `name` is not a declared input.

### `InputSnapshot.metadata`

```python
snapshot.metadata(name)
```

**Returns**

For a `Query` input, the metadata the query selected, one row per match.
For a `Stream` input, one `stream_uri` column with one row.

### `InputSnapshot.query`

```python
snapshot.query(*names).dataframe(*, start=None, end=None)
```

Reads several inputs into one frame. With no names it reads every declared
input.

**Returns**

A Polars DataFrame with columns `input`, `stream_uri`, `time`, `value` and
`text`.

**Raises**

`KeyError` when a name is not a declared input.

### Other attributes

| Attribute | Type | Meaning |
|---|---|---|
| `epoch` | `int` | the published data epoch being read |
| `graph_revision` | `int \| None` | the graph revision of the query assignments; `None` when every input is a `Stream` |

## `aq.Transition`

```python
aq.Transition(state, writes={})
```

**Parameters**

- `state`: the state to save. It must be JSON: dicts, lists, strings,
  numbers, booleans and `None`, with no NaN. Return `self.state` to keep
  the current state.
- `writes` (`dict[str, list[tuple[timestamp, value]]]`): readings to
  publish, keyed by output name. A timestamp is a `datetime`, an `int` of
  UTC nanoseconds or an ISO string. A numeric output takes a Python `int`
  or `float`. NumPy scalars are rejected, so convert them with `float()`.
  A text output takes a `str`.

The state and the writes are saved together in one request, so a restart
never sees one without the other.

**Example**

```python
return aq.Transition({"fits": self.state["fits"] + 1},
                     {"permeability": [(tick.scheduled_at, 2.0)]})
```
