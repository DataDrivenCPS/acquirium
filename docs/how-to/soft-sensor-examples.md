---
title: Soft sensor examples
---

These guides show how to compute derived streams with soft sensor apps. Start
with the calculation guide for the basic interface, or choose a worked example
for the pattern you need.

For diagrams of how streams, groups, and timestamps become a function's input
dataframe, start with [Soft sensor inputs](../explanation/soft-sensor-inputs.md).

- [Soft sensor calculations](write-soft-sensors.md): use built-in calculations,
  write Python functions, and choose windows, state, and per-group settings.
- [Data cleaning with multiple apps](chain-cleaning-apps.md): connect separate apps to
  reject invalid readings, interpolate gaps, and raise an alarm.
- [Data cleaning with multiple outputs](clean-data-with-multiple-outputs.md): keep
  the cleaning steps in one Python function and publish intermediate results.
- [Soft sensor parameters](parameterize-soft-sensors.md):
  give each sensor its own threshold without copying the calculation.

## Timestamp conventions

Use `pd.Timestamp("2026-01-01T10:00:40Z")` to create a sample timestamp.
The `Z` sets its time zone to UTC. Subtract `pd.Timedelta("5m")` to move
the timestamp five minutes earlier. App windows and as-of ages take the
same `pd.Timedelta` durations.

A sample has one timestamp, which you supply when inserting it:

| Where you see it | Name | Meaning |
| --- | --- | --- |
| An inserted `(timestamp, value)` pair | `timestamp` | The time the sample represents. |
| A function's input frame | `time` | That sample timestamp, as a UTC datetime. |
| A query's dataframe | `time` | The stored sample timestamp, displayed as a datetime. |

The function also receives `context["output_time"]`: the timestamp for the
**output** being calculated. A pointwise app emits at its input sample's time.
A rolling app emits at a point on its configured time grid; it may use samples
from several times to calculate that result. `emit(context, value)` stamps
the result with this output timestamp.

Function dataframe columns use `Datetime("us", "UTC")`; context values are
timezone-aware Python datetimes. Both have microsecond precision. A late
reading keeps the timestamp supplied with it.
