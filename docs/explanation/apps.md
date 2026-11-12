---
title: Derived stream updates
---

For an illustrated introduction to grouping, time windows, and the dataframe a
function receives, see [Soft sensor inputs](soft-sensor-inputs.md).

Most derived data in a treatment plant is computed once and then quietly goes
stale. Someone exports a month of turbidity, averages it in a spreadsheet, and
emails the result. A week later the lab corrects two readings, and the
spreadsheet still holds the old average. Nobody knows which reports used it.

Acquirium takes a different approach. An app does not describe a computation
to run once. It describes a relationship that should always hold: *this stream
is the one-minute mean of those readings*. Acquirium's job is to keep that
relationship true as the inputs change, including when they change in the
past.

## Recalculation after input changes

When a reading arrives or is corrected, recomputing everything would be
correct but wasteful. The app's window says exactly which results a reading can
affect. A pointwise calculation reads one timestamp, so a reading changes only
the result at its own timestamp. A one-minute bucket reads one minute, so only
that minute's result changes. A rolling window reads a span of time, so a
reading changes every window whose span covers it, which can mean several
results at later timestamps.

Those revised results are inputs to any app downstream, and the same rule
applies there. In the [turbidity tutorial](../tutorials/turbidity.md), two late
readings change one minute of the clean stream, which changes every window of
the sustained alarm whose fifteen minutes include it. Nothing else is
recomputed.

This is also why apps cannot choose arbitrary output timestamps or run on a
clock. If an app could emit whenever it liked, there would be no way to know
which of its results a given reading had influenced.

## Replacement chains

A cleaning app can declare an `aq.ReplacementOutput()`. Instead of describing
a new measurement, its output continues the identity of the one physical stream
its input selects, with that stream's quantity kind and, unless it converts
it, unit. A second replacement selects the first one's output and continues
the identity further. Each stage has at most one replacement, so the chain has
one final stage.

Normal measurement reads select that final stage. `view="raw"` selects the
original samples; `view="stored"` selects a particular physical stage. App
queries remain specific to that app.

App inputs follow the same authority. An alarm that selects a filter's
turbidity reads the final stage, so adding a stage moves the alarm to it and
recalculates its history. The stage being replaced is the exception: a
replacement binds exactly the stream it selects, which keeps each stage's
place in the chain explicit. An app that needs the original samples or one
particular stage asks for `view="raw"` or `view="stored"`.

A replacement is a complete maintained result. A filtered reading stays absent;
an empty result does not reveal the original value underneath. Each function
call returns its calculated series, and Acquirium keeps the result at that
call's scheduled timestamp. The existing window rules still determine which
results a late reading repairs.

The active plan records the chain. A historical read uses that epoch's chain
as well as its values. If the final stage fails, its last successful values
remain stale. A new stage that has never succeeded has no values; it does not
silently fall back to an earlier stage. See
[replacement outputs](../reference/apps.md#replacement-outputs).

## Group identity and membership

The input selection decides which streams feed a calculation. Grouped by tank,
a selection produces one calculation per tank, each with its own output stream.
That output's identity comes from the app's name, the group and the output
port, and not from the particular streams that happen to be in the group.

So when someone adds a second temperature probe to a tank, the tank's group
gains an input, its results are recomputed, and its output stream keeps the
same identity. Apps downstream that read it never notice the change, and
neither do the queries that find it.

Group membership follows the plant model as it is now, across the whole stored
history. If a probe is moved to another tank in the model, both tanks are
recomputed as though the probe had always belonged to its new tank. That is the
right answer when the model was wrong. It is the wrong answer when the probe
really moved on a particular date, and Acquirium has no way yet to record a
move that takes effect only from a point in time.

## Function repeatability

Because a correction can reach back weeks, an app's function may run again on
old data at any time, and it must produce the same result for the same inputs.
That rules out anything the function could observe that is not part of its
input: the current time, a file that may have changed, a live web service, a
global variable left over from an earlier call. Everything a function needs
arrives in its frame and its context.

The same reasoning applies to the code itself. Acquirium saves a copy of the
calculation file under its content hash when you deploy. Editing the original
file leaves the running calculation unchanged. To use the edit, rebuild its
`PythonFunction.from_file(...)` declaration and deploy the app again. Acquirium
recomputes history with the new code. The source path can stay the same.

## Published snapshots and failure recovery

A correction to a source reading and the derived changes successfully computed
from it become visible together, in one published snapshot called an epoch.
For a healthy chain, the average and its alarm update together.

Earlier snapshots stay readable. That is what lets you look at an alarm as it
was when someone acted on it, even after later data withdrew it.

If a function fails, Acquirium retains its last successful outputs. Its
downstream consumers also retain their outputs and do not execute against stale
inputs. Raw readings and independent branches continue updating.

App inspection reports `failed` for a calculation error and `blocked` for its
downstream consumers, with `stale=True` and per-operator failure details. Thus an
epoch can contain results of different freshness. Publication of a write does
not promise that every app is current.

After a corrected deployment or an explicit retry, Acquirium recomputes the
affected branch from current inputs. It includes changes received while the
branch was blocked and clears the failure status on success. The retained
values and failure status survive restart. See
[recovering from a failed calculation](../how-to/check-deploy-apps.md#recovery-from-a-failed-calculation).

A failure to commit the database still stops publication: no part of an epoch
can become visible without a successful transaction.

[Escapement core](escapement-core.md) describes how the engine implements all
this: how deployments become plans, how epochs commit, and what happens on
restart.
