---
title: How derived streams stay current
---

Most derived data in a treatment plant is computed once and then quietly goes
stale. Someone exports a month of turbidity, averages it in a spreadsheet, and
emails the result. A week later the lab corrects two readings, and the
spreadsheet still holds the old average. Nobody knows which reports used it.

Acquirium takes a different approach. An app does not describe a computation
to run once. It describes a relationship that should always hold: *this stream
is the one-minute mean of those readings*. Acquirium's job is to keep that
relationship true as the inputs change, including when they change in the
past.

## A change touches only what depends on it

When a reading arrives or is corrected, recomputing everything would be
correct but wasteful. The app's window says exactly which results a reading can
affect. A pointwise calculation reads one timestamp, so a reading changes only
the result at its own timestamp. A one-minute bucket reads one minute, so only
that minute's result changes. A rolling window reads a span of time, so a
reading changes every window whose span covers it, which can mean several
results at later timestamps.

Those revised results are inputs to any app downstream, and the same rule
applies there. In the [turbidity tutorial](../tutorials/turbidity.md), two late
readings change one minute's average, which changes one gap-filled value, which
changes every window of the sustained alarm whose fifteen minutes include it,
which changes the plant-wide count. Nothing else is recomputed.

This is also why apps cannot choose arbitrary output timestamps or run on a
clock. If an app could emit whenever it liked, there would be no way to know
which of its results a given reading had influenced.

## Groups give results a stable identity

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

## Functions must be repeatable

Because a correction can reach back weeks, an app's function may run again on
old data at any time, and it must produce the same result for the same inputs.
That rules out anything the function could observe that is not part of its
input: the current time, a file that may have changed, a live web service, a
global variable left over from an earlier call. Everything a function needs
arrives in its frame and its context.

The same reasoning applies to the code itself. Acquirium records a digest of the
calculation file when the app is deployed. If the file changed underneath it,
old and new results would silently come from different code. So a change of
code is a change of deployment: you put the new version at a new path, deploy
it, and Acquirium recomputes the app's history with it.

## Readers see a consistent snapshot

A correction to a source reading and every derived change it causes become
visible together, in one published snapshot called an epoch. A reader sees
either the state before the correction or the state after it, never a
half-updated chain where the average has changed and the alarm has not.

Earlier snapshots stay readable. That is what lets you look at an alarm as it
was when someone acted on it, even after later data withdrew it.

If a function fails, nothing from that epoch is published, and the engine
pauses. The write that triggered it is safely stored, but readers keep seeing
the previous snapshot until the cause is fixed and the work resumes; see
[recovering from a failed calculation](../how-to/check-deploy-apps.md#recover-from-a-failed-calculation).
The pause affects every app, not just the one that failed, because publishing
the others would break the guarantee that each snapshot is consistent.

[Escapement core](escapement-core.md) describes how the engine implements all
this: how deployments become plans, how epochs commit, and what happens on
restart.
