---
title: Apps
---

An app calculates derived values from stored sensor readings and keeps those
values up to date as the inputs change. For example, an app might convert
temperatures to a common unit, calculate a plant-wide average, or identify
readings that exceed a threshold. The same calculation handles new readings,
late arrivals, and corrections to data already processed.

## Recomputed windows

Consider a ten-minute rolling average. When a reading is corrected, the
correction can affect averages for the following ten minutes. Recalculating
those averages also requires readings before the corrected timestamp.
Acquirium therefore distinguishes the interval of output being replaced from
the wider interval of input needed to calculate it.

Calculations use the readings currently available in storage. They do not wait
for every sensor to report or for an interval to become final. A late arrival
can revise an earlier result, just as a correction can.

An app declares these dependencies through `lookback` and `lookahead`, and uses
`every` when it needs fixed time buckets. The runtime uses the declarations to
select the affected output interval and load its input data. The app's
`transform` method then calculates results from the latest available readings.
It does not have to maintain a separate calculation for corrections.

Each output value is identified by its stream and timestamp. Publishing a
result replaces the previous values in the output interval, including removing
values that no longer appear in the result. For example, if a corrected
reading falls below an alarm threshold, the corresponding alarm disappears.
Downstream apps receive that removal as an input change. Assigning an empty
table removes the port's results in that interval; leaving the port unassigned
preserves them. Results outside the output interval are left unchanged.

The database commits output changes and processing progress in the same
transaction. If a process stops before the transaction commits, the runtime
can repeat the calculation when it restarts. This requires `transform` to
produce the same result for the same input batch. External side effects, such
as sending a notification, cannot be rolled back with the database transaction
and should be handled outside the transform. Repeated execution and different
ingestion batch boundaries must converge to the same result.

## Queries bind apps to the plant

An app selects its inputs through `build_query`. The query might find every
water-temperature measurement in a treatment tank, or pair flow and pressure
measurements on each pump. The compiler resolves each query match to a set of
input streams, called a *binding*.

With `grouping="per_match"`, the runtime calls the app separately for each
match. This suits calculations such as converting every sensor's readings to
Celsius. With `grouping="all_matches"`, one call receives all selected streams,
which allows a calculation to combine readings from several sensors. Every app
must explicitly choose one of these values; a missing or invalid grouping is
rejected when the app is instantiated.

When the plant model changes, the runtime compiles the queries again. If the
matched inputs or their query context have changed, it schedules a repair of
the retained output. For example, adding a sensor to an aggregate may require
recalculating that aggregate's earlier results.

## Two output identities

After deciding which inputs a calculation should receive, you also need to
choose how other queries will identify its output. Acquirium provides two
output declarations for this purpose.

For a conversion performed separately on each sensor, `output.stream` derives
a name from the app, the output port, and the bound inputs. Each sensor gets
its own derived stream, and recompiling the same inputs produces the same
stream identity. You can therefore add sensors without assigning a new output
name to each one.

For a result such as a plant-wide total, `output.named` lets you choose a name
that remains the same when sensors join or leave the calculation. The name is
scoped to the app, under the source `derived:<app-name>`. Each named output
must have a single owner: multiple per-match bindings cannot publish to the
same named stream.

The naming declaration does not choose which inputs the app receives. An
aggregate still needs `grouping="all_matches"`, whether its output name is
generated or supplied by the author.

Other apps can query derived streams in the same way as measured streams.
These dependencies form a directed acyclic graph, which the scheduler uses
to run producers before their consumers. Queries can select outputs by their
declared metadata and by the app that produced them. For example,
`measurement(quantity_kind="temperature", app="normalize-temperatures")`
selects temperature outputs from that app.

## Deployment and recovery

Deployment validation completes before a new definition becomes active. Work
prepared for an old definition cannot publish after the deployment is replaced
or removed. Updating code preserves processing progress; use
[explicit reprocessing](../how-to/check-deploy-apps.md#apply-code-changes-to-retained-history)
to apply the changed calculation to retained history.

A failed calculation blocks its dependent branch, while independent branches
can continue. The runtime stores processing progress in the database so it can
resume after a restart. The
[internals explanation](materialization-internals.md#scheduling-and-recovery)
describes how snapshots, revisions, and transactions support this behavior.

## Why "materialization"

The documentation calls this process *incremental materialization*. The derived
streams are stored calculation results, similar to materialized views in a
database. The runtime updates the affected parts of those results as input
revisions arrive, rather than recalculating the entire history for every write.

[Materialization internals](materialization-internals.md) explains revision
tracking, scheduling, publication, recovery, and the storage backends.
