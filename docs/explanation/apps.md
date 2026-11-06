---
title: How derived streams stay current
---

Suppose a late reading changes a minute's average. A downstream alarm based on
that average must change too. An app records the relationship between inputs,
calculation, and outputs, so Acquirium can maintain the results as data changes.

The window determines which results a reading can affect. A pointwise conversion
changes at that reading's timestamp. A one-minute average changes in that minute's
bucket. A rolling average may change at several later timestamps. Dependent apps
then recalculate from those revised results.

The input selection has a different job: it decides which streams belong in the
calculation. A selection grouped by tank produces one result stream per tank.
Adding a probe to the tank changes that group's inputs. Its output keeps the same
identity, so downstream apps continue to find it.

Membership follows the current plant model for the whole stored history. Moving
a probe to another tank recalculates both tanks as though it had always belonged
to the new one. Use graph edits when that is the intended correction; they do
not record a move that took effect only from a particular date.

Source changes and their derived changes become visible together in a published
snapshot. A reader sees the previous snapshot until the calculation succeeds.
Earlier snapshots remain available, which lets us revisit an alarm as it looked
before a correction. A failed calculation leaves the write accepted but
unpublished; [recovery](../how-to/check-deploy-apps.md#recover-from-a-failed-calculation)
resumes it after the cause is fixed.

The [turbidity tutorial](../tutorials/turbidity.md) follows one such correction
through a chain. For implementation details, see [Escapement core](escapement-core.md).
