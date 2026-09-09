# Processing contract

Derived streams reflect the current, corrected inputs. Calculations use the
readings available in storage without waiting for sensors to finish reporting
an interval. A late arrival or correction causes affected output timestamps to
be recalculated. If a result is no longer present, such as an alarm whose
corrected input falls below its threshold, it is removed. Downstream
calculations must observe that removal.

## Input and output windows

Each invocation owns an output window and reads an input window that may be
wider. The extra readings provide context for the calculation; they must not
overwrite results outside the output window. Fixed-bucket calculations read
complete buckets. A trailing rolling calculation reads preceding context and,
when an earlier reading is corrected, revisits subsequent outputs that depend
on it. All window bounds are UTC and inclusive at microsecond precision.

## Publication and recovery

Assigning a table to a port replaces its stored results in the output window.
An empty table removes those results, while leaving the port unassigned
preserves them. Output changes and consumed input progress commit in one
transaction so recovery can repeat uncommitted work. Repeated execution and
different ingestion batch boundaries must converge to the same result.

One coordinator schedules work through a bounded executor. A failed calculation
blocks its dependent branch, while independent branches remain runnable.
Deployment validation completes before activation, and obsolete work cannot
publish after a deployment is replaced or removed. Code changes preserve
processing progress; explicit reprocessing applies those changes to retained
history.

## Acceptance checks

- Together and split ingestion produce equal fixed-bucket and rolling results.
- Late corrections repair old results without corrupting boundary context.
- Empty replacement removes rows and propagates through an application chain.
- Restart preserves progress and pending reprocessing.
- Failed deployment leaves the active definition intact.
- Failed transforms leave healthy branches runnable.
- Worker capacity bounds transformations and loaded batches.
- PostgreSQL batch reads share a Repeatable Read snapshot.
