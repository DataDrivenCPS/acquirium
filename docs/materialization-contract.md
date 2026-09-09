# Processing contract

Derived streams describe the current, corrected inputs. Calculations use all
available readings; they do not wait for sensors to finish an interval.
Late readings and corrections recompute affected output timestamps. A result
that disappears (including an alarm corrected below its threshold) is removed,
and downstream calculations observe that removal.

Each invocation owns an output window and reads a possibly wider input window.
Context rows outside the output window must never overwrite historical results.
Fixed buckets read complete buckets. A trailing rolling calculation reads its
preceding context and recomputes subsequent outputs affected by a correction.
Windows are UTC and inclusive at microsecond precision.

Assigning a port replaces its output window, including when the assigned table
is empty. Leaving a port unassigned leaves it unchanged. Output changes and
consumed input progress commit in one transaction. Repeated execution and
different ingestion batch boundaries must converge to the same result.

One coordinator schedules work through a bounded executor. Failure blocks only
the affected dependency branch. Deployment validation precedes activation;
obsolete work cannot publish after a deployment is replaced or removed.
Progress survives code changes. Explicit reprocessing repairs retained history.

## Acceptance checks

- Together and split ingestion produce equal fixed-bucket and rolling results.
- Late corrections repair old results without corrupting boundary context.
- Empty replacement removes rows and propagates through an application chain.
- Restart preserves progress and pending reprocessing.
- Failed deployment leaves the active definition intact.
- Failed transforms leave healthy branches runnable.
- Worker capacity bounds transformations and loaded batches.
- PostgreSQL batch reads share a Repeatable Read snapshot.
