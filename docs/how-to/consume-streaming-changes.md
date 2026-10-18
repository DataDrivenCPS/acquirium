---
title: Consume committed streaming changes
---

`client.changes()` reads a bounded page from Escapement's durable signed feed.
It contains committed source and derived changes; accepted unpublished work never
appears. The [end-to-end workload](../../examples/streaming/README.md) demonstrates
continuous production, consumption, retry, and restart over real HTTP.

```python
page = client.changes(epoch=0, ordinal=0, limit=1000)
for change in page["changes"]:
    print(change["epoch"], change["weight"], change["row"])
cursor = page["cursor"]
next_page = client.changes(**cursor, limit=1000)
```

The cursor is the exclusive `(epoch, ordinal)` of the last returned change.
`(0, 0)` starts at the beginning. Pass exactly the returned cursor to resume.
Cursors belong to one engine database and survive server restart. They are not
portable to a new database. An empty page retains the input cursor. `limit` is
between 1 and 10,000; the feed is paged polling, not a pushed HTTP subscription.

The response contains:

| Field | Meaning |
| --- | --- |
| `changes` | Ordered native signed rows, ascending by epoch and ordinal |
| `cursor` | Exclusive continuation cursor |
| `published_epoch` | Publication frontier captured for this page |
| `complete_through` | All changes through this epoch have been returned by this cursor's scan |
| `has_more` | More rows remain at or below this page's captured frontier |

Each change preserves the native structure:

```json
{
  "epoch": 5,
  "ordinal": 0,
  "weight": -1,
  "row": {
    "key": {"stream": "urn:acquirium#...", "time": 1000000000, "sample": 0},
    "data": {"value": 30.0, "quality": 0},
    "revision": 12
  }
}
```

Weight `-1` retracts that complete row; `+1` inserts a complete row. A correction
normally retracts the old row and inserts the replacement. A deletion only
retracts. Preserve integer UTC nanosecond keys and revisions. Event time (`time`)
is distinct from publication epoch and feed order.

A page may split one epoch or even one correction pair. Consumers requiring
atomic visibility must buffer partial epochs and apply only rows whose epoch is
at or below `complete_through`. `has_more=False` means the page exhausted its
captured publication cut; new epochs may already be available on the next poll.
Epochs without changes can advance `complete_through` without advancing the row
cursor. Values may be coalesced within an epoch, so the feed describes committed
relation changes rather than one event for every accepted delivery.

Commit sink updates and the last **applied** cursor together. Saving a read cursor
before applying its rows loses work after a crash. Applying rows without saving
the cursor can replay side effects. The example sink atomically replaces its view
and committed cursor, keeping partial epochs only in memory. A crash replays those
partial pages. Notifications need their own durable delivery/outbox policy.

The server exposes the unfiltered native feed. Filter streams in the consumer,
while advancing over every scanned row; filtered rows still participate in epoch
completion. The example uses no sink-side filtering so its view can be verified
against every current source and soft-sensor stream.

For producers, `insert_timeseries*` and `delete_timeseries` accept `wait=0` for
durable admission without a publication wait. Their `timeout` parameter bounds
the HTTP call. On timeout/disconnect, acceptance is unknown: reuse the same source,
request ID, and payload. HTTP 503 with `Retry-After` means admission capacity was
exceeded. Persist source requests before delivery if they must survive source
process failure. Existing in-memory driver buffers alone do not provide that
guarantee. A failed function still pauses the global native queue; inspect status,
repair, and call `client.retry_engine()` explicitly.
