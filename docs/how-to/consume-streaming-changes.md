---
title: Streaming change feed
---

Sooner or later, data has to leave Acquirium. An alarm should page an operator,
a dashboard database needs its copy kept up to date, a report wants to know
which values changed overnight. Polling every stream for new rows would miss
corrections and deletions, and would keep re-reading what has not changed.
Instead, Acquirium keeps a feed of every committed change, source and derived
alike, and a consumer reads it page by page, remembering where it got to.

This guide builds a consumer that posts to a webhook whenever the `high-mean`
alarm from [calculating a derived stream](write-soft-sensors.md) is raised or
cleared. The finished program is
[`examples/streaming/alarm_webhook.py`](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/streaming/alarm_webhook.py).

## Change pages and cursors

```python
page = client.changes(epoch=0, ordinal=0, limit=3)
page["changes"][0]
```
```text
{'epoch': 2,
 'ordinal': 0,
 'weight': 1,
 'row': {'key': {'stream': 'urn:acquirium#7cf58971-2f06-503c-83b5-6da39e9177b9',
                 'time': 1794330610909771000},
         'data': {'value': 0.0},
         'revision': 2}}
```

This is the wave's first reading. The `key` says which stream and which moment
the row belongs to, and `data` holds the value. A `weight` of 1 means the row
was inserted.

`(0, 0)` starts at the beginning of the feed. Each page returns a `cursor`
that advances through complete epochs, including those with no changes. If a
page ends within an epoch, the cursor identifies the last change in that epoch.
Passing it back continues from that position:

```python
page = client.changes(**page["cursor"], limit=1000)
```

The feed is pull-based: you poll it, and a page holds at most 10,000 changes.
Cursors survive a server restart, but belong to one server's data and mean
nothing on another.

## Epoch contents

Changes are published in *epochs*, and one epoch holds a source change together
with everything it caused. Here is one epoch from the sine-wave server, with the
rolling `wave-spread` deployed, and with stream names, times and values
shortened:

```text
epoch  ordinal  weight  stream  time         value
   44        0      +1  wave    17:10:52.58  0.0
   44        1      -1  spread  17:10:54     0.293
   44        2      +1  spread  17:10:54     1.0
   44        3      +1  spread  17:10:56     0.0
```

A new wave reading arrived. It changed the spread of the window ending at
17:10:54, so that result appears twice: retracted with weight −1 at its old
value, then inserted at its new one. It also opened the next window, at
17:10:56. A correction to a source reading looks the same, a retraction and an
insertion; a deletion is a retraction alone.

The `time` is when a reading was taken, and the `epoch` is when the change was
published. The two are unrelated: a correction to last month's data arrives in
today's epoch.

## Alarm stream selection

The feed names streams by their identifiers. Ask a query for the one behind the
alarm:

```python
alarm = client.query().measurement(alias="alarm", app="high-mean")
stream = alarm.data().stream_info()["stream_uri"][0]
```

The consumer reads every change in the feed and skips those for other streams.

## Complete epoch buffering

A retraction on its own means the alarm was withdrawn. A retraction followed by
an insertion at the same timestamp means only its value changed, and the alarm
is still on. To tell the two apart, the consumer has to see the whole epoch,
but a page can end in the middle of one.

Each page reports `complete_through`, the last epoch the feed has returned in
full. The consumer keeps a list, `waiting`, of changes it has read but not yet
handled, and after each page takes out those that are now complete:

```python
waiting += page["changes"]

done = page["complete_through"]
ready = [change for change in waiting if change["epoch"] <= done]
waiting = [change for change in waiting if change["epoch"] > done]
```

Then it gathers the alarm's changes by epoch and timestamp, and reads off what
each group means:

```python
touched = {}
for change in ready:
    key = change["row"]["key"]
    if key["stream"] == stream:
        touched.setdefault((change["epoch"], key["time"]), []).append(change)

for (epoch, alarm_time), changes in touched.items():
    weights = [change["weight"] for change in changes]
    if weights == [1]:
        event = "raised"
    elif weights == [-1]:
        event = "cleared"
    else:
        continue  # retracted and inserted again: the alarm's value was revised
```

## Webhook delivery

For each event, post to the webhook:

```python
requests.post(
    WEBHOOK,
    json={
        "id": f"{epoch}/{alarm_time}",
        "event": event,
        "time": alarm_time,
        "value": changes[-1]["row"]["data"]["value"],
    },
    timeout=10,
).raise_for_status()
```

Run it with any URL that accepts a POST:

```sh
WEBHOOK=https://example.com/hook uv run --locked python examples/streaming/alarm_webhook.py
```

On the sine-wave server, the receiver sees this:

```text
{'id': '644/1794333060000000000', 'event': 'raised', 'time': 1794333060000000000, 'value': 1.0}
{'id': '647/1794333060000000000', 'event': 'cleared', 'time': 1794333060000000000, 'value': 0.569…}
{'id': '652/1794333068000000000', 'event': 'raised', 'time': 1794333068000000000, 'value': 1.0}
{'id': '655/1794333068000000000', 'event': 'cleared', 'time': 1794333068000000000, 'value': 0.569…}
```

Each alarm is raised and then cleared for the same timestamp. The alarm reads
four-second averages, and an average is published as soon as its bucket has a
reading. Here the bucket's first reading is the wave's peak, so the average
starts at 1.0 and the alarm is raised. The next readings in the same bucket
pull the average below 0.5, and the alarm is withdrawn.

That is the consumer working correctly: it reports what the published results
say, and those results change as data arrives. An alarm that should page
someone needs a calculation that does not change its mind so easily, such as the
sustained alarm in the
[turbidity tutorial](../tutorials/turbidity.md#sustained-turbidity-threshold).
Even then, a late reading can withdraw an alarm that has already gone out, as
the same tutorial [shows](../tutorials/turbidity.md#late-readings-and-alarm-recalculation),
so the receiver should expect `cleared` events for alarms it has already
announced.

## Cursor persistence and restart

A consumer that stops must resume without missing an alarm. After handling
the completed epochs, it saves the position just after the last complete epoch.
The largest unsigned 64-bit ordinal represents the end of an epoch.

```python
if done > saved_through:
    SAVED.write_text(json.dumps({"epoch": done, "ordinal": 2**64 - 1}))
    saved_through = done
```

`ready` holds every complete change, including those for other streams, so the
saved position moves past the skipped changes too. Changes still waiting are
not saved; after a restart they are simply read again.

The position is saved after posting. If the consumer crashes in between, it
posts the same events again when it restarts. Saving first would be worse: a
crash would lose the events instead. A repeated post carries the same `id`,
built from the epoch and the alarm's timestamp, so the receiver can drop
duplicates. If the receiver cannot do that, write each event to a table in
your own database in the same transaction as the position, and send from that
table, marking each event as sent.

On its first run, with nothing saved, the consumer reads through the whole
history without posting, so nobody is paged for alarms that are long over. The
finished program does this with a `catching_up` flag that turns off once a
page reports `has_more=False`, meaning it has caught up with everything
published.

The fields of a page are listed in the [HTTP API reference](../reference/server/http-api.md).
How writes are acknowledged and published is described in
[Escapement core](../explanation/escapement-core.md#writes).
