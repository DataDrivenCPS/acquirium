# Streaming examples

Deploy the high-mean alarm from the
[soft sensor calculation guide](../../docs/how-to/write-soft-sensors.md), then
run the webhook consumer:

```sh
WEBHOOK=https://example.com/hook uv run --locked python -m examples.streaming.alarm_webhook
```

[alarm_webhook.py](alarm_webhook.py) posts when the alarm is raised or cleared.
The [streaming guide](../../docs/how-to/consume-streaming-changes.md) walks
through the change feed, retractions, and cursor checkpoints.

The other modules illustrate a source and sink that can resume across restarts:

- [producer.py](producer.py) records a request before sending it. If its
  receipt never arrives, it replays the saved request ID and payload within
  Escapement's bounded retry window.
- [consumer.py](consumer.py) buffers partial epochs and saves the last complete
  epoch's visible rows and cursor together. Restarting rereads uncommitted
  pages. Use a checkpoint with the engine database that issued it.
- [state.py](state.py) replaces the example's JSON source and sink files atomically.
