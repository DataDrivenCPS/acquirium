# End-to-end streaming workload

Run continuously until Ctrl-C:

```sh
uv run --locked python -m examples.streaming.workload
```

Run a finite burst workload with server and consumer restarts:

```sh
uv run --locked python -m examples.streaming.workload \
  --data-dir /tmp/acquirium-streaming \
  --ticks 60 --burst 4 --interval 0.05 --page-size 7 --restart-every 20
```

Each run appends requests to the same source session and resumes the consumer's
saved cursor. `--ticks` counts additional accepted requests, not observations.
The interval is between bursts; publication runs independently. `--restart-every`
restarts the owned local server and consumer after a burst crosses each threshold.
These are graceful restarts; integration tests also kill the actual server.
Use a dedicated directory. One workload process holds its directory lock.

```text
source journal → WorkloadDriver → HTTP admission → Escapement → signed change feed
                                                   │                 │
                               tank means → hottest tank → alarm     │
                                                                     ▼
                                            consumer view + saved cursor
```

The driver generates ordinary probe samples, late readings, corrections, and
explicit deletes. `wait=0` returns a durable receipt before waiting for the
Python DAG to publish. The consumer runs concurrently, using its own HTTP session.
It consumes both source and derived changes, including signed retractions.
Acquirium's only engine/store remains Escapement; the JSON consumer view is a
client-owned demonstration sink, not another server backend.

Read these files:

- [producer.py](producer.py): a real `acquirium.Driver` subclass, source request
  journal, stable retry identities, and latency observations.
- [consumer.py](consumer.py): paged feed consumption, epoch buffering, and atomic
  checkpointing of visible rows together with the committed cursor.
- [workload.py](workload.py): continuous scheduling, independent consumer thread,
  graceful restarts, metrics, and independent full-history verification.
- [state.py](state.py): synchronized atomic JSON replacement for these example
  source/sink files. The engine retains its own native durable state.

## Recovery contract

The source persists a request before its HTTP attempt. It saves the receipt only
after the response arrives. A rejected delivery, transport timeout, lost response,
or source restart redelivers the same ID and payload. Nothing advances past an
unacknowledged request. An accepted but unpublished request remains the engine's
responsibility. Retry deduplication follows Escapement's bounded retry ledger;
this is not an unlimited end-to-end exactly-once guarantee.

Feed pages may split an epoch or a retraction/insertion pair. The consumer stages
partial epochs and applies only changes at or below `complete_through`. It saves
the complete-epoch view and its cursor in one fsynced replacement. A consumer
restart rereads uncommitted pages; it never exposes half of an epoch. Checkpoints
are scoped to the same engine database; do not reuse one with a fresh database.

The example records files under the workload directory:

| Path | Owner and purpose |
| --- | --- |
| `server/` | Native engine, semantic graph, and deployment intent |
| `source/session.json`, `source/requests/*.json` | Source identity, immutable request payloads, receipts, and observed timestamps |
| `consumer.json` | Consumer's complete-epoch view, cursor, and processed-change count |
| `metrics.json` | Last successfully drained and verified run |

An unavailable server is retried with the same request for up to `--timeout`
seconds without progress. Capacity rejection uses HTTP 503 and `Retry-After`.
A paused native function or a nonretryable HTTP error stops the run visibly.
Repair the artifact/environment and retry the processor before resuming. Ctrl-C
stops generation, retries any last unacknowledged request, drains publication and
consumption, verifies the final result, then shuts down the owned server.

## Measurements and limits

The run prints admitted requests, backlog, publication epoch, and consumer frontier
while it runs. At the end it compares **all current raw and derived values** in
both the consumer view and a pinned server snapshot with an independent Python
recomputation of the source journal. Verification must pass before `metrics.json`
is written. Alarm clear/retraction is part of that comparison.

The report includes peak observed pending requests, sampled server peak RSS,
delivery retries, restart count, and p50/p95/max latency. Publication and
consumption latency are **observed upper bounds**, measured from durable source
record creation to polling that proves publication/complete-epoch consumption.
They include source queueing, outages/restarts, HTTP/polling delay, and native
execution. They are not exact commit timestamps or throughput guarantees.
Historical request latencies are retained when the workload resumes; elapsed time
and peak backlog/RSS describe the current run. Memory is process high-water RSS,
not current RSS or the summed memory of transient Python worker processes.

This is an integration/soak workload, not a production broker adapter or an
isolated engine benchmark. It does not run Ray or an MQTT broker. The source
journal is retained for verification; the JSON sink rewrites its current view and
buffers a whole epoch. Both are deliberately small examples. Native current
state still fits in RAM and its history/feed are unpruned. Use finite runs to
establish operating bounds before claiming unattended capacity. The native
benchmark harness can isolate engine costs from these source/sink costs later.

Initial local smoke measurements on 2026-10-18 (macOS arm64, Python 3.12):

| Requests | Restarts | Peak observed backlog | Publication p95 upper bound | Server peak RSS |
| --- | --- | --- | --- | --- |
| 120 | 0 | 55 | 1.30 s | 735 MiB |
| 60 | 3 | 20 | 8.15 s | 743 MiB |

Both used bursts of four requests every 0.05 seconds and passed full result
verification. The restart run used seven-row feed pages; the steady run used
128-row pages. Other tests were active on the machine. Restart measurements
include time when observation is interrupted by shutdown/startup. These small
runs prove the workload and recovery path, not a supported operating envelope.
