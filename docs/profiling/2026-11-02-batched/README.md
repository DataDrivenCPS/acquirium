# Sustained ingest with batched admission

Measured on 2026-11-02 with release Escapement built from `6244ebf`. Python
3.12.11, Linux x86_64, 16 logical CPUs, on a shared developer machine whose
load average was between 3.3 and 4.5. Each case is one run of the
[harness](../../../examples/operating_limits/README.md#sustained-ingest), set
up as in the [earlier measurements](../2026-11-02-sustained/README.md): 100
streams, a native 15-minute mean of each, 20 s per rate. Each case's directory
holds its `results.json` and `summary.md`.

Two things changed in Escapement:
- `ingest_many` admits several requests in one transaction, and concurrent
  `ingest` calls share one;
- `Mutation.put` builds a numeric reading without raising and discarding an
  exception inside, which had cost several times the rest of the call.

| Requests | Senders | Sustained before | Sustained now | p50 there | Most published before | Most published now | Requests/commit there |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 ([req1k](req1k/summary.md)) | 1 | 50,000 | 50,000 | 51 ms | 72,000 | 82,000 | 1.0 |
| 1,000, 8 to a call ([req1k-calls8](req1k-calls8/summary.md)) | 1 | | 200,000 | 214 ms | | 229,000 | 8.0 |
| 1,000 ([req1k-4senders](req1k-4senders/summary.md)) | 4 | | 150,000 | 164 ms | | 152,000 | 2.4 |
| 10,000 ([req10k](req10k/summary.md)) | 1 | 200,000 | 250,000 | 318 ms | 227,000 | 255,000 | 1.0 |
| 10,000 ([req10k-4senders](req10k-4senders/summary.md)) | 4 | 200,000 | 250,000 | 334 ms | 194,000 | 276,000 | 1.3 |

Rates are readings a second; "most published" is the highest rate any step
published. Earlier figures are from the
[earlier measurements](../2026-11-02-sustained/README.md); 1,000-reading
requests from four senders were not measured there.

## What it shows

- **Requests in one call cost a quarter as much each.** A call of eight
  1,000-reading requests spent 16 ms in `ingest`, 2 ms a request against 8 ms
  for one alone. One sender reached 229,000 readings a second against 82,000.
  A transaction costs about 7 ms, plus about 1 µs a reading, however many
  requests it holds.
- **Concurrent senders share commits only once they queue.** Four senders of
  1,000-reading requests sustained 150,000 a second, three times what one
  sender sustains, with 2.4 requests to a commit there. Up to 50,000 a second each request
  still committed alone and cost what it did before: requests share a commit
  only when they arrive while another commit or an epoch holds the store.
- **Epochs holding the store are now the limit for small requests.** Four
  senders of 1,000-reading requests at 150,000 a second spent 91% of their
  time in `ingest`, whose median was 19 ms and p99 143 ms, though committing
  2.4 requests takes about 10 ms. The rest is waiting: epochs hold the store
  while they read their requests back and while they publish. Those two
  stages took 23 and 42 ms, about ten times a second, including their own
  waits for the store.
- **Four senders of 10,000-reading requests found the epochs' limit.** At
  300,000 a second they published 276,000. Epochs grew to 21 requests and
  725 ms each, and the backlog took 1.04 s to drain, just over the one-second
  allowance. This is the first case where epochs, not admission, fell behind.
- **Generating requests in Python had limited the earlier senders.**
  `Mutation.put` with a number tried the value as text first and discarded
  the exception. That ran at 1.5M calls a second on one thread, and collapsed
  to 0.2M across four threads, which contended for the GIL. It now checks the
  value's type, and runs at 13.5M calls a second on one thread and 12.1M
  across four. The earlier four-sender runs spent up to half their time
  generating requests for this reason. One sender of 10,000-reading requests
  now spends 12% of its time generating instead of 25%, which accounts for
  its higher ceiling, since its requests still commit one at a time.
  Acquirium's core builds every numeric reading with `Mutation.put`.
- **Batched calls make epochs larger.** Each epoch then applies a whole call:
  up to 100,000 a second, calls of eight gave a median latency of about 60 ms,
  against 32–51 ms for single requests up to 50,000, since an epoch of 8,000
  readings takes about 50 ms.

## The envelope today

Through the engine, 100 streams with a mean of each sustain:
- 200,000 readings a second from one sender in calls of eight requests of
  1,000;
- 150,000 from four senders of single 1,000-reading requests;
- 250,000 in requests of 10,000.

Median latency there is 0.16–0.32 s. A lone sender of small requests is still
limited to about one commit per request, so it should batch with
`ingest_many`.

The next limit is how long epochs hold the store. Keeping accepted requests in
memory, so that an epoch need not read them back from the inbox and parse
them, would shorten that.

## Through the server

Acquirium's core used to hold a lock of its own around `ingest`, so server
requests never shared a commit. It now only orders submissions against
closing. Client threads posted 1,000-reading Arrow requests to a local
server with `wait=0`, as fast as it answered, for 8 s, on 10 streams per
sender; the two versions were alternated, three runs each, on 2026-11-03.

| Senders | Requests/s with the lock | Requests/s without | Requests/commit without | p50 with | p50 without |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 55–56 | 46–56 | 1.0 | 17 ms | 17 ms |
| 2 | 63–66 | 64–69 | 1.1–1.2 | 26 ms | 27 ms |
| 4 | 60–72 | 92–94 | 1.8–1.9 | 49–53 ms | 38–40 ms |
| 6 | 70–72 | 99–112 | 2.3–2.5 | 77–79 ms | 47–52 ms |
| 8 | 60–72 | 120–125 | 2.5–2.6 | 102–109 ms | 56–58 ms |

With the lock, requests queued for it: throughput stopped at about 70 a
second from four senders on, and latency grew by about 13 ms a sender.
Without it, the senders' requests shared commits, and eight senders reached
120–125 a second. With 8 senders of 100-reading requests, 90–92 requests a
second became 171–201.

The first runs of this put every response about 40 ms late. `aq.init`'s
server bound its listening socket without naming TCP, so asyncio did not
set `TCP_NODELAY` on its connections, and each response on a kept-alive
connection waited for the client's delayed ACK. One sender then managed 17
requests a second, and the fixed wait kept senders whose requests had
shared a commit in step, so four senders were slower without the lock. The
server now binds a TCP socket. The public `acquirium server` command was not
affected.

## Not yet measured

- Sustained, paced ingest through the HTTP server and drivers.
- The PostgreSQL store, whose commits cross the network.
- Reads and corrections while ingest is sustained.
