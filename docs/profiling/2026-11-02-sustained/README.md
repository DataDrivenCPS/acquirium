# Publication latency under sustained ingest

Measured on 2026-11-02 with release Escapement built from `dbab4f2`, Python
3.12.11, Linux x86_64, 16 logical CPUs and 93 GiB of memory, on a shared
developer machine whose load average was between 1.7 and 3.5. Each case is one
run. The [harness](../../../examples/operating_limits/README.md#sustained-ingest)
drives the native engine with Acquirium's server defaults and wakes the
processor after every request, as the server does. Each case's directory
holds its `results.json` and `summary.md`.

100 streams each receive readings, and a native 15-minute mean of every
stream is maintained. Each rate is offered for 20 s, and its first 2 s are
left out. A request's latency runs from submission until the epoch that
applied it is published.

Requests of 1,000 readings, one sender ([req1k](req1k/summary.md)):

| Offered/s | Published/s | p50 ms | p99 ms | Max ms | Requests/epoch | Epoch ms | Ingest p50 ms | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 9,999 | 33 | 77 | 84 | 1.0 | 25 | 8.1 | sustained |
| 25,000 | 25,051 | 32 | 115 | 156 | 1.0 | 25 | 7.8 | sustained |
| 50,000 | 50,017 | 44 | 164 | 200 | 1.4 | 28 | 7.9 | sustained |
| 100,000 | 72,381 | 66 | 204 | 241 | 3.3 | 45 | 9.1 | admission-limited |

Requests of 10,000 readings, one sender ([req10k](req10k/summary.md)), and
the same after loading 16M readings of history
([req10k-history16m](req10k-history16m/summary.md)):

| Offered/s | Published/s | p50 ms | p99 ms | Max ms | Requests/epoch | Epoch ms | Ingest p50 ms | Result | p50 ms, 16M history | p99 ms, 16M history |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 10,000 | 10,000 | 61 | 105 | 105 | 1.0 | 50 | 16.3 | sustained | | |
| 25,000 | 25,000 | 64 | 175 | 175 | 1.0 | 56 | 17.3 | sustained | | |
| 50,000 | 50,000 | 60 | 196 | 196 | 1.0 | 54 | 15.7 | sustained | 68 | 195 |
| 100,000 | 99,986 | 64 | 171 | 216 | 1.0 | 57 | 16.3 | sustained | 67 | 222 |
| 150,000 | 149,698 | 83 | 259 | 320 | 1.1 | 66 | 17.5 | sustained | 97 | 311 |
| 200,000 | 201,631 | 151 | 328 | 368 | 1.9 | 93 | 17.9 | sustained | 162 | 374 |
| 250,000 | 227,211 | 216 | 441 | 525 | 3.3 | 143 | 24.3 | admission-limited | | |

## What it shows

- **The engine kept up at every rate it was offered.** No case left a
  backlog: what remained when a rate ended published within 0.18 s. Each ramp
  stopped because requests could not be submitted faster, not because epochs
  fell behind.
- **Admission is the limit.** Each request is one durable DuckDB write:
  about 8 ms for 1,000 readings and 16 ms for 10,000, so most of it is a fixed
  cost per request. It takes the same store lock as an epoch's read of its
  requests and its commit, so it also waits for those. One sender of
  1,000-reading requests reached about 72,000 readings a second, spending 89%
  of its time in `ingest`; with 10,000-reading requests it reached about
  225,000.
- **More senders do not raise it.** Four senders of 10,000-reading requests
  ([req10k-4senders](req10k-4senders/summary.md)) published 193,000 readings a
  second at most. They queue on the same lock, and at 250,000 a second they
  spent half their time generating requests, which share Python's GIL.
  Contention also made latency worse: at 200,000 a second, p50 was 411 ms and
  p99 1.1 s, against 151 and 328 ms from one sender, because epochs grew to
  3.6 requests and 179 ms.
- **Latency is about one epoch plus one `ingest` until near the ceiling.**
  With one request per epoch, p50 was 32–44 ms for 1,000-reading requests and
  60–64 ms for 10,000. An epoch of 1,000 readings took 25 ms: 13 ms computing
  and 10 ms publishing. Computing re-averages each stream's whole current
  15-minute bucket, up to 900 readings, however few arrived. Near the ceiling
  epochs take several requests, and p50 rose to 151 ms at 200,000 a second.
- **History changes it little.** On top of 16M readings the same rates
  published with p50 3–14 ms higher. Two runs without history differed by
  2–6 ms.

## The envelope today

Through the engine, 100 streams with a mean of each sustain about 50,000
readings a second in requests of 1,000, and 200,000 in requests of 10,000,
with a median publication latency under 0.2 s. Small requests cost the most:
from the two request sizes, `ingest` costs about 7 ms per request plus about
1 µs per reading, so a driver sending a few readings at a time pays most of
7 ms for each request. The HTTP server's normalization
adds to these costs; see the
[bulk ingest](../../../examples/bulk_ingest/README.md) measurements.

Raising the ceiling means making admission cheaper: writing several requests
in one transaction, or accepting requests without waiting for an epoch's
commit. [Batched admission](../2026-11-02-batched/README.md) does the first.
It also found that generating requests was slow for a reason of its own:
`Mutation.put` raised and discarded an exception for every number.

## Not yet measured

- Sustained ingest through the HTTP server and drivers.
- Python functions and plans of thousands of operators under sustained ingest.
- Reads and corrections while ingest is sustained.
- The PostgreSQL store, whose admission writes go over the network.
