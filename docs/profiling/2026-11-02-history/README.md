# Memory and restart cost against history size

Measured on 2026-11-02 with release Escapement built from `e663c44`,
Python 3.12.11, Linux x86_64, 16 logical CPUs and 93 GiB of memory. These are
single sequential runs on a shared developer machine, whose load average from
other work was between 4 and 10; they are not repeated estimates. The
[harness](../../../examples/operating_limits/README.md) drives the native
engine directly with Acquirium's server defaults; [summary.md](summary.md)
and [results.json](results.json) hold every measurement, and
[before-summary.md](before-summary.md) the same sweep on `b539b60`, before
Escapement packed rows written while it runs.

100 streams receive one reading a second each, and a native 15-minute mean of
every stream is maintained as they arrive. Loading and reopening each run in
a fresh process.

| Readings | Load | Readings/s | Memory after load | Bytes/row | Reopen | Memory after reopen | Bytes/row | On disk |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1M | 4.4 s | 229,000 | 279 MiB | 292 | 0.13 s | 101 MiB | 106 | 40 MiB |
| 4M | 16.2 s | 247,000 | 746 MiB | 195 | 0.47 s | 297 MiB | 78 | 73 MiB |
| 16M | 67.2 s | 238,000 | 1,838 MiB | 120 | 1.78 s | 1,058 MiB | 69 | 186 MiB |
| 32M | 135.4 s | 236,000 | 2,854 MiB | 93 | 3.38 s | 2,053 MiB | 67 | 317 MiB |

Bytes per row count the derived means as rows; there are about 1,100 per
million readings. Memory is above the process's footprint before the engine
opened, and its peak stayed within 15 MiB of the final figure. Before packing,
the same loads held 349, 971, 2,604 and 4,547 MiB.

## What it costs

- **A row costs about 66 bytes, loaded or written.** From 16M to 32M rows,
  memory after a load grew by 67 bytes a row and after a reopen by 65. Before
  packing, a row written while the engine ran cost 127 bytes until the next
  restart: written rows sat in a persistent tree whose nodes fill only
  halfway in time order. Escapement now moves them into sorted runs as they
  accumulate.
- **Ingest holds up to about 0.8 GiB more than a reopen.** Each stream keeps
  up to 16,384 recent rows in the tree before they move to a run, and the
  allocator keeps memory those rows freed. The excess grew from 0.2 GiB at
  1M readings to 0.8 GiB at 32M, and stopped growing with history.
- **The server adds about 470 MiB.** An idle `aq.init(exact_only=True)`
  server holds 473 MiB after starting in 10 s, with a peak of 636 MiB. The
  embedding model, loaded without `exact_only`, adds to that.
- **Restart takes about 0.1 s per million rows.** The engine scans its log
  once and rebuilds the snapshot; no checkpoint is involved.
- **Disk is not the limit.** The log takes about 10 bytes a reading at 32M,
  since DuckDB compresses the regular timestamps and three-decimal values.
- **Reads and repairs do not slow with history.** One stream's last hour
  (3,601 readings) reads in under 0.6 ms at every size, and its whole history
  at about 0.8 ms per 100,000 readings. Correcting a reading halfway through
  the history, and repairing its mean, publishes in 20–23 ms at every size.
- **Ingest holds about 235,000 readings a second** through the engine with
  native means, in requests of 10,000. The inbox never filled. HTTP and
  Python functions cost more per reading; see the
  [bulk ingest](../../../examples/bulk_ingest/README.md) and
  [model workload](../2026-10-18-model/README.md) measurements.

## Wide plans

With 10,000 streams and a mean of each, so 10,000 operators, loading 4M
readings took 193 s, a tenth of the rate above, and correcting one reading
took 37 s to publish ([wide-results.jsonl](wide-results.jsonl)). Without the
means, 2M readings into 10,000 streams load in 10 s. The cost is per epoch:
every operator's task copies the snapshot's map of streams, so an epoch costs
operators × streams, however little changed. `b539b60` measures the same.
The log also took 697 MiB on disk for 4M readings, against 73 MiB with 100
streams.

## The envelope today

Server memory is about 0.5 GiB, plus 66 bytes per row, plus up to 0.8 GiB
while 100 streams ingest. Derived rows count as rows. For example:

| Rows held | Memory |
| ---: | ---: |
| 10M | 1.1–1.9 GiB |
| 100M | 6.6–7.4 GiB |
| 300M | 19–20 GiB |

100 sensors at one reading a second write 8.6M readings a day, so a year is
about 3.2 billion rows. Without retention, that does not fit in memory on
one machine. A modest deployment fits: 1,000 points every minute is 526M
readings a year, about 33 GiB.

Plans of thousands of operators are outside the envelope until an epoch's
cost no longer grows with the number of streams.

## Not yet measured

- Sustained ingest against publication latency, and the rate at which the
  inbox stops draining.
- Text readings, which store a string per row.
- The PostgreSQL store, whose open scans the same log over the network.
- Python functions over long histories, beyond the per-window costs already
  in the [membrane example](../../../examples/membrane_monitoring/README.md).
