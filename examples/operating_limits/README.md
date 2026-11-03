# Memory and restart cost against history size

See the [results](../../docs/profiling/2026-11-02-history/README.md).

Run from the repository root, using a **new** output directory:

```sh
uv run --locked python -m examples.operating_limits.run \
  --output /tmp/acquirium-history --sizes 1e6 4e6 16e6 32e6 --derived
```

Each size is a number of source readings, spread over `--streams` streams
(default 100) with one reading per stream every `--interval` seconds (default
1). Values random-walk at three decimals, as a sensor reports them. With
`--derived`, a native 15-minute mean of every stream is maintained as the
readings arrive. The engine uses Acquirium's server defaults: two workers,
128 requests per epoch, 100,000 pending requests.

Each size runs in two fresh processes against one DuckDB file:

1. **Load** sends time-major requests of 10,000 readings, as drivers do, waits
   until every one is published, and records the time, the process's resident
   memory, its peak, and the database's size on disk.
2. **Reopen** opens the same database, records the time and resident memory,
   reads the first stream's last hour and its whole history, then corrects one
   reading halfway through it. It checks that the correction and its
   15-minute mean are published, and records how long that took.

Memory is measured above the process's footprint before the engine opens, so
it excludes the interpreter but includes everything the engine allocates.
The database is deleted after each size. `results.json` holds every
measurement and `summary.md` the table.

No HTTP server is involved. The server adds a fixed footprint, measured
separately in the results.

## Sustained ingest

`sustained.py` measures publication latency while readings arrive at a steady
rate, and finds the rate at which ingest stops keeping up:

```sh
uv run --locked python -m examples.operating_limits.sustained \
  --output /tmp/acquirium-sustained --derived --request-readings 10000
```

It offers each of `--rates` readings per second in turn (default 10,000 up to
300,000) for `--duration` seconds (default 20), in requests of
`--request-readings` (default 1,000). `--batch-requests` sends that many
requests at a time with `ingest_many`, which admits them in one transaction;
`ingest` times are then per call.
`--senders` splits the streams among that many threads, each pacing its share
of the rate as a separate driver would; the engine admits requests that
arrive together in one transaction. `--history` loads that many readings
first. The engine uses the same
defaults as the history harness, and the processor is woken after every
request, as Acquirium's server does.

Each request's latency runs from just before `ingest` until the processor
reports the epoch that applied it, both on the processor's clock. A line about
once a second shows the last second's sent and published readings, epochs,
pending requests, latency percentiles and the slowest `ingest` calls. The
first `--warmup` seconds of each rate (default 2) are left out of its summary.
The summary also gives the requests admitted per transaction, from the
engine's status.

A rate is **sustained** when the senders reach 95% of it and the backlog left
at its end publishes within a second. Otherwise it is **falling behind** if
the senders kept up but the engine did not, **admission-limited** if the
senders spent most of their time inside `ingest`, and **sender-limited** if
they spent it generating requests. The ramp stops at the first rate that is
not sustained, unless `--keep-going` is given. At the end, every stream's
reading count and the first stream's last complete mean are checked.
`results.json` holds every measurement and `summary.md` the table.
