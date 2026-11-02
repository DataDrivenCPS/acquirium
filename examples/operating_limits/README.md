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
