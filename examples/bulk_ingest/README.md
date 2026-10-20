# Bulk ingest through a soft-sensor chain

Measures how a chain of soft sensors keeps up with a large CSV backfill. Run from
the repository root, using a **new** output directory:

```sh
uv run --locked python -m examples.bulk_ingest.run --output /tmp/acquirium-bulk
```

The run:

1. Generates one wide CSV per day: four turbidity columns (one per location in
   `model.ttl`) and `--filler-columns` unmodelled columns, one row every 10 s.
   Every column loses two spans of rows: 78 minutes, which the fill covers, and
   3 hours, which it leaves empty.
2. Starts a local server whose `BulkIngestDriver` watches an empty directory and
   declares the four turbidity streams up front.
3. Deploys the six apps in `chain.py`:

   ```
   raw turbidity -> snap (1 min) -> fill (gaps < 2 h) -> plant mean/max/reporting
                                                      -> high -> sustained -> plant alarm count
   ```

4. Moves the CSVs into the watched directory and waits until every reading is
   published.
5. Checks every output against an independent recomputation from the generated
   values.

It prints insert time, throughput, epochs, requests per epoch, and per-app work
from `client.engine_status()["apps"]`: groups evaluated, input rows handed to the
functions, changed output rows, and compute seconds (summed over operators that
may run in parallel). `summary.json` in the output directory holds the same
numbers.

`--days`, `--interval`, and `--filler-columns` scale the workload. The filler
columns stand in for the many unmodelled SCADA tags in a real export: they cost
ingest but no soft-sensor work.

Baseline (2026-10-19, defaults: 2 days, 60 filler columns): 1,006,848 readings in
45.6 s, 102 epochs at 1.00 requests per epoch. The fill app read 2.8M input rows
to change 10,808 output rows.

After drivers stopped waiting on each request (same machine, same day): 4 epochs
at 25.5 requests per epoch, 38.3–43.5 s across two runs, 57,024 groups evaluated
against 64,426. Engine compute is about 95% of the insert time. The fill app's
input rows barely change (2.7M), because each rolling window rereads its own
4-hour span whatever the epoch count; the CSV driver's column-major reshape already
kept each turbidity column's file in roughly one request.
