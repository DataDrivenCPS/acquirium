# Sustained model workload

This benchmark isolates Escapement's native Python pool from HTTP and semantic
graph startup. The [streaming workload](../streaming/README.md) exercises those
application paths separately. Both use Escapement as the only numeric engine.

```sh
uv run --locked python -m examples.model_workload.run \
  --output /tmp/model-pool2 --pool-size 2 --duration 10 --rates 2 120 2
```

Each phase lasts `--duration` seconds: low traffic, burst, and recovery. Every
source request supplies four input roles to each of four independent operators.
Each operator runs a synthetic radial-basis model whose immutable arrays load
once per process. The default 2,048 copies use 10 MiB of model arrays per worker;
interpreter, libraries, shared pages, and temporary inference arrays add RSS.
`--copies` changes model memory and calculation cost together. Repeated basis
functions deliberately supply load, rather than model accuracy. This fixture is
not a trained water-quality model.

Compare `--pool-size 1`, `2`, and `4` in separate sequential runs. Native operator
concurrency stays at `--units` (default four), so this measures the resident pool
limit without changing DAG width. A job still processes its affected timestamp
groups serially. `--epoch-requests` defaults to eight. It changes job size when
the inbox builds up; compare group counts as well as job service time.
Use `--workers` to set scheduler concurrency independently of `--units`, for
example `--units 1 --workers 4 --pool-size 4` to test a narrow DAG with spare
execution capacity. The default remains one scheduler slot per unit.

The generator uses a monotonic schedule, skips missed slots, and retries one
blocked request with the same identity when the bounded inbox rejects admission.
It reports actual admissions, missed slots, and capacity retries. It does not
claim to maintain the requested rate while backpressured. The diagnostic JSONL
source journal is not a durable production delivery protocol.

Use `--max-jobs 25 --restart` to exercise model reloads. Recycling follows native
job counts, not source request counts. Restart drains after the burst, checks
every current source and derived value, closes/reopens the same DuckDB database,
and checks again before continuing. It tests persisted results and renewed cold
workers; existing native/HTTP recovery tests cover crashing with pending work.

Corrections replace old pressure readings; deletes remove old conductivity
readings and retract predictions. A final correction and deletion guarantee
those paths even in tiny runs. An independent scalar oracle uses 128 basis
functions, with neither the worker module nor its NumPy implementation, to
recompute every current prediction. Verification also checks every raw reading,
timestamp, sample key, and quality flag. Reports are written only after success.

Each new output directory contains the model, DuckDB file, source journal,
environment and exact native source identity, completed epoch reports, full
sampled telemetry JSONL, flattened samples CSV, and `result.json`. Histograms and
the worker registry remain bounded inside the native engine; the finite harness
retains its diagnostic samples outside it. Sample interval is approximately
50 ms; admission, observation, and verification overhead are included.

`publication_observed_ms` runs from successful admission to observing the native
applied frontier. It is an upper bound including polling delay, not an exact
commit timestamp, and excludes rejected admission attempts. The warmup and final
correction/delete requests are included. Worker RSS sums root processes only,
counts shared pages per process, and misses unsampled peaks. Server memory is
separate. Histograms report percentile **upper bounds**, not interpolated
percentiles. See [HTTP telemetry definitions](../../docs/reference/http-api.md).

Export plots after running several fixed pools:

```sh
uv run --locked python -m examples.model_workload.plot \
  /tmp/model-pool1 /tmp/model-pool2 /tmp/model-pool4 \
  --output /tmp/model-comparison
```

This writes standalone SVG and PNG charts. Fixed pool comparisons guide later
batching, group parallelism, and scaling decisions. Automatic scaling is not
part of this iteration.

See the [recorded fixed-pool comparisons](../../docs/profiling/2026-10-18-model/README.md)
for measured throughput, queueing, RSS, recycling, and the narrow-operator limit.
