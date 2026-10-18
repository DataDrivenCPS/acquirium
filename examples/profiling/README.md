# Profile soft-sensor execution

See the [initial results](../../docs/profiling/2026-10-18/README.md) and
[native worker pool](../../docs/plans/escapement-python-workers.md).
The [implemented pool comparison](../../docs/profiling/2026-10-18-pool/README.md)
includes native profiles and end-to-end workload measurements.

Run from the repository root, using a **new** output directory:

```sh
uv run --locked python -m examples.profiling.run \
  --output /tmp/acquirium-profile --workers 1,2,4 --samples 5
```

Execution defaults to the native warm pool. Pass `--execution fresh` with a
different output directory for a fresh-process comparison. Both modes have
paired instrumented/control cases. Pool limits match the configured worker
count; native pool counters are saved in each case's result and summary.

For a short check:

```sh
uv run --locked python -m examples.profiling.run \
  --output /tmp/acquirium-profile-small --workers 2 --scenarios tiny --samples 2
```

This profiles the native Engine directly with the same Python calculations and
DAG shape as the Acquirium tank-temperature example. Inputs are resolved fixtures;
HTTP, graph compilation, producer journaling, and consumer checkpointing are
excluded. Use the [streaming workload](../streaming/README.md) to measure the whole
application. Production engine code and production workers are unchanged.

## What is measured

Each worker-count/scenario pair has an uninstrumented control and an instrumented
run. Each case's engine owner runs in a fresh Python process and a temporary native database.
Activation and a history-seeding warmup epoch are excluded; measured epochs apply
corrections to existing observations. Case order is reproducibly shuffled.
Every measured epoch is checked against independent full-history recomputation of
all source and derived results. Temporary engine databases are removed; reports
and diagnostic launchers are retained.

| Scenario | Tanks | Retained buckets | Samples/probe/bucket | Requests/measured epoch |
| --- | ---: | ---: | ---: | ---: |
| `tiny` | 2 | 1 | 1 | 1 |
| `batch8` | 2 | 8 | 1 | 8 |
| `wide8` | 8 | 1 | 1 | 1 |
| `dense500` | 2 | 1 | 500 | 1 |
| `history128` | 2 | 128 | 1 | 1 |

Each tank has two probes. `batch8` repairs eight buckets in one epoch, giving
eight groups to each operator in a single worker invocation. `dense500` changes
one sample per probe, requiring the complete dense bucket to be read and
recomputed. `history128` repairs only the first retained bucket, probing whether
unrelated history causes additional work. These are different workload shapes,
not an uncontrolled sweep of a single input-size variable.

Native reports measure epoch preparation, computation, publication, total time,
requests, peak operator concurrency, and per-operator wall time. Separate process
baselines measure a fresh interpreter's complete lifetime with `pass` and with
Polars/PyArrow imports; those are not startup-only timings.

## Actual worker instrumentation

Escapement's public `Engine(python=...)` option points at a diagnostic interpreter
launcher. The launcher receives the **actual embedded worker script** extracted
by the installed native engine. It checks that script's reviewed SHA-256, then
injects lightweight timing calls around these boundaries:

- standard-library, Polars, and PyArrow imports;
- calculation-module loading and static worker/schema setup;
- JSON context and Arrow input reading/conversion;
- each actual `(frame, context)` function call;
- validation, output conversion, and Arrow writing;
- final port metadata writing.

For pooled execution, imports and profiler preparation are attributed to the
first job of a process. Subsequent jobs get separate profiles with zero import
time and `cold_worker=false`. Warmup initializes the same artifact used by all
operators, so the measured epochs exercise warm reuse. Worker CSVs include PID
and cold/warm classification. Initial cold setup remains in retained seed files.

The calculation artifact, its native digest validation, input/output files,
schema checks, output ownership, scheduling, and publication remain the native
implementation. This is trusted-code instrumentation of the reviewed worker, not
a replacement executor or a copied simulation. The launcher refuses a changed
worker script until the instrumentation boundaries are reviewed and its digest
is updated. No sibling Escapement checkout is needed to run the profiler.

Launcher imports/AST preparation are measured separately as `wrapper_prepare`.
Some standard-library imports are already cached by the launcher; numeric
libraries are imported in the actual worker body. The difference between native
operator wall time and attributed worker stages is labeled **external and
unattributed**, covering Rust grouping/serialization/diff, process startup/exit,
five-millisecond completion polling, profiler report I/O, and other uninstrumented
work. It is not a pure process-startup or pure Arrow-exchange measurement.

Worker stages are means per invocation, including every group in that invocation.
Parallel operator durations cannot be summed to infer epoch latency. Epoch
comparisons use medians of the native wall-time reports. Instrumented/control
deltas reveal instrumentation overhead and sampling noise; negative deltas do
not mean the profiler speeds execution up. Warmup is excluded from summaries but
its worker files are retained for inspection.

## Artifacts and interpretation

The output contains `environment.json`, `summary.json`, `summary.md`, `epochs.csv`,
`workers.csv`, and a directory per case with native reports and raw worker profiles. Environment
data records interpreter/library/native versions, the installed Escapement Git
revision where available, calculation/launcher digests, logical CPU count, and
load averages. The machine is not isolated from background activity.

Generate standalone PNG and SVG figures after the run:

```sh
uv run --locked python -m examples.profiling.plot /tmp/acquirium-profile/summary.json
```

To compare pooled and fresh runs at two workers, render the pooled summary with
`--fresh-summary /tmp/acquirium-fresh/summary.json`; this also exports
`pool-comparison.png` and `pool-comparison.svg`.

Start by comparing uninstrumented `compute_ms` across worker limits. Then inspect
the actual worker stages to distinguish repeated imports, input/output handling,
and calculation cost. `total_ms_per_request` measures amortized epoch service
cost under controlled batching; it is **not** a request's observed end-to-end
latency. This harness does not measure queueing, sustainable arrival rates,
unbounded history, native-only operators, adaptive scaling, or security isolation.
