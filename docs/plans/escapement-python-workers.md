# Persistent Python workers in Escapement

Initial fixed pool implemented on 2026-10-18. Bundle/environment identities,
group chunking, adaptive scaling, and stateful operators below remain proposals.
The [execution profile](../profiling/2026-10-18/README.md) justifies this as the
native engine change: repeated imports dominate the example calculations.

The first implementation grows on demand, keys processes by module digest and
interpreter path, reuses them across entrypoints/epochs, and bounds residents,
job count, and age. `workers` controls active operator jobs; `python_pool_size`
defaults to that count and can bound residents separately. Zero provides a fresh
process control for profiling. `python_worker_stats()` reports reuse, recycling,
and occupancy. Failure discards uncertain workers and resets the pool before retry.
Full bundle/environment identity is still a subsequent extension.
The [pool comparison](../profiling/2026-10-18-pool/README.md) records correctness,
warm execution, and steady/restart workload results.

## Operator semantics

Keep the existing pure calculation contract as the default:
`f(frame, frozen_context) -> outputs`. Results must depend only on declared
inputs, context, and the immutable calculation release. Process lifetime,
invocation order, and replica selection must not change observable results.
Retaining an immutable model or a semantically transparent cache is compatible
with this contract. Reading a changing external service, wall clock, or mutable
global counter is not. Pin or declare random seeds when randomness is required.

Purity is a declared contract, supported by reuse/replication tests; a Python
heap hash cannot establish it. A cache can change the heap while preserving pure
results. An external dependency can change results while the heap stays the
same. Native extension memory, file descriptors, threads, and sockets make
general process-state hashing unsuitable for correctness decisions.

Use content hashes to identify immutable releases instead. Extend Escapement's
current single-file SHA with a bundle manifest covering helpers and model assets,
an environment/dependency identity, and a worker protocol version. Validate these
before leasing a worker. This supplies reproducible identity, not proof of purity.

Stateful algorithms need a distinct, future operator contract, for example
`f(state, frame, context) -> (new_state, outputs)`, with a partition key and a
versioned state schema. Escapement must own checkpoints in its DuckDB store and
commit state and outputs in the same epoch. Corrections, deletions, and late data
require restoring an earlier checkpoint and replaying affected input. Serialize
work within a state partition; parallelize independent partitions. An arbitrary
live Python heap is not a durable checkpoint. Stateful support should follow the
pure pool, after checkpoint/replay semantics are implemented and tested natively.

## Native pool and API direction

Escapement owns the pool, leases, deadlines, and failure handling. Acquirium keeps
compiling semantic plans into native operators. There is no additional Acquirium
work queue, computation scheduler, or execution database.

An illustrative release descriptor contains `bundle_digest`,
`environment_digest`, `entrypoint`, `protocol_version`, and `semantics="pure"`.
Proposed engine settings separate `max_active_jobs` from `max_resident_workers`
and a warm-worker target. Preserve the existing `workers` setting as the initial
active-job limit rather than add overlapping controls immediately. Names and
constructor placement should follow the native API when implemented.

Pool instances by bundle, environment, and protocol identity, routing an
entrypoint with each job. The current example's three functions share one module,
so two resident copies could run both tank means, then hottest, then alarm.
Separate bundles/environments require separate instances; resident memory needs
its own bound even when active concurrency is small. A shared bundle is the
trusted isolation boundary. Reject incompatible identities rather than mutate
an existing process in place.

Each process imports once, handshakes with the engine, and moves between idle,
leased, and dead states. Permit one job at a time per process. Each job carries
its operator/epoch identity, frozen context, entrypoint, and isolated scratch
directory. Begin with existing Arrow files and a framed control channel; changing
transport can be a later measured optimization. Keep native artifact validation,
output/schema checks, timeouts, output/log bounds, and process reaping. Bound
lifetime/job count and recycle workers if memory grows.

The native inbox and epoch remain authoritative. A lease ID is only an in-memory
correlation token. A timeout or crash discards the worker and fails the epoch;
the publication frontier stays unchanged. Existing explicit retry reruns the
unpublished prefix with fresh workers. Never reuse a worker with uncertain
protocol state or publish partial derived results.

Initially reuse workers across operators and epochs with the existing native
operator scheduler and a fixed resident limit of two for this example. Workers
initialize on first demand rather than prelaunch. A subsequent
native extension can split independent affected groups of a pure operator into
bounded chunks and lease several replicas. Preserve group identity and stable
result assembly, then publish only when every required chunk succeeds. Parallel
groups cannot shorten dependencies between mean, hottest, and alarm. This work
does not introduce overlapping publication epochs.

## Scaling after warm execution is measured

Borrow warm environments and immutable function versions from FaaS practice.
[Lambda's lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html)
describes retaining initialization across invocations. Here the engine explicitly
owns worker lifetime and serial leasing. No separate FaaS deployment platform is
needed for an embedded engine pool.

Measure cold/warm jobs, queued runnable jobs, queue age, active/resident workers,
restart counts, memory, and full job service time. The latter includes Arrow I/O,
calculation, and validation, not just the function call. Source requests coalesce
into epochs and fan out into different numbers of jobs, so source backlog alone
cannot size the pool.

A starting feed-forward estimate is
`desired = ceil(job_arrival_rate * mean_warm_service_time / target_utilization)`.
This assumes comparable independent jobs; heterogeneous jobs need per-class
estimates. Smooth both estimates, include queue-age feedback, and clamp demand to
available DAG parallelism, memory, and configured bounds. Scale up with limited
steps; scale down after a longer quiet interval, retaining a minimum warm pool.
Apply hysteresis and account for workers still initializing to avoid oscillation
and repeated cold starts. If arrival exceeds the bounded service capacity,
admission backpressure still applies.

[Knative's concurrency controls](https://knative.dev/docs/serving/autoscaling/concurrency/)
distinguish a target from a hard limit; its
[stable and panic windows](https://knative.dev/docs/serving/autoscaling/kpa-specific/)
illustrate smoothing and faster burst response. Those are useful design
principles, not proposed runtime dependencies. Start with fixed limits, compare
measured warm performance, and then tune a small controller through burst and
recovery experiments. A PID controller is unnecessary until simpler feedback
has demonstrated a specific limitation.

## Implementation status and follow-ups

1. Implemented the native framed protocol and fixed bounded pool, preserving current
   pure function signatures and Arrow schemas.
2. Verified repeated invocation, release separation, crashes, timeout/reaping,
   malformed output, retry, shutdown, and job/age recycling. Chain results match
   independent recomputation across fresh processes, warm reuse, and replicas.
3. Reran native profiles and the end-to-end workload, including historical repair
   and restart. Warm/cold timings, latency, backlog, and server RSS are recorded;
   runnable-job queue age and per-worker RSS remain instrumentation work.
4. Next, add group chunking if wide operators are a measured bottleneck, then bounded
   adaptive scaling if variable load warrants it.
5. Add explicit stateful operators only with native checkpoint, atomic commit,
   and correction replay support.
