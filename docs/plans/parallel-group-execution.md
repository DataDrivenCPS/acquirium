# Concurrency across sensor pipelines

Decision: keep concurrency across independent sensor/equipment pipelines as the
default. Defer splitting one operator's temporal groups until a representative
workload demonstrates that it limits useful concurrency. No new runtime
scheduler or group-execution API is introduced by this investigation.

## Two different meanings of group

Acquirium's semantic selection group is an entity such as a tank or device.
The app compiler turns each selected entity into a separate native operator.
Many devices therefore already give Escapement independent ready operators to
schedule concurrently. A soft sensor can consume several physical probes;
the independent entity is not necessarily one physical sensor.

Escapement's temporal group is one timestamp or fixed time bucket within an
operator. Currently, all affected temporal groups of an operator are passed to
one Python job and processed serially. Parallel group execution would split
these timestamps/buckets, rather than create concurrency between devices.

An input passing through its device's DAG is a useful logical unit of work.
Physically, the current native scheduler runs ready operators within one epoch;
it does not assign an entire DAG traversal to one process. Multiple stages may
lease different warm processes. Independent branches overlap, shared joins wait
for all parents, and the complete epoch publishes atomically. Publication epochs
themselves do not overlap.

## Preserve shared dependencies

A calibration/conversion chain for one device can be independent of another
device's chain. A tank average combines probes, and a plant alarm may combine
several tanks. Those are joins in the shared DAG. Treating each raw reading as
an isolated, complete DAG execution could duplicate shared work or omit inputs
needed by the same timestamp/window. Corrections and deletes must also propagate
to all affected dependent results, including retractions.

Keep the dependency graph and epoch semantics in Escapement. Use semantic
entity grouping to expose independent device work. There is no need to introduce
another Acquirium task queue or one executor for each sensor.

## Measurement correction

The initial narrow model benchmark set native `workers` equal to `units`.
Its one-operator run therefore also had one scheduler slot. It demonstrated the
configured bottleneck but did not independently isolate operator width from
the native worker limit. The harness now accepts `--workers` separately and
records the effective setting. The corrected diagnostic uses one operator,
four native scheduler slots, and a four-process pool cap; its evidence is in
[narrow-workers4](../profiling/2026-10-18-model/narrow-workers4/).

That diagnostic tests a particular narrow, timestamp-heavy workload. It is not
evidence that a deployment with many independent device pipelines needs group
splitting. The four-operator comparisons remain valid: they held scheduler
concurrency at four while changing only the resident pool cap.

## Next representative experiment

Measure many independent device pipelines with several stages per device and
an explicit shared aggregate/alarm downstream. Keep the native worker and pool
limits fixed while increasing device count. Include corrections/deletes and
verify per-device and joined results independently. Measure completed source
requests, publication latency, active workers, pool waiting, and memory; a source
request is not equivalent to a Python job.

If ready independent operators keep the configured workers occupied, temporal
group splitting adds complexity without exposing more execution capacity. If
a few operators accumulate large batches while workers remain available, that
is the case for a bounded group-execution implementation.

## Implementation shape if needed

Prepare each operator's affected inputs once, dispatch bounded chunks through
the native scheduler's shared worker budget, then assemble and validate its
complete output before releasing dependent operators. Operator and chunk work
must compete for the same global slots; nested per-operator thread pools would
multiply outstanding work and complicate fairness.

Keep group identity and deterministic result order, enforce input/output limits
over the whole operator, and retain atomic publication. On failure, stop issuing
chunks, join all in-flight work, then let the engine reset the worker pool and
retry the unpublished prefix. Immutable models may be replicated; stateful
operators still need a separate checkpoint/replay contract. Chunk size should
amortize Arrow/control overhead and be measured before choosing a default.
