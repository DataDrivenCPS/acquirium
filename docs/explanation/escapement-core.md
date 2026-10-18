---
title: Escapement core
---

Escapement is the sole timeseries store and execution engine. A server owns one
native Engine and background Processor backed by `escapement.duckdb`. Its durable
inbox records accepted mutations and plan activations. Each epoch applies a
contiguous request prefix, evaluates affected groups in dependency order, and
commits source/derived values, version history, progress, and provenance together.
Readers see a complete published snapshot throughout computation.

Acquirium owns Oxigraph/ontology inference, stream metadata, semantic query
compilation, unit resolution, and transport. The stream lookup is rebuilt from
RDF, not another SQL registry. `apps.json` records desired declarations and an
activation request identity using synchronized atomic file replacement. The
engine records accepted and active plans; Acquirium does not store independent
execution frontiers or repair queues. Restart reconciles saved intent with the
native active plan and replays unapplied accepted work.

Each deployment compiles all app declarations into a complete native plan.
Structural output metadata is resolved until downstream selections stabilize;
non-stabilizing declarations fail. A plan ID identifies immutable content, while
each activation attempt has its own retry identity. Returning to an earlier plan
therefore activates it anew. Engine validation rejects cycles and conflicting
output ownership.

Graph transactions and timeseries epochs are separate. App inspection identifies
the source graph generation used to compile the plan; it does not promise an
atomic graph/data transaction. DataObject captures one published epoch for its
multi-stream fetch, and each paged scan reads a pinned snapshot or explicit epoch.

HTTP ingestion reports acceptance and publication separately. Clients may wait
for a receipt or inspect the native accepted/applied frontiers. A timeout after
acceptance leaves work pending. A failed function publishes none of its epoch and
pauses the processor, including unrelated branches. Repair and retry are explicit;
there is no second Acquirium scheduler attempting to bypass the failed prefix.

`GET /engine/changes` exposes the native signed feed with an exclusive cursor,
captured publication cut, and complete-epoch frontier. It uses native history;
there is no second server-side feed ledger. Consumer-owned sink updates and
checkpoints must commit together, buffering any epoch split across pages. The
[streaming workload](../../examples/streaming/README.md) exercises durable source
delivery, asynchronous publication, complete-epoch consumption, and restart.

The initial contract supports finite numeric observations, explicit deletes,
pointwise/fixed-bucket functions, UTC nanoseconds, and half-open ranges. Each
request is bounded by native admission limits (including at most 10,000 input
rows). Chunk larger imports explicitly; chunks have separate atomic acceptance.
The retry ledger retains a bounded set of recent request identities.

The current native engine keeps current relations in memory and does not prune
its historical change feed. Stream-statistics reads currently scan a pinned
relation. Retention, larger-history storage, native statistics, richer windows,
and typed values should be developed in Escapement rather than recreated in
Acquirium.
