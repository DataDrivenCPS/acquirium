# Topology-epoch materialization

Acquirium materialization is driven by an immutable resolved topology epoch.
An epoch is identified by the published graph revision, graph digest, and the
definitions selected by named transformation deployments plus their pinned
state-revision identities. Definitions are immutable; deployments are the
small mutable map that selects one definition generation per name. Construction is a
control-plane operation: it evaluates late-binding selectors once, persists the
resolved binding inputs/outputs/metadata, records definition and state-revision
identities, validates global output ownership, and stores a directed acyclic
dependency graph.  Workers receive only these persisted rows.

The executable digest covers the entrypoint's complete Python module. Both the
deployment boundary and every fresh worker load recompute and verify it, so a
mutable import path can never silently execute code different from the selected
definition. Binding resolution is declarative; topology construction does not
run an unversioned resolver callback.

## State machine

```text
constructing -> ready -> reconciling -> active
       |          |          |            |
       +----------+----------+------------+--> superseded -> compacted
                         \
                          +--> failed
```

`constructing` is a candidate while the graph is resolved. It does not replace
the current desired topology until resolution and global DAG validation both
succeed. An epoch with work is
`reconciling`; an epoch with no retained input work is immediately sealable.
Newer desired epochs supersede every older desired epoch.  Supersession changes
eligibility only: old canonical rows remain visible until a newer component
seal publishes a complete replacement.

The dependency DAG rejects self-cycles, cycles, and ambiguous output owners.
Weakly connected DAG nodes form a visibility component.  This deliberately
makes the component boundary include every dependency path, so a visible
component cannot contain a mixture of old and new epoch outputs.

## Claim contract

`topology_epoch_claims` is the one ownership mechanism for construction,
reconcile work, and component sealing.  A claim has `(kind, target_id)`, an
owner, a monotonically increasing attempt, and an expiry.  Claim acquisition is
atomic; a live claim suppresses duplicates, and an expired claim is reclaimed
with the next attempt. Long-running execution renews its lease while computing.
Failures use bounded exponential backoff; fresh work sorts ahead of retries, and
a repeatedly failing partition becomes terminal instead of starving the queue.
Multiple identical workers may claim independent partitions concurrently.
Claims provide liveness and duplicate suppression only.
They do not encode desired state or correctness.  All durable transitions are
idempotent and validate the immutable epoch ID, binding digest, persisted input
version vector, and upstream frontier at commit time.

## Reconciliation and sealing

Each node/range work item stores the raw input version vector, its input
`read_interval`, and the upstream work IDs that constitute its dependency
frontier. Execution writes only to `topology_epoch_outputs`, an epoch-private
staged overlay. A commit is accepted only for the live claim and current epoch,
with no raw input change intersecting the work's `read_interval` and with a
committed upstream frontier. A newer raw version outside that interval does
not invalidate pointwise work for an earlier event-time range. If exact change
history is unavailable, validation falls back to conservative invalidation. A
stale worker may finish its process, but it cannot commit to canonical storage.

### Event-time invalidation

The raw stream version vector is a discovery cursor, not itself a staleness
condition. It tells the scheduler which publications must be examined. When a
stream advances, the scheduler resolves those versions to changed event-time
ranges and compares them with the work's input `read_interval`:

| Change | Pointwise work | Windowed work |
| --- | --- | --- |
| New value outside the work's read interval | Keep the result | Keep the result |
| New value inside the work's read interval | Recompute | Recompute |
| Correction or delete inside the read interval | Recompute | Recompute |

For pointwise work at timestamp `t`, the write and read ranges are the single
event-time point around `t`. A lookback, look-ahead, or window policy expands
the read range to include every input the transform can observe. Thus a later
append normally does not invalidate an earlier pointwise result, but it does
invalidate a windowed result when it falls inside that result's input halo.

Canonical rows retain the stream version that last wrote them, allowing normal
publications to be resolved to exact event-time points. Tombstones are included
in this lookup, so deletes and corrections are treated like any other change.
If those canonical version markers are unavailable—for example after history
has been compacted—the bucketed `stream_change_ranges` manifest is used as a
safe, conservative fallback. The fallback may invalidate more work than
necessary, but it never silently accepts an unverified result.

A component seal is one transaction.  It verifies that every component work
item is committed, locks the current epoch pointer, constructs range
replacement mutations from the staged overlay, and invokes the existing
canonical publication protocol.  Only then does it mark the component sealed.
The final component seal activates the epoch.  Publication receipts remain
idempotent and are the only canonical stream mutation path.

## Data frontiers and retention

Canonical input publications append range-manifest work to the current epoch;
the topology and its resolved bindings do not change.  A data frontier creates
new deterministic work IDs and re-seals the affected dependency component.
The active epoch pointer therefore always names the latest fully visible
topology, while each component's monotonically increasing frontier advances as
canonical data arrives. A seal publishes exactly one component frontier. Once
published, its staged rows and work records are discarded; the compact input
version vector retained per binding is sufficient to derive the next frontier.
If inputs advance before a frontier seals, overlapping changes coalesce into a
replacement frontier and fence older work. Disjoint changes leave completed
work eligible to seal first; the next frontier then owns only the newer ranges.

This separates two cases that can occur in an open-load workload:

1. A worker can commit an earlier range after a later, disjoint append has
   arrived. The newer stream version does not make the earlier result stale.
2. If a new change overlaps existing frontier work—or affects it through a
   downstream impact policy—the current frontier is replaced and its old work
   is fenced. The worker may finish, but its result is discarded and the
   replacement frontier recomputes the affected range.

Consequently, continuously appended streams can make progress while
materialization is running. Batch uploads of historical data follow the same
event-time rule: the uploaded event-time ranges become dirty, while corrections
or deletes in already materialized history invalidate the affected ranges. If
only the conservative manifest is available, its bucket ranges may dirty a
larger safe interval.

Compaction follows reachability, not graph revision: any superseded epoch not
named by the candidate, current, or active pointer can lose its resolved
topology and private execution state. Epoch identity and canonical publication
receipts remain. Compaction never changes canonical streams.
