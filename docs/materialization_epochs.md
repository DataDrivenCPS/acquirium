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

Each node/range work item stores the raw input version vector and the upstream
work IDs that constitute its dependency frontier.  Execution writes only to
`topology_epoch_outputs`, an epoch-private staged overlay.  A commit is
accepted only for the live claim and current epoch, with unchanged raw input
versions and committed upstream frontier.  A stale worker may finish its
process, but it cannot commit to canonical storage.

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
If inputs advance before a frontier seals, the component coalesces all changes
since its last sealed vector into a replacement frontier and fences older work.

Compaction follows reachability, not graph revision: any superseded epoch not
named by the candidate, current, or active pointer can lose its resolved
topology and private execution state. Epoch identity and canonical publication
receipts remain. Compaction never changes canonical streams.
