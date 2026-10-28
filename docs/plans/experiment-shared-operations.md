# Consolidate experiment semantic resolution, stream resets, and metadata reads

Status: implemented and verified (2026-10-22).

Follow-up to [experiment refinement](experiment-refinement.md) and
[experiment tightening](experiment-tightening.md). The implementation baseline
is commit `688faec`. Stages 1 and 2 address duplicated policy and persistence
behavior; stage 3 consolidates the remaining metadata hydration policy.

## Objective and scope

Give semantic value resolution, revisioned stream resets, and experiment document
hydration one implementation each. Extend the existing metadata and storage
mechanisms. Each stage must replace the overlapping implementations it takes over.

Preserve public Python APIs, HTTP payloads, saved documents, IDs, migration
history, and outbox formats. Keep artifact storage unchanged. Identical backend
row-frame helpers and a broader storage-protocol redesign are outside this pass;
document the transaction contract of the reset operation introduced here.

Snapshot the production files before implementation and report the final source,
test, and documentation deltas separately. Completion requires net fewer
production Python lines, counting new helpers and files. Exclude whitespace-only
changes from the claimed reduction. Also report the policies and execution paths
removed; relocating functions alone does not satisfy the objective.

## Stage 1: share semantic value resolution

Current overlap:

- `Declaration.resolve()` in `ExperimentDeclarations.py` resolves registered
  semantic attributes and preserves their scalar/list shape in pinned terms.
- `ExperimentMetadataGraph.validate()` separately checks resolvability for
  standalone/non-strict use and restores raw semantic spelling to the projection.
- `Client.metadata.plan_write()` handles URI detection, scalar/list values,
  resolver failures, and raw-value triples for ordinary metadata writes.

Extract the shared value-resolution operation beside the existing metadata
writer. Route declaration preparation and experiment graph preparation through
it, and make `plan_write()` use the same operation. Keep role applicability,
writable-attribute checks, and predicate selection owned by the shared writer.
Keep declaration field selection and persistence of `_resolved_semantics` with
the declaration code.

The shared result must let callers retain both canonical terms and original
spelling without another resolution pass. Use a small function/result shaped by
these callers; avoid a resolver registry or a new metadata-writing framework.
Remove the nested declaration resolver and the standalone resolvability loop
once their behavior is covered by the shared operation.

Preserve the distinct caller policies explicitly:

- Strict writes reject unresolved semantic values before SQL writes begin.
- Standalone/non-strict projection can omit an unresolved semantic field while
  retaining its original value in the complete document. Preserve existing
  whole-field behavior for lists with an unresolved member.
- Preserve scalar/list shape in pinned terms, null positions where already
  supported, and the projection's omission of null elements. Keep raw spelling,
  URI-valued inputs, and user metadata independent of semantic fields.
- Repeated declarations reuse saved resolutions. Recording and outbox replay
  use committed terms without invoking a live resolver. Resolution stays outside
  the SQL writer transaction, and client-supplied pins remain untrusted.

Verify ordinary metadata insertion, hand/model declaration equivalence, aliases
and full URIs, list/null cases, non-strict unresolved values, invalid attribute
roles, source-owned overrides, and restart/replay without a resolver. Extend the
existing metadata, adapter, and refinement tests for any uncovered differences.

## Stage 2: share revisioned stream resets

`DuckDBStore._replace_frame()` and `TimescaleStore._replace_frame()` delete a
stream's samples and write its reset marker. `ExperimentStore._delete_records()`
repeats that sequence for the streams belonging to deleted runs.

Introduce one shared reset operation in the storage/publication layer. It takes
the caller's open connection and a collection of stream reference URIs, physically
removes their samples, and advances their reset markers at one global revision.
Return the revision so replacement can insert its new frame at that same revision.
Use the existing revision allocation and SQL execution mechanisms, with only the
necessary backend adaptation.

Route both backends' replacement methods and experiment cleanup through it.
Remove their duplicated sample-delete/reset-marker SQL. Public replacement
methods retain their writer lock and transaction wrapper. Experiment deletion
retains its existing outer transaction covering observations, run state, artifact
ownership, stream registrations, and queued graph operations.

Keep ownership and reporting responsibilities explicit:

- The reset operation does not commit, acquire an independent write connection,
  perform graph I/O, or delete stream registrations and experiment records.
- Replacement keeps its registration and remains a reset even when the new
  frame is empty or there were no previous samples. An empty collection of
  references is a no-op. Deduplicate references within a batch.
- Experiment cleanup selects only owned streams, retains external-reference
  guards, and keeps the existing live-sample removal counts. Physical deletion
  still removes tombstoned rows as well.
- Preserve one revision per replacement or deletion batch, reset-frontier
  semantics, stream-version behavior, timestamp handling, and downstream
  materialization invalidation. A failure rolls back samples, revisions, reset
  markers, and the caller's other SQL changes together.
- Artifact reattachment protection, post-commit file cleanup, metadata delivery
  locks, and outbox clear ordering remain with their current owners.

Verify normal and empty replacement, multiple streams in one deletion, rollback
after reset and before completion, exact removal counts, reference guards, and
materialization invalidation on both SQL backends. Reuse the publication,
time-series, experiment deletion, and refinement contracts; add a batch/rollback
case only where existing coverage does not exercise the new shared boundary.

## Stage 3: share metadata hydration

`ExperimentMetadataGraph.read()` and `ExperimentMetadata.read_observation_metadata()`
both assemble subject URIs and remove reserved lifecycle fields. Ordinary reads
and observation reads also check required document fields separately.

Put one batch-hydration operation on `ExperimentMetadata`. Accept requests keyed
by object kind and identifier so IDs from different kinds remain distinct. Under
the existing delivery lock, synchronize once, read all requested subjects through
`read_documents()` once, validate the required identity fields, and extract user
metadata with one field-filtering implementation.

Make ordinary reads, lifecycle-row hydration, and observation hydration consume
that result. Observation hydration must combine run and variable subjects in the
same batch. Remove the separate writer-side read implementation and the duplicate
presence checks and field extraction. Retain needed public forwarding methods;
they must delegate to this single policy.

Preserve empty-selection behavior, duplicate input rows and their order, legacy
UUID-to-URI mapping, exact JSON values and semantic spelling, source precedence,
and the existing result field names. Missing authoritative documents and graph
delivery failures still fail strict reads. Do not introduce a SQL metadata
fallback or reconstruct documents from flattened query projections.

Verify mixed run/variable batches, repeated identifiers, empty selections, missing
documents of either kind, interrupted delivery, graph edits after declaration,
and HTTP hydration of each result batch once. Use the existing document and
experiment metadata tests; measure backend reads only to verify the batching
contract, rather than asserting internal helper call sequences.

## Delivery and final verification

Implement the stages as separately reviewable changes in the order above. Run
focused tests after each stage, then verify the combined result with:

- The full unit suite.
- Experiment, migration, outbox, publication, and time-series contracts on
  DuckDB and a real TimescaleDB, without skipped PostgreSQL cases.
- Both local HTTP model integration tests and the runnable whole-model guide.
- A production line-count comparison and a check that the superseded resolver,
  reset SQL, and hydration implementations have actually been removed.

The current verified baseline is 1,273 unit tests, 110 backend contracts, both
model integration tests, and the guide. Report actual final results and update
this plan with the removed duplication, code-volume delta, and any remaining
limitations. This pass adds no endpoints, schema migrations, caches, or artifact
storage changes.

## Implementation results

The three stages now use the existing metadata writer, revision publisher, and
metadata delivery owner:

- `Client.metadata.resolve_values()` owns URI detection, scalar/list validation,
  null preservation, resolver failures, and raw spellings. Declarations pin its
  canonical values; `plan_write()` consumes those pins without resolving again.
  Non-strict projection omits unresolved whole fields in the same pass. The
  declaration's nested resolver, the graph adapter's resolvability preflight,
  and its separate raw-spelling reconstruction were removed.
- `RevisionPublisher.reset()` owns physical sample deletion and reset-marker
  updates for both backends, experiment deletion, and materialization full
  rebuilds. Review found that fourth consumer while implementing stage 2; it
  supplies its existing publication revision so all outputs retain one frontier.
  The operation uses the caller's connection and transaction. Registrations,
  deletion counts, ownership guards, outbox writes, and artifact cleanup retain
  their existing owners. Reset SQL now has one implementation.
- `ExperimentMetadata._hydrate()` loads requests keyed by kind and ID under the
  existing delivery lock. Ordinary and observation reads share identity checks
  and reserved-field filtering. The writer-side reader and separate observation
  hydration policy were removed. Run and variable documents still share one
  backend read, including duplicate rows and legacy UUID identifiers.

Production Python decreased from 29,177 to 29,170 physical lines: **7 fewer**.
Nonblank production lines decreased from 25,411 to 25,399: **12 fewer**, so the
reduction does not depend on deleting whitespace. This is a small volume change;
the substantive consolidation is the removal of duplicated policies and SQL.
Tests gained 167 lines covering semantic lists/nulls, saved pins, non-strict
resolution, batch reset/deletion rollback, tombstones, mixed-kind hydration,
duplicate rows, empty selections, and missing authoritative documents.

Documentation gained 59 lines relative to the proposed plan; the new plan is
216 lines relative to commit `688faec`.

Verification completed:

- Full unit suite: **1,281 passed**.
- Combined backend contracts: **144 passed**, including DuckDB and real
  TimescaleDB, with no PostgreSQL cases skipped.
- After sharing materialization's full-reset path, its focused unit suites:
  **61 passed**.
- Both local HTTP model integrations: **2 passed**.
- The Python example extracted from `record-a-whole-model.md`: completed with
  its Pyomo declarations, scenarios, queries, and assertions intact.
- `git diff --check`: clean.

The initial combined backend runs encountered a TimescaleDB background
compression deadlock during cleanup of historical test samples. PostgreSQL logs
identified the competing compression worker, and the affected test passed alone
without production changes. The contract fixture now disables automatic
compression scheduling only for its temporary schema before inserting samples.
Production compression settings are unchanged; the full contract rerun includes
the materialization invalidation and recovery contracts on both SQL backends.

Public APIs, stored formats, and artifact storage are unchanged. The deferred
database-artifact proposal, backend row-frame helpers, and broader protocol
redesign remain outside this pass.
