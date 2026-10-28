# Refine experiment declarations, recording, and metadata

Status: implemented (2026-10-22). All five stages are complete.

Implementation owners are `ExperimentDeclarations.py`, `ExperimentValues.py`,
`ExperimentMetadata.py`, and `ExperimentStore.record_batch()`. Saved object
results assemble user values directly from documents. Clients use one recording
request; compatibility observation endpoints adapt into the same operation.

Verified with 1,271 unit tests, 103 migration/outbox/experiment/publication/
time-series contracts across DuckDB and a real TimescaleDB (no skipped
PostgreSQL cases), both local-server model tests, and the runnable model guide.
Contracts include failure rollback, replay, upgrade conflicts/interruption,
source ownership, non-UTC PostgreSQL, and artifact reattachment.

## Objective

Give declaration preparation, recording, and metadata consistency one owner
each. Remove alternate execution paths and compensating code as those owners
take over their responsibilities. Keep the explicit adapter interface and its
input/output namespaces.

The main simplification is one declaration representation and one recording
operation. Moving methods between files alone does not satisfy this plan.

## Target ownership

| Concern | Owner |
| --- | --- |
| Reading model values and interpreting model indices | Adapter readers |
| Declaration validation and normalization | Shared declaration preparation |
| JSON conversion and artifact/sample preparation | Shared value preparation |
| Stream references, sample revisions, observation ordering, run state, artifact ownership | Experiment recording transaction |
| Descriptive documents, RDF projections, outbox delivery, graph freshness | Metadata component |
| HTTP encoding, decoding, and error mapping | Transport handlers |

SQL remains authoritative for the recording ledger. The graph remains
authoritative for descriptive metadata. Complete documents preserve arbitrary
JSON; RDF projections support filtering and resolved semantic values.

## Behavior to preserve

- Keep `adapters.define()`, the thin Pyomo builder, input/output namespaces,
  output as the default, `record_values()`, and `sample(at=..., model_time=...)`.
- Keep model time separate from recorded timestamps. Preserve reader freezing,
  explicit indexing, unit-conversion checks, and labeled reader errors.
- Preserve saved study/variable/run IDs, stream reference URIs, typed keys,
  timestamps, recorded values, intervals, annotations, and artifact associations.
- Keep arbitrary string-keyed JSON, nested nulls and empty containers, and
  original semantic spelling. Preserve source ownership and the existing
  distinction between user-field overrides and additive semantic assertions.
- Preserve exact document matching for `metadata={...}` and projected-attribute
  filtering for ordinary attribute expressions. They have different semantics;
  a refactor must not silently substitute one for the other.
- Preserve revision tracking, stream versions, materialization invalidation,
  timezone behavior, and deletion/reset semantics on both SQL backends.
- Preserve strict experiment metadata reads and the existing explicit
  fresh/eventual distinction for general graph queries.
- Keep graph flush before outbox acknowledgment, replay after interruption,
  missing-graph detection, shared delivery serialization, and artifact ownership
  checks through file removal.

Two intentional changes accompany the refinement:

1. Variable user metadata gets one public interpretation, regardless of how the
   variable was declared. Semantic and source fields have explicit properties;
   `.metadata` contains the user object. Legacy hand-declared metadata
   presentation and query paths must be documented and converted explicitly.
   Run metadata, including conveniences such as `where(scenario=...)`, retains
   its current contract.
2. Experiment recordings use one recording request. SQL sample writes and
   observations commit together. A rejected batch no longer leaves new samples
   behind because a later observation request failed.

## Stage 1: establish the metadata boundary

Introduce one small metadata component around the existing writer, document
reader, and outbox implementation. It owns graph delivery locks, generation
checks, flush/acknowledgment ordering, and document hydration. It accepts
resolved mutations using the caller's SQL transaction and exposes batched reads
with the required freshness.

Make backend dependencies explicit. Require the document-read, graph-write,
clear, and durable-flush capabilities used by production. Supply an explicit
standalone factory with a close/lifetime contract for direct ledger callers;
the ledger constructor must not choose a backend or resolver policy implicitly.
Tests inject their backend explicitly.

Consolidate timestamp parsing, portable SQL execution, and native document
scanning where the implementations are identical. Move pure graph-record
construction out of `ExperimentStore`, so migrations do not import the client
and ledger implementation to construct metadata records.

Delete as part of this stage:

- Metadata hydration repeated by HTTP handlers after ledger methods hydrate it.
- The always-empty `unpublished` handling, while retaining strict delivery
  failures and any transport response field needed for compatibility.
- Production query-client capabilities used only by test helpers; keep such
  helpers in tests or use a shared, actually needed backend adapter.
- Unused deletion result fields, `DeclaredVariables._role`, constants/imports,
  and the runtime `SELECT NULL` placeholders for removed metadata columns.

Acceptance: existing metadata, deletion-race, outbox, and upgrade contracts pass
without changing their observable behavior. HTTP reads hydrate a batch once.

## Stage 2: normalize all variable declarations

Define one internal declaration record containing identity, label, role, kind,
semantic fields, index keys/observed links, source information, and the user
metadata object. Keep fixed fields explicit. Arbitrary user JSON remains under
the metadata namespace.

Both hand-declaration builders and model adapters produce that record through
the same normalization functions. Custom `ModelAdapter` implementations still
pass through validation. Resolve semantic identifiers during declaration
preparation; carry canonical values with the declaration rather than in
parallel `_units`/`_quantities` maps. Preserve original spelling separately.

Convert legacy hand-declaration fields using one documented mapping: known
semantic/index/source fields map to explicit fields; existing namespaced JSON
is preserved; other descriptive fields become user metadata. Merge identical
duplicate values. If an old descriptive field conflicts with an existing
namespaced value, abort the upgrade before mutation, preserve the original data,
and identify the fields requiring repair. Never silently overwrite either
value. Update declaration idempotency comparisons to use the normalized record.

Upgrade existing graph documents through the metadata component:

1. Validate the graph generation and drain pending operations in their old
   representation before reading declarations.
2. Read authoritative declaration documents outside the SQL writer transaction,
   serialized with metadata mutations. Reuse existing resolved RDF terms;
   retain unresolved legacy spelling without guessing a replacement.
3. Prepare normalized documents and resolved projection changes.
4. Atomically enqueue idempotent updates and their scheduled format/version
   marker in SQL. Completion depends on the existing delivery frontier reaching
   the scheduled updates; scheduling alone is not successful delivery.
5. After restart, replay pending upgrades before exposing normalized reads.

Use the existing append-only migration runner for SQL changes and the existing
outbox/state for the graph upgrade. Keep previous migrations unchanged. A
missing acknowledged graph remains a restore error. Existing outbox formats
must remain readable until pending entries have been drained or normalized.

Delete as part of this stage:

- Adapter-origin checks and format-1/format-2 branches in normal recording code.
- `Study._metadata` and other mirrors of fields already held by declarations.
- Repeated descriptor validation implemented separately by the builder and
  `declare_from()`; both call the shared validator.
- Circular imports from adapters into private value helpers in `Experiments`.

Acceptance: hand and model declarations normalize identically; legacy fixtures
upgrade without losing metadata or changing identifiers; interrupted upgrades
resume; redeclaration and unit checks still work without a resolver on replay.
The intentional `.metadata`/query-path changes have explicit before/after tests
and documentation.

## Stage 3: assemble document results directly

Restructure `metadata_objects()` so complete documents provide user-value
shapes. Apply selection, exclusion, and casts once to those values. Use query
results for node matches, aliases, relationships, IDs/labels, and resolved
semantic attributes.

Fetch documents once per result batch. Reconstruct flattened attributes only
for fields without a saved document; keep that legacy/raw-RDF fallback isolated.
Avoid constructing a projected user subtree merely to replace it with its
document. Preserve query relationship multiplicity and group array projections
without multiplying object results.

Delete the document-backed reconstruct/overwrite path and its duplicate path
handling. Retain shared list-aware selection and exclusion rules, including
numeric indices, null positions, and casts.

Acceptance: existing object/table tests pass, including graph-only attributes,
mixed document/RDF nodes, multiple aliases, lists, nested empty values, explicit
indices, wildcard selections, drops, and semantic attributes. Object and table
filters retain the same effective user-field values and source precedence.

## Stage 4: implement one server recording transaction

Add an experiment `record_batch` operation for scalar/JSON/text/log values,
artifact bytes, sample rows, and references to existing stream intervals. Use a
small discriminated payload so each entry has one recording form. The server
derives owned source IDs and stream identities from the run, variable ID, and
typed key; display labels never determine storage identity.

Separate value preparation from execution. Read and validate every selected
model value before sending a recording request. Prepare table bytes once per
recording operation. Server validation remains authoritative.

Expose connection-scoped internal primitives for stream-reference upserts and
revisioned sample publication in both storage backends. Existing public stream
and publication methods wrap those primitives with their own transactions;
experiment recording composes them using its transaction. Do not call methods
that independently commit from inside the recording transaction.

The server operation must:

1. Prepare and validate input, declaration semantics, and graph mutations before
   holding the SQL writer lock. Coordinate metadata delivery before taking that
   lock; never wait for inference or a resolver while holding it.
2. Recheck run state and variable ownership within the writer transaction.
3. Upsert owned stream references, write samples through canonical revision
   primitives, register artifact ownership, append ordered observations, and
   enqueue resolved graph mutations in that same transaction.
4. Commit and return ordered observation receipts and stream identities.
   Subsequent graph delivery follows the existing durable outbox contract.

Stream metadata mutations target run-owned source graphs, so delivery needs
explicit internal graph targets. Preserve the default target of old pending
events. Coalesce updates only where operation ordering is preserved: source
clears are barriers and cannot move across later registrations or mutations.
Graph delivery must not resynchronize stream registrations through a second,
independently committed SQL write.

The atomic guarantee covers SQL state and durable graph intent. Artifact files
and the graph are separate stores. Keep content-addressed file handling and
orphan recovery, and keep graph replay after a successful SQL commit. Do not
claim exactly-once HTTP retries; retain current receipt semantics unless a
separate idempotency requirement is approved.

Acceptance: inject failures at stream registration, sample insertion, artifact
registration, observation insertion, and outbox enqueue. No newly recorded SQL
state survives a failed batch. Graph/flush/ack failures after SQL commit leave
replayable intent. Verify both SQL backends, non-UTC PostgreSQL, stable stream
identities, deletion resets, materialization revision behavior, and the artifact
reattachment race.

## Stage 5: route clients through the single recording path

Make ordinary variable `record()` and existing convenience methods, adapter
`record_values()`, and adapter `sample()` prepare inputs for the same server
operation. Preserve their documented return shapes and input validation.
Existing observation endpoints can be thin adapters into the same internal
operation where transport compatibility is useful.

Delete as part of this stage:

- The ordinary versus adapter/keyed time-series recording implementations.
- Client stream-registration caches and pre-record registration requests.
- Parallel semantic-resolution maps on bound model selections.
- Table serialization repeated during validation and transport preparation.
- Always-failing prototype shims for `record_all()`, `declare_from(role=...)`,
  and automatic Pyomo options, if they have no released compatibility obligation.

Keep recovery for artifacts and samples left by older versions. The new atomic
write path removes a cause of orphan samples; it does not prove old stores have
none. Keep keyed declarations, recorded stream references, and live annotations.

Acceptance: a first or subsequent model sample uses one recording request;
`record_values()` also uses one. Reader failure sends no recording request;
server rejection leaves no new samples or observations. Existing scalar, file,
table, keyed/unkeyed history, resume, and deletion behavior remains covered.

## Delivery and verification

Implement each stage as a reviewable commit or small commit series. Keep the
existing public paths working until their replacement passes its contract
tests, then remove the duplicate path in the same stage. Stage 3 can proceed
after stage 1 independently of the recording transaction work.

For each stage, run its focused behavioral tests. Before completion run the
full unit suite, schema/outbox/experiment/publication contracts against DuckDB
and a real TimescaleDB, plus the local-server model tests and runnable model
guide. PostgreSQL cases must execute rather than be skipped for final acceptance.
Update tests whose expected behavior intentionally changes, especially rejected
sample batches, declaration metadata presentation, and request budgets.

Update the experiment reference, model-recording guide, metadata guide,
integration plan, migration/recovery documentation, and changelog as the relevant
stage lands. Replace obsolete SQL-authoritative adapter descriptions with links
to the current storage boundary. Mark this plan implemented only after every
stage and its documentation is complete.

Success means one declaration preparation path, one recording execution path,
one metadata consistency boundary, and direct document assembly for saved user
values. Preserve the failure guarantees while reducing the number of places
that must know how those guarantees are achieved.
