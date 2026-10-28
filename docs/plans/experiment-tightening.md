# Tighten experiment declarations and recording

Status: implemented and verified (2026-10-22). All three stages are complete.

Follow-up to [experiment refinement](experiment-refinement.md). The refinement
consolidated ownership and recording transactions, but increased production
Python by 373 lines. This pass should reduce the implementation as well as the
number of repeated conversions.

## Implementation results

- Declaration input normalization, saved-document decoding, serialization, and
  semantic resolution now have separate operations. Saved handles and catalog
  reads use the current document codec; descriptive graph edits normalize at
  that input boundary. New declarations prepare once; replay compares normalized
  declarations without resolving them. Keyed observations
  serialize from their prepared representation. Resolution reads declaration
  fields directly without serializing the user metadata object.
- Ordinary and model recording share value, artifact, reference, and sample
  payload preparation. Adapter readers prepare their payloads within the labeled
  error handler, before dispatch. The prepared-table wrapper and the second value
  conversion pass are removed. Stream handles are constructed only for ordinary
  sample recording, whose public return value needs them.
- `_prepare_recording()` produces a concrete batch of prepared observations,
  registrations, sample data, and graph updates. `_write_recording()` persists
  it using the existing transaction and locks. Compatibility observation methods
  translate their payloads into that same operation. Run and variable ownership
  are checked once before preparation and again inside the transaction.

Measured against the working-tree snapshot taken before this cleanup:

| Production file | Before | After | Delta |
| --- | ---: | ---: | ---: |
| `ExperimentDeclarations.py` | 172 | 182 | +10 |
| `ExperimentValues.py` | 152 | 205 | +53 |
| `ModelAdapters.py` | 159 | 141 | -18 |
| `Experiments.py` | 2,064 | 2,017 | -47 |
| `Client/client.py` | 1,098 | 1,096 | -2 |
| `StreamMetadata.py` (EOF whitespace) | 176 | 174 | -2 |
| **Total changed production Python** | **3,821** | **3,815** | **-6** |

All production Python totals 29,177 lines, down from 29,183. Four lines were
removed by the implementation cleanup; two trailing blank lines were removed
from the new stream helper when staging the commit. Tests add 57 lines,
including typed-key/document isolation coverage and checks that terminal runs,
deleted variables, or changed ownership between preparation and commit prevent
sample and observation writes. Numeric model readers also reject stream
references without sending a recording request. A graph-edit regression test
also preserves the user namespace when reopening a variable. Documentation changes
record these results in this plan.

A direct DuckDB measurement of 100 observations alternating between two variables
reduced run-state reads from 202 to 2 and variable-ownership reads from 200 to 2.
Both versions returned all 100 observations in sequence.

Final verification:

- 1,273 unit tests passed.
- 110 experiment, migration, outbox, publication, and time-series contracts
  passed on DuckDB and real TimescaleDB, with no PostgreSQL skips.
- Both local HTTP model integration tests passed.
- The runnable whole-model guide completed successfully.
- `git diff --check` passed. The test TimescaleDB service was stopped afterward.

## Success criteria

- Net fewer production Python lines across all changed and new source files.
  Moving code into new files does not count as a reduction.
- Declarations have one canonical in-memory representation, with explicit input
  normalization and serialization boundaries.
- Ordinary recordings and adapter recordings share payload preparation.
- Recording preparation produces the data needed by one SQL executor, without
  decoding transport forms again inside the transaction.
- Existing public behavior, saved formats, and consistency guarantees pass the
  affected tests on DuckDB and a real TimescaleDB.

Before implementation, snapshot the current production files and record their
line counts. Compare the finished cleanup against that snapshot: the refinement
is still uncommitted, so a comparison against Git HEAD would mix the two efforts.
Report production, tests, and documentation separately. Do not compress formatting
or remove compatibility behavior to meet the line-count criterion.

## 1. Normalize declarations once per boundary

The current path repeatedly converts a declaration into a dictionary and back.
`prepare_declaration()` normalizes metadata and validates keys, then
`validate_declaration()` repeats parts of that work. `Declaration.document()`
normalizes again. `declare_batch()` prepares fresh declarations twice, while
saved handle hydration and catalog rendering also revisit normalization.

Refactor `ExperimentDeclarations.py` around three concrete operations: normalize
external or legacy input into a validated declaration, decode the current saved
format, and serialize a validated declaration. Serialization should only produce
the saved document; it should not invoke legacy upgrading or semantic resolution.
Keep one canonical representation of keyed observations and derive the wire
representation from it.

Update `_Builder._make()`, `Study.declare_from()`, `ExperimentVariable`, and
`VariableCatalog.frame()` to use these operations directly. In
`ExperimentStore.declare_batch()`, prepare each input once, compare its normalized
declaration with the saved declaration, and resolve semantics only for a new
declaration. Extract the repeated equality projection that excludes resolved
semantic fields. Do not trust client-supplied resolutions.

Acceptance: preserve the public `Component` constructor and detached/frozen
reader behavior, typed keys, raw semantic spelling, JSON nulls and empty
containers, source-unit assertions, and legacy conflict errors. Repeating a
declaration must remain idempotent without running semantic resolution again.
Keep the existing legacy upgrade and its durable replay guarantees.

## 2. Share client recording preparation

`_record_snapshot()` and ordinary variable recording build overlapping value,
artifact, and sample payloads. `attach_experiment_bytes()` independently encodes
the artifact payload. Adapter reads validate values that the snapshot path then
converts and checks again.

Introduce a small shared preparation path using the existing value utilities in
`ExperimentValues.py`. Route ordinary values, references, samples, tables, and
attachments through it. Remove the corresponding inline payload construction
from `_record_snapshot()`, variable recording methods, and
`AcquiriumClient.attach_experiment_bytes()`. Keep HTTP dispatch in the client.

Allow adapter recording to consume already prepared values without another JSON,
sample, or table conversion. Use the existing prepared-table mechanism where it
fits; add a prepared representation only where it removes duplicated work and
code. Avoid a generic serializer registry or a second set of public entry types.

Acceptance: ordinary convenience methods retain their return shapes; adapter
recording reads and validates every selected reader before sending one request;
reader failures send none. Preview remains local and returns the original table
type. A recorded table is serialized once. Preserve labeled reader errors,
key validation, timestamp behavior, and model-time separation. Server validation
remains authoritative for requests from every client.

## 3. Separate batch preparation from SQL execution

`ExperimentStore._record_batch()` currently loads ownership information, validates
transport forms, prepares frames and artifacts, constructs graph updates, and
writes the ledger. It mutates entry dictionaries into prepared records and keeps
related data in parallel collections. Repeated variables also trigger repeated
lookups and run-state checks.

Split it into preparation and execution functions with a small concrete prepared
batch representation. Prepared observations should carry only what persistence
and receipts need. Gather shared stream registrations, sample frames, and resolved
metadata updates during preparation. Replace repeated preflight ownership reads
with one run lookup and batched reads for unique variable IDs.

The executor should accept the prepared batch and use the existing
connection-scoped storage primitives. It must retain authoritative run,
ownership, and artifact-reference checks inside the write transaction, with one
commit covering samples, observations, references, and queued metadata. Remove
transport decoding and mutable transport dictionaries from that function. Keep
compatibility endpoints as adapters into this same operation.

Preserve the existing lock distinction: sample batches serialize metadata
delivery, while value/artifact-only recording can proceed during artifact
deletion cleanup. Keep file writes and artifact ownership checks under their
existing protection. Pure value/frame and graph preparation stays outside the
SQL writer transaction; outbox flush and acknowledgment ordering stays intact.

Acceptance: rejected batches leave no partial ledger or sample writes; receipts,
observation ordering, revisions, stream identities, reset/delete behavior, and
artifact reattachment continue to work on both backends.

## Scope and verification

Implement the three stages as separately reviewable changes. Keep public APIs,
HTTP payloads, saved documents, schema versions, and outbox formats stable. Leave
metadata ownership, graph-query semantics, and backend publication machinery as
established by the refinement. Add no new migrations, endpoints, or caches.

Run focused declaration/adapter tests after stage 1, client and model recording
tests after stage 2, and rollback/outbox/publication contracts after stage 3.
Extend existing tests only for behavior gaps exposed by the cleanup; avoid tests
that merely assert helper structure or internal call sequences.

Before completion, run the full unit suite, affected experiment, migration,
outbox, publication, and time-series contracts on DuckDB and real TimescaleDB
without PostgreSQL skips, both local HTTP model integration tests, and the runnable
whole-model guide. The refinement baseline was 1,271 unit tests, 103 contracts,
two model integration tests, and the guide. Report actual results and the final
line-count delta, along with which duplicate conversions and branches were
removed. If a stage grows production code, simplify it before proceeding.
