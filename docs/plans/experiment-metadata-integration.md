# Experiment metadata integration with PR #120

The experiment branch was rebased on PR #120 at `9b8f051` and now includes
its update through `c52ea13`.
The previous branch tip is saved as
`backup/experiment-interface-before-pr120-20261014` (`c23f3fd`).
The materialization changes from main were replayed too.

## Working implementation

- Studies, variables, runs, and stream annotations get stable graph nodes.
  Applications create these through the existing experiment recording API.
- The `experiments` source graph is managed by the experiment ledger. It uses
  #120's `plan_write()` for semantic predicates, resolution, and nested values.
  Run metadata is attached to its run, not to a shared plant measurement point.
- `study.experiments.where(scenario="baseline")` and attribute expressions use
  the existing query compiler. Variable selections also accept semantic filters
  such as `study.variables.where(unit="PSI")`.
- Graph-selected run IDs are passed to parameterized ledger queries. POST
  selection endpoints keep large ID lists out of URL query strings and avoid
  transferring unselected runs or observations.
- `.query()` on either collection exposes the native query builder, including
  `include()`, `metadata()`, `metadata_objects()`, `schema()`, `options()`, and `facets()`.
- Scalar variable nodes can carry units without registering a sample stream.
  Upstream entity queries now accept these attributes directly.
- Saved variable, run, annotation, and observation metadata reads use
  `.metadata_objects()` through a local graph client. Reads batch unique IDs
  instead of fetching metadata separately for every observation.
- The adapter removes lifecycle fields, restores supplied semantic strings
  from `<key>.raw`. Existing experiment
  objects keep their metadata shape and unit spelling; native queries expose
  resolved values and raw strings separately.
- The shared QUDT index includes currency codes as exact unit spellings. This
  was needed because semantic declaration validation exposed that `USD` did
  not resolve despite `qudt:currencyCode "USD"` being present in bundled data.
  The change belongs in the shared resolver and can be upstreamed independently.
- Attribute discovery waits for the graph revision it caches. Otherwise an
  expression built immediately after a metadata write could reject a valid new
  key and cache the stale discovery result under the new graph version.
  This fixes the shared registry for plant metadata too.
- Experiment frames use #120's scalar type selection. Saved objects use Object
  columns to avoid silently coercing mixed or differently shaped values.
- Nulls and empty containers remove descriptive metadata, matching #120.
  Recorded JSON and log observations remain data and retain those values.
- SQL still owns lifecycle, observation ordering, artifacts, intervals, and
  original metadata snapshots. The graph is a rebuildable query view for now.

## Remaining difference in object retrieval

#120 now provides `.metadata_objects()` and preserves resolved free text under
`<key>.raw`. Entity queries can also read measurement attributes, so
`.entity(uri=point).include("all")` includes the point's unit.

For example, a scalar variable declared with `unit="PSI"` has a QUDT URI in
the graph and `"PSI"` under `unit.raw`. Both string and URI filters work, and
`unit.raw` supports exact text filtering. Its saved `.metadata` object still
gets `"PSI"` from the graph's `unit.raw` field.

The shared object method now omits missing fields, including nested fields
introduced by other nodes. `{"mode": "baseline"}` stays that shape even when
another node has `duration`. The table still uses a shared schema. The experiment
adapter no longer needs its own null cleanup.

Round-trip tests also found shapes that the shared method cannot yet recover:

- Lists of objects become objects of lists.
- Nested lists flatten; missing list positions disappear.
- A scalar becomes a one-element list when another node uses a list for that key.
- Multi-valued semantic attributes have separate rows rather than an ordered list.
- Metadata keys `id` and `type` overlap query identity and managed RDF classes.

`saved_metadata()` retains the SQL snapshot for affected fields only. Graph
read failures propagate; there is no silent fallback to SQL on a failed read.
Storage-only ledger callers can still inspect SQL snapshots without a graph.

Next steps:

1. Resolve the remaining container shapes in the shared object API.
   Keep normalized structs for tables; per-node field presence is fixed.
2. Remove the field-level `saved_metadata()` compatibility path after the
   round-trip tests pass without it.
3. Decide whether SQL metadata snapshots remain the durable recovery source or
   whether graph metadata becomes authoritative. Removing the snapshots requires
   an explicit migration and a recovery strategy for writes across both stores.
4. Keep the spelling adapter: experiment objects retain supplied strings while
   native queries expose resolved values and `<key>.raw`.

## Recovery and existing data

The first experiment mutation or graph query backfills existing SQL records.
Opening a Study also ensures that its metadata is available to attribute
discovery. Later metadata writes update their own node; recording scalar values
and sample batches does not republish experiment metadata.

An interrupted graph update leaves the committed SQL record intact and marks
the view for repair. The next experiment graph query rebuilds it before reading.
Restart also triggers a backfill. Queries wait for fresh inference so a completed
metadata write is visible immediately, and a read that finds a record in SQL
waits until that record's graph node is written.

A backfill reuses the URI each metadata string first resolved to, so a resolver
that is still loading or has since changed cannot alter or fail saved records.
A record the graph cannot hold, such as a legacy run with a now-reserved key,
is left out. Reading it, or querying its Study through the graph, raises its
error; every other record reads normally. Fixing the SQL row publishes it on
the next read. Recreating the SQL ledger rebuilds an
empty experiment view, including when storage is outside the server data directory.

Older declarations with bare UUID IDs get absolute graph URIs under
`urn:acquirium:experiment:variable/`. Their SQL IDs and observation associations
stay unchanged. Variable filters compare graph URIs, while recording and HTTP
selection continue to use the saved IDs.

Existing metadata must satisfy #120's key and semantic-value rules to backfill.
Invalid legacy declarations should be corrected explicitly; raw SQL records
remain available for recovery if graph backfill fails. HTTP metadata reads
require successful backfill. Lifecycle field names (`status`, `role`,
`kind`, identifiers, timestamps, `label`, and interval fields) are reserved at the
top level. Put similarly named descriptive fields inside a nested context object.

## Simplification and removal plan

Removed now:

- Experiment-specific DataFrame dtype inference, including the mixed-value
  special case, in favor of #120's shared typing helpers.
- The need for a separate expression language or metadata filter compiler.
- The variable-role extension of the query registry. Only known experiment
  fields and fresh graph reads need an adapter now.
- SQL as the ordinary object-read source for supported metadata shapes.
- The duplicate null cleanup on object reads, now handled upstream.

Retained temporarily:

- `where(metadata={...})` and the HTTP metadata filter for compatibility. New
  examples use direct attributes. After callers migrate, remove the SQL-side
  `_metadata_matches()` path and its transport parameter.
- SQL metadata snapshots and the field-level shape compatibility path, pending
  the round-trip fixes and persistence decision above.
- The graph projection adapter, which publishes lifecycle state and repairs
  interrupted writes from the durable SQL ledger.
- SQL-backed `.frame()` row layouts for existing experiment analysis. Use
  `.query().include(...).metadata()` for native typed metadata tables.

Further implementation work:

- Remove shape compatibility code once the upstream object reader preserves
  the original containers and fields.
- Add an explicit graph-authoritative metadata migration only if that storage
  decision is made. This branch does not delete existing SQL metadata columns.

## Read API naming under consideration

The current API uses `.frame()` for declarations, runs, and observation records,
and `.dataframe()` for time-series samples. The similar names do not explain
that difference well.

Consider `.history()` for the table of observation records. Keep `.dataframe()`
for fetching samples: those samples describe the recorded process, while the
history describes the recording calls. For example, a run recorded in three
batches has three history rows and many sample rows.

No methods have been renamed. The proposed `.one()` selectors and
`run.dataframe(variable)` are also not implemented. Any naming change should
be settled alongside that read API, then applied across the reference, guide,
notebook, and script.

No upstream PR branch was modified or pushed.
