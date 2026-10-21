# Experiment metadata integration with PR #120

The experiment branch was rebased on PR #120 at `9b8f051` and now includes
its update through `a7eed89`.
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
- The server publishes queued experiment writes before reporting the graph
  version, as it already did before SPARQL reads. Discovery is cached by that
  version, so a version that missed queued runs kept their new metadata keys
  unknown: filtering on a key written by the run just finished failed.
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

Objects come back in the shape they were saved: lists of objects, nested
lists, and a scalar on one node next to a list on another. Each object holds
only the fields saved on its node. `id` is the saved id, or else the node URI;
the adapter keeps only a saved one. The table still uses a shared schema.

Round-trip tests still find three cases the shared method does not return as
saved:

- `None` inside a list. Upstream treats `None` as removal, so the list keeps
  its order but loses that position: `["A", None, "B"]` reads as `["A", "B"]`.
- A top-level `type` key, which is the node's RDF class upstream.
- A list on a semantic attribute such as `unit`, which reads as one row per
  value rather than an ordered list.

`saved_metadata()` retains the SQL snapshot for these fields only. Graph
read failures propagate; there is no silent fallback to SQL on a failed read.
Storage-only ledger callers can still inspect SQL snapshots without a graph.

Next steps:

1. Decide whether experiment metadata keeps `None` list positions or adopts
   #120's removal rule, and whether `type` becomes a reserved key.
2. Remove `saved_metadata()` once those cases no longer need SQL.
3. Decide whether SQL metadata snapshots remain the durable recovery source or
   whether graph metadata becomes authoritative. Removing the snapshots requires
   an explicit migration and a recovery strategy for writes across both stores.
4. Keep the spelling adapter: experiment objects retain supplied strings while
   native queries expose resolved values and `<key>.raw`.

## Recovery and existing data

The first experiment graph read backfills existing SQL records.
Opening a Study also ensures that its metadata is available to attribute
discovery. Later metadata writes queue their node, and the next graph read publishes
the queue in one update: each graph update re-runs inference over the whole
plant model, so a sweep of many runs costs one rebuild rather than one per
start and finish. Experiment reads and the server's SPARQL queries, including
attribute discovery, publish the queue first, so neither misses a write.
Recording scalar values and sample batches queues nothing.

An interrupted graph update leaves the committed SQL record intact and marks
the view for repair. The next experiment graph query rebuilds it before reading.
Restart also triggers a backfill. Queries wait for fresh inference so a completed
metadata write is visible immediately, and a read that finds a record in SQL
waits until that record's graph node is written. Recreating the SQL ledger
rebuilds an empty experiment view, including when storage is outside the server
data directory.

A backfill reuses the URI each metadata string first resolved to, so a resolver
that is still loading or has since changed cannot alter or fail saved records.
A record the graph cannot hold, such as a legacy run with a now-reserved key,
is left out. Reading it, or querying its Study through the graph, raises its
error; every other record reads normally. Fixing the SQL row publishes it on
the next read.

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
- SQL restoration of lists of objects, nested lists, scalars next to lists,
  and saved `id` keys, now returned as saved upstream.

Retained temporarily:

- `where(metadata={...})` and the HTTP metadata filter for compatibility. New
  examples use direct attributes. After callers migrate, remove the SQL-side
  `_metadata_matches()` path and its transport parameter.
- SQL metadata snapshots and the field-level compatibility path for the three
  cases above, pending those decisions and the persistence decision.
- The graph projection adapter, which publishes lifecycle state and repairs
  interrupted writes from the durable SQL ledger.
- SQL-backed `.frame()` row layouts for existing experiment analysis. Use
  `.query().include(...).metadata()` for native typed metadata tables.

Further implementation work:

- Remove the remaining compatibility code once the three cases above are
  settled.
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
