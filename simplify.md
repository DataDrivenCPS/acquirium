# Simplification Plan: `gtf-app-movement-redo` branch

A review of the branch diff against `main` (~9,100 insertions across 120 files) identified
ways to reduce code size and complexity while preserving correctness. The branch replaces
the old `Apps/` runtime with the query-driven materialization system: topology epochs,
publication protocol, services, experiments, effects, and artifact-backed stateful
transforms. The design is sound; the cost is ~5,500 new source lines, much of it the same
logic written more than once.

Suggested sequencing: 1 → 7 → 2/3 → 5, then the rest. Each step is independently
verifiable against the existing contract tests (`tests/integration/materialization_targets.py`
parameterizes over backends, which is the safety net for the backend merges).

---

## Tier 1 — Mechanical deduplication (~1,200+ lines, no semantic change)

> **Status: DONE** (2026-08-23). Items 1–5 implemented:
> `dialect.py` now holds the Postgres connection adapter plus shared
> timestamp/JSON codec mixins; `schema.py` is the single dialect-parameterized
> DDL. `MaterializationPostgres` subclasses `MaterializationDuckDB` (549 → 54
> lines); the `Support` classes are merged away; epoch `snapshot()` is shared
> via `_staged_dependency_rows`/`_dependency_intervals`/`_live_rows` hooks
> (epoch_postgres 306 → 112 lines); dead code removed (`BindingDiff`,
> `diff_bindings`, `validate_binding_topology`, `EpochComponent`,
> `activate_ready`, `StaleAttemptError`, `record_change_ranges`,
> `PlanPartition`, `WorkLease`, `InputSnapshot`, `RangeManifestStore`,
> `GraphRevision`). Storage/materialization package: 2,985 → 2,354 lines.
>
> Bug found and fixed while unifying: DuckDB's Python `rowcount` is always
> -1, so every rowcount-based guard (`fail_work`, work claiming,
> `plan_data_changes` insert counting, `keep_experiment`, lease completion)
> silently passed on DuckDB. All such sites now count affected rows portably
> via a shared `_changed()` helper using `RETURNING 1`.
>
> Also: the PG epoch contract test now clears prior-run topology state in its
> fixture (it asserts a fresh-topology trace but the testing DB persists);
> the stale-deployment failures observed there are a live demonstration of
> Tier 2 item 6's validate-all-deployments coupling.

### 1. Unify `MaterializationPostgres` with `MaterializationDuckDB`

`src/acquirium/Storage/materialization/postgres.py` (549 lines) is a full hand-copy of
`duckdb.py` (581 lines) — the same ~47 methods reimplemented with `%s` placeholders.

The epoch stores already demonstrate the fix: `TopologyEpochPostgres` **subclasses**
`TopologyEpochDuckDB` through a tiny `_PostgresConnection` adapter that rewrites `?`→`%s`,
overriding only ~8 dialect-specific methods (307 lines vs 1,270).

- Apply the same pattern: one shared store class written against the adapter interface;
  a Postgres subclass carrying only DDL and `= ANY(%s::text[])`-style overrides.
- Move `_PostgresStoreAdapter` / `_PostgresConnection` out of `epoch_postgres.py` into a
  shared module — both backend stores will use it.
- Saves ~450 lines and removes a live drift hazard: two independent copies of
  lease-expiry, experiment-claim, and hint-coalescing logic.

### 2. Collapse the schema DDL (six copies → one)

The support-table DDL is written **four times** (`MaterializationDuckDB.__init__`,
`MaterializationSupportDuckDB.__init__`, and both Postgres twins) and the epoch DDL
**twice** — differing only in `VARCHAR`/`TEXT`, `TIMESTAMP`/`TIMESTAMPTZ`, `JSON`/`JSONB`.

Since only the `Support` subclasses are ever instantiated, the base classes' DDL blocks
are dead code that still must be kept in sync.

- Single table list + three-token dialect substitution: ~350 lines → ~80.
- Every future migration becomes a one-place edit.

### 3. Dissolve the `Support` subclass split

`MaterializationSupportDuckDB` exists only to skip one table's DDL and add
`promote_state_revision` (itself duplicated verbatim in `support_postgres.py`). The base
classes are never used directly.

- Merge each pair into one class (or into the unified class from item 1).
- Removes two files and the confusing "support" concept.

### 4. Share `snapshot()` between the epoch stores (~70 duplicated lines)

The base class already uses the override-small-helpers pattern (`_retained_ranges`,
`_stream_versions`, `_canonical_rows` are dialect overrides). Extend it two more steps:

- Add overridable `_staged_output_rows(...)` and `_timeseries_window(...)` helpers.
- The staged-overlay/baseline-merge orchestration (recency window function,
  replaced-interval filtering) then lives once, in the base. It is subtle and is the last
  place to keep two copies.

### 5. Delete dead code

Zero callers found for:

- `bindings.py`: `BindingDiff`, `diff_bindings`, `validate_binding_topology` (~60 lines;
  superseded by `epoch_common.global_dag`)
- `epochs.py`: `EpochComponent` dataclass
- `epoch_duckdb.py`: `activate_ready()`
- `materialization/duckdb.py`: `StaleAttemptError`; `record_change_ranges` /
  `change_ranges` (publication writes `stream_change_ranges` directly via SQL; the only
  remaining caller is one contract test)

Also housekeeping: `benchmarks/microbatch_*` result directories, `batch_example/.data/`,
and `system_paper/` are untracked in the repo — gitignore them before they get committed.

---

## Tier 2 — Structural consolidation (biggest understandability wins)

> **Status: DONE** (2026-08-23). Items 6–11 implemented:
>
> - (7) `_propagate_dirty` + `_component_raw_ranges` are the single copy of
>   dirty-range propagation; `construct_epoch` and `plan_data_changes` both
>   call them (the inline Kahn's-algorithm re-implementation is gone).
>   Promotion-policy clamping is a parameter of the shared function because
>   it must apply *before* ranges reach consumers.
> - (6) `ensure_epoch` no longer resolves queries at all: the
>   `state_revision_resolver` is now a zero-arg bulk read
>   (`active_state_revisions()`, binding_id → revision) and the `graph`
>   parameter is gone. Query resolution now happens exactly once per epoch,
>   in `construct_epoch` (plus the fail-fast pass at deploy time).
> - (9) One definition registry: `topology_epoch_definitions` is gone; both
>   stores use the shared content-addressed `materialization_definitions`
>   table (single template in `schema.py`).
> - (10) Manager: `_ensure_current_epoch()`, `_after_canonical_publish()`,
>   and `_empty_receipt()` replace the three repeated blocks.
> - (11) `Transformation`/`StatefulTransformation` share one
>   `_QueryTransformBase.__init_subclass__`.
> - (8) Scaled back deliberately: after Tier 1 each lease flavor exists in
>   exactly one class, so only the expiry-recovery idiom was shared
>   (`_recover_expired_leases`). A fully parameterized lease framework was
>   judged to obscure more than it deduplicates; migrating artifact/effect
>   leases onto the epoch-claims table would change durable schemas for
>   little practical gain.

### 6. Stop resolving bindings in `ensure_epoch`

`resolve_bindings` (which executes SPARQL) runs up to **three times** per epoch:
`_validate_deployments` at deploy, `ensure_epoch` (only to enumerate binding IDs for the
state-revision resolver), and `construct_epoch` (the authoritative pass).

- Replace the per-binding `state_revision_resolver` callback with one bulk
  `active_state_revisions()` read (binding_id → revision) from the support store; sort,
  digest, done.
- Pins for bindings absent from the topology are harmlessly ignored at construction.
- Deletes the deadlock-avoidance dance/comment at the top of `ensure_epoch` and removes
  its `graph` parameter.
- (Optional, bigger UX call: drop validate-on-deploy and let the epoch's existing
  `failed` status carry validation errors — changes deploy from fail-fast to async.)

### 7. Extract shared dirty-range propagation

`construct_epoch` and `plan_data_changes` share ~120 lines of the correctness-critical
core: load bindings/components/edges, topological order (`plan_data_changes` even
re-implements Kahn's algorithm inline that `global_dag` already provides), compute
`component_retained`, propagate via `_affected_ranges`, emit `_work_rows`.

- Extract one shared `_plan_component_work(conn, epoch, bindings, edges, raw_changes, ...)`.
- Highest-value refactor on the branch: the impact-policy/window-composition semantics
  are the hardest thing here to get right and today exist in two copies that could
  silently diverge. Promotion-policy handling stays a construct-side wrapper.

### 8. Unify the four lease mechanisms

Currently: `topology_epoch_claims` (claim/renew/release with attempt fencing),
artifact-request leases (`lease_owner`/`lease_expires_at`/`attempt` + expiry sweep),
effect-intent leases (identical columns, identical sweep), experiment `execution_claim`.

- Fold at least the artifact and effect leases onto one shared lease helper (or the
  epoch-claim table).
- Concurrency idioms to verify drop from four to two; ~150 lines of SQL → ~60.

### 9. Merge the two definition registries

`materialization_definitions` and `topology_epoch_definitions` have identical schemas and
identical `register_definition` methods; transformations register in one store,
experiments/services in the other, and the manager must know which.

- One table (both stores read it), one registration path.

### 10. Deduplicate manager-level repetition (`Server/manager.py`)

- Three spots repeat "read `published_version`; if ≥ 0, `ensure_graph_epoch(revision,
  digest)`" (`deploy_transformation`, `remove_transformation`,
  `promote_state_revision`) → factor `_ensure_current_epoch()`.
- Three insert methods hand-construct the empty `PublicationReceipt` → one
  `_empty_receipt()` helper, or let `publish` accept an empty table.
- The replace branch of `insert_timeseries` re-inlines
  `publish → plan_data_changes → notify_service_changes`, which `publish()` already
  owns → route through a shared post-publication hook.

### 11. Deduplicate `__init_subclass__` in `Materialization/api.py`

`Transformation` and `StatefulTransformation` carry byte-identical ~22-line validation
bodies; `_Application` is a third variant.

- One shared `_attach_definition(cls, ...)` function (or a common transform base). ~45
  lines saved.
- Output-spec validation lives both in `OutputSpec.__post_init__` and in
  `topology._output_spec` — pick one canonical normalizer.

---

## Tier 3 — Trimming the state machine (semantic; discuss before doing)

### 12. Drop the `ready` epoch status

`ready` is nearly an alias for `reconciling`: construct sets it only for "components but
no work," the first commit flips it to `reconciling`, and every claim query matches
`IN ('reconciling', 'ready')`. Collapsing them removes a state and several membership
checks. Also consider deriving `candidate_epoch_id` as "the unique epoch in
`constructing` status" instead of maintaining a stored pointer by hand.

### 13. Move timezone conversion into the connection adapter

`epoch_duckdb` juggles `_now`/`_stored_timestamp`/`_aware` and nearly every query wraps
parameters in `.replace(tzinfo=None)`. If the DuckDB connection adapter converted
datetimes at the boundary (the same place the Postgres adapter rewrites placeholders),
the shared state-machine code deals exclusively in aware UTC datetimes.

### 14. Efficiency nit with a size payoff

`claim_next_work` fetches all pending rows then issues one `SELECT` per upstream
dependency per candidate (N+1). A `NOT EXISTS` subquery against `topology_epoch_work`
selects only ready work in one statement — fewer lines and fewer round trips.

---

## Rough totals

- **Tier 1:** ~1,200–1,500 lines removed, zero behavior change; converts every remaining
  backend pair to the proven "shared state machine + dialect adapter" pattern so exactly
  one copy of every correctness-bearing decision exists.
- **Tier 2:** ~300–400 more lines, but the largest understandability gain — one
  dirty-propagation function, one lease idiom, one definition registry, one
  publication side-effect path.
- **Tier 3:** small line counts; each removes a concept a reader must otherwise verify.
