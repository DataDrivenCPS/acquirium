# Materialization runtime backlog

This file tracks implementation notes for `materialization_runtime_plan.md`.
Completed items should stay here as a concise record; unresolved work is kept
in phase order so the replacement can be delivered incrementally.

## In progress

- [x] Phase 1 foundation: dual-write coalescible, half-open event-time range
  manifests from canonical publications on both storage backends. The old
  changed-key manifest remains temporarily for the active legacy runtime.
- [x] Add backend-neutral UTC microsecond range algebra and serializable impact
  policies (`Materialization/impact.py`).
- [x] Add stable binding and definition declaration primitives
  (`Materialization/bindings.py`, `Materialization/definitions.py`).
- [x] Add the first public transformation declaration API; execution and
  registration remain pending.

## Next

- [x] Add `Storage/materialization` range-manifest request/result types, IDs,
  and DuckDB/PostgreSQL implementations. Canonical publications emit the same
  queryable half-open ranges on both backends.
- [x] Characterize PostgreSQL parity and concurrent-writer behavior for the
  new range manifests. The shared backend contract checks canonical range
  emission/query behavior and opposite-order concurrent writers.
- [x] Persist definitions, deployments, bindings, and graph revision/rebind
  requests (Phase 2). DuckDB and PostgreSQL definition/deployment/staging-
  binding persistence, graph revision recording, rebind-request insertion,
  rebind leasing, explicit binding resolution, selector expansion, topology
  validation, and atomic activation are implemented through Phase 5.
- [x] Phase 3: durable plans/partitions, range-manifest safety scans,
  impact-aware stale rejection, leases/retries, Arrow snapshots, replacement
  commits/tombstones, receipts/progress, bounded scheduler execution, and
  internal lease/snapshot/commit/fail transport on DuckDB and PostgreSQL.

## Deferred by design

- [x] Phase 4 runtime regression coverage: bounded local and opt-in fixed Ray
  pools, durable-definition execution, active-deployment server drain loop,
  non-committing Arrow preview, and DuckDB restart recovery. A 10,000-row
  backfill permits concurrent singleton ingest, two-hop durable DAGs converge,
  and 1,000 idle logical bindings remain durable rows sharing the fixed pool.
- [ ] Benchmark-host acceptance measurements: capture the plan's exact latency,
  memory, and 10-million-row figures on representative hardware before claiming
  its hardware-specific performance gates.
- [x] Phase 4 foundation: bounded local executor, scalar/Arrow compute adapter,
  immutable execution context, and output ownership/range validation.
- [x] Phase 4 registration surface: transformation registration, status,
  start/pause, listing, and explicit rebind endpoints plus client methods.
- [x] Phase 5: generation-scoped graph reconciliation. Staged bindings and
  definitions use isolated output rows, retained-history bootstrap plans,
  manifest-driven tail safety scans, canonical promotion/tombstones, and an
  atomic active-pointer swap. Direct, per-input, by-entity, single, empty, and
  manually reconciled declarations are covered; PostgreSQL promotion has a
  live smoke test.
- [x] Phase 6 generic artifact-backed state: content-addressed, digest-verified
  filesystem artifacts; generic producer request/lease/complete/fail and
  promotion endpoints; candidate and active revisions; DuckDB/PostgreSQL
  lifecycle persistence; pinned plans; promotion invalidations; worker-local
  class/artifact caches; and age-guarded orphan collection are implemented.
  The public generic calibration example and DuckDB/PostgreSQL storage/worker
  restart recovery show that durable artifacts, rather than worker memory,
  reproduce class transformations.
- [x] Phase 7: bounded experiments and provenance. Immutable definitions,
  schema-validated frozen run snapshots, nested metadata, metrics, immutable
  artifact references, run-owned output identities, bounded execution, terminal
  status records, metadata filtering, rerun, keep/collection, and DuckDB /
  PostgreSQL persistence are implemented. The load-shifting-style example uses
  the run context rather than hand-written provenance bookkeeping.
- [x] Phase 8: services and effect delivery. Immutable service packages have
  durable registration/start/stop/health state, a dedicated bounded executor,
  merged at-least-once data/graph hints, current authoritative Arrow snapshots
  with version tokens, safety-scan recovery, and an effect-only context.
  Webhook effects are leased, retried with exponential backoff, deduplicated by
  idempotency key, and terminally dead-lettered. The streaming-dashboard
  example uses these APIs without polling backend internals.
- [ ] Phase 9: remove the superseded continuous app runtime, then complete
  naming/schema diagnostics, benchmark characterization, and documentation
  cleanup after replacement feature coverage is accepted.

## Notes

- Existing uncommitted continuous-runtime changes predate this implementation
  and are intentionally left untouched.
- The plan explicitly does not require backward compatibility with old app
  endpoints; removal should occur only after equivalent range contracts pass.
