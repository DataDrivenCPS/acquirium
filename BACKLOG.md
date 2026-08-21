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

- [ ] Add `Storage/materialization` range-manifest request/result types, IDs,
  and DuckDB/PostgreSQL implementations.
- [ ] Characterize PostgreSQL parity and concurrent-writer behavior for the
  new range manifests; DuckDB publication behavior has focused coverage.
- [ ] Persist definitions, deployments, bindings, and graph revision/rebind
  requests (Phase 2). DuckDB and PostgreSQL definition/deployment/staging-
  binding persistence, graph revision recording, rebind-request insertion,
  rebind leasing, explicit binding resolution, and pure topology validation
  are now in place. Built-in selector expansion and atomic topology activation
  remain deferred to Phase 5.
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
- [ ] Stateful artifacts/training, experiments, services, effects, and old
  continuous-runtime removal (Phases 6–9).

## Notes

- Existing uncommitted continuous-runtime changes predate this implementation
  and are intentionally left untouched.
- The plan explicitly does not require backward compatibility with old app
  endpoints; removal should occur only after equivalent range contracts pass.
