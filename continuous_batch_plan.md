# Implementation plan: continuous latest-state microbatch runtime

This is the concrete execution plan for [`continuous_batch.md`](continuous_batch.md).
It is written to be handed to an implementing agent. The design doc is the
authority on semantics; this document maps it onto the current tree, fixes the
decisions the design left open, and orders the work into verifiable phases.

**Clean slate.** There is no backwards compatibility to preserve. Old runtime
state, old endpoints, old client methods, and old on-disk databases are all
disposable. Delete rather than adapt. Deployments initialize the new schema.

## Ground rules for the implementing agent

- Python only. The core is not written in Rust; the entire continuous core goes
  behind one narrow interface (`ContinuousStore`, Phase 1) so a future native
  port stays possible. Do not add Rust, Cython, or new serialization layers.
- Use `uv` for everything: `uv run pytest`, `uv run python`, `uv sync --locked --all-extras`.
  Never bare `python3`/`pip`.
- Test commands: `make unit-test` (service-free), `make testing-up` +
  `make integration-test` (compose-backed). Both backends (DuckDB and
  Timescale/Postgres) must pass the same contract suite before any phase that
  depends on it starts.
- Do not add `Co-Authored-By: Claude` trailers to commits.
- Follow `AGENTS.md` conventions (4-space indent, type hints on public
  interfaces, snake_case, focused modules).
- Each phase below is one or more reviewable PRs against `main` (or stacked on
  the previous phase's branch). Do not mix phases in one PR.

## Findings surfaced during planning (read first)

1. **Timescale compression conflicts with the design.**
   `timescale_store.ensure_table` enables a 7-day compression policy on the
   `timeseries` hypertable. The continuous model upserts corrections and
   updates `last_stream_version` on rows of any age, which compressed chunks
   restrict. Compression is therefore **disabled in v1** (Decision 3 below);
   revisit together with a retention policy after v1.
2. **The design doc's "current path" does not match this branch.** The
   cursor/outbox/completion-event machinery the design says it removes exists
   only as stale `.pyc` files under `src/acquirium/Apps/__pycache__/` (from an
   earlier branch). What this tree actually runs — and what gets replaced — is
   the keep-alive/interval loop described in the Orientation table. Do not hunt
   for cursor/outbox source; it isn't here.
3. **The concurrency benchmark's source is gone** (only `__pycache__` remains
   in `benchmarks/`). Phase 5 rebuilds it as `benchmarks/continuous_latency.py`
   to measure the design doc's acceptance gates.
4. **Batch-size selection is not a correctness mechanism.** In
   `next_app_batch`, publication selection only bounds batch size;
   completeness comes from the per-ref `(from_version, to_version]` range scan
   over `stream_change_keys`. Any publication not selected but whose versions
   fall inside the range is still picked up by the scan — which is exactly why
   `publication_seq` allocation order never needs to match commit order on
   Postgres. Keep this property; do not "optimize" the range scan into a
   scan-by-selected-publication.

## Orientation: what exists today

The current tree (branch `gtf-app-movement-redo`, based on `main` at `b43b1a4`)
runs apps on an **interval keep-alive loop**, not the cursor/outbox pipeline the
design doc's motivation section describes (that lived on an earlier branch;
only `.pyc` remnants remain under `src/acquirium/Apps/__pycache__/`). What you
are replacing here is the keep-alive model:

| Area | Files | What it does today |
| --- | --- | --- |
| Storage protocol | `src/acquirium/Storage/base.py` | `TimeseriesStore` Protocol (upsert/replace/bulk insert, stream registry, reads, logs, begin/commit/rollback) |
| DuckDB backend | `src/acquirium/Storage/duckdb_store.py` | Embedded, server-owned. `timeseries(ref_id INTEGER, ts TIMESTAMP, numeric_value, text_value, UNIQUE(ref_id, ts))`; `ref_ids` maps `ref_uri → ref_id`; naive-UTC timestamps; one `threading.Lock` serializes writes; per-operation connections; `begin()/commit()` span via `_tx_conn` |
| Postgres backend | `src/acquirium/Storage/timescale_store.py` | `timeseries(ref_uri TEXT, ts TIMESTAMPTZ, …, UNIQUE(ref_uri, ts))` hypertable + **7-day compression policy**; single `self.conn` psycopg connection |
| Server | `src/acquirium/Server/app.py` | FastAPI; `/apps/register`, `/apps/run` (keep_alive+interval), `/apps/stop`, `/apps/delete`, `/apps/list`; `/insert_timeseries`, `/insert_timeseries_arrow`, `/timeseries` (Arrow IPC out); lifespan restores apps + starts config drivers/apps |
| Manager | `src/acquirium/Server/manager.py` | Owns graph store + timeseries store; `insert_timeseries`, `insert_timeseries_batch`, `insert_timeseries_arrow(source_id, table)` |
| App runtime | `src/acquirium/Apps/runner.py` | `@ray.remote class AppRunner`: register/setup (load source, `build_query`, `build_app`) + `run()` with keep-alive asyncio loop dispatching stateless `_app_run_task` per tick; outputs emitted via `Apps/output_emission.PersistSink` (HTTP insert + direct webhook POST) |
| Supervisor | `src/acquirium/Apps/supervisor.py` | `AppSupervisor` in the FastAPI process; in-memory `running` flags; `restore_app_specs()` rebuilds `AppSpec`s from the graph + `app.json`/`run.json` |
| App model | `src/acquirium/Apps/base.py`, `mapped.py`, `execution.py` | `App` (build_query/build_app/run → `list[Output]`), `MappedApp` (per-stream `transform`, deterministic derived identity via `mapped_output_identity`), validation in `execution.validate_outputs` |
| Client | `src/acquirium/Client/acquirium.py` (~510–682), `Client/client.py` | `register_app`, `run_app(keep_alive, interval)`, `stop_app`, `delete_app` |
| Drivers | `src/acquirium/Drivers/Driver.py` | Buffered `collect()`/`flush()` → `insert_observations` → client Arrow insert. Drivers keep their collection loops — only their write path changes |
| Models | `src/acquirium/internals/models.py` | `AppSpec` (incl. `resume_keep_alive`, `run_interval`, …), `AppRunRequest`, `AppContext`, `AppOutputSpec` |
| Tests | `tests/test_timeseries_store_contract.py` | **The pattern to copy**: one suite parameterized over `["duckdb", "timescale"]`, skipping when Postgres is unavailable |

## Fixed decisions (do not relitigate)

1. **Module layout.** New subpackage `src/acquirium/Storage/continuous/`:
   - `types.py` — dataclasses/pydantic models: `Mutation`, `PublicationRequest`,
     `PublicationReceipt`, `AppBatch`, `BatchInputRange`, `CommitRequest`,
     `CommitResult`, `BootstrapState`, plus the `ContinuousStore` Protocol.
   - `duckdb.py` — `ContinuousDuckDB(store: DuckDBStore)`; shares the store's
     `_lock` and connection factory so DuckDB keeps exactly one write
     coordinator in the server process.
   - `postgres.py` — `ContinuousPostgres(pool)`; uses a `psycopg_pool.ConnectionPool`
     (new dependency `psycopg-pool`), not TimescaleStore's single `self.conn`.
   - `ids.py` — publication-id/batch-id/payload-hash derivation (pure functions,
     shared by both backends and by tests).
   - Router and compactor go in `src/acquirium/Server/router.py` and
     `Server/compactor.py`. Actor changes stay in `src/acquirium/Apps/`.
2. **Stream keying.** DuckDB keys all new tables by `ref_id INTEGER` (join
   through `ref_ids`, per the existing zonemap rationale); Postgres keys by
   `ref_uri TEXT`. The `ContinuousStore` API speaks `ref_uri` only — id
   resolution is private to `duckdb.py`.
3. **Timescale compression is disabled for v1.** Corrections and
   `last_stream_version` updates must be able to land on any row at any age;
   compressed chunks restrict that. In `timescale_store.ensure_table`, drop the
   `ALTER TABLE … SET (timescaledb.compress …)` block and the
   `add_compression_policy` call. Leave a comment pointing at this plan.
   Revisit compression together with retention policy, post-v1.
4. **Payload hash (exact spec).** After normalization (dedupe `(ref_uri, ts)`
   keeping the last operation), sort rows by `(ref_uri, ts)` and hash
   `sha256("\n".join(f"{ref_uri}|{ts_us}|{op}|{num}|{txt}"))` where `ts_us` is
   integer microseconds since epoch UTC, `op` is `u` or `d`, `num` is `repr(float(v))`
   or empty, `txt` is the raw string or empty. Hex digest. Implemented once in
   `ids.py`, used by both backends and asserted identical in the contract suite.
5. **Tail batch id (exact spec).** `sha256(f"{generation}:" + ";".join(f"{ref_uri},{from_v},{to_v}" for sorted ranges))`
   hex digest. Bootstrap page id: `sha256(f"{bootstrap_id}:{start_ordinal}:{end_ordinal}")`.
   App output publication id: `app:{app_id}:{batch_id}`. Driver publication id:
   client-generated `uuid4` string assigned when the batch is collected,
   preserved across retries.
6. **Internal transport.** The two internal endpoints exchange Arrow IPC
   streams. Batch metadata rides in Arrow **schema metadata** under the key
   `acquirium_batch` as JSON — no multipart, no second request. See Phase 2 for
   the exact contract.
7. **Events and triggers.** `Output.event` remains a text-valued row on the
   app's event stream (as `PersistSink` does today) and flows through the same
   publication path. `Output.trigger` becomes a durable row in
   `app_webhook_intents` written inside the commit transaction; a server-side
   delivery worker POSTs and retries. `PersistSink`'s direct-POST path is
   deleted (keep `normalize_trigger_url`).
8. **v1 app scope.** Continuous execution supports `MappedApp` subclasses and
   any `App` whose `run` consumes only `ctx.inputs` (page-separable,
   latest-state). `preview`/`debug` (`execution.preview_app`,
   `prepare_app_debug`) stay read-only against live reads and are untouched
   except where shared types change. Bounded experiments (`app_proposal.md`)
   are out of scope.

---

## Phase 1 — Durable model + `ContinuousStore` on both backends

**This phase is the spec.** Nothing else starts until the contract suite is
green on DuckDB and Postgres.

### 1a. Schema

Extend `ensure_table` in both backends (recreate-not-migrate; DuckDB files and
Postgres schemas from before this change are recreated). Logical DDL — adapt
types and keying per Decision 2 (`<key>` = `ref_id INTEGER` on DuckDB,
`ref_uri TEXT` on Postgres; DuckDB timestamps stay naive-UTC `TIMESTAMP`):

```sql
ALTER TABLE timeseries ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE timeseries ADD COLUMN last_stream_version BIGINT NOT NULL DEFAULT 0;

CREATE TABLE stream_heads (
    <key> PRIMARY KEY,
    current_version BIGINT NOT NULL,
    retained_from_version BIGINT NOT NULL
);

CREATE TABLE stream_publications (
    publication_seq BIGINT PRIMARY KEY,   -- DuckDB: sequence; PG: BIGSERIAL
    publication_id TEXT UNIQUE NOT NULL,
    payload_hash TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    versions_json TEXT NOT NULL,          -- JSON {ref_uri: version}
    committed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE stream_change_keys (
    publication_seq BIGINT NOT NULL,
    publication_row INTEGER NOT NULL,
    <key> NOT NULL,
    stream_version BIGINT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (publication_seq, publication_row)
);
CREATE INDEX stream_change_keys_ref_version ON stream_change_keys (<key>, stream_version);

CREATE TABLE app_runtime (
    app_id TEXT PRIMARY KEY,
    generation BIGINT NOT NULL,
    status TEXT NOT NULL,          -- registered|bootstrapping|active|stopping|stopped|failed
    topology_version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE app_subscriptions (
    app_id TEXT NOT NULL, generation BIGINT NOT NULL,
    <key> NOT NULL, stream_version BIGINT NOT NULL,
    PRIMARY KEY (app_id, generation, <key>)
);
CREATE INDEX app_subscriptions_ref ON app_subscriptions (<key>);

CREATE TABLE app_batch_commits (
    app_id TEXT NOT NULL, generation BIGINT NOT NULL, batch_id TEXT NOT NULL,
    batch_kind TEXT NOT NULL,      -- 'tail' | 'bootstrap'
    rows_inserted BIGINT NOT NULL, committed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (app_id, generation, batch_id)
);

CREATE TABLE app_batch_inputs (
    app_id TEXT NOT NULL, generation BIGINT NOT NULL, batch_id TEXT NOT NULL,
    <key> NOT NULL, from_version BIGINT NOT NULL, to_version BIGINT NOT NULL,
    PRIMARY KEY (app_id, generation, batch_id, <key>)
);

CREATE TABLE app_bootstraps (
    bootstrap_id TEXT PRIMARY KEY, app_id TEXT NOT NULL, generation BIGINT NOT NULL,
    status TEXT NOT NULL,          -- staging|transforming|finalizing|done|failed
    next_ordinal BIGINT NOT NULL
);
CREATE TABLE app_bootstrap_streams (
    bootstrap_id TEXT NOT NULL, <key> NOT NULL, stream_version BIGINT NOT NULL,
    PRIMARY KEY (bootstrap_id, <key>)
);
CREATE TABLE app_bootstrap_rows (
    bootstrap_id TEXT NOT NULL, ordinal BIGINT NOT NULL,
    <key> NOT NULL, ts TIMESTAMPTZ NOT NULL,
    numeric_value DOUBLE PRECISION, text_value TEXT,
    PRIMARY KEY (bootstrap_id, ordinal)
);
CREATE TABLE app_bootstrap_outputs (
    bootstrap_id TEXT NOT NULL, ordinal BIGINT NOT NULL,
    output_ref_uri TEXT NOT NULL, ts TIMESTAMPTZ NOT NULL,
    operation TEXT NOT NULL,       -- 'upsert' | 'delete'
    numeric_value DOUBLE PRECISION, text_value TEXT,
    PRIMARY KEY (bootstrap_id, ordinal)
);

CREATE TABLE app_webhook_intents (
    app_id TEXT NOT NULL, generation BIGINT NOT NULL, batch_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    url TEXT NOT NULL, payload_json TEXT NOT NULL,
    status TEXT NOT NULL,          -- pending|delivered|failed
    attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TIMESTAMPTZ,
    PRIMARY KEY (app_id, generation, batch_id, seq)
);
```

Existing read paths must hide tombstones: add `AND NOT deleted` to
`DuckDBStore.timeseries` / `timeseries_info*` and the Timescale equivalents,
and to the `timeseries_streams` view in both backends.

### 1b. `ContinuousStore` protocol

```python
class ContinuousStore(Protocol):
    # writer path — one transaction, steps 1–10 of the design doc
    def publish(self, req: PublicationRequest) -> PublicationReceipt: ...
    # PublicationRequest: publication_id: str, mutations: pa.Table
    #   (operation, ref_uri, ts, numeric_value, text_value)
    # PublicationReceipt: publication_id, payload_hash, row_count,
    #   versions: dict[str, int], deduplicated: bool

    # consumer path
    def next_app_batch(self, app_id: str, generation: int,
                       target_keys: int = 50_000) -> AppBatch | None: ...
    # AppBatch: batch_id, batch_kind, generation, has_more: bool,
    #   inputs: list[BatchInputRange(ref_uri, from_version, to_version)],
    #   rows: pa.Table (operation, ref_uri, ts, numeric_value, text_value)
    def commit_app_batch(self, req: CommitRequest) -> CommitResult: ...
    # CommitRequest: app_id, generation, batch_id, batch_kind,
    #   inputs: list[BatchInputRange], outputs: pa.Table,
    #   webhook_intents: list[WebhookIntent]
    # CommitResult: rows_inserted, already_committed: bool,
    #   output_versions: dict[str, int]

    # lifecycle / bootstrap
    def register_app_runtime(self, app_id: str) -> None: ...
    def begin_bootstrap(self, app_id: str, input_ref_uris: list[str],
                        output_ref_uris: list[str]) -> BootstrapState: ...
    def bootstrap_page(self, bootstrap_id: str, page_size: int) -> BootstrapPage | None: ...
    def commit_bootstrap_page(self, bootstrap_id: str, page_id: str,
                              outputs: pa.Table) -> None: ...
    def finalize_bootstrap(self, bootstrap_id: str) -> None: ...
    def set_app_status(self, app_id: str, status: str) -> None: ...
    def app_runtime(self, app_id: str) -> AppRuntimeRow | None: ...
    def reset_app(self, app_id: str) -> int: ...           # new generation
    def delete_app_runtime(self, app_id: str) -> None: ...

    # router / compactor support
    def subscription_index(self) -> dict[str, list[str]]: ...   # ref_uri -> app_ids
    def lagging_apps(self) -> list[str]: ...    # subscription version < head
    def compact(self, chunk_rows: int = 100_000) -> CompactReport: ...
    def metrics(self) -> dict[str, Any]: ...
```

### 1c. Algorithms (per design doc; implementation notes)

**`publish` transaction.** DuckDB: inside `store._lock` via `_write_conn`.
Postgres: one pooled connection, default isolation;
`SELECT … FROM stream_heads WHERE <key> IN (…) ORDER BY <key> FOR UPDATE`
(insert missing heads first with `ON CONFLICT DO NOTHING`, then lock). Steps:
normalize (reuse the `_prepare_frame` dedupe idiom: unique on `(ref_uri, ts)`
keep last), hash via `ids.py`, receipt-check
(`SELECT payload_hash FROM stream_publications WHERE publication_id = ?` —
equal hash ⇒ return stored receipt with `deduplicated=True`; different hash ⇒
raise `PublicationConflict`), lock heads sorted, increment each head once,
upsert canonical rows (`deleted=FALSE`) or tombstones (`deleted=TRUE`, values
NULL) setting `last_stream_version` to the new head version, insert one
`stream_change_keys` row per normalized key, insert the receipt, commit. Return
the receipt; the caller (Manager) wakes the router after commit.

**`next_app_batch` snapshot read.** Postgres: `BEGIN ISOLATION LEVEL
REPEATABLE READ`. DuckDB: one connection, one transaction (snapshot
isolation). Inside the transaction:
1. Read `app_subscriptions` for `(app_id, generation)` → `from_version[ref]`.
2. Find pending publications: `SELECT DISTINCT publication_seq, row_count`
   joined through `stream_change_keys` where `<key>` subscribed and
   `stream_version > from_version[ref]`; order by `publication_seq`;
   accumulate `row_count` until `target_keys` is reached, always taking at
   least one (the oversized-publication rule).
3. `to_version[ref]` = max `stream_version` per ref across the selected
   publications' change keys. **Correctness note:** completeness comes from the
   range scan in step 4, not from publication selection — any unselected
   publication whose versions fall inside `(from, to]` is still picked up by
   the range scan, which is why `publication_seq` allocation order never
   matters. Selection only bounds batch size.
4. Distinct keys: `SELECT DISTINCT <key>, ts FROM stream_change_keys WHERE
   <key> = ? AND stream_version > from AND stream_version <= to` per ref (or
   one query with a VALUES join).
5. Left-join to `timeseries`; **skip rows whose `last_stream_version` >
   `to_version[ref]`** (a later batch delivers the newer value); a key with no
   canonical row or `deleted=TRUE` becomes an `operation='delete'` row; others
   `'upsert'`.
6. Materialize the Arrow table fully, close the transaction, return `AppBatch`
   with `has_more` = (step 2 stopped early). If step 2 finds nothing, return
   `None`. A batch whose row set is empty but whose ranges advance is still
   returned (cursor-only batch) and must be committed.

If `app_runtime.status == 'bootstrapping'`, `next_app_batch` returns bootstrap
pages (`batch_kind='bootstrap'`) until staging is exhausted, then the finalize
step flips to tail batches. Wrap this dispatch inside `next_app_batch` so the
actor loop has a single call.

**`commit_app_batch` transaction.** Recompute the batch id from
`(generation, sorted input ranges)` via `ids.py` and verify it matches
`req.batch_id`. Idempotent claim: insert into `app_batch_commits`; on conflict
return the stored result with `already_committed=True`. Then, in the same
transaction: publish outputs via the shared publication code path with
`publication_id = f"app:{app_id}:{batch_id}"` (skip entirely when outputs are
empty — no empty publication), insert `app_batch_inputs`, advance
`app_subscriptions` monotonically (`SET stream_version = GREATEST(stream_version, to_version)`),
insert webhook intents, record `rows_inserted`, commit. Return output versions
so the server can wake downstream apps.

**`begin_bootstrap`.** One repeatable-read/snapshot transaction: read
`stream_heads` for the input refs, copy every live (`NOT deleted`) canonical
row for those refs into `app_bootstrap_rows` ordered by `(ref, ts)` with
`ROW_NUMBER() - 1` as ordinal, write `app_bootstrap_streams` at captured
versions, create `app_subscriptions` at those versions, set status
`bootstrapping`, commit.

**`finalize_bootstrap`.** One transaction: compute replacement for each
app-owned output ref — staged upserts, plus tombstones for every existing
canonical `(ref, ts)` **not** present in staging (anti-join) so downstream
converges; publish that as one publication
(`publication_id = f"bootstrap:{bootstrap_id}"`); delete staging rows; set
`app_runtime.status='active'`; commit.

**`compact`.** Per ref: `safe = MIN(stream_version)` over subscriptions of
apps whose `app_runtime.status IN ('active','bootstrapping')` (no such
subscriber ⇒ head). Delete `stream_change_keys` with `stream_version <= safe`
in `chunk_rows` chunks; advance `stream_heads.retained_from_version`.
Receipts are never deleted.

### 1d. Wire the writer path through `publish`

- `Manager` gains `self.continuous: ContinuousStore` (constructed per backend
  in `Manager.__init__`/`from_env`) and a
  `publish(publication_id, table) -> PublicationReceipt` method that calls the
  store then `self.router.wake(receipt.versions.keys())` (router lands in
  Phase 2; until then the hook is a no-op callable).
- `insert_timeseries`, `insert_timeseries_batch`, `insert_timeseries_arrow`
  become thin wrappers that build a mutation table (all `operation='upsert'`)
  and call `publish`. `insert_timeseries_arrow` publishes the **whole request
  body as one publication** (the current per-source split at
  `Server/app.py:720` goes away — the writer defines the atomic set).
- HTTP: `/insert_timeseries_arrow` and `/insert_timeseries` accept an optional
  `publication_id` (header `X-Acquirium-Publication-Id` for the Arrow endpoint,
  body field for the JSON one); the server generates a uuid when absent, and the
  response carries the receipt. `Client/client.py` threads it through;
  `Drivers/Driver.py` assigns `uuid4()` when a flush batch is collected and
  reuses it on retry.
- Add a deletion path: `Manager.delete_timeseries(ref_uri, timestamps | range)`
  publishing `operation='delete'` mutations (needed by tests and retraction).

### 1e. Contract suite (the phase gate)

`tests/test_continuous_store_contract.py`, fixture copied from
`tests/test_timeseries_store_contract.py` (params `["duckdb", "timescale"]`,
skip when PG unavailable). Cover, per the design doc's list:

- singleton and multirow publications; exactly one head increment per affected
  stream per publication; duplicate-key normalization inside one publication
- retry with same id + same payload → same receipt, `deduplicated=True`; same
  id + different payload → `PublicationConflict`
- atomicity: canonical rows, tombstones, heads, receipts, manifests all present
  or all absent (inject a failure mid-transaction)
- overlapping concurrent PG writers on shared refs (threads) — no deadlock, both
  commit (sorted locking)
- `next_app_batch`: pending selection, oversized publication taken whole,
  superseded-key skip (write v11 after snapshot-visible v10 → key deferred),
  cursor-only batches, correct `(from, to]` semantics across multi-stream
  publications, intermediate corrections coalesced to one row
- `commit_app_batch`: idempotent duplicate commit returns stored result;
  batch-id verification rejects tampered ranges; monotonic subscription
  advance; empty-output batch advances versions without a publication; rollback
  leaves no partial state; webhook intents recorded
- bootstrap: begin/page/finalize; snapshot excludes concurrent writes (which
  stay pending for tail); no partial output visible before finalize; finalize
  emits tombstones for stale prior output
- compaction: floors advance, retained receipts, stopped app below floor →
  `reset_app` path; resume above floor works
- payload-hash and batch-id derivation are backend-independent (pure `ids.py`
  unit tests too)

Also update existing tests touched by 1d (insert paths now produce receipts,
reads hide tombstones).

---

## Phase 2 — Server: internal endpoints, router, compactor, lifecycle

### 2a. Internal batch endpoints (`Server/app.py`)

```
POST /internal/apps/{app_id}/batches/next
  body: JSON {"generation": int, "target_keys": int}
  200: Arrow IPC stream; schema metadata key "acquirium_batch" =
       {"batch_id","batch_kind","generation","has_more",
        "inputs":[{"ref_uri","from_version","to_version"}]}
  204: no pending work
  409: generation mismatch (actor must refetch runtime state)

POST /internal/apps/{app_id}/batches/{batch_id}/commit
  body: Arrow IPC stream of output mutations; schema metadata
       "acquirium_commit" = {"generation","batch_kind","inputs":[…],
        "webhook_intents":[{"url","payload"}]}
  200: JSON {"rows_inserted","already_committed","output_versions"}
```

Actors never open backend databases; these endpoints are their only storage
access. Add matching methods to `Client/client.py`
(`next_app_batch(app_id, generation)`, `commit_app_batch(...)`) returning the
parsed structures.

### 2b. `ChangeRouter` (`Server/router.py`)

Runs as asyncio tasks in the FastAPI lifespan. State per app:
`{pending: bool, in_flight: bool}` — nothing durable.

- `wake(ref_uris)` (called by `Manager.publish` post-commit, threadsafe via
  `loop.call_soon_threadsafe`): map refs → app_ids through a cached
  subscription index (refreshed on lifecycle changes and on each safety scan),
  set pending bits, and schedule dispatch after ≤ 50 ms coalescing
  (config `router_coalesce_ms`).
- Dispatch: for each ready app not `in_flight`, clear pending, set in_flight,
  fire `actor.process_pending.remote()`; a monitor task awaits the ref, clears
  in_flight, and re-dispatches when the pending bit was set meanwhile or the
  actor reported `has_more`.
- Safety scan every 1 s (`router_safety_scan_s`): `continuous.lagging_apps()`
  → set pending bits. This recovers lost wake-ups and lost actor responses.
- Startup: trigger every app whose `app_runtime.status ∈ {active, bootstrapping}`.

### 2c. Compactor (`Server/compactor.py`)

Lifespan task: every 60 s (`compaction_interval_s`) call
`continuous.compact(chunk_rows=100_000)`; log the report; expose the last
report via the metrics endpoint.

### 2d. Lifecycle endpoints and supervisor rework

Replace the run/stop surface (see also the Phase 5 removal inventory):

- `POST /apps/register` — unchanged shape; additionally calls
  `continuous.register_app_runtime(app_id)` (status `registered`, generation 1).
- `POST /apps/start` `{app_id}` — supervisor asks the runtime store: no prior
  effects → begin bootstrap (actor drives paging); retained cursor → status
  `active` + router trigger; cursor below a retained floor → `reset_app` then
  bootstrap. Returns `{status, generation}`.
- `POST /apps/stop` `{app_id}` — set status `stopping`; actor observes it at
  its next transaction boundary; supervisor flips to `stopped` when the actor
  confirms (`ray.get(actor.confirm_stopped.remote())`).
- `POST /apps/reset` `{app_id}` — stop-drain, `continuous.reset_app` (new
  generation), then the start path (reconciliation).
- `POST /apps/delete` — stop-drain, tombstone-publish all app-owned output
  streams (retraction), `delete_app_runtime`, existing graph/actor/source
  teardown (`AppSupervisor._teardown_app`).
- `GET /apps/list` / status now report from `app_runtime` (DB), not the
  in-memory `running` flag.
- Registering changed app code (`replace=True`) bumps generation and follows
  the reset path.
- `_start_config_apps` (`Server/app.py:218`): `autostart` now calls
  `start_app`; delete the `keep_alive`/`interval` config keys.
- Server restart: `restore_app_specs` + actor respawn as today (minus
  `run.json`), then router startup triggers. Recovery is entirely from
  `app_runtime`/`app_subscriptions`/`app_bootstraps`.

### 2e. Webhook delivery worker

Lifespan task polling `app_webhook_intents` where `status='pending' and
(next_attempt_at is null or <= now)`: POST (reuse `normalize_trigger_url`),
exponential backoff into `next_attempt_at`, cap attempts (config, default 10)
then `failed`. Independent of batch commits by construction.

### 2f. Tests

`tests/integration/test_continuous_server.py`: endpoint round-trips (Arrow in
and out), router coalescing (two publishes inside 50 ms → one dispatch),
safety-scan recovery (drop a wake on purpose), lifecycle transitions including
stop-at-boundary, compactor progress, webhook retry. Unit tests for router
state logic with a fake actor handle (no Ray).

---

## Phase 3 — Actor runtime: `process_pending`

### 3a. `AppRunner` rewrite (`Apps/runner.py`)

Keep: constructor, `register`, `deregister`, `_persist_source`, `_load_app`,
`build_query`, `_sync_dynamic_outputs`, `build_app`, `setup`, `status`, graph
helpers. Delete: `run`, `_run_loop`, `_dispatch_run`, `_monitor_run`,
`_persist_run_state`, `run.json` handling, `_app_run_task`, keep-alive fields.

New core:

```python
async def process_pending(self) -> dict:
    # Returns {"processed": int, "has_more": bool, "status": str}
    runtime = self.acquirium_cli.app_runtime(self.spec.name)
    if runtime.status in ("stopping", "stopped"): return ...
    batch = self.acquirium_cli.next_app_batch(self.spec.name, runtime.generation)
    if batch is None: return {"processed": 0, "has_more": False, ...}
    inputs = InputBatch.from_arrow(batch.rows, mappings=self._mappings)
    outputs = self._transform(inputs)          # in-actor, no Ray task
    self._validate(outputs)                    # declared output refs only
    result = self.acquirium_cli.commit_app_batch(..., batch_id=batch.batch_id,
                                                 inputs=batch.inputs, outputs=outputs)
    return {"processed": batch.rows.num_rows, "has_more": batch.has_more, ...}
```

One batch in flight per generation — the router's `in_flight` bit plus the
actor being single-threaded per call already guarantees it; assert the
generation on every commit. The router re-invokes while `has_more`. Query
refresh (the old per-tick `graph_status` poll) moves to: refresh
`build_query`/`_sync_dynamic_outputs` when the server's `source_version`
changed, checked at most once per `process_pending` turn; a selector change
that adds outputs bumps `topology_version` and follows the reset path
(Phase 2d) — v1 may implement "selector expansion ⇒ reset" and note it.

Commit failure: log, leave versions unadvanced, return; the safety scan
retries. A poisoned batch (transform raises deterministically) sets status
`failed` after N consecutive failures (config, default 3) and stops dispatch
until reset — do not hot-loop.

### 3b. Transform contract

- `AppContext` gains `inputs: InputBatch | None`. `InputBatch` (new,
  `Apps/input_batch.py`): wraps the Arrow table; `.frames()` yields
  `(input_ref_uri, polars.DataFrame(operation, ts, value))` per stream;
  `.mapped_streams(app)` resolves `MappedStream`s for `MappedApp` using
  `resolve_mappings` identity (input_ref → output identity), including
  delete propagation (input delete rows → output delete rows at the same ts,
  unless the transform overrides `resolve_deletes`).
- `MappedApp` continuous path: framework iterates `inputs.mapped_streams`,
  calls the existing `transform(stream, ctx)` per stream with `stream.values`
  now being the batch frame (upserts only), converts returned frames to upsert
  mutations, and appends propagated deletes. `MappedApp.input_data`/`streams`
  remain only for preview/debug.
- Purity enforcement: during `process_pending`, bind the app to a
  `ContinuousGuard` wrapper (extend the `ReadOnlyAcquirium` idiom in
  `Apps/execution.py`) that raises on **reads** of live timeseries
  (`timeseries`, `query.data`, `timeseries_info`, `sql_query`) as well as all
  mutations. Graph reads stay allowed (queries were built at setup). Add
  `Output.delete(point_uri=..., ref_name=..., timestamps=[...])` to
  `Apps/base.py`; extend `execution.validate_outputs` to accept it and to
  reject mutations targeting undeclared output streams (already mostly there).
- Heavy parallel work: `App.parallelism: int = 1` class attribute; when > 1
  the actor shards the pinned `InputBatch` by input stream across stateless
  Ray tasks and commits once after all shards return; any shard failure aborts
  the commit. Implement last within this phase; everything else must not
  depend on it.

### 3c. Bootstrap driving (actor side)

`process_pending` handles `batch_kind='bootstrap'` transparently: transform the
page, commit via the same endpoint (`page_id` as batch id), loop while
`has_more`; when staging is exhausted the server runs `finalize_bootstrap` and
the next turn returns tail batches. Restart at any point resumes from
`app_bootstraps.next_ordinal`.

### 3d. Tests

- Unit (`tests/unit/test_continuous_runner.py`): `InputBatch` framing, delete
  propagation, `ContinuousGuard` rejections, mapped identity stability, shard
  fan-out logic with fakes.
- Integration (`tests/integration/test_continuous_runtime.py`): end-to-end on
  the compose stack — publish → router → actor → output publication → downstream
  app (multi-hop chain, no completion events); restart the server mid-stream;
  kill the actor between next and commit (no version advance, safety-scan
  retry); duplicate commit after a lost response; stop/start; a `MappedApp`
  unit-conversion app over both backends; full-history bootstrap of an app
  added after ingestion, including correctness of tombstone reconciliation
  after a selector change/reset.

---

## Phase 4 — Removal inventory (clean slate)

Do this as its own PR after Phase 3 lands, so the diff is pure deletion:

- `internals/models.py`: delete `AppRunRequest`; delete `AppSpec.resume_keep_alive`,
  `run_interval`, `run_start`, `run_end`, `run_params`. Add
  `AppStartRequest`/`AppResetRequest` (Phase 2 introduces them; this PR removes
  the old ones and stragglers).
- `Apps/runner.py`, `Apps/supervisor.py`: any remaining keep-alive/run-loop
  code, `run.json` read/write, `restore_app_specs`'s `run.json` block,
  in-memory `running`/`started_at`/`stopped_at` bookkeeping.
- `Apps/output_emission.py`: delete `PersistSink`; keep `normalize_trigger_url`
  (move to `Server/webhooks.py` if the module empties out).
- `Server/app.py`: `/apps/run`; keep-alive/interval branches in
  `_start_config_apps`; `run_app` request models.
- `Client/acquirium.py` + `Client/client.py`: `run_app` and its
  `keep_alive`/`interval` parameters → `start_app`/`reset_app`; update
  `Client/app_display.py` and `cli.py` (`acquirium app …` subcommands: `run` →
  `start`, add `reset`).
- Old-schema remnants: none of the cursor/outbox modules exist in this tree —
  but delete the stale `.pyc`s under `src/acquirium/Apps/__pycache__/`
  (changefeed/outbox/watermark/etc.) so nobody greps into ghosts.
- Tests: rewrite/retire `tests/unit/test_app_cli.py`, `test_config_apps.py`,
  `test_app_restore.py`, `test_app_query_refresh.py`, `test_apps_output.py`
  against the new lifecycle.
- Docs: update `docs/apps.md`, `docs/data-stream-lifecycle.md`,
  `docs/http-api.md`, `docs/drivers.md`, `README.md` examples, and the
  `acquirium.*.toml` sample configs (drop `keep_alive`/`interval` keys).
  `CHANGELOG.md` entry describing the breaking change.

## Phase 5 — Metrics and acceptance benchmark

- `GET /internal/continuous/metrics`: JSON from `continuous.metrics()` merged
  with router/compactor state — per-ref head/floor, per-app version lag and
  status, manifest rows/bytes, bootstrap age, last compaction report, batch
  counts/latency histograms (record in-process; no new dependencies), and
  end-to-end latency (input publication commit → output publication commit,
  measured by the benchmark, not the server).
- `benchmarks/continuous_latency.py` (the old benchmark source is gone; only
  `__pycache__` remains): N apps (`value + 1` MappedApp), one input row per
  second each, measures p50/p95 input-commit→output-commit latency, failed
  batches, and drain time after stopping input. Runs against the compose stack
  via env from `tests/conftest.py`; reports DuckDB and Postgres separately.
- **Acceptance gate (from the design doc):** 64 apps, one row/sec: zero failed
  batches, p50 < 500 ms, p95 < 2 s, drain < 2 s, on both backends. Tune only
  after metrics exist; the defaults table in the design doc is the starting
  configuration (50 ms coalesce, 50k target keys, 1 s scan, 60 s/100k
  compaction).

## Invariants that must never be violated (re-verify at every phase)

1. Values are only readable through canonical `timeseries`; manifests carry
   keys, never values.
2. Every stream head advances exactly once per publication that touches it.
3. Output publication, input-range provenance, subscription advance, and
   webhook intent are one transaction — all or nothing.
4. A canonical row whose `last_stream_version` exceeds the batch's
   `to_version` is never delivered in that batch.
5. Publication receipts survive compaction; a retried `publication_id` with a
   different payload hash is always an error.
6. Ray actors and the router hold no state that recovery depends on.
7. DuckDB writes all pass through the single server-process write coordinator
   (`DuckDBStore._lock`); actors never open backend databases.

## Left open (implementer's choice, note the decision in code)

- Exact FastAPI wiring of the router wake across worker threads
  (`call_soon_threadsafe` vs. an `asyncio.Queue`).
- Whether `bootstrap_page` streams pages via the `next` endpoint response or a
  dedicated `/internal/apps/{app_id}/bootstrap/…` pair — either is fine if the
  actor-side loop stays a single `process_pending` code path.
- Polars vs. pure-pyarrow inside `InputBatch` framing.
- Backoff constants for webhook delivery and the failed-batch circuit breaker.
