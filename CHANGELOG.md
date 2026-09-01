# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.y.z`, the public API is considered unstable and may
change in any release.

## [Unreleased]

## [0.4.0a4] - 2026-09-01

### Fixed
- Dependency bounds. `pyshifty`, `pyontoenv`, `numpy`, and `ipykernel` were
  declared as either exact pins or open-ended ranges. Published releases
  carried `pyshifty>=0.3.0`, so a fresh `pip install acquirium` pulled
  pyshifty 0.4.x, whose API change crashed the server at startup. All four
  now use compatible-release ranges (`pyshifty>=0.3.0,<0.4`,
  `pyontoenv>=0.6.0,<0.7`, `numpy>=2.3,<3`, `ipykernel>=7.0.1,<8`), so new
  installs stay on tested majors without forcing exact versions on
  downstream environments.

## [0.4.0a3] - 2026-08-30

### Fixed
- CSV ingestion no longer types columns from a 100-row sample; every cell is
  read as text (values were cast to text downstream regardless). Files where a
  numeric column carries a status word (`LOW`, `OK`) past the sample window, or
  holds `0` for hundreds of rows before its first decimal reading, previously
  failed to read.
- `Query.metadata()` (explore and `Q`) infers its result-table column types
  from all rows. A sparse column — a point label bound on few rows — was
  previously typed from the first 100 rows and the first real value failed the
  build.

### Removed
- The `infer_schema_length` CSV driver key: with every cell read as text it
  has no effect.

## [0.4.0a2] - 2026-08-27

### Added
- **Explicit driver-author contract.** Ingest drivers declare streams with
  `declare()` before reporting observations; the platform owns datasource and
  stream registration, value-kind inference, lossless buffering, retry
  retention, and final shutdown flushes. File reads return `FileBatch`, whose
  cursor advances only after successful insertion.
- Public `to_timestamp()` and `to_observations()` driver helpers, including
  native/ISO/common timestamp parsing, split date/time columns, Unix epochs,
  timezone handling, conservative column-name discovery, and explicit
  `date_format` / `day_first` controls.
- `CSVIngestDriver` customization hooks: `prepare_frame(df, path)` runs on the
  raw frame before reshaping and `declare_stream(ref_name)` attaches metadata
  per discovered stream. New config keys `null_values` and
  `infer_schema_length` pass through to the CSV reader.
- **Point labels in displays.** Placeholder points minted at registration get
  a default `rdfs:label` (`<source_id>__<ref_name>`); explore queries fetch
  each data node's label, `DataObject.metadata()` gains a `point_label`
  column, `Query.metadata()` shows `<alias>.label` columns, and wide
  dataframe columns use the label instead of the URI where it is unique.
- Explore query aliases are checked for uniqueness: duplicate explicit
  aliases raise, derived aliases are uniquified.
- Documentation reorganized into tutorials, how-to guides, reference, and
  explanation (`docs/_index.md`), with a query cookbook and driver tutorials.

### Changed
- File-driver configuration is centralized under `[[drivers]]`: `source_id`,
  `watch_dir`, and `glob` are explicit and required. CSV/XLSX/Parquet layout is
  also explicit; stream names are preserved exactly rather than sanitized.
- Driver graph polling has its own cadence, independent of data ticks. Graph
  content and graph-file insertion use separate APIs.
- **Stream registration checks point metadata.** `register_streams()` and
  `resolve_point_metadata()` now live on `AcquiriumClient` (the `Acquirium`
  facade delegates). A stream registered against an existing `point_uri` is
  checked against the graph: missing metadata fields are added, conflicting
  values raise `ValueError` before anything is inserted. A stream without a
  `point_uri` mints a placeholder point (`<ref_uri>__point`); a stream with
  neither `point_uri` nor `ref_name` is rejected.
- A stream unit that differs from its point's unit is accepted when the server
  reports the two convertible; it is recorded on the external reference as the
  storage unit and converted on read. Unconvertible units still raise.
- `Query.dataframe()` and `DataObject.dataframe()` share
  `shape/start/end/limit/order/include_ref/compact` with identical defaults
  (`"wide"`, `compact=True`), so `q.dataframe(...)` equals
  `q.data(...).dataframe(...)`. On `DataObject` the window applies client-side
  to the fetched frame.

### Fixed
- Warm start re-registers any bundled ontology (water, s223, QUDT quantity
  kinds, ref-schema) missing from an existing store instead of skipping
  registration whenever the store held any graph.
- Concurrent `/resolve_record` and `insert_graph` calls could deadlock the
  server on ontoenv's store-backed graph view (GIL/mutex inversion).
  `named_graph()` now returns a cached in-memory copy built under the store
  lock.
- The explore query cache key distinguishes queries run with and without
  ontology dependencies.

### Removed
- **Breaking:** `TabularIngestBase`, implicit per-file datasource identity,
  automatic wide/narrow layout selection, and implicit graph path detection.
  Specialized file drivers now implement `read(path, cursor)` explicitly and
  may call the plain tabular conversion helpers.

## [0.4.0a1] - 2026-08-14

### Added
- **Source-owned graph writes.** Every deployment graph write names its owner: `insert_graph(..., source_id=...)` and `sparql_update(..., source_id=...)` on both `Acquirium` and `AcquiriumClient`, and a `source_id` field on `POST /insert_graph` / `POST /sparql_update`. `"plant"` is the reserved owner of the shared plant model; every other source writes its own graph, whose URI is a pure function of the source ID (`urn:acquirium:graph:data:source:<id>`). Components do not pass an owner themselves: `Driver.insert_graph()` / `Driver.sparql_update()` and `App.insert_graph()` / `App.sparql_update()` always write the component's own graph, and an app's graph owner is `app:<name>` (`acquirium.Apps.base.app_source_id`).
- **Derived query cache with explicit freshness.** The server keeps the inferred graph and the resolved ontology/shape closure in a separate, disposable query dataset. A graph write marks it stale and schedules a single-flight background rebuild; reads return the last complete published generation instead of blocking. `wait_for_fresh=true` (on `/sparql`, `/sparql_json`, and `AcquiriumClient.sparql_query`) waits for pending inference instead.
- **Graph freshness status.** `GET /graph_version` returns `{version, source_version, published_version, is_current, rebuild_in_progress}`; `version` is retained for existing pollers and equals `source_version`. New `Acquirium.graph_status()` / `AcquiriumClient.graph_status()` return the whole document; `graph_version()` still returns the counter.
- **Read-only SPARQL 1.1 Protocol endpoint** `GET /sparql`, accepting SELECT, ASK, CONSTRUCT, and DESCRIBE. The response serialization comes from the `Accept` header (SPARQL Results JSON/XML, CSV, TSV; Turtle, N-Triples, RDF/XML, JSON-LD), defaulting to Results JSON for SELECT/ASK and Turtle for CONSTRUCT/DESCRIBE. Dataset-selection and update protocol parameters are deliberately not exposed.
- `POST /validate_graph`, with `Acquirium.validate_graph()` / `AcquiriumClient.validate_graph()` — SHACL validation of all registered deployment data against the ontology shape closure.
- `docs/graph-backend-architecture.md` and `docs/http-api.md`.
- `make test-timing` — the compose-backed suite plus the slowest test cases (`PYTEST_DURATIONS=0` lists all).
- **Explore query interface.** `acq.explore()` builds the new `Query`: `entity()` / `related()` / `measurement()` pattern verbs; one attribute vocabulary (type, process, cp_type, medium, substance, quantity_kind, unit, enumeration_kind, data_source) shared by `where()` filtering (with `Not()` exclusion and lists-as-OR), `include()` / `drop()` / `with_columns()` column controls (invertible, dotted `"alias.attr"` targeting, `required=`), and `options()` / `facets()` faceted exploration; `alias()` / `refocus()` pointer control; `to_sparql()` / `execute()` / `metadata()` / `data()` / `dataframe()` terminals. Attribute tables are generated into the docstrings of every attribute-taking method.
- **Client-side multi-hop traversal.** `related`/`measurement` traversal runs as client-side BFS instead of join-explosive SPARQL property paths. `via=` takes a predicate, a predicate list, `"any"`/`"all"`, or a step-pattern constant; `direction="upstream"/"downstream"` maps to inspectable constants (`UPSTREAM_EQUIPMENT`, `DOWNSTREAM_EQUIPMENT`, `UPSTREAM_PROPERTY`, `DOWNSTREAM_PROPERTY`); `nearest=` returns closest matches per source; `max_depth` defaults to 3 (`0` = unbounded). Attribute predicates plus `rdfs:subClassOf`, `s223:hasProperty`, `ref:hasExternalReference`, and `s223:cnx` are hidden from `via="any"` by default (`hide()` / `unhide()` / `hidden_predicates()`).
- **Multi-measurement UNION compilation.** Queries with several measurement nodes compile as UNION branches: M+N rows with nulls where a node has no data, instead of a cross product that empties the whole result.
- `kind="process"` resolution: the process taxonomy is its own resolver kind and is excluded from `kind="class"`.
- `POST /sparql_json` accepting a JSON body (resolved traversal queries exceed URL limits); the client always POSTs.
- `POST /resolve_conversion`: joint source/target unit resolution that picks the best *convertible* pair; `DataObject.convert_to` uses it.
- `Acquirium()` waits for `/health` (default 60 s, `health_timeout=None` to skip) and raises `ConnectionError` when the server is unreachable.
- `include(required=True)` drops rows lacking the attribute instead of binding null.

### Changed
- **Breaking: `use_union` / `include_union` renamed to `include_dependencies`** everywhere it selects the ontology/shape closure — `Query.execute()`, `metadata()`, `dataframe()`, `data()`, `options()`, `facets()`, `AcquiriumClient.sparql_query()`, `GET`/`POST /sparql_json`, `GET /sparql`, and `GET /export_graph`. The default stays `True`.
- **Breaking: `source_id` is required on graph writes.** `insert_graph()` and `sparql_update()` take it as a keyword-only argument with no default, and each entry passed to `register_streams()` must carry a non-empty `source_id`.
- **Public graph read views.** Reads expose the deployment graph — the inferred union of every registered source graph — with or without ontology/shape dependencies. Neither view is the plant graph alone; `export_graph(include_dependencies=False)` returns all deployment data rather than just the plant graph.
- App and driver runners poll `graph_status()["source_version"]` instead of `graph_version()` to decide when to rebuild their queries.
- **DuckDB timeseries rows are keyed by an `INTEGER ref_id`** resolved through a new `ref_ids` table, instead of the `ref_uri` string (integer columns prune far better on min-max zonemaps, and the rows are narrower). The API still speaks `ref_uri` and the `timeseries_streams` view joins the string back in, so SQL against the view is unaffected. **Breaking on disk:** a database whose `timeseries` table is keyed by `ref_uri` cannot be opened by this version — recreate it.
- **DuckDB opens a connection per operation** against the shared in-process database instance, so reads no longer wait on writes. Writes are serialised by a lock and each runs in its own transaction; `begin()` / `commit()` / `rollback()` spans use a dedicated transaction connection, and their uncommitted writes are invisible to reads.
- DuckDB keeps no secondary indexes on the `timeseries` table: the `UNIQUE (ref_id, ts)` constraint already serves the point lookups, and ART index maintenance cost every insert and bloated the WAL.
- An explicit `value_mode` now wins over the stream's registered `value_kind`, so a numeric read of a text-kind stream returns the numeric column it filtered on rather than an all-NULL text column.
- The numeric/text value split is vectorized in `acquirium.Storage.values` (`typed_value_series`) instead of running per row — 27–54× faster on bulk insert. Mixed-type batches and ints outside Int64 still take the row-wise path, which preserves their exact semantics.
- Derived-cache publication uses Oxigraph's low-latency `load` for payloads up to 8 MiB and `bulk_load` above it, avoiding SST-file creation for small caches.
- The dependency closure is published as its own query graph unioned at query time, instead of being duplicated into a second combined graph.
- The explore query cache key accounts for `include_dependencies`, so results from the two views no longer collide.
- Explore aliases must be unique: a duplicate explicit `alias()` is rejected, and derived aliases are uniquified.
- The explore builder is the main `Query` interface; the legacy query class is renamed to `Q`.
- `stop_app` takes a required `app_id=`; the never-implemented `run_id=` parameter is gone.
- `Output.event` requires `point_uri` in its signature (it always raised without one).
- Wide dataframes order value columns alphabetically (case-insensitive) after `time`; `metadata()` returns real nulls instead of `"None"` strings; attribute columns interleave directly after their node's column.
- `--verbose` scopes DEBUG to `acquirium.*` loggers; root logging stays at INFO so solver libraries don't flood logs.
- `client.insert_log` defaults `log_time` to timezone-aware UTC.
- pyontoenv upgraded to 0.6.0.

### Fixed
- Derived-cache rebuild race: publication released the store lock before retiring the rebuild owner, so a write landing in that window scheduled no follow-up and left the cache a generation behind for every later read (or raised "derived cache remained stale after rebuild" on a read-only query).
- `validate()` snapshots its inputs under the store lock and runs SHACL outside it; holding the lock across a full validation stalled every concurrent query and write.
- Ontology-graph exclusion is now a live check on graph-URI shape rather than a set frozen at construction, so a source graph registered after startup can no longer be catalogued by OntoEnv as an ontology.
- The value column's type could flip between batches of a single DuckDB read; it is now resolved once per read, probed over the queried range and independent of `LIMIT`.
- `DataObject` alias access returns a stable row order: deduplication maintains order and rows sort on `(time, point_uri)`, which is total. Deployment reads union every source graph, so an alias can cover points registered by several components, and the old time-only sort let a text-valued point land on any row.
- `Driver.source_id` raises a clear error when read before it is set, instead of Python's generic attribute message.
- `AcquiriumClient.insert_graph` raised `NameError` for multi-line RDF text not starting with `<`, `@`, or `#`.
- The embedding model cache honors `FASTEMBED_CACHE_PATH`, so a pre-warmed cache (e.g. in the Docker image) is actually used.
- BENICIA deployment config: `model` and `watch_dir` resolved against the wrong base and the committed historical parquet sat outside the watched directory; the shipped config now replays it.
- Empty `DataObject` keeps its client; `convert_to` on an empty result is a no-op.
- Traversal pruning crash when `include()` was combined with a traversal edge.

### Removed
- The `via=` shortcut system; direction step patterns became inspectable constants.
- `App.docker_image` / `App.entrypoint` / `App.command` (dead since apps moved to Ray actors) and the stale `python -m acquirium.Apps.worker` references in example apps.
- `scripts/benchmark/` (stale; cited files that no longer exist).

## [0.4.0a0] - 2026-07-22

### Added
- **Ray driver backend.** `[[drivers]]` now run as Ray actors managed by the server instead of background threads in the CLI process. New driver-management HTTP endpoints — `POST /drivers/start`, `POST /drivers/stop`, `GET /drivers/list` — and a matching `acquirium driver` CLI group: `acquirium driver start CONFIG`, `acquirium driver list`, and `acquirium driver stop --name X` (each accepting `--server-url` / `--server-port`, or `--config` for the server address).
- **SPARQL UPDATE support.** `AcquiriumClient.sparql_update(update)` and the backing `POST /sparql_update` endpoint run an INSERT/DELETE against the graph store and bump the graph version.
- **App build phase.** New `App.build_app(ctx)` hook, run once during registration, plus an `AppContext.state` field and `AppContext.params`. `Acquirium.register_app(...)` gained a `params=` argument (stored with the app and passed to `build_app` via `ctx.params`) and a `replace=False` argument; `POST /apps/register` gained a `replace` query parameter that gracefully tears down an existing app of the same name before re-registering (otherwise the endpoint returns `409`).
- **App deletion / teardown.** `Acquirium.delete_app(app_id)` and `AcquiriumClient.delete_app(app_id)`, backed by a new `POST /apps/delete` endpoint, which stops the app, strips its registration triples from the graph, kills its actor, and removes its persisted source.
- **App restore after restart.** Apps registered by a previous server run are rebuilt from the persistent graph on startup and their actors respawned (build phase re-run; run phase can rebuild on failure).
- `ParquetIngestDriver` (`acquirium.Drivers.BuiltInDrivers.parquet_ingest:ParquetIngestDriver`) — watches a directory for `*.parquet` / `*.pq` files and ingests new rows, supporting the same wide/narrow/`auto` formats as `CSVIngestDriver` and preserving native column dtypes.
- `CSVIngestDriver` is now exported from the top-level `acquirium` package.
- `Driver.data_dir()` — resolves the Acquirium data directory (env `ACQUIRIUM_DATA_DIR` > `[server] data_dir` > `<config_dir>/.acquirium`) so driver state lands inside the data dir.
- **Python 3.11–3.14 support** (`requires-python` lowered from `>=3.12` to `>=3.11`, with classifiers for 3.11–3.14).
- `streamlit` added to the `watertap` optional extra (for the model-agnostic input GUI in the WaterTAP deployment).
- `Query.metadata()` gained a `use_union` keyword (default `True`).

### Changed
- **Driver execution model.** Drivers no longer run as in-process background threads. `acquirium server` submits `[[drivers]]` to the server to run as Ray actors that connect back over HTTP; `[server] enabled = false` now *submits* the config's `[[drivers]]` to the remote server in `[driver]` (equivalent to `acquirium driver start`) instead of running a local thread loop. File-based driver specs must therefore resolve on the server host.
- **App execution model.** Apps run inside Ray `AppRunner` actors rather than Docker containers. The app's Python source is shipped to the server and loaded by the actor.
- **Package restructure into dedicated packages.** Driver code moved under `acquirium.Drivers`: `acquirium.Driver` → `acquirium.Drivers.Driver`, and `acquirium.BuiltinDrivers.*` → `acquirium.Drivers.BuiltInDrivers.*` (note the capitalization change `Builtin` → `BuiltIn`). This changes `[[drivers]]` `spec` strings — e.g. `acquirium.BuiltinDrivers.system_metrics:SystemMetricsDriver` becomes `acquirium.Drivers.BuiltInDrivers.system_metrics:SystemMetricsDriver` (**Breaking:** existing configs and imports must be updated).
- **`--workers` must be 1.** `acquirium server` now refuses `workers > 1` on every timeseries backend (previously allowed on the timescale backend), because the embedded Oxigraph graph store is single-process (one RocksDB writer). The `-w/--workers` help text and behavior reflect this.
- **App status/listing semantics.** `Acquirium.list_app_runs(app_id=...)` now lists *registered apps*, or one app's build/run status when `app_id` is given (previously listed active keep-alive runs). `run_app`, `stop_app`, `list_app_runs`, and `delete_app` return a rich `AppsResponse` display object.
- **WaterTAPDriver rewritten to a mapping-JSON contract.** The shipped `acquirium.Drivers.BuiltInDrivers.watertap:WaterTAPDriver` is now fully config-driven: it reads points from a model's `watertap-mapping.json` (`properties` → Pyomo variables) and resolves `watertap_build_spec` / `watertap_change_inputs_spec` / `watertap_solve_spec` callables, driving a `build -> change_inputs -> solve` cycle each tick. New config keys include `watertap_mapping_path`, `watertap_build_spec`, `watertap_solve_spec`, `watertap_change_inputs_spec`, `watertap_build_kwargs`, and `watertap_result_attr`.
- **Default driver state location** moved from `<config_dir>/.acquirium/drivers/` to `<data_dir>/drivers/` (see `Driver.data_dir()`), so state lives inside the resolved data directory rather than beside the config file.
- **Graph-store and DuckDB concurrency model** reworked for consistent concurrent querying (DuckDB now follows the vendor-recommended connection model), plus added locking around shared state.
- **Batched stream-ref registration** on the server (new batch registration paths in the DuckDB and TimescaleDB stores) for faster bulk stream registration.
- `pyproject.toml`: added `ray>=2.56.0` to runtime dependencies; pinned `py-rust-stemmers>=0.1.7` via `constraint-dependencies` so `fastembed` installs wheel-only on Python 3.14.

### Removed
- **`docker>=7.1.0`** runtime dependency.
- **Breaking:** `docker_image`, `entrypoint`, and `command` parameters on `Acquirium.register_app(...)`, and the `docker_image`, `module`, `entrypoint`, and `command` fields on the `AppSpec` model / `POST /apps/register` request body. App execution no longer goes through Docker.
- Internal driver-only thread machinery in the CLI (`_run_driver_only_mode`, `_run_driver_loop`) — replaced by the Ray submission path described above.

### Fixed
- **`Query.filter_by_medium`** now filters on the correct predicate (`OF_MEDIUM` instead of `HAS_MEDIUM`), so medium filters match the model's actual property.
- **Class-membership query performance.** `rdf:type` / `subClassOf*` traversals are now anchored at the target class inside a sub-`SELECT`, so Oxigraph evaluates the subclass path backward from the class instead of forward from every typed individual — roughly `~6s → ~0.03s` when a deep-hierarchy vocabulary (e.g. QUDT, ~16k individuals) is in the union graph.
- `Query.metadata()` now de-duplicates its result rows.

## [0.3.1] - 2026-05-29

### Added
- Bundled default ontologies inside the `acquirium` package
  (`acquirium/_ontologies/`): NAWI Water, ASHRAE 223P, QUDT units +
  quantity kinds, and Brick's `ref-schema`. They ship inside the wheel
  and load at server startup — no separate `ontologies/` directory or
  HTTP fetch required.
- Versionless canonical IRIs for QUDT: bundled QUDT 3.2.1 is rewritten
  on load to `https://qudt.org/vocab/unit` and
  `https://qudt.org/vocab/quantitykind`. `owl:versionIRI` is preserved
  so the original version is still recorded in the graph.
- `[ontologies] sources` in `acquirium.toml`: a list of strings (load
  as-is) or `{ source = "...", as = "<canonical IRI>" }` tables (parse,
  rewrite declared ontology IRI to the canonical key, replace any
  pre-existing graph at that IRI). Use this to override a bundled
  ontology with a different file or URL.
- `AcquiriumClient.compact_uri` / `expand_uri` for round-tripping
  between full URIs and `prefix:local` CURIEs, plus
  `AcquiriumClient.namespace_manager` exposing the server's bound
  prefixes as an rdflib `NamespaceManager`.
- `tests/unit/test_bundled_ontologies.py`,
  `tests/unit/test_embedding_cache.py`, and tests covering CURIE
  prefix conversion and namespace transfer on graph insert.

### Changed
- The QUDT converter is now built lazily from the in-store QUDT graph
  (`graph_store.named_graph(QUDT_UNIT_IRI)`) instead of re-parsing the
  bundled TTL. User overrides at the canonical QUDT IRI are honored
  automatically.
- Client URI/CURIE handling now uses rdflib's `NamespaceManager`
  instead of manual longest-prefix string matching; query results and
  metadata render identifiers as compact `prefix:local` CURIEs.
- Prefix bindings declared in inserted Turtle/RDF (`@prefix`) are now
  propagated into the query dataset, so they survive into the
  `/namespace/list` endpoint. `OxigraphGraphStore.namespace_manager`
  now returns the query dataset's manager.
- Query result assembly is unified in the client `DataObject` and the
  wide→long reshape uses native Polars expressions instead of nested
  Python loops.
- Bumped `pyontoenv` to `0.6.0a2`.

### Removed
- `ACQUIRIUM_ONTOLOGY_IRIS`, `ACQUIRIUM_ONTOLOGY_DEPENDENCIES`, and
  `ACQUIRIUM_GRAPH_NAME` environment variables; the legacy
  `[ontologies]` name-to-IRI map and `[server] ontology_dependencies`
  /`graph_name` keys. The bundled IRIs are fixed by the package and
  overrides go through `[ontologies] sources` only.
- `OxigraphGraphStore.qualify_uri`, `_uri`, `register_ontology`,
  `ensure_ontology_root`, and the `qudt_converter` / `base_namespace`
  / `ontologies_dir` constructor parameters on `OxigraphGraphStore` —
  all unused after the refactor. (`Manager.__init__` still accepts
  `qudt_converter` and `qudt_graph` for callers who want to inject a
  pre-built converter.)
- `AcquiriumClient.list_namespaces` and `strip_namespace`, replaced by
  `namespace_manager` / `compact_uri` / `expand_uri`.

## [0.3.0] - 2026-05-27

### Added
- `Driver.state` — a persistent key-value store (`DriverState`) scoped per
  driver instance, surviving restarts. Tabular ingest drivers use it to
  persist per-file row offsets so already-ingested rows are not re-read.
- `acquirium server -v` / `--verbose` (and `ACQUIRIUM_VERBOSE=1`) to enable
  DEBUG logging across `acquirium.*` (server, storage, drivers).
- Backend namespace service: new endpoint exposing the configured
  namespace map; the client now resolves URIs against the server's
  namespace list instead of hardcoding them.
- Public tabular ingest hooks on `TabularIngestBase`: subclassable
  `read_frame`, `ensure_streams_registered`, `skip_cols`, and a
  `_is_timeseries_frame` fast path for drivers that already produce a
  normalized `(ts, ref_name, value)` frame.
- `CSVIngestDriver`: per-column ignore filter (`skip_cols`), applied
  before parsing so excluded columns never enter the polars frame.
- `XLSXIngestDriver`: `skip_cols` is applied to the merged sheet frame
  before slicing.
- `acquirium.internals._log` module with `configure_logging()` and a
  `timed_debug` context manager; FastAPI middleware emits one DEBUG line
  per HTTP request with elapsed ms.

### Changed
- Graph store is now split into separate Oxigraph source and query
  datasets; ontoenv state is persisted under `env_root` and reused across
  restarts so cold startup avoids the full directory crawl.
- Closure-cache invalidation is decoupled from source writes — ordinary
  instance-data inserts no longer trigger an owl:imports closure rebuild.
- Embedding indexes are now deterministic: graph-concept and QUDT-concept
  extraction sort and dedupe their inputs, so successive builds over the
  same vocabulary produce identical embeddings.
- Bundled QUDT ontologies upgraded to **3.2.1** (`qudt_unit.ttl`,
  `qudt_qk.ttl`); the default `_ONTOLOGY_IRIS` map in `OxigraphGraphStore`
  was updated to match the new versioned IRIs.
- `Query` / `DataObject`: `ref_uri` is hidden from results and
  visualisations by default; pass `include_ref_uri=True` to opt back in.
- Tabular driver base module renamed from `_tabular_base` to public
  `tabular_base`; `TabularIngestBase` (no leading underscore) is the
  supported subclass entry point.
- No-op graph-change notifications are now skipped, avoiding spurious
  driver `on_graph_change()` ticks.

### Removed
- **Breaking:** `acquirium.BuiltinDrivers._tabular_base` and
  `_TabularIngestBase` — import from
  `acquirium.BuiltinDrivers.tabular_base` and subclass
  `TabularIngestBase` instead. The compatibility shim has been deleted.
- Bundled `ontologies/Brick.ttl` — no longer referenced by the default
  ontology set.

## [0.2.0] - 2026-05-19

### Added
- `Acquirium.resolve_point_metadata()` — joint resolution of a point's
  semantic fields (`unit`, `quantity_kind`, `medium`, `substance`) so
  related siblings disambiguate each other.
- `AcquiriumClient.resolve_record()` / `resolve_record_uris()` and the
  matching `POST /resolve_record` server endpoint for joint, context-aware
  record resolution.
- `resolve_text` / `Query` text-matching now accepts a `context` argument
  for context-based reranking.
- `ConceptResolver`: a single declarative tiered cascade
  (graph → converter → qudt) that replaces the previous ad-hoc resolution
  paths, with case-sensitive and case-insensitive exact-match tiers before
  semantic fallback.
- `[ontologies]` table in `acquirium.toml` (and `ACQUIRIUM_ONTOLOGY_IRIS`
  env var) to override the ontology IRIs that feed the embedding indexes.

### Changed
- Graph store is now backed by Oxigraph through ontoenv and serves a
  cached data-graph closure; ontoenv's `.ontoenv` metadata lives under the
  configured `env_root`.
- Embedding indexes are built from the ontoenv-loaded vocabulary graphs
  rather than scanning the merged dataset.
- Graph index updates are synchronous and incremental; the async
  freshness / `wait_for_embedding` machinery has been removed and
  `AcquiriumClient.insert_graph()` returns once the indexes reflect the
  new triples.
- Exact matches in `resolve_text` are no longer demoted by a later
  semantic tier.

### Removed
- **Breaking:** `Acquirium.register_stream()`. Use the batched
  `Acquirium.register_streams([{...}])` for a single stream as well.
- **Breaking:** `wait_for_embedding` parameter on
  `AcquiriumClient.insert_graph()` and the `/insert_graph` request body —
  insertion now always waits for index readiness.

## [0.1.1] - 2026-05-19

### Changed
- Default `timeseries_backend` is now `duckdb` when no `acquirium.toml` is
  present, so `pip install acquirium && acquirium server` starts a working
  server with no external services. Set
  `ACQUIRIUM_TIMESERIES_BACKEND=timescale` (or configure it in
  `acquirium.toml`) to opt back into Postgres/TimescaleDB.
- `ipykernel` moved out of the runtime dependency set into a `notebook`
  optional extra; install with `pip install "acquirium[notebook]"`.

## [0.1.0] - 2026-05-19

### Added
- Initial public release on PyPI.
- `acquirium` CLI entry point.
- Client API (`Acquirium`, `Query`, `DataObject`).
- Apps framework (`App`, `Output`, `AppContext`).
- Driver framework (`Driver`, `IngestDriver`, `EventIngestDriver`, `PollingIngestDriver`).
- Built-in drivers and WaterTAP integration (optional `watertap` extra).
- Text matcher backed by FastEmbed with QUDT and graph indexes.
- Grafana dashboard helpers.

[Unreleased]: https://github.com/DataDrivenCPS/acquirium/compare/v0.4.0a4...HEAD
[0.4.0a4]: https://github.com/DataDrivenCPS/acquirium/compare/v0.4.0a3...v0.4.0a4
[0.4.0a3]: https://github.com/DataDrivenCPS/acquirium/compare/v0.4.0a2...v0.4.0a3
[0.4.0a2]: https://github.com/DataDrivenCPS/acquirium/compare/v0.4.0a1...v0.4.0a2
[0.4.0a1]: https://github.com/DataDrivenCPS/acquirium/compare/v0.4.0a0...v0.4.0a1
[0.4.0a0]: https://github.com/DataDrivenCPS/acquirium/compare/v0.3.1...v0.4.0a0
[0.3.1]: https://github.com/DataDrivenCPS/acquirium/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/DataDrivenCPS/acquirium/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/DataDrivenCPS/acquirium/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/DataDrivenCPS/acquirium/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/DataDrivenCPS/acquirium/releases/tag/v0.1.0
