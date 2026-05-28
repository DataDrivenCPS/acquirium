# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.y.z`, the public API is considered unstable and may
change in any release.

## [Unreleased]

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
- `tests/unit/test_bundled_ontologies.py` and
  `tests/unit/test_embedding_cache.py`.

### Changed
- The QUDT converter is now built lazily from the in-store QUDT graph
  (`graph_store.named_graph(QUDT_UNIT_IRI)`) instead of re-parsing the
  bundled TTL. User overrides at the canonical QUDT IRI are honored
  automatically.
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

[Unreleased]: https://github.com/DataDrivenCPS/acquirium/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/DataDrivenCPS/acquirium/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/DataDrivenCPS/acquirium/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/DataDrivenCPS/acquirium/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/DataDrivenCPS/acquirium/releases/tag/v0.1.0
