# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.y.z`, the public API is considered unstable and may
change in any release.

## [Unreleased]

## [0.2.1] - 2026-05-20

### Added
- `Query.timeseries(..., include_ref=True)` opt-in to keep the per-`ref_uri`
  `ref` column in narrow time-series output for callers that need it.

### Changed
- `Query.timeseries()` now keys results by the data-node alias instead of
  `ref_uri`. Narrow output columns are
  `["data_alias", "point_id", "time", "value_numeric", "value_text"]`;
  wide output column names use the data-node alias, disambiguated as
  `f"{alias}__{point_local}"` when one alias resolves to multiple points.
  Rows sharing the same `(data_alias, point_id, time)` across multiple
  `ref_uris` are merged first-wins.
- Driver loop runs the graph-version watcher on its own thread so a
  long-running `Driver.tick()` (e.g. bulk CSV ingest) no longer starves
  `Driver.on_graph_change()`.
- `pyproject.toml` migrated from the deprecated `tool.uv.dev-dependencies`
  field to `dependency-groups.dev` to silence uv's deprecation warning.
- Updated bundled water ontology (`ontologies/water.ttl`).

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

[Unreleased]: https://github.com/DataDrivenCPS/acquirium/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/DataDrivenCPS/acquirium/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/DataDrivenCPS/acquirium/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/DataDrivenCPS/acquirium/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/DataDrivenCPS/acquirium/releases/tag/v0.1.0
