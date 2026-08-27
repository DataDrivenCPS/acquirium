---
title: Server internals
---

<!-- TODO: intro -->

## Storage backends

Both backends store the same logical schema: a `timeseries` table (one row
per stream and timestamp, with a `numeric_value` and a `text_value` column),
the `streams` reference table, and the logbook.
They key the rows differently.
Timescale uses `ref_uri` directly; duckdb uses an integer `ref_id` and maps it
back through a `ref_ids` table.
Reads expose `ref_uri` either way.

`duckdb` is the default: one file under the data directory, with no extra
services to install or run.
Reads run on their own connections, so a long scan does not block a driver's
inserts.
There is no compression or retention; the file grows with the data.

`timescale` stores the same tables in Postgres with the TimescaleDB
extension, addressed by `pg_dsn`.
The `timeseries` table is a hypertable, and chunks older than 7 days are
compressed automatically.

The graph store is embedded either way.
Switching the timeseries backend does not lift the single-process constraint.

## The graph store

The semantic model lives in an embedded Oxigraph store under `graph_path`.
Two datasets are kept: the source of record (one graph per data owner plus one
per ontology) and a query dataset holding the inferred deployment data and the
resolved ontology and shape triples.
Queries run against both by default; the derived data is rebuilt in the
background when something changes, and a reader gets the last complete version
until it is ready.
Pass `wait_for_fresh=True` when a query must see the current generation.
The [graph backend guide](graph-backend.md) covers this in
full.

The store keeps a source-data generation, exposed as `GET /graph_version`
along with the state of the derived query cache (`source_version`,
`published_version`, `is_current`, `rebuild_in_progress`).
`source_version` advances on every mutation.
Clients poll it to invalidate caches; drivers and apps use it for their
graph-change hooks.

The store is guarded by a single lock, so a heavy SPARQL query delays other
graph operations until it finishes.
The [querying guide](../how-to/debug-an-empty-query.md#when-a-query-returns-nothing-or-moreless-than-you-expect) covers how to
keep queries bounded.

## The embedding indexes

Free-text resolution is served by two vector indexes built at startup: one
over the water and s223 ontologies (classes, predicates, substances,
processes) and one over QUDT (units, quantity kinds).
They are built from the ontologies only.
Inserted plant data is never indexed.
This is why free text resolves classes and units but not instance labels.

The first build is the expensive part of a first start: 5-10 minutes for the
two indexes together, most of it the QUDT one.
The result is cached under `data_dir/embedding_cache`, keyed by ontology
content, so later starts reuse it and a changed ontology triggers a rebuild
automatically.
`GET /embedding_status` reports the state of both indexes.
