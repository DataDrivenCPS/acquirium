# Graph Backend Architecture

This document describes the graph backend that exists today: its persistent
graphs, derived query views, update rules, and the interfaces application and
driver authors should use. The graph backend is owned by the Acquirium server;
drivers and apps access it through the Acquirium client/HTTP API. They do not
open Oxigraph directly and the graph store is not a Ray actor.

## Design goals

The backend separates three concerns that otherwise become easy to mix up:

- **Deployment data** is the plant model and the RDF asserted by Acquirium,
  drivers, and apps. It is persisted and is the input to inference and
  validation.
- **Ontology and shape data** defines vocabulary, `owl:imports`, SHACL shapes,
  and SHACL-AF rules. It is managed by OntoEnv, usually from bundled ontologies
  and configured ontology sources.
- **Derived query data** is disposable cache data created from the first two.
  It is rebuilt rather than edited by callers.

The important rule is: write only deployment data. Never write a derived graph
or an OntoEnv-managed ontology graph.

## Components and process boundary

`Manager` creates one `OxigraphGraphStore` in the FastAPI server process. The
store uses a file-backed Oxigraph/RocksDB source dataset, a separate file-backed
query dataset, and an in-process reentrant lock to coordinate writes and cache
rebuilds. Oxigraph's files must be opened by this server process only.

Ray actors are used for `DriverRunner` and `AppRunner`, not for the graph store.
They talk to the server over the normal client API. This gives all drivers one
authoritative graph writer and avoids distributing a file-backed database handle
to actor processes.

## Persistent graph roles

The source dataset contains both deployment and ontology named graphs. A small
JSON `GraphRegistry` beside the store is the allow-list that identifies which
named graphs count as deployment data. Enumerating all named graphs is
incorrect: that would accidentally include ontology graphs.

| Role | Persistence | Contents | How it is written |
| --- | --- | --- | --- |
| Plant data graph | source dataset | The shared plant model; it is the reserved `plant` source. | `insert_graph(..., source_id="plant")` or `sparql_update(..., source_id="plant")`. |
| Acquirium data graph | source dataset | Acquirium-owned bookkeeping, such as logs. | Server internals only. |
| Source data graph | source dataset | RDF owned by one driver, app, or metadata source. | `insert_graph(..., source_id="...")`; source-scoped SPARQL update. |
| Ontology/shape graph | source dataset | Ontologies, shapes, rules, and imports managed through OntoEnv. | Server configuration/startup; not application or driver writes. |
| Dependency cache | memory | Imports closure minus asserted deployment data. | Backend only. |
| Inferred-data graph | query dataset | `shifty.infer` output. Replaced after every derived rebuild. | Backend only. |
| Dependency query graph | query dataset | Resolved ontology/shape dependency cache. Replaced only when its imports closure changes. | Backend only. |

The plant graph URI is stable (`urn:acquirium#MainGraph`). Acquirium's graph is
also stable. A source graph URI is deterministic from its `source_id`; asking
for a source graph creates and persists its registry entry when necessary. A
source ID is therefore an ownership boundary, not merely a timeseries label.

### Public graph views

Public reads expose exactly two views; neither exposes the legacy plant graph
by itself:

| View | Contents | Selector |
| --- | --- | --- |
| Deployment graph | Inferred union of every registered deployment/source graph. | `include_dependencies=false` for SPARQL and export. |
| Deployment graph with dependencies | The same inferred deployment graph plus resolved ontology and shape triples. | `include_dependencies=true` for SPARQL and export. |

The plant graph is the explicit reserved `source_id="plant"` write target. It
is not a public read view.

At present, deleting an app removes its registration triples from its source
graph. There is no public API that removes a source graph or its registry entry;
an empty source graph may remain registered. Do not depend on unregistering a
source as part of a normal driver shutdown.

## Derived-graph pipeline

The pipeline below is the authoritative description of what queries and
validation see.

```text
registered plant, Acquirium, and source data graphs
                         │
                         ▼
          source_data = RDF set-union of all registered data graphs
                         │
                         │  owl:imports resolution through OntoEnv
                         ▼
 dependency_cache = import closure minus source_data  (shapes/rules/vocabulary)
                         │
                         ├───────────────► validate(source_data, dependency_cache)
                         │                  with graph_mode="union"
                         ▼
       shifty.infer(source_data, dependency_cache)
                         │
                         ▼
              inferred_data query graph
                         │
                         ├───────────────► `include_dependencies=False`
                         │
                         ▼
 dependency_query_graph = dependency_cache
                         │
                         └──── Oxigraph default-graph union with inferred_data
                                      │
                                      └──► `include_dependencies=True` (the API default)
```

The dependency cache contains only the triples added by resolving imports; it
does not duplicate asserted data. Import declarations may live in the plant
graph or in any registered source graph. This matters when a driver contributes
a model that imports an ontology containing its SHACL rules.

`shifty.infer` is run over the complete deployment-data union, not one source
at a time. Consequently, rules can relate the plant model to driver/app
metadata, and inference/validation see the same full deployment boundary.

## When derived data is refreshed

The backend tracks a source-data version and a narrower closure version. It
also maintains the versions represented by the published query cache and one
``rebuild_in_progress`` flag. Those four values form a small state machine:

```text
write committed → source version advances → published cache is stale
                                           │
fresh query / refresh_union               ▼
──────────────────────────────→ one rebuild owner snapshots inputs
                                    │
                       other fresh readers wait on a condition
                                    │
                                    ▼
                 Shifty inference runs outside the store lock
                                    │
                                    ▼
                 publish inferred data under the lock
              (and dependencies only if the closure changed)
                                    │
                  generation changed while building?
                           ├── no → wake waiting readers
                           └── yes → rebuild once from latest snapshot
```

At most one inference rebuild is in flight. A burst of writes during a rebuild
does not create competing rebuilders: it causes the owner to discard the stale
result and make one coalesced follow-up attempt from the newest generation.
The default public query returns the last complete published graph immediately
while this process runs. Pass `wait_for_fresh=True` when a caller must wait for
the current generation. Neither mode ever exposes a partially replaced graph.

| Event | Source version | Dependency cache | Inference/query graphs |
| --- | --- | --- | --- |
| Append ordinary data triples | increments | remains valid | stale; one background rebuild is scheduled. |
| Replace a data graph | increments | marked stale | stale; one background rebuild is scheduled. |
| Add data containing `owl:imports` or an `owl:Ontology` declaration | increments | marked stale | stale; one background rebuild is scheduled. |
| SPARQL UPDATE of a data graph | increments | conservatively marked stale | stale; one background rebuild is scheduled. |
| OntoEnv adds/removes/replaces an ontology graph | increments | invalidated and rebuilt when needed | stale; rebuilt on the next query that needs a query view. |
| Call `validate_graph` | unchanged | brought current if needed | not required; validation uses source data and shapes directly. |

Queries normally use eventual consistency: selecting either query view returns
the last complete published graph and schedules the single rebuild owner when
needed. `wait_for_fresh=True` instead waits for the current generation.
`Manager.insert_graph` also synchronizes stream-reference registrations after
the write. That synchronization deliberately waits for the fresh inferred view: a
deployment may use ontology rules to complete a stream-to-point association.

The query dataset is cache storage. It may safely be deleted/rebuilt by the
backend; no caller may treat it as a system of record.

For monitoring, `GET /graph_version` reports the store-owned
`source_version`, the `published_version` contained in the last complete query
cache, `is_current`, and `rebuild_in_progress`. The status is a snapshot, not
a freshness guarantee for a subsequent request; use `wait_for_fresh=True` on
that query when the query itself must be current.

## Query and export semantics

`include_dependencies` selects whether queries include ontology/shape triples along with
inferred deployment data. It does not mean an RDF dataset union of every
stored graph.

| API setting | Default graph queried | Use it for |
| --- | --- | --- |
| `include_dependencies=False` | `inferred_data` | Operational queries over asserted and inferred deployment facts. |
| `include_dependencies=True` | Native union of `inferred_data` and `dependency_query_graph` | The default; use when queries may need ontology hierarchy, definitions, shapes, or rules as well as inferred facts. |

`export_graph(include_dependencies=False)` returns all registered deployment
data. `export_graph(include_dependencies=True)` adds resolved import
dependencies. It is an export view, not the same thing as either query cache.

The standards-compatible `/sparql` endpoint serializes every supported query
form through Oxigraph. The compatibility `/sparql_json` endpoint retains its
`{"columns": [...], "rows": [...]}` response contract.

## How to program against the backend

Use `Acquirium`/`AcquiriumClient` from drivers, apps, and external programs.
Do not import `OxigraphGraphStore`, access `GraphRegistry`, or choose derived
graph URIs outside server code.

### Plant model owner

Insert the shared plant model without a source ID. Use `replace=True` only when
the complete plant graph is being intentionally replaced.

```python
aq.insert_graph(plant_turtle, format="turtle", replace=True, source_id="plant")
```

### Driver lifecycle

Use one stable, non-empty `source_id` for the lifetime of a driver deployment.
The typical lifecycle is:

1. Call `register_datasource(source_id)` at startup. This creates the source
   data graph if needed and records the datasource in it. It is idempotent.
2. If the driver owns RDF metadata or a model fragment, append it with
   `self.insert_graph(..., replace=False)`. The driver base class supplies
   `self.source_id` automatically, so driver code cannot accidentally write to
   another source graph. Use
   `replace=True` only to replace that driver's own contribution; it cannot
   replace the plant graph.
3. Register stream metadata with `register_streams`. The high-level client
   groups registrations by `source_id` and writes each group to its matching
   source graph. Stream identity is the pair `(source_id, ref_name)`.
4. Insert observations with `insert_timeseries(source_id=..., ref_name=...)`.
   Timeseries rows live in the timeseries backend; graph registration is what
   provides their semantic linkage and value kind.
5. On restart, repeat steps 1–3 safely. On ordinary shutdown, stop ingesting;
   there is currently no general source-graph deletion lifecycle.

```python
aq.register_datasource("weather-station-1")
self.insert_graph(metadata_turtle, replace=False)
aq.register_streams([
    {"source_id": "weather-station-1", "ref_name": "air_temp", "point_uri": point_uri},
])
aq.insert_timeseries(
    source_id="weather-station-1",
    ref_name="air_temp",
    values=observations,
)
```

### Apps

Apps write their registration metadata to `source_id="app:<app-name>"`.
`AppRunner` exposes this as `self.source_id` and binds the same value onto the
loaded `App` instance. Both app and driver code can use
`self.insert_graph(...)` and `self.sparql_update(...)`; these helpers always
target that instance's owned graph and intentionally do not accept a
`source_id` argument. App teardown issues a source-scoped SPARQL update against
that same graph, so it removes only the app's registration triples. App and
driver code should not use unscoped SPARQL updates to alter another component's
data.

### Updates, queries, and validation

- Use `insert_graph` for whole RDF documents. Prefer append mode for additive
  metadata; replacement is graph-owner scoped.
- Use `sparql_update(update, source_id=...)` only for targeted changes to data
  owned by that source. Use the reserved `source_id="plant"` for the shared
  plant model.
- Query with the default `include_dependencies=True` unless it is intentional to exclude
  ontology/shape triples. Query results already include inference.
- Call `validate_graph()` after loading/changing a model or metadata when the
  caller needs a conformance decision. It returns `conforms`, a Turtle SHACL
  report, and a human-readable result string.
- Poll `graph_status()["source_version"]` only when maintaining a local cached
  query plan or result. It is not required for ordinary one-shot API calls;
  use `published_version`/`is_current` only for monitoring, not as a substitute
  for `wait_for_fresh=True` on a query that must be current.

### Server-side write batches

`Manager.graph_write_batch()` is available to server-side workflows that make
several graph writes as one logical load. It commits each write normally, but
defers derived-cache scheduling and stream-reference synchronization until the
outermost scope exits. It is intentionally not exposed as a client context
manager: independent HTTP requests cannot share a reliable process-local
scope. Remote callers should prefer one complete RDF insert where practical.

```python
with manager.graph_write_batch():
    manager.insert_graph(plant_turtle, source_id="plant", replace=True)
    manager.insert_graph(metadata_turtle, source_id="weather-station-1", replace=False)
```

## Concurrency and operational constraints

The graph store lock protects source writes, registry changes, and cache
publication. The rebuild owner snapshots the source inputs under that lock,
releases it while Shifty performs inference, then reacquires it for publication.
Query-cache selection is synchronized, then Oxigraph executes the query against
a repeatable-read snapshot while publication is locked; result processing then
continues without Acquirium's broad lock. A reader therefore observes a
complete current cache, never the temporary empty graph during a rebuild.

The derived rebuild path records separate debug timings for data snapshot,
shape snapshot, Shifty inference, and serialization/bulk-load publication.
Use these measurements before moving work to another language: Oxigraph and
native SPARQL JSON serialization are already Rust-backed, while RDFLib graph
copying and Python result materialization remain the likely Python-owned costs.

Do not run two Acquirium server processes against the same graph-store path.
RocksDB/Oxigraph locking is intentionally treated as a fatal startup error,
rather than silently falling back to a different store. Scale driver and app
actors freely through the HTTP API; scale the graph backend by giving its
single server process suitable resources or by designing an explicit remote
service boundary, not by sharing its database files.

## Open questions and intentionally unsettled design choices

The following are not guarantees of the current implementation. They are areas
where the desired policy or mechanism has not been settled yet.

- **Source deletion and retention.** There is no public source-deletion API and
  the registry deliberately retains an empty source graph. A future lifecycle
  for deleting a source graph, its stream registrations, and possibly its
  timeseries data still needs an explicit retention and provenance policy.
- **Inference cost and scheduling.** Rebuilds already run in one background,
  single-flight worker; ordinary readers can use the last complete cache and
  server-side write batches coalesce scheduling. The remaining decision is
  whether production workloads need a bounded fresh-read deadline, a debounce
  policy for sustained writes, or a separate worker/service boundary.
- **Concurrency limits.** The current lock/snapshot behavior is correct for the
  tested embedded-store setup, but operational reader/writer load testing at
  representative production sizes is still needed before changing lock scope or
  promising throughput targets. Use
  [`graph_store_concurrency.py`](../benchmarks/graph_store_concurrency.py) for
  the Acquirium-level workload and record its raw JSON with any conclusion.
- **Remote graph service.** A separate service may be appropriate for
  multi-host scaling, but it would need an explicit deployment, persistence,
  failure-recovery, authentication, and API design. A Ray actor by itself does
  not solve those concerns.
- **Authorization by source.** `source_id` establishes ownership in the data
  model, not access control. If multiple tenants or untrusted drivers can call
  the API, authentication and authorization rules for source-scoped writes are
  still required.
