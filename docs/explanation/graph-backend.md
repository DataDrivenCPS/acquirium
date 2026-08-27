---
title: Graph backend architecture
---

<!-- TODO: intro -->

This is a guide to the graph backend: the persistent graphs, the derived
query views, the update rules, and the interfaces application and driver
authors use.
The graph backend is owned by the acquirium server.
Drivers and apps access it through the client and the HTTP API; they do not
open Oxigraph directly, and the graph store is not a Ray actor.

## Design goals

The backend separates three kinds of data:

- **Deployment data** is the plant model and the RDF asserted by Acquirium,
  drivers, and apps. It is persisted and is the input to inference and
  validation.
- **Ontology and shape data** defines vocabulary, `owl:imports`, SHACL shapes,
  and SHACL-AF rules. It is managed by OntoEnv, usually from bundled ontologies
  and configured ontology sources.
- **Derived query data** is disposable cache data created from the first two.
  It is rebuilt rather than edited by callers.

Note that callers only write deployment data.
Never write a derived graph or an OntoEnv-managed ontology graph.

## Components and process boundary

`Manager` creates one `OxigraphGraphStore` in the FastAPI server process. The
store uses a file-backed Oxigraph/RocksDB source dataset, a separate file-backed
query dataset, and an in-process reentrant lock to coordinate writes and cache
rebuilds. Oxigraph's files must be opened by this server process only.

Ray actors are used for `DriverRunner` and `AppRunner`, not for the graph
store; the actors talk to the server over the normal client API.
This way there is a single authoritative graph writer, and a file-backed
database handle is never shared with actor processes.

## Persistent graph roles

The source dataset contains both deployment and ontology named graphs.
Deployment data graphs are recognized by URI shape, not tracked as separate
persisted state: the plant and Acquirium graphs are two fixed URIs, and every
source graph lives under the `urn:acquirium:graph:data:source:` prefix.
Enumerating all named graphs by that shape is how the backend distinguishes
them from ontology graphs, which live under unrelated, externally-defined
namespaces (e.g. `https://qudt.org/...`).

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
also stable. A source graph URI is a pure function of its `source_id`; asking
for a source graph computes that URI without any lookup, and the graph itself
comes into existence in Oxigraph on first write.
This means a source ID is an ownership boundary, not just a timeseries label.

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
graph. There is no public API that removes a source graph; Oxigraph retains a
named graph's identity even once every triple has been removed from it, so an
empty source graph may remain around. Do not depend on unregistering a source
as part of a normal driver shutdown.

## Derived-graph pipeline

The pipeline below shows what queries and validation see.

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

`shifty.infer` runs over the complete deployment-data union, not one source
at a time.
This means rules can relate the plant model to driver and app metadata, and
inference and validation see the same full deployment boundary.

## When derived data is refreshed

The backend tracks a source-data version and a narrower closure version.
It also maintains the versions represented by the published query cache and a
`rebuild_in_progress` flag.
Together they work like this:

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

At most one inference rebuild runs at a time.
Writes arriving during a rebuild do not start competing rebuilds; the owner
discards the stale result and makes one follow-up attempt from the newest
generation.
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
cache, `is_current`, and `rebuild_in_progress`. Note that the status is a snapshot; it does not guarantee freshness for a
subsequent request. Use `wait_for_fresh=True` when the query itself must be
current.

## Query and export semantics

`include_dependencies` selects whether queries include ontology/shape triples
along with inferred deployment data.
Note that it does not mean an RDF dataset union of every stored graph.

| API setting | Default graph queried | Use it for |
| --- | --- | --- |
| `include_dependencies=False` | `inferred_data` | Operational queries over asserted and inferred deployment facts. |
| `include_dependencies=True` | Native union of `inferred_data` and `dependency_query_graph` | The default; use when queries may need ontology hierarchy, definitions, shapes, or rules as well as inferred facts. |

`export_graph(include_dependencies=False)` returns all registered deployment
data. `export_graph(include_dependencies=True)` adds resolved import
dependencies.
Note that this is an export view; it is not the same thing as either query
cache.

The standards-compatible `/sparql` endpoint serializes every supported query
form through Oxigraph. The compatibility `/sparql_json` endpoint retains its
`{"columns": [...], "rows": [...]}` response contract.

## How to program against the backend

Use `Acquirium`/`AcquiriumClient` from drivers, apps, and external programs.
Do not import `OxigraphGraphStore` or choose derived graph URIs outside server
code.

### Plant model owner

Insert the shared plant model with the reserved `source_id="plant"`. Use
`replace=True` only when the complete plant graph is being intentionally
replaced.

```python
aq.insert_graph(plant_turtle, format="turtle", replace=True, source_id="plant")
```

### Driver lifecycle

Use one stable, non-empty `source_id` for the lifetime of a driver deployment.
The typical lifecycle is:

1. Assign `self.source_id` and call `self.declare(...)` for every stream during
   setup or discovery. The platform registers the datasource and declarations
   before observations are inserted.
2. If the driver owns RDF metadata or a model fragment, append it with
   `self.insert_graph(..., replace=False)`. The driver base class supplies
   `self.source_id` automatically, so driver code cannot accidentally write to
   another source graph. Use
   `replace=True` only to replace that driver's own contribution; it cannot
   replace the plant graph.
3. Report observations with `add()` or return a canonical observation frame.
   The platform batches insertion and retains accepted rows across recoverable
   insertion failures.
4. On shutdown, quiesce external producers in `stop()`; the platform performs
   a final flush afterward.

```python
self.source_id = "weather-station-1"
self.declare(
    "air_temp",
    point_uri=point_uri,
    value_kind="numeric",
)
self.insert_graph(metadata_turtle, replace=False)
self.add("air_temp", value, timestamp)
```

External programs can still call `register_datasource()`, `register_streams()`,
and `insert_timeseries()` directly. Those are lower-level client operations,
not extra lifecycle work required of driver authors.

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
- Prefer one complete RDF insert over several small ones. Each `insert_graph`
  request synchronizes stream references against a fresh inferred view, so a
  loop of small inserts runs one inference pass per request.

## Concurrency and operational constraints

The graph store lock protects source writes and cache publication. The
rebuild owner snapshots the source inputs under that lock, releases it while
Shifty performs inference, then reacquires it for publication.
Query-cache selection is synchronized, then Oxigraph executes the query against
a repeatable-read snapshot while publication is locked; result processing then
continues without Acquirium's broad lock. A reader therefore observes a
complete current cache, never the temporary empty graph during a rebuild.

The derived rebuild path records separate debug timings for data snapshot,
shape snapshot, Shifty inference, and serialization/bulk-load publication.
Use these measurements before moving work to another language: Oxigraph and
native SPARQL JSON serialization are already Rust-backed, while RDFLib graph
copying and Python result materialization remain the likely Python-owned costs.

Do not run two acquirium server processes against the same graph-store path;
RocksDB/Oxigraph locking is treated as a fatal startup error rather than a
silent fallback to a different store.
Driver and app actors scale freely through the HTTP API.
The graph backend scales by giving its single server process suitable
resources, or by an explicit remote service boundary; never by sharing its
database files.

## Open questions and intentionally unsettled design choices

The following are not guarantees of the current implementation. They are areas
where the desired policy or mechanism has not been settled yet.

- **Source deletion and retention.** There is no public source-deletion API,
  and Oxigraph retains an empty source graph's identity once created. A
  future lifecycle for deleting a source graph, its stream registrations,
  and possibly its timeseries data still needs an explicit retention and
  provenance policy.
- **Inference cost and scheduling.** Rebuilds already run in one background,
  single-flight worker and ordinary readers can use the last complete cache.
  There is deliberately no write-batching mechanism: every graph write arrives
  as its own HTTP request, so a process-local scope cannot span a multi-source
  load, and coalescing one would first need a request carrying several graphs.
  The remaining decisions are whether production workloads need such a bulk
  insert, a bounded fresh-read deadline, a debounce policy for sustained
  writes, or a separate worker/service boundary.
- **Concurrency limits.** The current lock/snapshot behavior is correct for the
  tested embedded-store setup, but operational reader/writer load testing at
  representative production sizes is still needed before changing lock scope or
  promising throughput targets.
- **Remote graph service.** A separate service may be appropriate for
  multi-host scaling, but it would need an explicit deployment, persistence,
  failure-recovery, authentication, and API design. A Ray actor by itself does
  not solve those concerns.
- **Authorization by source.** `source_id` establishes ownership in the data
  model, not access control. If multiple tenants or untrusted drivers can call
  the API, authentication and authorization rules for source-scoped writes are
  still required.
