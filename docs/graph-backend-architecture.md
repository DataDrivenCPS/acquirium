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
| Plant data graph | source dataset | The shared plant model; this is the legacy default graph target. | `insert_graph(..., source_id=None)` or an unscoped SPARQL update. |
| Acquirium data graph | source dataset | Acquirium-owned bookkeeping, such as logs. | Server internals only. |
| Source data graph | source dataset | RDF owned by one driver, app, or metadata source. | `insert_graph(..., source_id="...")`; source-scoped SPARQL update. |
| Ontology/shape graph | source dataset | Ontologies, shapes, rules, and imports managed through OntoEnv. | Server configuration/startup; not application or driver writes. |
| Dependency cache | memory | Imports closure minus asserted deployment data. | Backend only. |
| Inferred-data graph | query dataset | `shifty.infer` output. | Backend only. |
| Inferred-data-with-shapes graph | query dataset | Inferred data plus the dependency cache. | Backend only. |

The plant graph URI is stable (`urn:acquirium#MainGraph`). Acquirium's graph is
also stable. A source graph URI is deterministic from its `source_id`; asking
for a source graph creates and persists its registry entry when necessary. A
source ID is therefore an ownership boundary, not merely a timeseries label.

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
                         ├───────────────► `use_union=False`
                         │
                         ▼
 inferred_data_with_shapes = inferred_data + dependency_cache
                         │
                         └───────────────► `use_union=True` (the API default)
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
                 publish both complete query graphs under the lock
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

## Query and export semantics

`use_union` selects whether queries include ontology/shape triples along with
inferred deployment data. It does not mean an RDF dataset union of every
stored graph.

| API setting | Default graph queried | Use it for |
| --- | --- | --- |
| `use_union=False` | `inferred_data` | Operational queries over asserted and inferred deployment facts. |
| `use_union=True` | `inferred_data_with_shapes` | The default; use when queries may need ontology hierarchy, definitions, shapes, or rules as well as inferred facts. |

`export_graph(include_union=False)` returns only the plant data graph for
backwards compatibility. `export_graph(include_union=True)` returns the union
of all registered deployment data plus resolved import dependencies. It is an
export view, not the same thing as either query cache.

SPARQL `CONSTRUCT`/`DESCRIBE` use the existing RDFLib conversion path. `SELECT`
and `ASK` may use Oxigraph's native result serialization internally, but the
public `/sparql_json` response contract remains `{"columns": [...], "rows": [...]}.`

## How to program against the backend

Use `Acquirium`/`AcquiriumClient` from drivers, apps, and external programs.
Do not import `OxigraphGraphStore`, access `GraphRegistry`, or choose derived
graph URIs outside server code.

### Plant model owner

Insert the shared plant model without a source ID. Use `replace=True` only when
the complete plant graph is being intentionally replaced.

```python
aq.insert_graph(plant_turtle, format="turtle", replace=True)
```

### Driver lifecycle

Use one stable, non-empty `source_id` for the lifetime of a driver deployment.
The typical lifecycle is:

1. Call `register_datasource(source_id)` at startup. This creates the source
   data graph if needed and records the datasource in it. It is idempotent.
2. If the driver owns RDF metadata or a model fragment, append it with
   `insert_graph(..., source_id=source_id, replace=False)`. Use
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
aq.insert_graph(metadata_turtle, replace=False, source_id="weather-station-1")
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
App teardown issues a source-scoped SPARQL update against that same graph, so
it removes only the app's registration triples. App and driver code should not
use unscoped SPARQL updates to alter another component's data.

### Updates, queries, and validation

- Use `insert_graph` for whole RDF documents. Prefer append mode for additive
  metadata; replacement is graph-owner scoped.
- Use `sparql_update(update, source_id=...)` only for targeted changes to data
  owned by that source. Omitting `source_id` updates the plant graph for
  backwards compatibility.
- Query with the default `use_union=True` unless it is intentional to exclude
  ontology/shape triples. Query results already include inference.
- Call `validate_graph()` after loading/changing a model or metadata when the
  caller needs a conformance decision. It returns `conforms`, a Turtle SHACL
  report, and a human-readable result string.
- Poll `graph_version()` only when maintaining a local cached query plan or
  result. It is not required for ordinary one-shot API calls.

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

- **Source deletion and retention.** The registry can remove an entry, but the
  public lifecycle/API for deleting a source graph, its stream registrations,
  and possibly its timeseries data has not been designed. In particular, the
  retention policy for a removed driver needs a product decision.
- **Inference cost and scheduling.** Inference is rebuilt synchronously when a
  query needs an outdated view. Whether large deployments should expose an
  asynchronous rebuild/status API, coalesce writes, or use a background worker
  needs measurement against real graph sizes and latency requirements.
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
