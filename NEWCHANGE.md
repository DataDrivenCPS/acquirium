# First-class model resources and streams

Status: proposed

This document proposes the model-resource and stream layer that should land
before the Experiment interface. It is deliberately independent of Studies,
Experiments, and experiment-variable storage. The Experiment change can then
consume these public objects instead of defining its own `Point` and
`RecordedSeries` concepts.

## Stacking and delivery

The implementation should be developed as a new PR based on
`local-runtime-init` (PR #108), not on the Experiment interface branch. After
it is merged, the Experiment PR should be rebased onto it, just as the current
Experiment PR was stacked on the local-runtime change.

The lower PR owns:

- discovering model properties that do not yet have streams;
- materializing one query result as a client-bound resource handle;
- listing the registered streams associated with a point;
- reading a stream through the existing time-series APIs;
- the storage and HTTP interfaces needed for that stream catalog.

It must not import Experiment code, add Experiment tables, or encode
Experiment-specific filtering. A later Experiment commit will add variable to
point bindings and use the resource and stream handles defined here.

## Problem

Acquirium currently exposes the pieces of the stream lifecycle, but callers
have to compose them at a low level:

- the query interface discovers entities and data-bearing measurements;
- `register_streams()` connects source-local stream names to plant points;
- `reference_uri()` calculates the canonical storage URI;
- `timeseries_df()` and `timeseries_batches()` read stored samples;
- `DataObject` combines streams selected by a graph query.

There is no public object representing one modeled property together with its
registered streams. There is also no public object representing a stored
stream independently of the code that produced it.

This becomes especially awkward when independently authored systems need to
meet at the plant model. A model author and a data producer can coordinate by
copying the same URI literal into both projects, but that makes metadata look
like a naming convention rather than a useful integration layer.

The desired model is:

1. The plant model owns the identity and semantics of physical properties.
2. A caller discovers a property from the model and receives a handle.
3. Drivers, applications, and later Experiments bind data to that handle.
4. The handle can enumerate the distinct streams that describe the property.
5. Every stream reads through Acquirium's existing time-series storage.

URIs remain the durable identity underneath the interface. The improvement is
that most application code selects and passes model-derived handles instead
of constructing coordinated URI strings.

## Goals

- Make model properties discoverable before they have data.
- Give strict query selection a small, reusable result object.
- Make a semantic point's registered streams inspectable without writing
  SPARQL or reconstructing reference URIs.
- Represent the same stream consistently regardless of whether it was
  produced by a driver, application, or future Experiment.
- Keep sample storage and retrieval in the existing time-series module.
- Preserve source identity and provenance when several streams describe the
  same property.
- Keep the public API small while hiding graph, SQL, HTTP, and unit-resolution
  details behind it.

## Non-goals

- Automatically decide that two similarly named properties are equivalent.
- Merge streams merely because they share a point or timestamps.
- Add plotting, alignment, resampling, or comparison methods.
- Replace `Query`, `DataObject`, or the low-level time-series client.
- Add Experiment-variable bindings in this lower PR.
- Turn all RDF resources into a large object-relational framework.
- Make a stream handle a general-purpose writer. Existing producer-owned
  insertion interfaces remain responsible for writes.

## Proposed public interface

### Discover a property without requiring a stream

Add `Query.property()`. It follows the model's property relationships without
requiring the target to have an external reference:

```python
volume_query = (
    ac.query()
    .entity("watr:Tank", label="tank", alias="tank")
    .property(
        quantity_kind="qudtqk:Volume",
        alias="volume",
    )
)
```

This is intentionally distinct from `measurement()`:

- `property()` selects modeled properties whether or not data exists;
- `measurement()` selects data-bearing properties with registered external
  references.

`property()` should use the same attribute filtering, aliasing, connection
point handling, URI expansion, and query compilation machinery as the current
incremental query interface. It is an additional graph step, not a separate
model-query system.

### Materialize exactly one resource

Add a strict `Query.one()` terminal:

```python
tank_volume = volume_query.one("volume")
```

The alias is optional when the current query focus is the intended result.
`one()` executes the query and deduplicates the selected URI across result
rows. It then:

- returns the selected resource when exactly one URI matched;
- raises `LookupError` when nothing matched;
- raises an `AmbiguousResultError` containing the candidate URIs when several
  resources matched.

It must never silently choose the first fuzzy or graph-query result.

For a property query, the result is a `Point`. Other query nodes may return a
small base `Resource` handle. This design does not require rich `Entity`
objects in the first version.

### Point

`Point` represents one modeled observable or actuatable property:

```python
tank_volume.uri
tank_volume.label
tank_volume.unit
tank_volume.quantity_kind
tank_volume.streams
```

The handle is client-bound so that it can perform discovery. Its identity and
hash are based on the expanded URI; the client connection and cached metadata
do not participate in equality.

`ac.point(uri)` remains the explicit-URI constructor. Query materialization is
preferred when the application should discover the resource from a model.

Point metadata should be a small snapshot populated by `one()` when already
available. Missing metadata may be fetched lazily. The object should not try
to expose arbitrary RDF as Python attributes; callers still use `Query` for
general graph traversal.

### StreamCollection

`point.streams` is a lazy `StreamCollection` scoped to the point:

```python
catalog = tank_volume.streams.frame()

deployment = (
    tank_volume.streams
    .where(source_id="tank-level-driver")
    .one()
)
```

The first version should support only filters backed by stable stream
identity:

- `ref_uri`;
- `source_id`;
- `ref_name`;
- `value_kind`.

It should provide:

| Member | Result |
|---|---|
| `.where(...)` | Another lazy `StreamCollection` |
| `.all()` | `list[Stream]` |
| `.one()` | One `Stream`, with strict zero/many behavior |
| `.get(ref_uri)` | One stream with that exact persistent identity |
| `.frame()` | One Polars row per stream, without loading samples |

The catalog frame should initially contain:

| Column | Meaning |
|---|---|
| `ref_uri` | Canonical Acquirium storage identity |
| `source_id` | Identity of the producer/source |
| `ref_name` | Source-local stream name |
| `point_uri` | Modeled property described by the stream |
| `value_kind` | Numeric or text storage kind |
| `point_unit` | Unit asserted by the plant property, if present |
| `storage_unit` | Unit asserted by the external reference, if present |

Row counts and time extents should not be loaded by `.frame()`. They can be
requested through a stream's existing information path and later batched if a
catalog-wide stats view proves necessary.

### Stream

`Stream` is a read-oriented handle for one canonical `ref_uri`:

```python
stream.ref_uri
stream.source_id
stream.ref_name
stream.point
stream.value_kind

frame = stream.dataframe(start=start, end=end)
batches = stream.batches(start=start, end=end)
info = stream.info()
```

These methods are adapters, not a new storage implementation:

- `.dataframe()` delegates to `AcquiriumClient.timeseries_df()`;
- `.batches()` delegates to `timeseries_batches()`;
- `.info()` delegates to the existing time-series information endpoint.

Window, ordering, limit, timeout, and value-mode arguments retain their
current meanings. A `Stream` does not infer alignment or combine itself with
other streams.

`ac.stream(ref_uri)` may construct a direct handle when the caller already has
a canonical reference. It should hydrate source metadata from the stream
catalog on first use.

Stream equality and hashing use `ref_uri`. `str(stream)` returns `ref_uri` so
the handle remains convenient at existing string-oriented boundaries.

### Registration

`register_streams()` remains the canonical semantic registration operation.
It may return `list[Stream]` instead of `None`:

```python
[stream] = ac.register_streams(
    [{
        "source_id": "tank-level-driver",
        "ref_name": "volume",
        "point_uri": tank_volume.uri,
        "unit": "unit:M3",
        "value_kind": "numeric",
    }]
)
```

Existing callers that ignore the return value remain compatible. Producers
continue to insert rows with `insert_timeseries()`, batch insertion, or Arrow
insertion. Returning handles should not move write ownership onto `Stream`.

Passing a `Point` wherever `point_uri` is accepted should be supported. The
transport continues to serialize its URI:

```python
"point_uri": tank_volume
```

## Example: deployment data discovered from the model

```python
tank_volume = (
    ac.query()
    .entity("watr:Tank", label="tank", alias="tank")
    .property(quantity_kind="qudtqk:Volume", alias="volume")
    .one("volume")
)

available = tank_volume.streams.frame()

measured = (
    tank_volume.streams
    .where(source_id="plant-scada")
    .one()
    .dataframe(start="2026-09-01", end="2026-09-08")
)
```

The caller chooses a semantic property and then a provenance-bearing stream.
Acquirium does not collapse every stream attached to `tank_volume` into one
series.

## Relationship to `Query` and `DataObject`

The new handles complement the existing bulk query path:

- Use `Query.dataframe()` or `DataObject` to fetch many graph-selected streams
  together.
- Use `Point.streams` to inspect which individual registered streams describe
  one property.
- Use `Stream` when code needs to retain and read one stable stream identity.

Internally, `Point.streams` should reuse the stream registry rather than issue
a broad graph query and then reconstruct storage metadata. `DataObject` may
eventually use `Stream` descriptors internally, but that refactor is not
required for the initial PR.

## Storage design

The existing `streams` SQL table remains authoritative for stored-stream
identity. It already maps a canonical reference to `source_id`, `ref_name`,
`point_uri`, and `value_kind`. The plant graph remains authoritative for model
relationships and semantic metadata.

Add a backend-neutral storage operation resembling:

```python
list_streams(
    *,
    point_uri: str | None = None,
    ref_uri: str | None = None,
    source_id: str | None = None,
    ref_name: str | None = None,
    value_kind: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> StreamPage
```

Both DuckDB and TimescaleDB implementations must follow the same contract.
Filtering and pagination happen in SQL. Add or verify an index on
`streams(point_uri)`; exact lookup by `ref_uri` already uses the primary key.

The catalog service enriches registry rows with point and external-reference
units from the graph. Unit enrichment should be batched. It must not query
time-series samples.

Only streams registered through Acquirium's stream lifecycle appear in this
catalog. An arbitrary RDF external reference with no registered storage stream
is graph metadata, not a readable `Stream`.

## HTTP interface

Expose a resource-oriented read endpoint, for example:

```text
GET /streams?point_uri=...&source_id=...&ref_name=...&value_kind=...
GET /streams/{ref_uri}
```

Because `ref_uri` may contain reserved URL characters, exact reference lookup
may use a query parameter instead if path encoding proves unreliable:

```text
GET /streams/lookup?ref_uri=...
```

The response contains stream descriptors and pagination state, never sample
rows. Existing `/timeseries` and time-series information endpoints remain the
only sample-reading paths.

## Package ownership

Suggested ownership:

```text
src/acquirium/Client/resources.py   Resource and Point
src/acquirium/Client/streams.py     Stream and StreamCollection
src/acquirium/Client/explore/       property() and one()
src/acquirium/Storage/base.py       list_streams contract
src/acquirium/Storage/duckdb_store.py
src/acquirium/Storage/timescale_store.py
src/acquirium/Server/app.py         descriptor endpoints
```

The exact filenames are less important than the dependency direction:

```text
Query -> Resource/Point -> StreamCollection -> transport client
                                      |
                                      v
                          existing time-series reads
```

Neither `resources.py` nor `streams.py` should import future Experiment
modules.

## Later Experiment integration

After this foundation merges, the Experiment PR can add a normalized,
auditable binding between an experiment variable and a model property:

```python
study = ac.study.get("flexpse-scenarios")
tank_volume = volume_query.one("volume")

study.variables["initial tank volume"].bind(tank_volume)
study.variables["tank volume"].bind(tank_volume)
```

That later change should:

- store bindings separately from free-form variable metadata;
- allow binding after Experiments already exist;
- validate property type and unit compatibility;
- connect previously recorded time-series references to the selected point;
- use `Stream` for newly recorded and retrieved time-series observations;
- retain a compatibility path for the current `observed=` declaration form.

The lower stream/resource PR does not implement any of this. Its contract is
only that a `Point` is durable and client-bound, streams can be registered
against it, and those streams can be discovered and read later.

## Error behavior

- Strict selection of zero resources or streams raises `LookupError`.
- Strict selection of several resources or streams raises
  `AmbiguousResultError` and reports candidates.
- Looking up an unknown `ref_uri` raises `KeyError` or maps an HTTP 404 to a
  documented client exception.
- A stream registered against a different point cannot silently change its
  point association.
- Existing unit compatibility checks continue to reject contradictions.
- A point with no streams returns an empty collection and empty catalog frame;
  it is not an error.
- An unbound stream remains accessible by `ref_uri` but cannot appear under a
  point's stream collection.

## Compatibility and migration

- `measurement()`, `DataObject`, `timeseries_df()`, and all insertion methods
  retain their current behavior.
- Existing `register_streams()` callers may ignore its new return value.
- Existing URI strings remain accepted anywhere a `Point` is accepted.
- The current `streams` rows require no data migration.
- When the Experiment branch is rebased, its local `Point` definition should
  be removed and `RecordedSeries` should become either a compatibility alias
  for `Stream` or a deprecated wrapper around it.

## Testing

The lower PR should include:

1. Storage-contract tests for identical DuckDB and TimescaleDB stream listing,
   filtering, ordering, and pagination.
2. Query tests showing that `property()` finds a modeled property with no
   external reference.
3. `one()` tests for one URI repeated across several result rows, zero matches,
   and ambiguous matches.
4. Point tests with zero, one, and several streams.
5. Stream tests proving that reads delegate arguments to existing time-series
   APIs and do not fetch samples during catalog listing.
6. Registration tests for string and `Point` inputs and returned handles.
7. Unit tests for point-unit and storage-unit catalog metadata.
8. An end-to-end local-runtime example loading a small model, registering two
   sources against one point, discovering both, and reading each separately.

## Documentation

Document the concepts in this order:

1. A point represents a property in the plant model.
2. A stream represents one source's observations of that property.
3. Several streams may describe one point without becoming interchangeable.
4. Query selects semantic resources; the stream catalog selects provenance;
   the time-series interface reads samples.

The example should avoid hand-coordinated URIs. It should discover the point
from the model, register data using the returned handle, and show the two
distinct stream descriptors before reading either dataset.

## Acceptance criteria

The foundation PR is complete when this workflow is supported without any
Experiment imports or schema:

```python
point = (
    ac.query()
    .entity("watr:Tank", label="tank", alias="tank")
    .property(quantity_kind="qudtqk:Volume", alias="volume")
    .one("volume")
)

streams = point.streams.frame()
stream = point.streams.where(source_id="plant-scada").one()
samples = stream.dataframe(start=start, end=end)
```

The implementation is successful if this code uses the existing plant graph,
stream registry, and time-series reader while keeping URI construction,
storage joins, pagination, and unit enrichment behind the public objects.
