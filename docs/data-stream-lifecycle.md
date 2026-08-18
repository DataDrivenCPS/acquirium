# The data stream lifecycle

This is a guide to how a timeseries stream comes into existence, how its rows
are stored, and how a query finds them again.
It is background for driver authors and for anyone debugging ingestion; using
the data is covered in the [data guide](data.md).

## The identifier model

A stream has three identifiers.

| identifier | meaning | example |
|---|---|---|
| `source_id` | who writes the stream | `watertap-seawater-ro` |
| `ref_name` | which series, unique within the source | `P1-out-pressure` |
| `ref_uri` | the canonical URI of the stream | `urn:acquirium#399ce39c-...` |

`ref_uri` is computed from the other two.
The same `(source_id, ref_name)` pair always produces the same URI, and two
sources can use the same `ref_name` without colliding.

```python
acq.reference_uri("watertap-seawater-ro", "P1-out-pressure")
# urn:acquirium#399ce39c-e18d-5ad5-bd5c-9c9d053fe04d
```

Since the URI is computable, no lookup is needed at insert time.
A driver only needs its `source_id` and `ref_name` to insert rows.

These stream URIs can be attached to `point_uri`s in the semantic model.
A stream can be registered without being associated with a point, but then nothing in the model refers to
it, and semantic queries will not find it.

## What registration writes to the graph

`register_streams()` writes two connected pieces of RDF.

For the stream itself, a reference node under the computed `ref_uri`:

```turtle
<urn:acquirium#399ce39c-...>
    acq:sourceId  "watertap-seawater-ro" ;
    acq:refName   "P1-out-pressure" ;
    acq:valueKind "numeric" ;
    ref:storedAt  <urn:acquirium#TimescaleDB> .
```

And, when a `point_uri` was given, the point node with its semantic metadata
and the link between the two:

```turtle
<urn:swro/P1-out-pressure>
    a                        acq:VirtualPoint ;
    rdfs:label               "P1 outlet pressure" ;
    qudt:hasUnit             unit:PA ;
    qudt:hasQuantityKind     qudtqk:Pressure ;
    ref:hasExternalReference <urn:acquirium#399ce39c-...> .
```

Registration is idempotent and additive.
Registering the same stream again with a `point_uri` does not erase metadata written earlier.

**TODO:** We need to provide an interface to add, remove, replace streams. Also, auto register streams from a given graph.

## Registration and the streams table

The graph is the source of truth for streams, so the server keeps a derived
index: a `streams` table in the timeseries store, with one row per reference
node
(`ref_uri`, `point_uri`, `source_id`, `ref_name`, `value_kind`).
After every graph insert, the server scans for nodes carrying `acq:sourceId`
and `acq:refName` and upserts them into the table.
This sync is why registration must precede insertion: the insert path checks
the table, and a stream that was never registered has no row.

Note that this sync has two additional behaviors.

A later registration never erases an earlier link.
If a stream was first registered with a `point_uri` and later without one,
the table keeps the point.

The sync validates the canonical URI.
A hand-written reference node whose URI does not equal
`compute_ref_uri(source_id, ref_name)` fails the entire insert:

```text
Managed reference URI mismatch for point <...>: graph has <...>, expected
<...> from source_id='...', ref_name='...'
```

The same scan runs at server startup, so a bad reference node in a model file
prevents the server from starting.
Do not mint reference URIs by hand; use `acq.reference_uri()` or let
`register_streams()` compute them.

## The write path

A row goes through five steps between a driver and the store.

1. The client normalizes the observation frame (timestamps to UTC, values to
   strings) and sends it as an Arrow table over HTTP.
2. The server computes each row's `ref_uri` from `(source_id, ref_name)` and
   checks it against the `streams` table.
   An unregistered stream fails here, before anything is written.
3. Each value is placed in one of two columns by the stream's registered
   `value_kind`: numbers in `numeric_value`, everything else in `text_value`.
   A value on a numeric stream that does not parse as a number falls back to
   the text column instead of failing the batch.
4. Rows are deduplicated on `(ref_uri, ts)`, keeping the last.
5. The batch is written as a delete-then-insert on those pairs, in one
   transaction.

Step 5 makes ingestion idempotent: re-inserting the same timestamps replaces
those rows instead of duplicating them, so re-running an import or replaying
a file is safe.
Note that this also means an insert with changed values silently overwrites
the history at those timestamps.
`replace=True` on `insert_timeseries` clears the whole stream before
inserting.

Storage is one table, `timeseries(ref_uri, ts, numeric_value, text_value)`,
with a uniqueness constraint on `(ref_uri, ts)` and a check that only one of
the two value columns is set per row.

## The read path

A query follows the same links in reverse.

1. The query pattern compiles to SPARQL; each measurement node binds a point
   and follows its `ref:hasExternalReference` to the reference node.
   The point's unit and the reference node's unit both come along.
2. The client collects the distinct `(point_uri, ref_uri)` pairs from the
   result; these are the bindings a `DataObject` reports before fetching.
3. Values are fetched per `ref_uri` from the timeseries store, streamed back
   as Arrow batches.
4. When the reference node's unit differs from the point's, the values are
   converted during the fetch (see the
   [data guide](data.md#automatic-conversion)).

The graph determines which streams to read, and the timeseries store returns
their values; `ref_uri` is the join key between the two.
A point with no reference node yields metadata but no data.
A reference node with no point stores data that semantic queries cannot find.

## How graph inserts behave

Everything above writes RDF through `insert_graph`, so this section
describes how it behaves.

All inserted data lands in one main graph.
There is no per-source or per-driver separation, which is why
`replace=True` (the client default) is dangerous outside of loading a fresh
model: it clears the entire main graph, including streams registered by other
drivers and apps.

The ontologies (s223, the water ontology, QUDT, the reference schema) live in
their own graphs, separate from the main graph, and inserts never touch them.
Queries run against a union of the main graph and the ontology closure by
default.
This is why a model containing `wbs:P1 a s223:Pump` matches a query for
equipment: the subclass chain lives in the ontology graphs.

A model file that declares `owl:imports` triggers a recomputation of that
closure on insert.
Plain instance data does not, so stream registration stays cheap and the sync
from [Registration and the streams table](#registration-and-the-streams-table)
can run on every insert.

## App outputs are streams too

When an app that produces values (a soft sensor, for instance) is registered, each declared output becomes a point and a
reference node: `app:<name>` is the `source_id`, the output's point URI
string is the `ref_name`, and the computed `ref_uri` follows from the pair
as usual.
See the [apps guide](apps.md#outputs).
This means computed values are indistinguishable from measured ones at query
time.

