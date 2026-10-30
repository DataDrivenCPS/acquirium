---
title: The data stream lifecycle
---

A stream is identified by its `source_id` and a source-local `ref_name`.
`compute_ref_uri` derives its canonical reference URI from the two. Registering
the stream writes that reference, its `value_kind`, and its point's unit and
quantity kind to the source's RDF graph. One point can link to several
references; read an ambiguous point through an explicit reference.

The server builds its reference lookup from the inferred graph and rebuilds it
when the graph changes. It checks every reference is canonical and registered
to the writing source, and stores each value as its stream's kind. Escapement
keys readings by reference and UTC nanosecond timestamp.

A write returns a durable receipt. The epoch that applies it also applies every
derived calculation it affects. A correction replaces the current value and
keeps the old one as a version; a delete retracts it. A query follows point to
reference links and reads Arrow data from a published snapshot; a `DataObject`
reads all its streams at one epoch.

An app output's reference is derived from the app name, the semantic group and
the output port. Its RDF is registered when the plan compiles, so downstream
queries can select it. Only its app can write it. Removing an app retracts its
current results; earlier epochs stay readable. See
[the app reference](../reference/apps.md) and [the core](escapement-core.md).
