---
title: Explanation
---

These pages describe how Acquirium works and why it was designed that way.
Read them when you want to understand a behaviour rather than look up a call.

**Data queries**

- [Query model](query-model.md): what a query is, how free text is resolved, and how a pattern becomes a graph query.
- [Text resolution](text-resolution.md): how free text is matched to the vocabulary.
- [Values](values.md): when a reading is a number and when it is text.
- [Units](units.md): where units are recorded, and when values are converted.

**Data ingestion**

- [Drivers](drivers.md): how a source hands readings to the server.
- [Data stream lifecycle](stream-lifecycle.md): how a stream is identified, stored, and found again.

**Derived data and analysis**

- [Soft sensor inputs](soft-sensor-inputs.md): illustrated groups, time windows, function inputs, and one-to-many or many-to-one outputs.
- [Derived stream updates](apps.md): what a correction changes, and why.
- [Experiments](experiments.md): how Studies and runs keep the context of an analysis.

**Server components**

- [Server internals](server-internals.md): what one server process runs.
- [Escapement core](escapement-core.md): writes, history, snapshots, and recovery.
- [Graph updates and queries](graph-backend.md): ownership, inference, and fresh reads.
