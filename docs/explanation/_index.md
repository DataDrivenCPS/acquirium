---
title: Explanation
---

These pages describe how Acquirium works and why it was designed that way.
Read them when you want to understand a behaviour rather than look up a call.

**Finding data**

- [The query model](query-model.md): what a query is, how free text is resolved, and how a pattern becomes a graph query.
- [Text resolution](text-resolution.md): how free text is matched to the vocabulary.
- [Values](values.md): when a reading is a number and when it is text.
- [Units](units.md): where units are recorded, and when values are converted.

**Getting data in**

- [Drivers](drivers.md): how a source hands readings to the server.
- [The stream lifecycle](stream-lifecycle.md): how a stream is identified, stored, and found again.

**Derived data and analysis**

- [How derived streams stay current](apps.md): what a correction changes, and why.
- [Experiments](experiments.md): how Studies and runs keep the context of an analysis.

**Inside the server**

- [Server internals](server-internals.md): what one server process runs.
- [Escapement core](escapement-core.md): writes, history, snapshots, and recovery.
- [How graph changes reach queries](graph-backend.md): ownership, inference, and fresh reads.
