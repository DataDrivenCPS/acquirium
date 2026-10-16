---
title: Server internals
---

The server owns one Escapement processor and one Oxigraph/ontology store.
Escapement persists raw and derived observations in its DuckDB database and owns
all computation scheduling and publication. Acquirium resolves references and
units, compiles semantic app selections, and serves the HTTP/Python interfaces.
See [the core architecture](escapement-core.md) for receipts, epochs, deployment
intent, snapshots, and recovery.

The graph retains source-owned RDF and ontology graphs, plus a published inferred
query view. Queries can wait for a fresh generation. Free-text resolution uses
ontology-only embedding/exact indexes, cached under the data directory. Plant
instances are not embedded. `exact_only=true` builds exact surfaces without model
downloads. Driver actors connect back to the same server over HTTP; native Python
functions execute through Escapement's subprocess executor.
