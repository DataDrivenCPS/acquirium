---
title: The data stream lifecycle
---

A stream is identified by `source_id` and source-local `ref_name`.
`compute_ref_uri` mints its canonical external reference. Register its semantic
point, unit, and quantity kind in the source-owned RDF graph. One point can link
to several references; use an explicit reference for ambiguous reads.

The server rebuilds a reference lookup from inferred RDF. It validates canonical
reference identities and numeric declarations before passing observations to
Escapement. There is no SQL streams/ref_ids registry. The core keys observations
by stream reference, UTC nanosecond timestamp, and sample key (zero in Acquirium).

An insertion receives a durable native receipt. Its epoch applies accepted
source changes and all affected derived calculations together. Corrections replace
current values while preserving previous versions; explicit deletes retract them.
A semantic query follows point-to-reference links and fetches numeric Arrow data
from a committed core snapshot. DataObject uses one epoch across its stream reads.

App output references are derived from app name, semantic group, and output port.
Their structural RDF is registered during plan compilation so downstream semantic
queries can discover them. Native output ownership prevents direct source writes.
Removing an operator retracts current results, and earlier epochs remain readable.
See [native apps](../reference/apps.md) and [core internals](escapement-core.md).
